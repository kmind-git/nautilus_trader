// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software distributed under
//  the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//  KIND, either express or implied. See the License for the specific language governing
//  permissions and limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Admission bridge from external submissions into the native execution path.
//!
//! The kernel's `Strategy::submit_order` performs four steps before a
//! `SubmitOrder` command can safely enter the risk engine: construct the order,
//! register it in the `Cache` (the risk engine reads orders back out of the
//! cache), publish `OrderInitialized`, and dispatch the command to the risk
//! engine's queued endpoint. The engine cannot be reached by simply forwarding
//! a `SubmitOrder` command, which is why this bridge exists.
//!
//! [`OrderHubBridge`] replicates that native path for external requests while
//! adding the OrderHub admission semantics from the design contracts:
//!
//! - Idempotency: retries with the same request key return the original
//!   admission; a different fingerprint for the same key is rejected.
//! - Durable admission: the request is committed to the business journal
//!   before the native chain is entered.
//! - Deterministic order IDs: `client_order_id` is a pure function of
//!   `(journal_epoch, seq)`, so restart replays never reuse an ID.
//! - Attribution: the submitting strategy identity flows through the order,
//!   command, and resulting events unchanged.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use nautilus_common::cache::Cache;
use nautilus_common::clock::Clock;
use nautilus_common::messages::execution::{SubmitOrder, TradingCommand};
use nautilus_common::msgbus::{self, MessagingSwitchboard};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::enums::TimeInForce;
use nautilus_model::events::OrderEventAny;
use nautilus_model::identifiers::{ClientId, ClientOrderId, InstrumentId, StrategyId, TraderId};
use nautilus_model::orders::{LimitOrder, Order, OrderAny};
use nautilus_model::types::{Price, Quantity};

use crate::journal::{AdmissionRecord, BusinessJournal, JournalOutcome, RequestRecord};

/// Errors from the admission bridge.
#[derive(Debug)]
pub enum BridgeError {
    /// The same request key was retried with different business parameters.
    IdempotencyConflict {
        submitter_id: String,
        request_id: String,
        original_fingerprint: String,
        new_fingerprint: String,
    },
    /// The business journal failed (storage or encoding).
    Journal(String),
    /// The order could not be constructed from the request.
    Order(String),
    /// The order could not be registered in the cache.
    Cache(String),
    /// The readiness gate is not admitting new orders.
    NotReady {
        /// Current readiness state name.
        state: String,
        /// Halt or restoring reason.
        reason: String,
    },
}

impl std::error::Error for BridgeError {}

impl std::fmt::Display for BridgeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IdempotencyConflict {
                submitter_id,
                request_id,
                ..
            } => write!(
                f,
                "idempotency conflict for key {submitter_id}|SubmitOrder|{request_id}"
            ),
            Self::Journal(err) => write!(f, "journal error: {err}"),
            Self::Order(err) => write!(f, "order construction error: {err}"),
            Self::Cache(err) => write!(f, "cache registration error: {err}"),
            Self::NotReady { state, reason } => {
                write!(f, "order hub not ready ({state}): {reason}")
            }
        }
    }
}

impl From<crate::journal::JournalError> for BridgeError {
    fn from(err: crate::journal::JournalError) -> Self {
        Self::Journal(err.to_string())
    }
}

/// A validated external limit-order request.
///
/// Quantities and prices use the exact Nautilus domain types; the journal
/// stores them as decimal strings.
#[derive(Debug, Clone)]
pub struct LimitOrderRequest {
    /// Server-side submitter identity.
    pub submitter_id: String,
    /// Client-generated unique request ID, stable across retries.
    pub request_id: String,
    /// Strategy which owns this order (attribution, limits, PnL).
    pub strategy_id: StrategyId,
    pub instrument_id: InstrumentId,
    pub side: nautilus_model::enums::OrderSide,
    pub quantity: Quantity,
    pub price: Price,
    pub time_in_force: TimeInForce,
    pub post_only: bool,
    pub reduce_only: bool,
    /// Optional routing client for the execution engine.
    pub client_id: Option<ClientId>,
    /// Business deadline for first dispatch (ns since epoch), if any.
    pub submit_before_ns: Option<u64>,
}

/// The result of admitting a request through the bridge.
#[derive(Debug, Clone)]
pub struct AdmissionReceipt {
    /// Journal epoch of the admission.
    pub journal_epoch: u64,
    /// Committed journal sequence of the admission.
    pub seq: u64,
    /// The order ID assigned at first admission.
    pub client_order_id: ClientOrderId,
    /// `true` when this call was an idempotent retry of an existing request.
    pub already_admitted: bool,
}

/// Bridge into the native `Cache -> RiskEngine -> ExecutionEngine` chain.
#[derive(Debug)]
pub struct OrderHubBridge {
    trader_id: TraderId,
    clock: Rc<RefCell<dyn Clock>>,
    cache: Rc<RefCell<Cache>>,
    journal: Arc<BusinessJournal>,
    worker: Option<crate::worker::PersistenceWorker>,
    quota: Option<Rc<RefCell<crate::quota::QuotaLedger>>>,
    gate: Option<std::sync::Arc<crate::readiness::ReadinessGate>>,
}

impl OrderHubBridge {
    /// Creates a bridge over the node's shared clock, cache, and journal.
    ///
    /// Without a persistence worker the journal commits synchronously on the
    /// calling thread (M0 behaviour); install one with [`Self::with_worker`]
    /// for group-commit batching.
    pub fn new(
        trader_id: TraderId,
        clock: Rc<RefCell<dyn Clock>>,
        cache: Rc<RefCell<Cache>>,
        journal: Arc<BusinessJournal>,
    ) -> Self {
        Self {
            trader_id,
            clock,
            cache,
            journal,
            worker: None,
            quota: None,
            gate: None,
        }
    }

    /// Installs the group-commit persistence worker for durable writes.
    #[must_use]
    pub fn with_worker(mut self, worker: crate::worker::PersistenceWorker) -> Self {
        self.worker = Some(worker);
        self
    }

    /// Installs the readiness gate; submissions are refused unless READY.
    #[must_use]
    pub fn with_gate(mut self, gate: std::sync::Arc<crate::readiness::ReadinessGate>) -> Self {
        self.gate = Some(gate);
        self
    }

    /// Installs the per-strategy quota ledger for admission checks.
    #[must_use]
    pub fn with_quota(mut self, quota: Rc<RefCell<crate::quota::QuotaLedger>>) -> Self {
        self.quota = Some(quota);
        self
    }

    /// Admits and submits a limit order through the native execution path.
    ///
    /// Order of operations (M0 prototype: journal commits are synchronous):
    ///
    /// 1. Idempotency check against the journal's request index.
    /// 2. Durable admission commit (allocates `seq`, derives `client_order_id`).
    /// 3. Construct the `LimitOrder` and register it in the cache.
    /// 4. Publish `OrderInitialized` on the strategy events topic.
    /// 5. Dispatch `SubmitOrder` to the risk engine's queued endpoint.
    /// 6. Durable `Admitted` outcome commit.
    pub fn submit_limit_order(
        &self,
        req: &LimitOrderRequest,
    ) -> Result<AdmissionReceipt, BridgeError> {
        if let Some(gate) = &self.gate
            && !gate.is_admissible()
        {
            let state = gate.get();
            return Err(BridgeError::NotReady {
                state: state.as_str().to_string(),
                reason: gate.halt_reason(),
            });
        }

        let fingerprint = fingerprint(req);
        let ts_init_ns = self.clock.borrow().timestamp_ns().as_u64();

        // 1. Idempotency
        if let Some(record) =
            self.journal
                .find_request(&req.submitter_id, "SubmitOrder", &req.request_id)?
        {
            return self.receipt_for_existing(record, req, fingerprint);
        }

        // 1b. Quota: check + tentative hold (serialized on this thread).
        // Buys reserve notional against strategy and account budgets; sells
        // reserve sellable quantity against strategy holdings.
        let tentative = self.quota.as_ref().map(|ledger| {
            let mut ledger = ledger.borrow_mut();
            match req.side {
                nautilus_model::enums::OrderSide::Buy => {
                    let notional = notional_of(&req.quantity, &req.price)?;
                    ledger
                        .try_tentative_buy(req.strategy_id.as_ref(), notional)
                        .map_err(|err| BridgeError::Order(err.to_string()))
                }
                nautilus_model::enums::OrderSide::Sell => {
                    use rust_decimal::prelude::FromStr;
                    let quantity = rust_decimal::Decimal::from_str(&req.quantity.to_string())
                        .map_err(|err| BridgeError::Order(err.to_string()))?;
                    ledger
                        .try_tentative_sell(
                            req.strategy_id.as_ref(),
                            &req.instrument_id.to_string(),
                            quantity,
                        )
                        .map_err(|err| BridgeError::Order(err.to_string()))
                }
            }
        });
        if let Some(Err(err)) = &tentative {
            return Err(BridgeError::Order(err.to_string()));
        }

        // 2. Durable admission (group-commit worker when installed)
        let admission_record = AdmissionRecord {
            journal_epoch: 0, // assigned by the journal
            seq: 0,           // assigned by the journal
            submitter_id: req.submitter_id.clone(),
            request_id: req.request_id.clone(),
            request_kind: "SubmitOrder".to_string(),
            request_fingerprint: fingerprint,
            strategy_id: req.strategy_id.to_string(),
            instrument_id: req.instrument_id.to_string(),
            order_side: req.side.to_string(),
            quantity: req.quantity.to_string(),
            price: req.price.to_string(),
            time_in_force: req.time_in_force.to_string(),
            post_only: req.post_only,
            reduce_only: req.reduce_only,
            submit_before_ns: req.submit_before_ns,
            ts_init_ns,
        };
        let admission = match &self.worker {
            Some(worker) => worker.submit_admission(admission_record),
            None => self.journal.append_admission(admission_record),
        }
        .inspect_err(|_| release_tentative(tentative_ok(&tentative), &self.quota))?;

        let client_order_id = derive_client_order_id(admission.journal_epoch, admission.seq);

        // 3. Construct and register the order (mirrors `submit_order_native`)
        let order = LimitOrder::new_checked(
            self.trader_id,
            req.strategy_id,
            req.instrument_id,
            client_order_id,
            req.side,
            req.quantity,
            req.price,
            req.time_in_force,
            None, // expire_time
            req.post_only,
            req.reduce_only,
            false, // quote_quantity
            None,  // display_qty
            None,  // emulation_trigger
            None,  // trigger_instrument_id
            None,  // contingency_type
            None,  // order_list_id
            None,  // linked_order_ids
            None,  // parent_order_id
            None,  // exec_algorithm_id
            None,  // exec_algorithm_params
            None,  // exec_spawn_id
            None,  // tags
            UUID4::new(),
            UnixNanos::from(ts_init_ns),
        )
        .map_err(|err| BridgeError::Order(err.to_string()));
        let order = match order {
            Ok(order) => OrderAny::Limit(order),
            Err(err) => {
                release_tentative(tentative_ok(&tentative), &self.quota);
                return Err(err);
            }
        };

        {
            let mut cache = self.cache.borrow_mut();
            if let Err(err) = cache.add_order(order.clone(), None, req.client_id, true) {
                release_tentative(tentative_ok(&tentative), &self.quota);
                return Err(BridgeError::Cache(err.to_string()));
            }
        }

        // 4. Publish OrderInitialized on the strategy events topic
        let topic = format!("events.order.{}", order.strategy_id());
        let event = OrderEventAny::Initialized(order.init_event().clone());
        msgbus::publish_order_event(topic.into(), &event);

        // 5. Dispatch SubmitOrder to the risk engine (queued endpoint,
        //    matching the native strategy path; no emulator/algo for M0)
        let command = SubmitOrder::new(
            self.trader_id,
            req.client_id,
            req.strategy_id,
            req.instrument_id,
            order.client_order_id(),
            order.init_event().clone(),
            None, // exec_algorithm_id
            None, // position_id
            None, // params
            UUID4::new(),
            UnixNanos::from(ts_init_ns),
            None, // correlation_id
        );
        msgbus::send_trading_command(
            MessagingSwitchboard::risk_engine_queue_execute(),
            TradingCommand::SubmitOrder(command),
        );

        // 6. Durable Admitted outcome
        let admitted_outcome = JournalOutcome::Admitted {
            client_order_id: client_order_id.to_string(),
        };
        match &self.worker {
            Some(worker) => worker.submit_outcome(admission.seq, admitted_outcome)?,
            None => self
                .journal
                .append_outcome(admission.seq, admitted_outcome)?,
        };

        if let (Some(id), Some(ledger)) = (tentative_ok(&tentative), &self.quota) {
            let _ = ledger.borrow_mut().commit(id);
        }

        Ok(AdmissionReceipt {
            journal_epoch: admission.journal_epoch,
            seq: admission.seq,
            client_order_id,
            already_admitted: false,
        })
    }

    /// The journal epoch of the underlying business journal.
    #[must_use]
    pub fn journal_epoch(&self) -> u64 {
        self.journal.epoch()
    }

    /// Unique instrument IDs referenced by journal admissions.
    ///
    /// Instruments stay enumerable after their orders reach terminal states.
    pub fn journal_instrument_ids(&self) -> Vec<String> {
        let Ok(entries) = self.journal.scan_all() else {
            return Vec::new();
        };
        let mut ids: Vec<String> = entries
            .into_iter()
            .filter_map(|(_, entry)| match entry {
                crate::journal::JournalEntry::Admission(record) => Some(record.instrument_id),
                crate::journal::JournalEntry::Outcome { .. } => None,
            })
            .filter(|id| !id.starts_with("CANCEL:"))
            .collect();
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    /// The last committed journal sequence.
    ///
    /// # Errors
    ///
    /// Returns an error if the journal cannot be read.
    pub fn journal_last_seq(&self) -> Result<u64, crate::journal::JournalError> {
        self.journal.last_seq()
    }

    /// Durably records a cancel intent for `client_order_id` (M1 scope).
    ///
    /// Per the admission contract this persists the cancellation *request*
    /// only: acceptance means the intent is durable, not that the order is
    /// cancelled (fills may still arrive). Dispatching the cancel to the
    /// execution engine requires the node wiring and lands with the sandbox
    /// loop integration.
    pub fn cancel_order(
        &self,
        submitter_id: &str,
        request_id: &str,
        client_order_id: &str,
    ) -> Result<AdmissionReceipt, BridgeError> {
        let fingerprint = format!("CANCEL|{client_order_id}");

        if let Some(record) = self
            .journal
            .find_request(submitter_id, "CancelOrder", request_id)?
        {
            if record.admission.request_fingerprint != fingerprint {
                return Err(BridgeError::IdempotencyConflict {
                    submitter_id: submitter_id.to_string(),
                    request_id: request_id.to_string(),
                    original_fingerprint: record.admission.request_fingerprint,
                    new_fingerprint: fingerprint,
                });
            }
            let receipt_coid = record
                .admission
                .instrument_id
                .strip_prefix("CANCEL:")
                .unwrap_or(&record.admission.instrument_id)
                .to_string();
            return Ok(AdmissionReceipt {
                journal_epoch: record.admission.journal_epoch,
                seq: record.admission.seq,
                client_order_id: ClientOrderId::from(receipt_coid),
                already_admitted: true,
            });
        }

        let ts_init_ns = self.clock.borrow().timestamp_ns().as_u64();
        let admission_record = AdmissionRecord {
            journal_epoch: 0,
            seq: 0,
            submitter_id: submitter_id.to_string(),
            request_id: request_id.to_string(),
            request_kind: "CancelOrder".to_string(),
            request_fingerprint: fingerprint,
            strategy_id: "-".to_string(),
            instrument_id: format!("CANCEL:{client_order_id}"),
            order_side: "CANCEL".to_string(),
            quantity: "-".to_string(),
            price: "-".to_string(),
            time_in_force: "-".to_string(),
            post_only: false,
            reduce_only: false,
            submit_before_ns: None,
            ts_init_ns,
        };
        let admission = match &self.worker {
            Some(worker) => worker.submit_admission(admission_record)?,
            None => self.journal.append_admission(admission_record)?,
        };
        let outcome = JournalOutcome::Admitted {
            client_order_id: client_order_id.to_string(),
        };
        match &self.worker {
            Some(worker) => worker.submit_outcome(admission.seq, outcome)?,
            None => self.journal.append_outcome(admission.seq, outcome)?,
        };

        // Dispatch the cancel toward the execution engine when the order is
        // live in the cache (no-op if no execution engine is wired).
        let cancel_target = {
            let cache = self.cache.borrow();
            cache
                .order(&ClientOrderId::from(client_order_id))
                .map(|order| {
                    (
                        order.strategy_id(),
                        order.instrument_id(),
                        order.client_order_id(),
                    )
                })
        };
        if let Some((strategy_id, instrument_id, order_id)) = cancel_target {
            crate::exec_wiring::dispatch_cancel(
                self.trader_id,
                strategy_id,
                instrument_id,
                order_id,
                ts_init_ns,
            );
        }

        Ok(AdmissionReceipt {
            journal_epoch: admission.journal_epoch,
            seq: admission.seq,
            client_order_id: ClientOrderId::from(client_order_id),
            already_admitted: false,
        })
    }

    fn receipt_for_existing(
        &self,
        record: RequestRecord,
        req: &LimitOrderRequest,
        fingerprint: String,
    ) -> Result<AdmissionReceipt, BridgeError> {
        if record.admission.request_fingerprint != fingerprint {
            return Err(BridgeError::IdempotencyConflict {
                submitter_id: req.submitter_id.clone(),
                request_id: req.request_id.clone(),
                original_fingerprint: record.admission.request_fingerprint,
                new_fingerprint: fingerprint,
            });
        }
        let client_order_id = match &record.outcome {
            Some(
                JournalOutcome::Admitted {
                    client_order_id, ..
                }
                | JournalOutcome::DispatchStarted { client_order_id },
            ) => ClientOrderId::from(client_order_id.as_str()),
            _ => derive_client_order_id(record.admission.journal_epoch, record.admission.seq),
        };
        Ok(AdmissionReceipt {
            journal_epoch: record.admission.journal_epoch,
            seq: record.admission.seq,
            client_order_id,
            already_admitted: true,
        })
    }
}

/// Derives the order ID deterministically from `(journal_epoch, seq)`.
///
/// The epoch scoping prevents ID reuse after restoring an older backup, as
/// required by the admission contract.
#[must_use]
pub fn derive_client_order_id(journal_epoch: u64, seq: u64) -> ClientOrderId {
    ClientOrderId::from(format!("O-{journal_epoch}-{seq:012}"))
}

/// Canonical digest of the normalized business fields.
///
/// Covers the fields enumerated in the admission contract: strategy, routing,
/// instrument, side, quantity, price, order type, time in force, the trading
/// instruction flags, and `submit_before`. Transient fields (tokens, transport
/// deadlines) are excluded.
fn fingerprint(req: &LimitOrderRequest) -> String {
    let client_id = req
        .client_id
        .map_or_else(|| "-".to_string(), |id| id.to_string());
    let submit_before = req
        .submit_before_ns
        .map_or_else(|| "-".to_string(), |ns| ns.to_string());
    format!(
        "LIMIT|{}|{client_id}|{}|{}|{}|{}|{}|{}|{}|{submit_before}",
        req.strategy_id,
        req.instrument_id,
        req.side,
        req.quantity,
        req.price,
        req.time_in_force,
        req.post_only,
        req.reduce_only,
    )
}

/// Decimal notional of a limit order (quantity x price).
fn notional_of(quantity: &Quantity, price: &Price) -> Result<rust_decimal::Decimal, BridgeError> {
    use rust_decimal::prelude::FromStr;
    let qty = rust_decimal::Decimal::from_str(&quantity.to_string())
        .map_err(|err| BridgeError::Order(err.to_string()))?;
    let px = rust_decimal::Decimal::from_str(&price.to_string())
        .map_err(|err| BridgeError::Order(err.to_string()))?;
    Ok(qty * px)
}

fn tentative_ok(
    tentative: &Option<Result<crate::quota::TentativeId, BridgeError>>,
) -> Option<crate::quota::TentativeId> {
    match tentative {
        Some(Ok(id)) => Some(*id),
        _ => None,
    }
}

fn release_tentative(
    id: Option<crate::quota::TentativeId>,
    quota: &Option<Rc<RefCell<crate::quota::QuotaLedger>>>,
) {
    if let (Some(id), Some(ledger)) = (id, quota) {
        let _ = ledger.borrow_mut().release_tentative(id);
    }
}
