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

//! gRPC service over the admission bridge (M1).
//!
//! Threading model per the design: the bridge owns non-`Send` kernel state
//! (`Rc<RefCell<Cache>>`), so all four RPCs exchange plain value objects with
//! a dedicated core thread through a channel. The tonic handlers themselves
//! only await reply channels and never touch kernel state. WatchEvents reads
//! the journal (which is `Send + Sync`) directly.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::mpsc::Sender as CoreSender;
use std::sync::{Arc, mpsc};
use std::time::Duration;

use nautilus_common::cache::Cache;
use nautilus_model::identifiers::{InstrumentId, StrategyId};
use tokio::sync::oneshot;
use tonic::Request as RpcRequest;
use tonic::{Request, Response, Status, transport::Server};

use crate::bridge::{BridgeError, LimitOrderRequest, OrderHubBridge};
use crate::journal::{BusinessJournal, JournalEntry, JournalOutcome};
use orderhub_proto::orderhub::v1::order_hub_server::{OrderHub, OrderHubServer};
use orderhub_proto::orderhub::v1::{
    AdmissionEvent, AdmissionResponse, CancelOrderRequest, GetStateRequest, GetStateResponse,
    InstrumentInfo, JournalEvent, OrderView, OutcomeEvent, ProgressEvent, SubmitOrderRequest,
    WatchEventsRequest,
};

use nautilus_model::enums::{OrderSide, TimeInForce};
use nautilus_model::instruments::Instrument;
use nautilus_model::orders::Order;
use nautilus_model::types::{Price, Quantity};

/// Snapshot payload returned by the core thread for GetState.
#[derive(Debug)]
struct CoreSnapshot {
    epoch: u64,
    cursor: u64,
    orders: Vec<OrderView>,
    instruments: Vec<InstrumentInfo>,
}

enum CoreRequest {
    Submit {
        request: LimitOrderRequest,
        reply: oneshot::Sender<Result<crate::bridge::AdmissionReceipt, BridgeError>>,
    },
    Cancel {
        submitter_id: String,
        request_id: String,
        client_order_id: String,
        reply: oneshot::Sender<Result<crate::bridge::AdmissionReceipt, BridgeError>>,
    },
    Snapshot {
        reply: oneshot::Sender<CoreSnapshot>,
    },
}

/// Handle to the core thread owning the bridge, cache, and quota ledger.
#[derive(Debug, Clone)]
pub struct CoreHandle {
    tx: CoreSender<CoreRequest>,
    journal: Arc<BusinessJournal>,
    registry: Arc<crate::auth::SubmitterRegistry>,
    gate: std::sync::Arc<crate::readiness::ReadinessGate>,
}

impl CoreHandle {
    /// Spawns the core thread over an already-wired bridge and cache.
    ///
    /// # Panics
    ///
    /// Panics if the core thread cannot be spawned.
    #[allow(clippy::type_complexity)]
    pub fn spawn(
        setup: impl FnOnce() -> (
            OrderHubBridge,
            Rc<RefCell<Cache>>,
            Option<crate::sandbox::ExecEventRx>,
        ) + Send
        + 'static,
        journal: Arc<BusinessJournal>,
        registry: Arc<crate::auth::SubmitterRegistry>,
        gate: std::sync::Arc<crate::readiness::ReadinessGate>,
    ) -> Self {
        let (tx, rx) = mpsc::channel::<CoreRequest>();
        std::thread::Builder::new()
            .name("orderhub-core".to_string())
            .spawn(move || {
                let (bridge, cache, exec_rx) = setup();
                core_thread(&bridge, &cache, exec_rx, &rx);
            })
            .expect("failed to spawn core thread");
        Self {
            tx,
            journal,
            registry,
            gate,
        }
    }

    /// The shared readiness gate.
    #[must_use]
    pub fn gate(&self) -> std::sync::Arc<crate::readiness::ReadinessGate> {
        std::sync::Arc::clone(&self.gate)
    }

    async fn submit(
        &self,
        request: LimitOrderRequest,
    ) -> Result<crate::bridge::AdmissionReceipt, BridgeError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(CoreRequest::Submit {
                request,
                reply: reply_tx,
            })
            .map_err(|_| BridgeError::Journal("core thread stopped".to_string()))?;
        reply_rx
            .await
            .map_err(|_| BridgeError::Journal("core thread dropped reply".to_string()))?
    }

    async fn cancel(
        &self,
        submitter_id: String,
        request_id: String,
        client_order_id: String,
    ) -> Result<crate::bridge::AdmissionReceipt, BridgeError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(CoreRequest::Cancel {
                submitter_id,
                request_id,
                client_order_id,
                reply: reply_tx,
            })
            .map_err(|_| BridgeError::Journal("core thread stopped".to_string()))?;
        reply_rx
            .await
            .map_err(|_| BridgeError::Journal("core thread dropped reply".to_string()))?
    }

    async fn snapshot(&self) -> Result<CoreSnapshot, Status> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(CoreRequest::Snapshot { reply: reply_tx })
            .map_err(|_| Status::unavailable("core thread stopped"))?;
        reply_rx
            .await
            .map_err(|_| Status::unavailable("core thread dropped reply"))
    }
}

fn core_thread(
    bridge: &OrderHubBridge,
    cache: &Rc<RefCell<Cache>>,
    mut exec_rx: Option<crate::sandbox::ExecEventRx>,
    rx: &mpsc::Receiver<CoreRequest>,
) {
    loop {
        crate::sandbox::pump_exec_events(&mut exec_rx);
        match rx.recv_timeout(std::time::Duration::from_millis(5)) {
            Ok(request) => {
                handle_core_request(bridge, cache, request);
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
}

fn handle_core_request(bridge: &OrderHubBridge, cache: &Rc<RefCell<Cache>>, request: CoreRequest) {
    {
        match request {
            CoreRequest::Submit { request, reply } => {
                let _ = reply.send(bridge.submit_limit_order(&request));
            }
            CoreRequest::Cancel {
                submitter_id,
                request_id,
                client_order_id,
                reply,
            } => {
                let _ =
                    reply.send(bridge.cancel_order(&submitter_id, &request_id, &client_order_id));
            }
            CoreRequest::Snapshot { reply } => {
                let epoch = bridge.journal_epoch();
                let cursor = bridge.journal_last_seq().unwrap_or(0);
                // All non-closed orders (Initialized is not in the
                // orders-open index until the execution engine submits).
                let cache_ref = cache.borrow();
                let all_orders = cache_ref.orders(None, None, None, None, None);
                let open_orders: Vec<_> = all_orders
                    .iter()
                    .filter(|order| !order.is_closed())
                    .collect();
                let orders = open_orders
                    .iter()
                    .map(|order| OrderView {
                        client_order_id: order.client_order_id().to_string(),
                        strategy_id: order.strategy_id().to_string(),
                        instrument_id: order.instrument_id().to_string(),
                        side: order.order_side().to_string(),
                        quantity: order.quantity().to_string(),
                        price: order
                            .price()
                            .map_or_else(|| "-".to_string(), |p| p.to_string()),
                        status: order.status().to_string(),
                        filled_quantity: order.filled_qty().to_string(),
                    })
                    .collect();
                // Instrument metadata for every instrument with an open order.
                let mut instrument_ids: Vec<String> = open_orders
                    .iter()
                    .map(|order| order.instrument_id().to_string())
                    .collect();
                instrument_ids.extend(bridge.journal_instrument_ids());
                instrument_ids.sort_unstable();
                instrument_ids.dedup();
                let instruments = instrument_ids
                    .iter()
                    .filter_map(|id| {
                        let instrument_id =
                            nautilus_model::identifiers::InstrumentId::from(id.as_str());
                        let instrument = cache_ref.instrument(&instrument_id)?;
                        Some(InstrumentInfo {
                            instrument_id: id.clone(),
                            price_increment: instrument.price_increment().to_string(),
                            size_increment: instrument.size_increment().to_string(),
                            price_precision: u32::from(instrument.price_precision()),
                        })
                    })
                    .collect();
                drop(open_orders);
                drop(all_orders);
                drop(cache_ref);
                let _ = reply.send(CoreSnapshot {
                    epoch,
                    cursor,
                    orders,
                    instruments,
                });
            }
        }
    }
}

/// gRPC service implementing the four OrderHub RPCs.
#[derive(Debug)]
pub struct OrderHubService {
    core: CoreHandle,
}

impl OrderHubService {
    /// Wraps a core handle as the tonic service.
    #[must_use]
    pub fn new(core: CoreHandle) -> Self {
        Self { core }
    }

    /// Authenticates the bearer token to a submitter identity.
    fn authenticate<T>(
        &self,
        request: &RpcRequest<T>,
    ) -> Result<&crate::auth::SubmitterCredentials, Status> {
        let header = request
            .metadata()
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("");
        self.core
            .registry
            .authenticate_header(header)
            .ok_or_else(|| Status::permission_denied("invalid or missing token"))
    }

    /// Authenticates and verifies the request's strategy is authorized.
    fn authorize_strategy<T>(
        &self,
        request: &RpcRequest<T>,
        strategy_id: &str,
    ) -> Result<&crate::auth::SubmitterCredentials, Status> {
        let credentials = self.authenticate(request)?;
        if crate::auth::SubmitterRegistry::is_authorized(credentials, strategy_id) {
            Ok(credentials)
        } else {
            // Do not disclose whether the strategy exists for others.
            Err(Status::permission_denied(
                "strategy not authorized for this submitter",
            ))
        }
    }
}

/// Borrows the strategy id from a submit request before it is consumed.
fn req_ref_strategy(request: &RpcRequest<SubmitOrderRequest>) -> &str {
    request.get_ref().strategy_id.as_str()
}

#[tonic::async_trait]
impl OrderHub for OrderHubService {
    async fn submit_order(
        &self,
        request: Request<SubmitOrderRequest>,
    ) -> Result<Response<AdmissionResponse>, Status> {
        let credentials = self.authorize_strategy(&request, req_ref_strategy(&request))?;
        let req = request.into_inner();

        let side = match req.side.as_str() {
            "BUY" => OrderSide::Buy,
            "SELL" => OrderSide::Sell,
            other => return Err(Status::invalid_argument(format!("invalid side {other}"))),
        };
        let time_in_force = match req.time_in_force.as_str() {
            "GTC" => TimeInForce::Gtc,
            "IOC" => TimeInForce::Ioc,
            "FOK" => TimeInForce::Fok,
            other => {
                return Err(Status::invalid_argument(format!(
                    "invalid time_in_force {other}"
                )));
            }
        };
        let limit_request = LimitOrderRequest {
            submitter_id: credentials.submitter_id.clone(),
            request_id: req.request_id,
            strategy_id: StrategyId::from(req.strategy_id),
            instrument_id: InstrumentId::from(req.instrument_id),
            side,
            quantity: Quantity::from(req.quantity),
            price: Price::from(req.price),
            time_in_force,
            post_only: req.post_only,
            reduce_only: req.reduce_only,
            client_id: None,
            submit_before_ns: req.submit_before_ns,
        };

        let receipt = self
            .core
            .submit(limit_request)
            .await
            .map_err(|err| match err {
                BridgeError::IdempotencyConflict { .. } => Status::already_exists(err.to_string()),
                BridgeError::Order(message) => Status::invalid_argument(message),
                other => Status::internal(other.to_string()),
            })?;

        Ok(Response::new(AdmissionResponse {
            journal_epoch: receipt.journal_epoch.to_string(),
            seq: receipt.seq.to_string(),
            client_order_id: receipt.client_order_id.to_string(),
            already_admitted: receipt.already_admitted,
            result: "ADMITTED".to_string(),
            reason: String::new(),
        }))
    }

    async fn cancel_order(
        &self,
        request: Request<CancelOrderRequest>,
    ) -> Result<Response<AdmissionResponse>, Status> {
        let credentials = self.authenticate(&request)?;
        let req = request.into_inner();

        let receipt = self
            .core
            .cancel(
                credentials.submitter_id.clone(),
                req.request_id,
                req.client_order_id.clone(),
            )
            .await
            .map_err(|err| match err {
                BridgeError::IdempotencyConflict { .. } => Status::already_exists(err.to_string()),
                other => Status::internal(other.to_string()),
            })?;

        Ok(Response::new(AdmissionResponse {
            journal_epoch: receipt.journal_epoch.to_string(),
            seq: receipt.seq.to_string(),
            client_order_id: req.client_order_id,
            already_admitted: receipt.already_admitted,
            result: "CANCEL_INTENT_ACCEPTED".to_string(),
            reason: String::new(),
        }))
    }

    async fn get_state(
        &self,
        request: Request<GetStateRequest>,
    ) -> Result<Response<GetStateResponse>, Status> {
        self.authenticate(&request)?;
        let snapshot = self.core.snapshot().await?;
        let gate = self.core.gate();
        Ok(Response::new(GetStateResponse {
            journal_epoch: snapshot.epoch.to_string(),
            state_cursor: snapshot.cursor.to_string(),
            writable: gate.is_admissible(),
            readiness: gate.get().as_str().to_string(),
            active_orders: snapshot.orders,
            instruments: snapshot.instruments,
        }))
    }

    type WatchEventsStream = tokio_stream::wrappers::ReceiverStream<Result<JournalEvent, Status>>;

    async fn watch_events(
        &self,
        request: Request<WatchEventsRequest>,
    ) -> Result<Response<Self::WatchEventsStream>, Status> {
        self.authenticate(&request)?;
        let req = request.into_inner();
        let mut cursor: u64 = req
            .after
            .parse()
            .map_err(|_| Status::invalid_argument("after must be a u64 string"))?;

        let journal = Arc::clone(&self.core.journal);
        let (tx, rx) = tokio::sync::mpsc::channel(256);
        tokio::spawn(async move {
            loop {
                let entries = match journal.scan_after(cursor) {
                    Ok(entries) => entries,
                    Err(err) => {
                        let _ = tx.send(Err(Status::internal(err.to_string()))).await;
                        break;
                    }
                };
                if entries.is_empty() {
                    // Progress frame: advances the follower's cursor without
                    // fabricating business events (per the resync contract).
                    let high_water = journal.last_seq().unwrap_or(cursor);
                    if high_water > cursor {
                        cursor = high_water;
                    }
                    if tx
                        .send(Ok(JournalEvent {
                            seq: cursor.to_string(),
                            journal_epoch: journal.epoch().to_string(),
                            kind: Some(
                                orderhub_proto::orderhub::v1::journal_event::Kind::Progress(
                                    ProgressEvent {
                                        state_cursor: cursor.to_string(),
                                    },
                                ),
                            ),
                        }))
                        .await
                        .is_err()
                    {
                        break; // subscriber gone
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                for (seq, entry) in entries {
                    cursor = seq;
                    let event = JournalEvent {
                        seq: seq.to_string(),
                        journal_epoch: journal.epoch().to_string(),
                        kind: Some(match entry {
                            JournalEntry::Admission(record) => {
                                orderhub_proto::orderhub::v1::journal_event::Kind::Admission(
                                    AdmissionEvent {
                                        submitter_id: record.submitter_id,
                                        request_id: record.request_id,
                                        strategy_id: record.strategy_id,
                                        instrument_id: record.instrument_id,
                                        request_kind: "SubmitOrder".to_string(),
                                    },
                                )
                            }
                            JournalEntry::Outcome {
                                origin_seq,
                                outcome,
                            } => orderhub_proto::orderhub::v1::journal_event::Kind::Outcome(
                                OutcomeEvent {
                                    origin_seq: origin_seq.to_string(),
                                    client_order_id: match &outcome {
                                        JournalOutcome::Admitted { client_order_id }
                                        | JournalOutcome::DispatchStarted { client_order_id }
                                        | JournalOutcome::Filled {
                                            client_order_id, ..
                                        }
                                        | JournalOutcome::Canceled { client_order_id } => {
                                            client_order_id.clone()
                                        }
                                        JournalOutcome::Denied { .. } => String::new(),
                                    },
                                    outcome: match outcome {
                                        JournalOutcome::Admitted { .. } => "ADMITTED",
                                        JournalOutcome::DispatchStarted { .. } => {
                                            "DISPATCH_STARTED"
                                        }
                                        JournalOutcome::Denied { .. } => "DENIED",
                                        JournalOutcome::Filled { .. } => "FILLED",
                                        JournalOutcome::Canceled { .. } => "CANCELED",
                                    }
                                    .to_string(),
                                    reason: match outcome {
                                        JournalOutcome::Denied { reason } => reason,
                                        _ => String::new(),
                                    },
                                },
                            ),
                        }),
                    };
                    if tx.send(Ok(event)).await.is_err() {
                        return; // subscriber gone
                    }
                }
            }
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }
}

/// Serves the gRPC endpoint until the process ends.
///
/// # Errors
///
/// Returns an error if the server fails to bind or serve.
pub async fn serve(
    core: CoreHandle,
    addr: std::net::SocketAddr,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    Server::builder()
        .add_service(OrderHubServer::new(OrderHubService::new(core)))
        .serve(addr)
        .await?;
    Ok(())
}
