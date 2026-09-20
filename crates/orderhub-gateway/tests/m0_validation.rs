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

//! M0 validation tests for the OrderHub gateway prototype.
//!
//! These tests validate the M0 gate from the transformation plan:
//! the main-thread bridge into the native execution path is feasible
//! (order initialization, cache registration, risk endpoint dispatch,
//! denial propagation, attribution), and the local business journal
//! recovers its records and commits durably at a measurable cost.
//!
//! The message bus endpoints are process-global names, so this binary must
//! run single-threaded: `cargo test -p orderhub-gateway -- --test-threads=1`
//! (nextest isolates per process and is unaffected).

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

use ahash::AHashMap;

use nautilus_common::cache::Cache;
use nautilus_common::clock::{Clock, VirtualClock};
use nautilus_common::messages::execution::{SubmitOrder, TradingCommand};
use nautilus_common::msgbus::stubs::{
    TypedIntoMessageSavingHandler, get_typed_into_message_saving_handler,
};
use nautilus_common::msgbus::{self, MessagingSwitchboard};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::accounts::AccountAny;
use nautilus_model::data::QuoteTick;
use nautilus_model::enums::{AccountType, OrderSide, TimeInForce};
use nautilus_model::events::{AccountState, OrderEventAny};
use nautilus_model::identifiers::{AccountId, InstrumentId, StrategyId, TraderId};
use nautilus_model::instruments::{InstrumentAny, stubs::equity_aapl};
use nautilus_model::orders::Order;
use nautilus_model::types::{AccountBalance, Currency, Money, Price, Quantity};
use nautilus_portfolio::Portfolio;
use nautilus_risk::engine::{RiskEngine, config::RiskEngineConfig};
use orderhub_gateway::bridge::{LimitOrderRequest, OrderHubBridge};
use orderhub_gateway::journal::{AdmissionRecord, BusinessJournal, JournalEntry, JournalOutcome};

struct Harness {
    bridge: OrderHubBridge,
    cache: Rc<RefCell<Cache>>,
    journal: Arc<BusinessJournal>,
    // Keeps the risk engine alive: its message-bus handlers hold only weak
    // references, so dropping this Rc would silently swallow commands.
    #[expect(dead_code, reason = "liveness anchor only")]
    risk_engine: Rc<RefCell<RiskEngine>>,
    commands: TypedIntoMessageSavingHandler<TradingCommand>,
    events: TypedIntoMessageSavingHandler<OrderEventAny>,
}

fn harness(dir: &tempfile::TempDir) -> Harness {
    harness_with_config(dir, RiskEngineConfig::default())
}

fn harness_with_config(dir: &tempfile::TempDir, risk_config: RiskEngineConfig) -> Harness {
    let cache = Rc::new(RefCell::new(Cache::default()));
    {
        let mut cache_ref = cache.borrow_mut();
        cache_ref
            .add_instrument(InstrumentAny::Equity(equity_aapl()))
            .unwrap();
        // Cash account explicitly scoped to the XNAS venue so venue-based
        // account lookup in the risk engine resolves (stub accounts use SIM).
        let state = AccountState::new(
            AccountId::from("XNAS-001"),
            AccountType::Cash,
            vec![AccountBalance::new(
                Money::from("1000000 USD"),
                Money::from("0 USD"),
                Money::from("1000000 USD"),
            )],
            vec![],
            true,
            UUID4::new(),
            UnixNanos::from(0),
            UnixNanos::from(0),
            Some(Currency::USD()),
        );
        cache_ref
            .add_account(AccountAny::Cash(
                nautilus_model::accounts::CashAccount::new(state, true, false),
            ))
            .unwrap();
        cache_ref
            .add_quote(QuoteTick::new(
                InstrumentId::from("AAPL.XNAS"),
                Price::from("100.00"),
                Price::from("100.02"),
                Quantity::from("100"),
                Quantity::from("100"),
                UnixNanos::from(1),
                UnixNanos::from(1),
            ))
            .unwrap();
    }

    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let portfolio = Portfolio::new(clock.clone(), cache.clone(), None);
    let risk_engine = Rc::new(RefCell::new(RiskEngine::new(
        risk_config,
        portfolio,
        clock.clone(),
        cache.clone(),
    )));
    RiskEngine::register_msgbus_handlers(&risk_engine);

    // Collect commands handed to the execution engine queue, and order
    // events published at the execution engine process endpoint.
    let (command_handler, commands) = get_typed_into_message_saving_handler(None);
    msgbus::register_trading_command_endpoint(
        MessagingSwitchboard::exec_engine_queue_execute(),
        command_handler,
    );
    let (event_handler, events) = get_typed_into_message_saving_handler(None);
    msgbus::register_order_event_endpoint(
        MessagingSwitchboard::exec_engine_process(),
        event_handler,
    );

    let journal = Arc::new(BusinessJournal::open(&dir.path().join("journal.redb")).unwrap());
    let bridge = OrderHubBridge::new(
        TraderId::from("ORDER-HUB"),
        clock,
        cache.clone(),
        journal.clone(),
    );

    Harness {
        bridge,
        cache,
        journal,
        risk_engine,
        commands,
        events,
    }
}

fn aapl_request(request_id: &str, quantity: &str) -> LimitOrderRequest {
    LimitOrderRequest {
        submitter_id: "trader-1".to_string(),
        request_id: request_id.to_string(),
        strategy_id: StrategyId::from("ORDERHUB-S001"),
        instrument_id: InstrumentId::from("AAPL.XNAS"),
        side: OrderSide::Buy,
        quantity: Quantity::from(quantity),
        price: Price::from("150.00"),
        time_in_force: TimeInForce::Gtc,
        post_only: false,
        reduce_only: false,
        client_id: None,
        submit_before_ns: None,
    }
}

fn submit_commands(h: &Harness) -> Vec<SubmitOrder> {
    h.commands
        .get_messages()
        .iter()
        .filter_map(|cmd| match cmd {
            TradingCommand::SubmitOrder(submit) => Some(submit.clone()),
            _ => None,
        })
        .collect()
}

#[test]
fn m0_t1_submit_reaches_execution_queue_with_attribution() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness(&dir);

    let receipt = h
        .bridge
        .submit_limit_order(&aapl_request("req-1", "10"))
        .unwrap();
    assert!(!receipt.already_admitted);
    assert_eq!(receipt.journal_epoch, 1);
    assert_eq!(receipt.seq, 1);
    assert_eq!(receipt.client_order_id.to_string(), "O-1-000000000001");

    // Order registered in cache, still INITIALIZED (execution would move it on)
    {
        let cache = h.cache.borrow();
        let order = cache.order(&receipt.client_order_id).unwrap();
        assert_eq!(order.strategy_id(), StrategyId::from("ORDERHUB-S001"));
    }

    // Exactly one SubmitOrder reached the execution engine queue with full attribution
    let submits = submit_commands(&h);
    assert_eq!(submits.len(), 1);
    assert_eq!(submits[0].strategy_id, StrategyId::from("ORDERHUB-S001"));
    assert_eq!(submits[0].client_order_id, receipt.client_order_id);
    assert_eq!(submits[0].instrument_id, InstrumentId::from("AAPL.XNAS"));

    // No denials
    assert!(
        h.events
            .get_messages()
            .iter()
            .all(|event| !matches!(event, OrderEventAny::Denied(_)))
    );

    // Journal holds the admission with an Admitted outcome
    let record = h
        .journal
        .find_request("trader-1", "SubmitOrder", "req-1")
        .unwrap()
        .expect("admission journaled");
    assert_eq!(record.admission.seq, 1);
    assert!(matches!(
        record.outcome,
        Some(JournalOutcome::Admitted { .. })
    ));
}

#[test]
fn m0_t2_risk_denial_flows_back_with_attribution() {
    let dir = tempfile::tempdir().unwrap();
    // Max notional per order: 1 USD -> a 10 * 150.00 USD order must be denied
    // by the native RiskEngine (not by the bridge).
    let mut max_notional = AHashMap::new();
    max_notional.insert(InstrumentId::from("AAPL.XNAS"), rust_decimal::Decimal::ONE);
    let config = RiskEngineConfig {
        max_notional_per_order: max_notional,
        ..Default::default()
    };
    let h = harness_with_config(&dir, config);

    let receipt = h
        .bridge
        .submit_limit_order(&aapl_request("req-deny", "10"))
        .unwrap();
    assert!(!receipt.already_admitted);

    // The denial surfaced at the execution engine process endpoint
    let events = h.events.get_messages();
    let denials: Vec<_> = events
        .iter()
        .filter(|event| matches!(event, OrderEventAny::Denied(_)))
        .collect();
    assert_eq!(denials.len(), 1, "expected exactly one OrderDenied");
    if let OrderEventAny::Denied(denied) = denials[0] {
        assert_eq!(
            denied.strategy_id,
            StrategyId::from("ORDERHUB-S001"),
            "denial must carry submitter attribution"
        );
        assert_eq!(denied.client_order_id, receipt.client_order_id);
    }

    // Nothing reached the execution queue
    assert!(submit_commands(&h).is_empty());

    // Observing the denial appends a durable Denied outcome (M0: manual append;
    // M1 wires this into an automatic event subscription)
    h.journal
        .append_outcome(
            receipt.seq,
            JournalOutcome::Denied {
                reason: "MAX_NOTIONAL_PER_ORDER".to_string(),
            },
        )
        .unwrap();
    let record = h
        .journal
        .find_request("trader-1", "SubmitOrder", "req-deny")
        .unwrap()
        .unwrap();
    assert!(matches!(
        record.outcome,
        Some(JournalOutcome::Denied { .. })
    ));
}

#[test]
fn m0_t3_idempotent_retry_returns_original_admission() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness(&dir);

    let first = h
        .bridge
        .submit_limit_order(&aapl_request("req-1", "10"))
        .unwrap();
    let second = h
        .bridge
        .submit_limit_order(&aapl_request("req-1", "10"))
        .unwrap();

    assert!(second.already_admitted);
    assert_eq!(first.client_order_id, second.client_order_id);
    assert_eq!(first.seq, second.seq);

    // No duplicate command, no duplicate cache order, single admission record
    assert_eq!(submit_commands(&h).len(), 1);
    assert!(h.cache.borrow().order(&first.client_order_id).is_some());
    let admissions = h
        .journal
        .scan_all()
        .unwrap()
        .into_iter()
        .filter(|(_, entry)| matches!(entry, JournalEntry::Admission(_)))
        .count();
    assert_eq!(admissions, 1);
}

#[test]
fn m0_t4_conflicting_fingerprint_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness(&dir);

    h.bridge
        .submit_limit_order(&aapl_request("req-1", "10"))
        .unwrap();

    // Same key, different quantity -> conflict, original request untouched
    let err = h
        .bridge
        .submit_limit_order(&aapl_request("req-1", "20"))
        .unwrap_err();
    assert!(matches!(
        err,
        orderhub_gateway::bridge::BridgeError::IdempotencyConflict { .. }
    ));
    assert_eq!(submit_commands(&h).len(), 1);

    let admissions = h
        .journal
        .scan_all()
        .unwrap()
        .into_iter()
        .filter(|(_, entry)| matches!(entry, JournalEntry::Admission(_)))
        .count();
    assert_eq!(admissions, 1, "conflict must not create a second admission");
}

#[test]
fn m0_t5_journal_recovers_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("journal.redb");

    let record = |seq: u64| AdmissionRecord {
        journal_epoch: 1,
        seq,
        submitter_id: "trader-1".to_string(),
        request_id: format!("req-{seq}"),
        request_kind: "SubmitOrder".to_string(),
        request_fingerprint: format!("fp-{seq}"),
        strategy_id: "ORDERHUB-S001".to_string(),
        instrument_id: "AAPL.XNAS".to_string(),
        order_side: "BUY".to_string(),
        quantity: "10".to_string(),
        price: "150.00".to_string(),
        time_in_force: "GTC".to_string(),
        post_only: false,
        reduce_only: false,
        submit_before_ns: None,
        ts_init_ns: 1,
    };

    let journal = BusinessJournal::open(&path).unwrap();
    let a1 = journal.append_admission(record(1)).unwrap();
    let a2 = journal.append_admission(record(2)).unwrap();
    let a3 = journal.append_admission(record(3)).unwrap();
    assert_eq!((a1.seq, a2.seq, a3.seq), (1, 2, 3));
    journal
        .append_outcome(
            a2.seq,
            JournalOutcome::DispatchStarted {
                client_order_id: "O-1-000000000002".to_string(),
            },
        )
        .unwrap();
    drop(journal);

    // Reopen: sequences continue, all entries recover in order
    let journal = BusinessJournal::open(&path).unwrap();
    assert_eq!(journal.epoch(), 1);
    assert_eq!(journal.last_seq().unwrap(), 4);
    let entries = journal.scan_all().unwrap();
    assert_eq!(entries.len(), 4);
    assert!(matches!(entries[0].1, JournalEntry::Admission(_)));
    assert!(matches!(
        entries[3].1,
        JournalEntry::Outcome { origin_seq: 2, .. }
    ));

    // Idempotency index survived, and the latest outcome is retrievable
    let rec = journal
        .find_request("trader-1", "SubmitOrder", "req-2")
        .unwrap()
        .expect("indexed request");
    assert_eq!(rec.admission.seq, 2);
    assert!(matches!(
        rec.outcome,
        Some(JournalOutcome::DispatchStarted { .. })
    ));

    // A fresh admission continues the sequence without reuse
    let a4 = journal.append_admission(record(0)).unwrap();
    assert_eq!(a4.seq, 5);
}

#[test]
fn m0_t6_journal_durable_write_cost() {
    const WRITES: u64 = 2_000;

    let dir = tempfile::tempdir().unwrap();
    let journal = BusinessJournal::open(&dir.path().join("journal.redb")).unwrap();

    let record = |seq: u64| AdmissionRecord {
        journal_epoch: 1,
        seq,
        submitter_id: "bench".to_string(),
        request_id: format!("req-{seq}"),
        request_kind: "SubmitOrder".to_string(),
        request_fingerprint: "fp".to_string(),
        strategy_id: "ORDERHUB-S001".to_string(),
        instrument_id: "AAPL.XNAS".to_string(),
        order_side: "BUY".to_string(),
        quantity: "10".to_string(),
        price: "150.00".to_string(),
        time_in_force: "GTC".to_string(),
        post_only: false,
        reduce_only: false,
        submit_before_ns: None,
        ts_init_ns: 1,
    };

    let start = Instant::now();
    for i in 0..WRITES {
        journal.append_admission(record(i)).unwrap();
    }
    let elapsed = start.elapsed();

    let total_us = elapsed.as_micros() as f64;
    let avg_us = total_us / WRITES as f64;
    let ops_per_s = 1_000_000.0 / avg_us;
    println!(
        "M0 journal durable-commit cost: {WRITES} immediate-durability commits, \
         avg {avg_us:.1} us/op (~{ops_per_s:.0} commits/s), total {total_us:.0} us"
    );

    // The admission target is 2000 requests/s; even a 10x margin on the
    // synchronous per-commit cost (10 ms) would flag a pathological setup.
    // The actual budget decision belongs to the capacity plan, not this test.
    assert!(
        avg_us < 10_000.0,
        "durable commit cost {avg_us:.1} us/op exceeds the 10 ms sanity bound"
    );
    assert_eq!(journal.last_seq().unwrap(), WRITES);
}
