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

//! M1 acceptance: sandbox order lifecycle, cancel dispatch, mixed durable
//! throughput. Kernel assembly happens on the test thread (the process-global
//! message bus makes these tests serial: `--test-threads=1`).

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use nautilus_common::cache::Cache;
use nautilus_common::clock::Clock;
use nautilus_common::clock::VirtualClock;
use nautilus_common::msgbus;
use nautilus_common::runner::{
    SyncTradingCommandSender, drain_trading_cmd_queue, replace_exec_cmd_sender,
    trading_cmd_queue_is_empty,
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::accounts::AccountAny;
use nautilus_model::data::QuoteTick;
use nautilus_model::enums::{AccountType, OrderSide, TimeInForce};
use nautilus_model::events::AccountState;
use nautilus_model::identifiers::{AccountId, InstrumentId, TraderId, Venue};
use nautilus_model::instruments::{InstrumentAny, stubs::equity_aapl};
use nautilus_model::orders::Order;
use nautilus_model::types::{AccountBalance, Currency, Money, Price, Quantity};
use nautilus_portfolio::Portfolio;
use nautilus_risk::engine::RiskEngine;
use nautilus_risk::engine::config::RiskEngineConfig;

use orderhub_gateway::bridge::{LimitOrderRequest, OrderHubBridge};
use orderhub_gateway::journal::{BusinessJournal, JournalOutcome};
use orderhub_gateway::worker::PersistenceWorker;

struct SandboxHarness {
    bridge: OrderHubBridge,
    cache: Rc<RefCell<Cache>>,
    journal: Arc<BusinessJournal>,
    exec_rx: Option<orderhub_gateway::sandbox::ExecEventRx>,
}

fn harness(dir: &tempfile::TempDir) -> SandboxHarness {
    let cache = Rc::new(RefCell::new(Cache::default()));
    {
        let mut cache_ref = cache.borrow_mut();
        cache_ref
            .add_instrument(InstrumentAny::Equity(equity_aapl()))
            .unwrap();
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
        cache_ref.add_quote(seed_quote("150.00", "150.02")).unwrap();
    }

    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let portfolio = Portfolio::new(clock.clone(), cache.clone(), None);
    let risk_engine = Rc::new(RefCell::new(RiskEngine::new(
        RiskEngineConfig::default(),
        portfolio,
        clock.clone(),
        cache.clone(),
    )));
    RiskEngine::register_msgbus_handlers(&risk_engine);
    // Deferred command dispatch (re-entrancy safe), matching a live node.
    replace_exec_cmd_sender(std::sync::Arc::new(SyncTradingCommandSender));
    #[expect(clippy::mem_forget, reason = "keeps bus handlers alive")]
    std::mem::forget(risk_engine);

    let journal = Arc::new(BusinessJournal::open(&dir.path().join("journal.redb")).unwrap());
    let worker = PersistenceWorker::start(Arc::clone(&journal));
    let exec_rx = orderhub_gateway::sandbox::attach_sandbox_execution(
        TraderId::from("ORDER-HUB"),
        Venue::from("XNAS"),
        AccountId::from("XNAS-001"),
        Money::from("1000000 USD"),
        clock.clone(),
        cache.clone(),
    );
    orderhub_gateway::sandbox::attach_outcome_recorder(worker.clone());

    let quota = Rc::new(RefCell::new(orderhub_gateway::quota::QuotaLedger::new()));
    quota
        .borrow_mut()
        .set_budget("ORDERHUB-S001", rust_decimal::Decimal::from(1_000_000));

    let bridge = OrderHubBridge::new(
        TraderId::from("ORDER-HUB"),
        clock,
        cache.clone(),
        journal.clone(),
    )
    .with_worker(worker)
    .with_quota(quota);

    SandboxHarness {
        bridge,
        cache,
        journal,
        exec_rx,
    }
}

fn seed_quote(bid: &str, ask: &str) -> QuoteTick {
    QuoteTick::new(
        InstrumentId::from("AAPL.XNAS"),
        Price::from(bid),
        Price::from(ask),
        Quantity::from("100"),
        Quantity::from("100"),
        UnixNanos::from(1),
        UnixNanos::from(1),
    )
}

fn publish_quote(bid: &str, ask: &str) {
    msgbus::publish_quote("data.quotes.XNAS.AAPL".into(), &seed_quote(bid, ask));
}

fn aapl_request(request_id: &str) -> LimitOrderRequest {
    LimitOrderRequest {
        submitter_id: "trader-1".to_string(),
        request_id: request_id.to_string(),
        strategy_id: nautilus_model::identifiers::StrategyId::from("ORDERHUB-S001"),
        instrument_id: InstrumentId::from("AAPL.XNAS"),
        side: OrderSide::Buy,
        quantity: Quantity::from("10"),
        price: Price::from("150.00"),
        time_in_force: TimeInForce::Gtc,
        post_only: false,
        reduce_only: false,
        client_id: None,
        submit_before_ns: None,
    }
}

/// Drains queued trading commands and deferred execution events to completion.
fn drain_commands(h: &mut SandboxHarness) {
    for _ in 0..500 {
        drain_trading_cmd_queue();
        orderhub_gateway::sandbox::pump_exec_events(&mut h.exec_rx);
        drain_trading_cmd_queue();
        if trading_cmd_queue_is_empty() {
            return;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    panic!("command queue never drained");
}

fn journal_outcome(h: &SandboxHarness, request_id: &str) -> Option<JournalOutcome> {
    h.journal
        .find_request("trader-1", "SubmitOrder", request_id)
        .unwrap()
        .and_then(|record| record.outcome)
}

#[test]
fn m1_a1_sandbox_lifecycle_submitted_accepted_filled() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = harness(&dir);

    let receipt = h
        .bridge
        .submit_limit_order(&aapl_request("req-life"))
        .unwrap();
    let coid = receipt.client_order_id;
    drain_commands(&mut h);

    // A crossing ask drives the sandbox matching engine to fill the buy limit.
    publish_quote("149.99", "150.00");
    drain_commands(&mut h);

    // Terminal state reached in the cache
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let filled = {
            let cache = h.cache.borrow();
            cache
                .order(&coid)
                .is_some_and(|order| order.status() == nautilus_model::enums::OrderStatus::Filled)
        };
        if filled {
            break;
        }
        assert!(Instant::now() < deadline, "order never filled");
        std::thread::sleep(Duration::from_millis(5));
    }

    // The outcome recorder durably journaled the fill
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(JournalOutcome::Filled { .. }) = journal_outcome(&h, "req-life") {
            break;
        }
        assert!(Instant::now() < deadline, "fill outcome never journaled");
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn m1_a2_cancel_dispatch_reaches_terminal_canceled() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = harness(&dir);

    let receipt = h
        .bridge
        .submit_limit_order(&aapl_request("req-cancel"))
        .unwrap();
    let coid = receipt.client_order_id;
    drain_commands(&mut h);

    let coid_str = coid.to_string();
    let cancel_receipt = h
        .bridge
        .cancel_order("trader-1", "cancel-1", coid_str.as_str())
        .unwrap();
    assert!(!cancel_receipt.already_admitted);
    drain_commands(&mut h);

    // Retry is idempotent
    let retry = h
        .bridge
        .cancel_order("trader-1", "cancel-1", coid_str.as_str())
        .unwrap();
    assert!(retry.already_admitted);

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let canceled = {
            let cache = h.cache.borrow();
            cache
                .order(&coid)
                .is_some_and(|order| order.status() == nautilus_model::enums::OrderStatus::Canceled)
        };
        if canceled {
            break;
        }
        assert!(Instant::now() < deadline, "order never canceled");
        std::thread::sleep(Duration::from_millis(5));
    }

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(JournalOutcome::Canceled { .. }) = journal_outcome(&h, "req-cancel") {
            break;
        }
        assert!(Instant::now() < deadline, "cancel outcome never journaled");
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn m1_c1_mixed_submit_and_cancel_durable_throughput() {
    const SUBMITTERS: usize = 16;
    const PER_SUBMITTER: usize = 62;

    let dir = tempfile::tempdir().unwrap();
    let journal = Arc::new(BusinessJournal::open(&dir.path().join("journal.redb")).unwrap());
    let worker = PersistenceWorker::start(Arc::clone(&journal));

    // 1000 admissions (submits) + 1000 outcomes (cancel intents on prior
    // admissions) concurrently: the 1000 + 1000 mixed durable-write target.
    let start = Instant::now();
    std::thread::scope(|scope| {
        for t in 0..SUBMITTERS {
            let worker = worker.clone();
            scope.spawn(move || {
                for i in 0..PER_SUBMITTER {
                    let n = u64::try_from(t * PER_SUBMITTER + i).unwrap();
                    let record = crate::bench_admission(n);
                    let admitted = worker.submit_admission(record).unwrap();
                    worker
                        .submit_outcome(
                            admitted.seq,
                            JournalOutcome::Canceled {
                                client_order_id: format!("O-1-{:012}", admitted.seq),
                            },
                        )
                        .unwrap();
                }
            });
        }
    });
    let elapsed = start.elapsed();

    let total = SUBMITTERS * PER_SUBMITTER * 2; // admissions + outcomes
    let ops_per_s = total as f64 / elapsed.as_secs_f64();
    println!(
        "M1 mixed durable throughput: {total} writes in {:.3}s (~{ops_per_s:.0} writes/s)",
        elapsed.as_secs_f64()
    );
    assert!(
        ops_per_s >= 2_000.0,
        "mixed durable throughput {ops_per_s:.0}/s below 2000/s"
    );
    assert_eq!(
        journal.scan_all().unwrap().len(),
        total,
        "all durable writes committed"
    );
}

pub(crate) fn bench_admission(seq: u64) -> orderhub_gateway::journal::AdmissionRecord {
    orderhub_gateway::journal::AdmissionRecord {
        journal_epoch: 1,
        seq,
        submitter_id: "bench".to_string(),
        request_id: format!("req-mixed-{seq}"),
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
    }
}
