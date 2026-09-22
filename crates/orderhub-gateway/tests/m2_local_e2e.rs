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

//! Full-chain test against the real local simulated exchange process:
//! bridge -> risk -> execution engine -> FIX client -> exchange matching ->
//! execution reports -> deferred events -> cache/journal.
//!
//! Requires `D:\projects\zcodeworkspace\rust-trader\target\release\exchange.exe`
//! and its acceptor config declaring the ORDERHUB session (port 5001).

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use nautilus_common::cache::Cache;
use nautilus_common::clock::Clock;
use nautilus_common::clock::VirtualClock;
use nautilus_common::live::runner::replace_exec_event_sender;
use nautilus_common::messages::ExecutionEvent;
use nautilus_common::runner::{
    SyncTradingCommandSender, drain_trading_cmd_queue, replace_exec_cmd_sender,
    trading_cmd_queue_is_empty,
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::accounts::AccountAny;
use nautilus_model::enums::{AccountType, OrderSide, OrderStatus, TimeInForce};
use nautilus_model::events::AccountState;
use nautilus_model::identifiers::{AccountId, InstrumentId, StrategyId, TraderId, Venue};
use nautilus_model::instruments::{Equity, InstrumentAny};
use nautilus_model::orders::Order;
use nautilus_model::types::{AccountBalance, Currency, Money, Price, Quantity};
use nautilus_portfolio::Portfolio;
use nautilus_risk::engine::RiskEngine;
use nautilus_risk::engine::config::RiskEngineConfig;
use ustr::Ustr;

use orderhub_gateway::bridge::{LimitOrderRequest, OrderHubBridge};
use orderhub_gateway::journal::{BusinessJournal, JournalOutcome};
use orderhub_gateway::worker::PersistenceWorker;

const EXCHANGE_DIR: &str = r"D:\projects\zcodeworkspace\rust-trader";
const EXCHANGE_EXE: &str = r"D:\projects\zcodeworkspace\rust-trader\target\release\exchange.exe";

struct E2e {
    bridge: OrderHubBridge,
    cache: Rc<RefCell<Cache>>,
    journal: Arc<BusinessJournal>,
    pump: orderhub_gateway::local_client::LocalPump,
    exec_rx: tokio::sync::mpsc::UnboundedReceiver<ExecutionEvent>,
}

fn spawn_exchange() -> std::process::Child {
    std::process::Command::new(EXCHANGE_EXE)
        .arg("--server")
        .current_dir(EXCHANGE_DIR)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn exchange")
}

fn wait_for_port(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return;
        }
        assert!(Instant::now() < deadline, "exchange did not start");
        std::thread::sleep(Duration::from_millis(200));
    }
}

fn harness(dir: &tempfile::TempDir) -> E2e {
    let cache = Rc::new(RefCell::new(Cache::default()));
    {
        let mut cache_ref = cache.borrow_mut();
        let aapl = Equity::builder()
            .instrument_id(InstrumentId::from("AAPL.GOX"))
            .raw_symbol(nautilus_model::identifiers::Symbol::from("AAPL"))
            .isin(Ustr::from("LOCAL-AAPL"))
            .currency(Currency::USD())
            .price_precision(2)
            .price_increment(Price::from("0.01"))
            .ts_event(UnixNanos::default())
            .ts_init(UnixNanos::default())
            .build()
            .expect("AAPL.GOX instrument");
        cache_ref
            .add_instrument(InstrumentAny::Equity(aapl))
            .unwrap();
        let state = AccountState::new(
            AccountId::from("GOX-001"),
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
    }

    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let portfolio = Portfolio::new(Rc::clone(&clock), Rc::clone(&cache), None);
    let risk_engine = Rc::new(RefCell::new(RiskEngine::new(
        RiskEngineConfig::default(),
        portfolio,
        Rc::clone(&clock),
        Rc::clone(&cache),
    )));
    RiskEngine::register_msgbus_handlers(&risk_engine);
    #[expect(clippy::mem_forget, reason = "keeps bus handlers alive")]
    std::mem::forget(risk_engine);
    replace_exec_cmd_sender(std::sync::Arc::new(SyncTradingCommandSender));

    let (exec_tx, exec_rx) = tokio::sync::mpsc::unbounded_channel::<ExecutionEvent>();
    replace_exec_event_sender(exec_tx);

    let pump = orderhub_gateway::local_client::attach_local_execution(
        "127.0.0.1:5001",
        TraderId::from("ORDER-HUB"),
        Venue::from("GOX"),
        &AccountId::from("GOX-001"),
        Rc::clone(&clock),
        &cache,
    );

    let journal = Arc::new(BusinessJournal::open(&dir.path().join("journal.redb")).unwrap());
    let worker = PersistenceWorker::start(Arc::clone(&journal));
    orderhub_gateway::exec_wiring::attach_outcome_recorder(worker.clone());

    let quota = Rc::new(RefCell::new(orderhub_gateway::quota::QuotaLedger::new()));
    quota
        .borrow_mut()
        .set_budget("ORDERHUB-S001", rust_decimal::Decimal::from(1_000_000));
    // Pre-existing holdings so the sell side can provide liquidity (the
    // sellable-quantity ledger refuses sells without holdings, by design).
    quota.borrow_mut().add_position(
        "ORDERHUB-S001",
        "AAPL.GOX",
        rust_decimal::Decimal::from(100),
    );

    let bridge = OrderHubBridge::new(
        TraderId::from("ORDER-HUB"),
        clock,
        Rc::clone(&cache),
        Arc::clone(&journal),
    )
    .with_worker(worker)
    .with_quota(quota);

    E2e {
        bridge,
        cache,
        journal,
        pump,
        exec_rx,
    }
}

fn drain(h: &mut E2e) {
    for _ in 0..500 {
        drain_trading_cmd_queue();
        h.pump.pump();
        while let Ok(event) = h.exec_rx.try_recv() {
            if let ExecutionEvent::Order(order_event) = event {
                nautilus_common::msgbus::send_order_event(
                    nautilus_common::msgbus::MessagingSwitchboard::exec_engine_process(),
                    order_event,
                );
            }
        }
        drain_trading_cmd_queue();
        if trading_cmd_queue_is_empty() {
            return;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    panic!("queues never drained");
}

fn order_request(request_id: &str, side: OrderSide, price: &str) -> LimitOrderRequest {
    LimitOrderRequest {
        submitter_id: "trader-1".to_string(),
        request_id: request_id.to_string(),
        strategy_id: StrategyId::from("ORDERHUB-S001"),
        instrument_id: InstrumentId::from("AAPL.GOX"),
        side,
        quantity: Quantity::from("10"),
        price: Price::from(price),
        time_in_force: TimeInForce::Day,
        post_only: false,
        reduce_only: false,
        client_id: None,
        submit_before_ns: None,
    }
}

fn wait_status(
    h: &mut E2e,
    coid: &nautilus_model::identifiers::ClientOrderId,
    status: OrderStatus,
) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        // Keep pumping FIX reports and deferred events while waiting: the
        // venue replies asynchronously, so a one-shot drain is not enough.
        h.pump.pump();
        while let Ok(event) = h.exec_rx.try_recv() {
            if let ExecutionEvent::Order(order_event) = event {
                nautilus_common::msgbus::send_order_event(
                    nautilus_common::msgbus::MessagingSwitchboard::exec_engine_process(),
                    order_event,
                );
            }
        }
        drain_trading_cmd_queue();
        let current = {
            let cache = h.cache.borrow();
            cache.order(coid).map(|order| order.status())
        };
        if current == Some(status) {
            return;
        }
        assert!(Instant::now() < deadline, "order never reached {status:?}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn m2_local_e2e_full_chain_fill() {
    let mut exchange = spawn_exchange();
    wait_for_port(5001);

    let dir = tempfile::tempdir().unwrap();
    let mut h = harness(&dir);

    // 1. Resting sell provides liquidity.
    let sell = h
        .bridge
        .submit_limit_order(&order_request("req-sell", OrderSide::Sell, "100.10"))
        .expect("sell admitted");
    drain(&mut h);
    wait_status(&mut h, &sell.client_order_id, OrderStatus::Accepted);

    // 2. Crossing buy fills both sides through the real matching engine.
    let buy = h
        .bridge
        .submit_limit_order(&order_request("req-buy", OrderSide::Buy, "100.10"))
        .expect("buy admitted");
    drain(&mut h);
    wait_status(&mut h, &buy.client_order_id, OrderStatus::Filled);
    wait_status(&mut h, &sell.client_order_id, OrderStatus::Filled);

    // 3. The outcome recorder durably journaled both fills.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let sell_outcome = h
            .journal
            .find_request("trader-1", "SubmitOrder", "req-sell")
            .unwrap()
            .and_then(|r| r.outcome);
        let buy_outcome = h
            .journal
            .find_request("trader-1", "SubmitOrder", "req-buy")
            .unwrap()
            .and_then(|r| r.outcome);
        if matches!(sell_outcome, Some(JournalOutcome::Filled { .. }))
            && matches!(buy_outcome, Some(JournalOutcome::Filled { .. }))
        {
            break;
        }
        assert!(Instant::now() < deadline, "fill outcomes never journaled");
        drain(&mut h);
        std::thread::sleep(Duration::from_millis(5));
    }

    let _ = exchange.kill();
    let _ = exchange.wait();
}
