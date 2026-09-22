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

//! M1 closed-loop acceptance over a real gRPC socket against the local FIX
//! exchange process.
//!
//! One sequential scenario in a single test: the exchange is spawned first
//! (its FIX acceptor must be listening before the core thread's client
//! connects), kernel assembly happens on the core thread, the tonic server
//! runs on an ephemeral port, and a generated client completes
//! submit -> cancel -> state -> event-stream -> idempotent-retry plus auth
//! rejection.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use nautilus_common::cache::Cache;
use nautilus_common::clock::Clock;
use nautilus_common::clock::VirtualClock;
use nautilus_common::live::runner::replace_exec_event_sender;
use nautilus_common::messages::ExecutionEvent;
use nautilus_common::runner::{SyncTradingCommandSender, replace_exec_cmd_sender};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::accounts::AccountAny;
use nautilus_model::enums::AccountType;
use nautilus_model::events::AccountState;
use nautilus_model::identifiers::{AccountId, InstrumentId, TraderId};
use nautilus_model::instruments::{Equity, InstrumentAny};
use nautilus_model::types::{AccountBalance, Currency, Money, Price};
use nautilus_portfolio::Portfolio;
use nautilus_risk::engine::{RiskEngine, config::RiskEngineConfig};
use ustr::Ustr;

use orderhub_gateway::bridge::OrderHubBridge;
use orderhub_gateway::grpc::CoreHandle;
use orderhub_gateway::journal::BusinessJournal;
use orderhub_gateway::worker::PersistenceWorker;
use orderhub_proto::orderhub::v1::journal_event::Kind;
use orderhub_proto::orderhub::v1::order_hub_client::OrderHubClient;
use orderhub_proto::orderhub::v1::{
    CancelOrderRequest, GetStateRequest, SubmitOrderRequest, WatchEventsRequest,
};

use tonic::Request as RpcRequest;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;

const TOKEN: &str = "m1-test-token";
const EXCHANGE_DIR: &str = r"D:\projects\zcodeworkspace\rust-trader";
const EXCHANGE_EXE: &str = r"D:\projects\zcodeworkspace\rust-trader\target\release\exchange.exe";
const EXCHANGE_ADDR: &str = "127.0.0.1:5001";

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

fn core_setup(
    journal: Arc<BusinessJournal>,
    worker: PersistenceWorker,
) -> impl FnOnce() -> orderhub_gateway::grpc::CoreState + Send + 'static {
    let worker_for_recorder = worker.clone();
    move || {
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
        let portfolio = Portfolio::new(clock.clone(), cache.clone(), None);
        let risk_engine = Rc::new(RefCell::new(RiskEngine::new(
            RiskEngineConfig::default(),
            portfolio,
            clock.clone(),
            cache.clone(),
        )));
        RiskEngine::register_msgbus_handlers(&risk_engine);
        replace_exec_cmd_sender(Arc::new(SyncTradingCommandSender));
        let (exec_tx, exec_rx) = tokio::sync::mpsc::unbounded_channel::<ExecutionEvent>();
        replace_exec_event_sender(exec_tx);
        let pump = orderhub_gateway::local_client::attach_local_execution(
            EXCHANGE_ADDR,
            TraderId::from("ORDER-HUB"),
            nautilus_model::identifiers::Venue::from("GOX"),
            &AccountId::from("GOX-001"),
            clock.clone(),
            &cache,
        );
        orderhub_gateway::exec_wiring::attach_outcome_recorder(worker_for_recorder);
        // Keeps the engine alive for the process lifetime: bus handlers hold
        // weak references only (a real node owns its engines instead).
        #[expect(clippy::mem_forget, reason = "test keeps bus handlers alive")]
        std::mem::forget(risk_engine);

        let bridge =
            OrderHubBridge::new(TraderId::from("ORDER-HUB"), clock, cache.clone(), journal)
                .with_worker(worker);
        (bridge, cache, Some(exec_rx), Some(pump))
    }
}

fn submit_request(request_id: &str) -> SubmitOrderRequest {
    SubmitOrderRequest {
        submitter_id: "trader-1".to_string(),
        request_id: request_id.to_string(),
        strategy_id: "ORDERHUB-S001".to_string(),
        instrument_id: "AAPL.GOX".to_string(),
        side: "BUY".to_string(),
        quantity: "10".to_string(),
        price: "90.00".to_string(),       // resting: the book starts empty
        time_in_force: "DAY".to_string(), // the exchange rejects GTC
        post_only: false,
        reduce_only: false,
        submit_before_ns: None,
    }
}

fn authorized<T>(message: T) -> RpcRequest<T> {
    let mut request = RpcRequest::new(message);
    let token: MetadataValue<_> = format!("Bearer {TOKEN}").parse().unwrap();
    request.metadata_mut().insert("authorization", token);
    request
}

#[test]
fn m1_grpc_closed_loop() {
    let mut exchange = spawn_exchange();
    wait_for_port(5001);

    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async move {
        let dir = tempfile::tempdir().unwrap();
        let journal = Arc::new(BusinessJournal::open(&dir.path().join("journal.redb")).unwrap());
        let worker = PersistenceWorker::start(Arc::clone(&journal));
        let registry = Arc::new(orderhub_gateway::auth::SubmitterRegistry::new(vec![
            orderhub_gateway::auth::SubmitterCredentials {
                token: TOKEN.to_string(),
                submitter_id: "trader-1".to_string(),
                strategies: vec!["ORDERHUB-S001".to_string()],
            },
        ]));
        let gate = orderhub_gateway::readiness::ReadinessGate::new();
        gate.set(orderhub_gateway::readiness::Readiness::Ready, "");
        let core = CoreHandle::spawn(
            core_setup(Arc::clone(&journal), worker),
            journal,
            registry,
            gate,
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let server = tokio::spawn(async move {
            orderhub_gateway::grpc::serve(core, addr).await.unwrap();
        });

        let channel = Channel::builder(format!("http://{addr}").parse().unwrap())
            .connect()
            .await
            .unwrap();
        let mut client = OrderHubClient::new(channel);
        tokio::time::sleep(Duration::from_millis(50)).await;

        // 1. Auth rejection without token
        let unauthenticated = client
            .submit_order(RpcRequest::new(submit_request("req-noauth")))
            .await;
        assert!(unauthenticated.is_err(), "missing token must be rejected");

        // 2. Valid submission admitted
        let admitted = client
            .submit_order(authorized(submit_request("req-1")))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(admitted.result, "ADMITTED");
        assert!(!admitted.already_admitted);
        assert_eq!(admitted.seq, "1");
        assert_eq!(admitted.client_order_id, "O-1-000000000001");

        // 3. Idempotent retry returns the original admission
        let retried = client
            .submit_order(authorized(submit_request("req-1")))
            .await
            .unwrap()
            .into_inner();
        assert!(retried.already_admitted);
        assert_eq!(retried.client_order_id, admitted.client_order_id);

        // 4. Wait for the venue accept, then cancel over gRPC
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let state = client
                .get_state(authorized(GetStateRequest {}))
                .await
                .unwrap()
                .into_inner();
            if state.active_orders.len() == 1 && state.active_orders[0].status == "ACCEPTED" {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "order never accepted by the exchange"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let cancel = client
            .cancel_order(authorized(CancelOrderRequest {
                request_id: "cancel-1".to_string(),
                client_order_id: admitted.client_order_id.clone(),
                ..CancelOrderRequest::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(cancel.result, "CANCEL_INTENT_ACCEPTED");
        assert!(!cancel.already_admitted);

        // 5. Snapshot: the cancel reaches the venue; the terminal state has
        // no active orders, and instrument metadata stays enumerable.
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let state = client
                .get_state(authorized(GetStateRequest {}))
                .await
                .unwrap()
                .into_inner();
            if state.active_orders.is_empty() {
                assert_eq!(state.instruments.len(), 1);
                assert_eq!(state.instruments[0].instrument_id, "AAPL.GOX");
                assert_eq!(state.instruments[0].price_increment, "0.01");
                break;
            }
            assert!(Instant::now() < deadline, "order never canceled");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // 6. WatchEvents from cursor 0 delivers admissions and outcomes
        // (ADMITTED for the submit, CANCELED for the cancel).
        let stream = client
            .watch_events(authorized(WatchEventsRequest {
                after: "0".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();
        tokio::pin!(stream);
        let mut admissions = 0;
        let mut outcomes = 0;
        while admissions < 2 || outcomes < 2 {
            let event = tokio::time::timeout(Duration::from_secs(5), stream.message())
                .await
                .expect("event stream timed out")
                .unwrap()
                .expect("stream ended");
            match event.kind {
                Some(Kind::Admission(_)) => admissions += 1,
                Some(Kind::Outcome(outcome)) => {
                    assert!(!outcome.outcome.is_empty());
                    outcomes += 1;
                }
                Some(Kind::Progress(_)) | None => {}
            }
        }

        server.abort();
    });
    let _ = exchange.kill();
    let _ = exchange.wait();
}
