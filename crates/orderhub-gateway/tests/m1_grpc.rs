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

//! M1 closed-loop acceptance over a real gRPC socket.
//!
//! One sequential scenario in a single test: kernel assembly happens on the
//! core thread (message-bus and `Rc` kernel state must live there), the tonic
//! server runs on an ephemeral port, and a generated client completes
//! submit -> state -> event-stream -> idempotent-retry plus auth rejection.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use nautilus_common::cache::Cache;
use nautilus_common::clock::Clock;
use nautilus_common::clock::VirtualClock;
use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::accounts::AccountAny;
use nautilus_model::data::QuoteTick;
use nautilus_model::enums::AccountType;
use nautilus_model::events::AccountState;
use nautilus_model::identifiers::{AccountId, InstrumentId, TraderId};
use nautilus_model::instruments::{InstrumentAny, stubs::equity_aapl};
use nautilus_model::types::{AccountBalance, Currency, Money, Price, Quantity};
use nautilus_portfolio::Portfolio;
use nautilus_risk::engine::{RiskEngine, config::RiskEngineConfig};

use orderhub_gateway::bridge::OrderHubBridge;
use orderhub_gateway::grpc::CoreHandle;
use orderhub_gateway::journal::BusinessJournal;
use orderhub_gateway::worker::PersistenceWorker;
use orderhub_proto::orderhub::v1::journal_event::Kind;
use orderhub_proto::orderhub::v1::order_hub_client::OrderHubClient;
use orderhub_proto::orderhub::v1::{GetStateRequest, SubmitOrderRequest, WatchEventsRequest};

use tonic::Request as RpcRequest;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;

const TOKEN: &str = "m1-test-token";

fn core_setup(
    journal: Arc<BusinessJournal>,
    worker: PersistenceWorker,
) -> impl FnOnce() -> (
    OrderHubBridge,
    Rc<RefCell<Cache>>,
    Option<orderhub_gateway::sandbox::ExecEventRx>,
) + Send
+ 'static {
    let worker_for_recorder = worker.clone();
    move || {
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
            RiskEngineConfig::default(),
            portfolio,
            clock.clone(),
            cache.clone(),
        )));
        RiskEngine::register_msgbus_handlers(&risk_engine);
        let exec_rx = orderhub_gateway::sandbox::attach_sandbox_execution(
            TraderId::from("ORDER-HUB"),
            nautilus_model::identifiers::Venue::from("XNAS"),
            AccountId::from("XNAS-001"),
            Money::from("1000000 USD"),
            clock.clone(),
            cache.clone(),
        );
        orderhub_gateway::sandbox::attach_outcome_recorder(worker_for_recorder);
        // Keeps the engine alive for the process lifetime: bus handlers hold
        // weak references only (a real node owns its engines instead).
        #[expect(clippy::mem_forget, reason = "test keeps bus handlers alive")]
        std::mem::forget(risk_engine);

        let bridge =
            OrderHubBridge::new(TraderId::from("ORDER-HUB"), clock, cache.clone(), journal)
                .with_worker(worker);
        (bridge, cache, exec_rx)
    }
}

fn submit_request(request_id: &str) -> SubmitOrderRequest {
    SubmitOrderRequest {
        submitter_id: "trader-1".to_string(),
        request_id: request_id.to_string(),
        strategy_id: "ORDERHUB-S001".to_string(),
        instrument_id: "AAPL.XNAS".to_string(),
        side: "BUY".to_string(),
        quantity: "10".to_string(),
        price: "150.00".to_string(),
        time_in_force: "GTC".to_string(),
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
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async move {
        let dir = tempfile::tempdir().unwrap();
        let journal = Arc::new(BusinessJournal::open(&dir.path().join("journal.redb")).unwrap());
        let worker = PersistenceWorker::start(Arc::clone(&journal));
        let core = CoreHandle::spawn(core_setup(Arc::clone(&journal), worker), journal, TOKEN);

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

        // 4. Snapshot: the sandbox matching engine fills the resting buy
        // limit from the seed quote, so the terminal state has no active
        // orders; instrument metadata stays enumerable via the journal.
        let state = client
            .get_state(authorized(GetStateRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            state.active_orders.len(),
            0,
            "order reached a terminal fill"
        );
        assert_eq!(state.instruments.len(), 1);
        assert_eq!(state.instruments[0].instrument_id, "AAPL.XNAS");
        assert_eq!(state.instruments[0].price_increment, "0.01");

        // 5. WatchEvents from cursor 0 delivers admission + outcome events
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
        while admissions < 1 || outcomes < 2 {
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
}
