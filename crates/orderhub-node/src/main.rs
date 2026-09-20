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

//! OrderHub node binary (M1 scope).
//!
//! Assembles the M1 service stack: an in-memory kernel (cache, risk engine)
//! wired on the core thread, the group-commit journal, and the tonic server.
//! The execution-client wiring (sandbox adapter) is the remaining M1 step;
//! until then admitted orders stay INITIALIZED after passing risk checks.
//!
//! Configuration via environment:
//! - `ORDERHUB_ADDR`     listen address     (default `127.0.0.1:50051`)
//! - `ORDERHUB_JOURNAL`  journal file       (default `./orderhub-journal.redb`)
//! - `ORDERHUB_TOKEN`    shared auth token  (default `orderhub-dev-token`)

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use nautilus_common::cache::Cache;
use nautilus_common::clock::Clock;
use nautilus_common::clock::VirtualClock;
use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::accounts::AccountAny;
use nautilus_model::data::QuoteTick;
use nautilus_model::enums::AccountType;
use nautilus_model::events::AccountState;
use nautilus_model::identifiers::{AccountId, InstrumentId, TraderId, Venue};
use nautilus_model::instruments::{Equity, InstrumentAny};
use nautilus_model::types::{AccountBalance, Currency, Money, Price, Quantity};
use nautilus_portfolio::Portfolio;
use nautilus_risk::engine::RiskEngine;
use nautilus_risk::engine::config::RiskEngineConfig;

use orderhub_gateway::bridge::OrderHubBridge;
use orderhub_gateway::grpc::CoreHandle;
use orderhub_gateway::journal::BusinessJournal;
use orderhub_gateway::worker::PersistenceWorker;

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr: std::net::SocketAddr = std::env::var("ORDERHUB_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:50051".to_string())
        .parse()?;
    let journal_path = std::path::PathBuf::from(
        std::env::var("ORDERHUB_JOURNAL").unwrap_or_else(|_| "orderhub-journal.redb".to_string()),
    );
    // M2: per-submitter credential registry (JSON file of credential list).
    let registry = match std::env::var("ORDERHUB_SUBMITTERS") {
        Ok(path) => {
            let text = std::fs::read_to_string(&path)?;
            Arc::new(serde_json::from_str::<
                orderhub_gateway::auth::SubmitterRegistry,
            >(&text)?)
        }
        Err(_) => Arc::new(orderhub_gateway::auth::SubmitterRegistry::new(vec![
            orderhub_gateway::auth::SubmitterCredentials {
                token: std::env::var("ORDERHUB_TOKEN")
                    .unwrap_or_else(|_| "orderhub-dev-token".into()),
                submitter_id: "trader-1".to_string(),
                strategies: vec!["ORDERHUB-S001".to_string()],
            },
        ])),
    };

    // Readiness lifecycle: BOOTING -> RESTORING -> READY (HALTED on failure).
    let gate = orderhub_gateway::readiness::ReadinessGate::new();
    let journal = match BusinessJournal::open(&journal_path) {
        Ok(journal) => {
            gate.set(orderhub_gateway::readiness::Readiness::Restoring, "");
            journal
        }
        Err(err) => {
            gate.set(
                orderhub_gateway::readiness::Readiness::Halted,
                &format!("journal open failed: {err}"),
            );
            return Err(Box::new(err));
        }
    };
    let journal = Arc::new(journal);
    let worker = PersistenceWorker::start(Arc::clone(&journal));
    let recorder_worker = worker.clone();
    let gate_for_setup = gate.clone();
    let journal_for_setup = Arc::clone(&journal);
    let last_seq = journal.last_seq()?;
    let recovered = journal.scan_all()?.len();
    println!(
        "orderhub-node: journal={} epoch={} last_seq={} recovered_entries={recovered}",
        journal_path.display(),
        journal.epoch(),
        last_seq,
    );
    gate.set(orderhub_gateway::readiness::Readiness::Ready, "");

    let core = CoreHandle::spawn(
        move || core_setup(journal_for_setup, worker, recorder_worker, gate_for_setup),
        journal,
        registry,
        gate,
    );

    let runtime = tokio::runtime::Runtime::new()?;
    println!("orderhub-node: serving plaintext gRPC on {addr}");
    runtime.block_on(async move { orderhub_gateway::grpc::serve(core, addr).await })?;
    Ok(())
}

/// Builds the M1 kernel on the core thread (see `grpc::CoreHandle::spawn`).
fn core_setup(
    journal: Arc<BusinessJournal>,
    worker: PersistenceWorker,
    recorder_worker: PersistenceWorker,
    gate: std::sync::Arc<orderhub_gateway::readiness::ReadinessGate>,
) -> (
    OrderHubBridge,
    Rc<RefCell<Cache>>,
    Option<orderhub_gateway::sandbox::ExecEventRx>,
) {
    let cache = Rc::new(RefCell::new(Cache::default()));
    {
        let mut cache_ref = cache.borrow_mut();
        let aapl = Equity::builder()
            .instrument_id(InstrumentId::from("AAPL.XNAS"))
            .raw_symbol(nautilus_model::identifiers::Symbol::from("AAPL"))
            .isin(ustr::Ustr::from("US0378331005"))
            .currency(Currency::USD())
            .price_precision(2)
            .price_increment(Price::from("0.01"))
            .ts_event(UnixNanos::default())
            .ts_init(UnixNanos::default())
            .build()
            .expect("AAPL instrument definition");
        cache_ref
            .add_instrument(InstrumentAny::Equity(aapl))
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
    let portfolio = Portfolio::new(Rc::clone(&clock), Rc::clone(&cache), None);
    let risk_engine = Rc::new(RefCell::new(RiskEngine::new(
        RiskEngineConfig::default(),
        portfolio,
        Rc::clone(&clock),
        Rc::clone(&cache),
    )));
    RiskEngine::register_msgbus_handlers(&risk_engine);
    let exec_rx = orderhub_gateway::sandbox::attach_sandbox_execution(
        TraderId::from("ORDER-HUB"),
        Venue::from("XNAS"),
        AccountId::from("XNAS-001"),
        Money::from("1000000 USD"),
        Rc::clone(&clock),
        Rc::clone(&cache),
    );
    orderhub_gateway::sandbox::attach_outcome_recorder(recorder_worker);

    let quota = Rc::new(RefCell::new(orderhub_gateway::quota::QuotaLedger::new()));
    quota
        .borrow_mut()
        .set_budget("ORDERHUB-S001", rust_decimal::Decimal::from(1_000_000));
    // The bus handlers hold weak references; the node keeps the engine for
    // the process lifetime (a full LiveNode integration owns engines in its
    // trader state instead).
    #[expect(clippy::mem_forget, reason = "node keeps bus handlers alive")]
    std::mem::forget(risk_engine);

    let bridge = OrderHubBridge::new(
        TraderId::from("ORDER-HUB"),
        clock,
        Rc::clone(&cache),
        journal,
    )
    .with_worker(worker)
    .with_quota(quota)
    .with_gate(gate);
    (bridge, cache, exec_rx)
}
