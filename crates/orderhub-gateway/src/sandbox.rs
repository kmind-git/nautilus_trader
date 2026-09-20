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

//! Sandbox execution wiring for the M1 closed loop.
//!
//! Registers an [`ExecutionEngine`] on the message bus and attaches a
//! [`SandboxExecutionClient`] for the given venue, so admitted orders traverse
//! the full native chain: risk -> execution engine -> sandbox matching ->
//! Submitted / Accepted / Filled / Canceled events.
//!
//! Must be called on the core thread (same thread as the risk engine and
//! bridge), because the message-bus state and the engines' `Rc` wiring are
//! single-threaded. The engine is intentionally leaked: its bus handlers hold
//! weak references only, and a full node would own the engine instead.

use std::cell::RefCell;
use std::rc::Rc;

use nautilus_common::cache::Cache;
use nautilus_common::clients::ExecutionClient;
use nautilus_common::clock::Clock;
use nautilus_common::messages::execution::CancelOrder;
use nautilus_common::msgbus::MessagingSwitchboard;
use nautilus_common::msgbus::{self, TypedHandler};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_execution::engine::ExecutionEngine;
use nautilus_model::enums::AccountType;
use nautilus_model::enums::OmsType;
use nautilus_model::events::OrderEventAny;
use nautilus_model::identifiers::{AccountId, ClientId, ClientOrderId, TraderId, Venue};
use nautilus_model::types::Money;
use nautilus_sandbox::config::SandboxExecutionClientConfig;
use nautilus_sandbox::execution::SandboxExecutionClient;

use crate::worker::PersistenceWorker;

/// Sandbox client identifier.
pub const SANDBOX_CLIENT_ID: &str = "SANDBOX";

/// Registers the execution engine with a sandbox execution client.
///
/// `starting_balance` seeds the sandbox venue account.
///
/// # Panics
///
/// Panics if engine or client registration fails, or if no thread-local
/// tokio runtime context is available for the async `connect`.
pub type ExecEventRx =
    tokio::sync::mpsc::UnboundedReceiver<nautilus_common::messages::ExecutionEvent>;

#[allow(clippy::type_complexity)]
pub fn attach_sandbox_execution(
    trader_id: TraderId,
    venue: Venue,
    account_id: AccountId,
    starting_balance: Money,
    clock: Rc<RefCell<dyn Clock>>,
    cache: Rc<RefCell<Cache>>,
) -> Option<ExecEventRx> {
    let engine = Rc::new(RefCell::new(ExecutionEngine::new(
        clock.clone(),
        cache.clone(),
        None,
    )));
    ExecutionEngine::register_msgbus_handlers(&engine);

    let core = nautilus_execution::client::core::ExecutionClientCore::new(
        trader_id,
        ClientId::from(SANDBOX_CLIENT_ID),
        venue,
        OmsType::Netting,
        account_id,
        AccountType::Cash,
        None,
        cache.clone(),
    );
    let config = SandboxExecutionClientConfig::builder()
        .account_id(account_id)
        .venue(venue)
        .starting_balances(vec![starting_balance])
        .build();
    // Deferred execution events: the sandbox client pushes order events
    // through this channel instead of re-entering the engine synchronously.
    let (exec_tx, exec_rx) =
        tokio::sync::mpsc::unbounded_channel::<nautilus_common::messages::ExecutionEvent>();
    nautilus_common::live::runner::replace_exec_event_sender(exec_tx);

    let mut client = SandboxExecutionClient::new(core, config, clock, cache);
    // The sandbox `connect`/`start` only flip state and register bus
    // subscriptions; a throwaway mini-runtime is enough on the core thread.
    tokio::runtime::Runtime::new()
        .expect("mini runtime")
        .block_on(async {
            client.connect().await.expect("sandbox client connect");
            let _ = client.start();
        });
    let mut engine_ref = engine.borrow_mut();
    engine_ref
        .register_client(Box::new(client))
        .expect("register sandbox client");
    // Upstream separates registration from routing: bind the sandbox client
    // to its venue explicitly and make it the default route.
    engine_ref
        .register_venue_routing(ClientId::from(SANDBOX_CLIENT_ID), venue)
        .expect("register sandbox venue routing");
    engine_ref
        .set_default_client(ClientId::from(SANDBOX_CLIENT_ID))
        .expect("set sandbox default client");
    drop(engine_ref);

    // Bus handlers hold weak references; keep the engine for the process
    // lifetime (a full node owns its engines instead).
    #[expect(clippy::mem_forget, reason = "keeps bus handlers alive")]
    std::mem::forget(engine);
    Some(exec_rx)
}

/// Forwards queued execution events into the execution engine on the current
/// (core) thread. Mirrors the live runner's event loop forwarding.
pub fn pump_exec_events(rx: &mut Option<ExecEventRx>) {
    let Some(rx_ref) = rx else { return };
    while let Ok(event) = rx_ref.try_recv() {
        use nautilus_common::messages::ExecutionEvent;
        match event {
            ExecutionEvent::Order(order_event) => {
                msgbus::send_order_event(MessagingSwitchboard::exec_engine_process(), order_event);
            }
            ExecutionEvent::Account(state) => {
                msgbus::send_account_state(MessagingSwitchboard::exec_engine_process(), &state);
            }
            _ => {}
        }
    }
}

/// Subscribes a recorder that durably journals denial, fill, and cancellation
/// events from the `events.order.*` topics.
///
/// The origin admission sequence is recovered deterministically from the
/// client order ID (`O-{epoch}-{seq}`), so no extra coupling between the
/// bridge and the recorder is needed. Must be called on the core thread.
pub fn attach_outcome_recorder(worker: PersistenceWorker) {
    let handler = TypedHandler::from(move |event: &OrderEventAny| {
        let (client_order_id, outcome) = match event {
            OrderEventAny::Denied(denied) => (
                denied.client_order_id,
                crate::journal::JournalOutcome::Denied {
                    reason: denied.reason.to_string(),
                },
            ),
            OrderEventAny::Filled(filled) => (
                filled.client_order_id,
                crate::journal::JournalOutcome::Filled {
                    client_order_id: filled.client_order_id.to_string(),
                    filled_qty: filled.last_qty.to_string(),
                },
            ),
            OrderEventAny::Canceled(canceled) => (
                canceled.client_order_id,
                crate::journal::JournalOutcome::Canceled {
                    client_order_id: canceled.client_order_id.to_string(),
                },
            ),
            _ => return,
        };
        let Some(origin_seq) = seq_from_client_order_id(&client_order_id) else {
            return;
        };
        // Blocks until the group commit finishes; acceptable on this thread.
        let _ = worker.submit_outcome(origin_seq, outcome);
    });
    msgbus::subscribe_order_events("events.order.*".into(), handler, Some(10));
}

/// Parses the journal sequence out of a derived client order ID.
#[must_use]
pub fn seq_from_client_order_id(client_order_id: &ClientOrderId) -> Option<u64> {
    let value = client_order_id.to_string();
    let mut parts = value.splitn(3, '-');
    let epoch = parts.next()?;
    if epoch != "O" {
        return None;
    }
    parts.next()?; // epoch number (validated loosely)
    let seq = parts.next()?;
    seq.parse::<u64>().ok()
}

/// Builds and dispatches a `CancelOrder` command toward the execution engine.
pub fn dispatch_cancel(
    trader_id: TraderId,
    strategy_id: nautilus_model::identifiers::StrategyId,
    instrument_id: nautilus_model::identifiers::InstrumentId,
    client_order_id: ClientOrderId,
    ts_init_ns: u64,
) {
    let command = CancelOrder::new(
        trader_id,
        None, // venue_order_id
        strategy_id,
        instrument_id,
        client_order_id,
        None, // reason
        UUID4::new(),
        UnixNanos::from(ts_init_ns),
        None, // correlation_id
        None, // params?
    );
    msgbus::send_trading_command(
        MessagingSwitchboard::exec_engine_queue_execute(),
        nautilus_common::messages::execution::TradingCommand::CancelOrder(command),
    );
}
