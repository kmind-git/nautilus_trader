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

//! Execution-path plumbing shared by node assemblies and tests.
//!
//! The execution client itself is the local FIX exchange client
//! ([`crate::local_client`]); this module holds the pieces around it that the
//! core thread and the journal need regardless of venue wiring:
//!
//! - the deferred execution-event channel and its pump (re-entrancy safe
//!   forwarding into the execution engine);
//! - the durable outcome recorder subscribing to `events.order.*`;
//! - cancel-command dispatch toward the execution engine;
//! - the journal-sequence recovery from derived client order IDs.
//!
//! Must be used on the core thread (same thread as the risk engine and
//! bridge), because the message-bus state is single-threaded.

use nautilus_common::messages::execution::CancelOrder;
use nautilus_common::msgbus::MessagingSwitchboard;
use nautilus_common::msgbus::{self, TypedHandler};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::events::OrderEventAny;
use nautilus_model::identifiers::{ClientOrderId, InstrumentId, StrategyId, TraderId};

use crate::worker::PersistenceWorker;

/// Deferred execution events channel (order events and account states).
pub type ExecEventRx =
    tokio::sync::mpsc::UnboundedReceiver<nautilus_common::messages::ExecutionEvent>;

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
    strategy_id: StrategyId,
    instrument_id: InstrumentId,
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
