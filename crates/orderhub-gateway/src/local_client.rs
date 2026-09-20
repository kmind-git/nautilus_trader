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

//! Nautilus `ExecutionClient` over the local simulated exchange (FIX 4.2).
//!
//! The client sends orders via a [`FixWriter`] on the core thread; a
//! background reader thread delivers decoded events to a [`LocalPump`],
//! which converts them into `OrderEventAny` (via the common
//! `OrderEventFactory` and the cache) and forwards them through the
//! deferred execution-event channel, mirroring the sandbox client's
//! threading model.

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use nautilus_common::cache::Cache;
use nautilus_common::clients::ExecutionClient;
use nautilus_common::factories::event::OrderEventFactory;
use nautilus_common::live::runner::try_get_exec_event_sender;
use nautilus_common::messages::ExecutionEvent;
use nautilus_common::messages::execution::{
    CancelOrder, QueryAccount, QueryOrder, SubmitOrder, SubmitOrderList,
};
use nautilus_core::UnixNanos;
use nautilus_model::enums::AccountType;
use nautilus_model::enums::{LiquiditySide, OmsType, OrderSide};
use nautilus_model::identifiers::{
    AccountId, ClientId, ClientOrderId, TraderId, Venue, VenueOrderId,
};
use nautilus_model::instruments::Instrument;
use nautilus_model::orders::Order;
use nautilus_model::types::{Price, Quantity};

use rust_decimal::prelude::FromStr as _;

use crate::local_exchange::{ExecEvent, FixInitiator, FixWriter, Side};

/// Pump converting FIX reader events into deferred engine events.
pub struct LocalPump {
    reports: std::sync::mpsc::Receiver<ExecEvent>,
    factory: OrderEventFactory,
    cache: Rc<RefCell<Cache>>,
}

fn now_ns() -> UnixNanos {
    UnixNanos::from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or_default(),
    )
}

impl LocalPump {
    /// Drains all pending FIX events and forwards them into the deferred
    /// execution-event channel for the engine.
    pub fn pump(&mut self) {
        while let Ok(event) = self.reports.try_recv() {
            let Some(order_event) = self.translate(&event) else {
                continue;
            };
            if let Some(sender) = try_get_exec_event_sender() {
                let _ = sender.send(ExecutionEvent::Order(order_event));
            }
        }
    }

    fn instrument_quote_currency(
        &self,
        order: &nautilus_model::orders::OrderAny,
    ) -> nautilus_model::types::Currency {
        let cache = self.cache.borrow();
        cache
            .instrument(&order.instrument_id())
            .map_or(nautilus_model::types::Currency::USD(), |instrument| {
                instrument.quote_currency()
            })
    }

    fn translate(&self, event: &ExecEvent) -> Option<nautilus_model::events::OrderEventAny> {
        let cl_ord_id = match event {
            ExecEvent::New { cl_ord_id, .. }
            | ExecEvent::Fill { cl_ord_id, .. }
            | ExecEvent::Canceled { cl_ord_id, .. }
            | ExecEvent::Rejected { cl_ord_id, .. } => cl_ord_id.clone(),
            ExecEvent::Session(_) => return None,
        };
        let client_order_id = ClientOrderId::from(cl_ord_id.as_str());
        let order = {
            let cache = self.cache.borrow();
            cache.order(&client_order_id)?.clone()
        };
        let ts = UnixNanos::from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or_default(),
        );
        let venue_order_id = match event {
            ExecEvent::New { order_id, .. }
            | ExecEvent::Fill { order_id, .. }
            | ExecEvent::Canceled { order_id, .. } => VenueOrderId::from(order_id.as_str()),
            ExecEvent::Rejected { .. } => VenueOrderId::from("0"),
            ExecEvent::Session(_) => return None,
        };
        match event {
            ExecEvent::New { .. } => {
                Some(
                    self.factory
                        .generate_order_accepted(&order, venue_order_id, ts, ts),
                )
            }
            ExecEvent::Fill {
                complete,
                last_qty,
                last_px,
                ..
            } => {
                // The local exchange does not report liquidity flags; all
                // fills are treated as taker-side.
                if *complete {
                    Some(self.factory.generate_order_filled(
                        &order,
                        venue_order_id,
                        None,
                        nautilus_model::identifiers::TradeId::from(format!("T-{cl_ord_id}")),
                        Quantity::from(last_qty.to_string()),
                        Price::from(last_px.to_string()),
                        self.instrument_quote_currency(&order),
                        None,
                        LiquiditySide::Taker,
                        ts,
                        ts,
                    ))
                } else {
                    // Partial fills surface as an updated (remaining) order.
                    Some(self.factory.generate_order_updated(
                        &order,
                        venue_order_id,
                        order.quantity(),
                        order.price(),
                        None,
                        None,
                        ts,
                        ts,
                    ))
                }
            }
            ExecEvent::Canceled { .. } => {
                Some(
                    self.factory
                        .generate_order_canceled(&order, Some(venue_order_id), ts, ts),
                )
            }
            ExecEvent::Rejected { reason, .. } => Some(
                self.factory
                    .generate_order_rejected(&order, reason, ts, ts, false),
            ),
            ExecEvent::Session(_) => None,
        }
    }
}

/// FIX execution client for the local simulated exchange.
#[derive(Debug)]
pub struct LocalExchangeClient {
    writer: RefCell<Option<FixWriter>>,
    cache: Rc<RefCell<Cache>>,
    client_id: ClientId,
    account_id: AccountId,
    venue: Venue,
    connected: Cell<bool>,
    factory: OrderEventFactory,
}

impl LocalExchangeClient {
    /// Connects to the exchange and returns the pump for the core loop.
    ///
    /// # Errors
    ///
    /// Returns an error if the FIX logon handshake fails.
    pub fn connect_new(
        addr: &str,
        trader_id: TraderId,
        client_id: ClientId,
        account_id: AccountId,
        venue: Venue,
        cache: Rc<RefCell<Cache>>,
    ) -> Result<(Self, LocalPump), crate::local_exchange::FixError> {
        let initiator = FixInitiator::connect(addr, "ORDERHUB", "GOX", 30)?;
        let (writer, reports, _reader) = initiator.split();
        let client = Self {
            writer: RefCell::new(Some(writer)),
            cache: Rc::clone(&cache),
            client_id,
            account_id,
            venue,
            connected: Cell::new(true),
            factory: OrderEventFactory::new(trader_id, account_id, AccountType::Cash, None),
        };
        let pump = LocalPump {
            reports,
            factory: OrderEventFactory::new(trader_id, account_id, AccountType::Cash, None),
            cache,
        };
        Ok((client, pump))
    }

    fn submit(
        &self,
        client_order_id: &ClientOrderId,
        side: OrderSide,
        quantity: &Quantity,
        price: Option<Price>,
    ) -> anyhow::Result<()> {
        let mut writer = self
            .writer
            .borrow_mut()
            .take()
            .ok_or_else(|| anyhow::anyhow!("FIX writer unavailable"))?;
        let symbol = {
            let cache = self.cache.borrow();
            cache
                .order(client_order_id)
                .map(|order| order.instrument_id().symbol.to_string())
                .unwrap_or_default()
        };
        let result = writer.submit_order(
            client_order_id.as_ref(),
            &symbol,
            match side {
                OrderSide::Buy => Side::Buy,
                OrderSide::Sell => Side::Sell,
            },
            rust_decimal::Decimal::from_str(&quantity.to_string())
                .map_err(|e| anyhow::anyhow!("{e}"))
                .map_err(|e| anyhow::anyhow!("{e}"))?,
            price.map(|px| rust_decimal::Decimal::from_str(&px.to_string()).unwrap_or_default()),
        );
        *self.writer.borrow_mut() = Some(writer);
        result.map_err(|e| anyhow::anyhow!("{e}"))?;
        // Venue-agnostic ack that the order left the gateway (the engine
        // expects Submitted before the venue's Accepted).
        let order = {
            let cache = self.cache.borrow();
            cache
                .order(client_order_id)
                .ok_or_else(|| anyhow::anyhow!("order missing from cache"))?
                .clone()
        };
        if let Some(sender) = try_get_exec_event_sender() {
            let _ = sender.send(ExecutionEvent::Order(
                self.factory.generate_order_submitted(&order, now_ns()),
            ));
        }
        Ok(())
    }

    fn fresh_cancel_id(&self) -> String {
        format!("C-{}-{}", self.client_id, {
            // Unique per call within the process lifetime.
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.subsec_nanos())
                .unwrap_or_default()
        })
    }
}

impl ExecutionClient for LocalExchangeClient {
    fn generate_account_state(
        &self,
        _balances: Vec<nautilus_model::types::AccountBalance>,
        _margins: Vec<nautilus_model::types::MarginBalance>,
        _reported: bool,
        _ts_event: UnixNanos,
        _info: Option<nautilus_core::Params>,
    ) -> anyhow::Result<()> {
        anyhow::bail!("not supported by local exchange")
    }

    fn is_connected(&self) -> bool {
        self.connected.get()
    }

    fn client_id(&self) -> ClientId {
        self.client_id
    }

    fn account_id(&self) -> AccountId {
        self.account_id
    }

    fn venue(&self) -> Venue {
        self.venue
    }

    fn oms_type(&self) -> OmsType {
        OmsType::Netting
    }

    fn get_account(&self) -> Option<nautilus_model::accounts::AccountAny> {
        None
    }

    fn start(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        self.connected.set(false);
        *self.writer.borrow_mut() = None;
        Ok(())
    }

    fn submit_order(&self, cmd: SubmitOrder) -> anyhow::Result<()> {
        let order = {
            let cache = self.cache.borrow();
            cache
                .order(&cmd.client_order_id)
                .ok_or_else(|| anyhow::anyhow!("order missing from cache"))?
                .clone()
        };
        self.submit(
            &cmd.client_order_id,
            order.order_side(),
            &order.quantity(),
            order.price(),
        )
    }

    fn submit_order_list(&self, cmd: SubmitOrderList) -> anyhow::Result<()> {
        for client_order_id in &cmd.order_list.client_order_ids {
            let order = {
                let cache = self.cache.borrow();
                cache
                    .order(client_order_id)
                    .ok_or_else(|| anyhow::anyhow!("order missing from cache"))?
                    .clone()
            };
            self.submit(
                client_order_id,
                order.order_side(),
                &order.quantity(),
                order.price(),
            )?;
        }
        Ok(())
    }

    fn cancel_order(&self, cmd: CancelOrder) -> anyhow::Result<()> {
        let order = {
            let cache = self.cache.borrow();
            cache
                .order(&cmd.client_order_id)
                .ok_or_else(|| anyhow::anyhow!("order missing from cache"))?
                .clone()
        };
        let mut writer = self
            .writer
            .borrow_mut()
            .take()
            .ok_or_else(|| anyhow::anyhow!("FIX writer unavailable"))?;
        let symbol = order.instrument_id().symbol.to_string();
        let side = match order.order_side() {
            OrderSide::Buy => Side::Buy,
            OrderSide::Sell => Side::Sell,
        };
        let result = writer.cancel_order(
            &self.fresh_cancel_id(),
            cmd.client_order_id.as_ref(),
            &symbol,
            side,
        );
        *self.writer.borrow_mut() = Some(writer);
        result.map_err(|e| anyhow::anyhow!("{e}"))
    }

    fn query_account(&self, _cmd: QueryAccount) -> anyhow::Result<()> {
        anyhow::bail!("not supported by local exchange")
    }

    fn query_order(&self, _cmd: QueryOrder) -> anyhow::Result<()> {
        anyhow::bail!("not supported by local exchange")
    }
}

/// Registers engine + local FIX client; mirrors `attach_sandbox_execution`.
///
/// Returns the pump the core loop must call alongside command draining.
///
/// # Panics
///
/// Panics if engine or client registration fails.
pub fn attach_local_execution(
    addr: &str,
    trader_id: TraderId,
    venue: Venue,
    account_id: &AccountId,
    clock: Rc<RefCell<dyn nautilus_common::clock::Clock>>,
    cache: &Rc<RefCell<Cache>>,
) -> LocalPump {
    let engine = Rc::new(RefCell::new(
        nautilus_execution::engine::ExecutionEngine::new(clock, Rc::clone(cache), None),
    ));
    nautilus_execution::engine::ExecutionEngine::register_msgbus_handlers(&engine);

    let (client, pump) = LocalExchangeClient::connect_new(
        addr,
        trader_id,
        ClientId::from("GOX"),
        *account_id,
        venue,
        Rc::clone(cache),
    )
    .expect("connect local exchange");
    let mut engine_ref = engine.borrow_mut();
    engine_ref
        .register_client(Box::new(client))
        .expect("register local client");
    engine_ref
        .register_venue_routing(ClientId::from("GOX"), venue)
        .expect("register local routing");
    drop(engine_ref);

    #[expect(clippy::mem_forget, reason = "keeps bus handlers alive")]
    std::mem::forget(engine);
    pump
}
