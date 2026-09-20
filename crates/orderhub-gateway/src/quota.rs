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

//! Submitter quota ledger with account aggregation (M2).
//!
//! Extends the M1 per-strategy buy budget with:
//! - account-level total budget across all strategies (acceptance R02);
//! - a sellable-quantity ledger per (strategy, instrument) so sells reserve
//!   holdings instead of cash.
//!
//! All amounts are exact decimals. The "check + hold" under `&mut self`
//! serialization is unchanged: concurrent requests for one remaining budget
//! resolve strictly serially.

use std::collections::HashMap;

use rust_decimal::Decimal;

/// A unique tentative reservation token.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TentativeId(u64);

/// Why a tentative reservation was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuotaError {
    /// The strategy's remaining budget cannot cover the request.
    StrategyBudget {
        /// Strategy whose budget was exceeded.
        strategy_id: String,
        /// Notional the request wanted to reserve.
        requested: Decimal,
        /// Notional still available including tentative holds.
        available: Decimal,
    },
    /// The account-level total across all strategies is exhausted.
    AccountBudget {
        /// Notional the request wanted to reserve.
        requested: Decimal,
        /// Account notional still available.
        available: Decimal,
    },
    /// The strategy holds insufficient sellable quantity.
    SellableQuantity {
        /// Strategy attempting the sale.
        strategy_id: String,
        /// Instrument of the sale.
        instrument_id: String,
        /// Quantity the request wanted to sell.
        requested: Decimal,
        /// Sellable quantity still available.
        available: Decimal,
    },
    /// A tentative reservation token was not found at commit/release time.
    UnknownTentative(TentativeId),
}

impl std::error::Error for QuotaError {}

impl std::fmt::Display for QuotaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StrategyBudget {
                strategy_id,
                requested,
                available,
            } => write!(
                f,
                "strategy budget for {strategy_id}: requested {requested}, available {available}"
            ),
            Self::AccountBudget {
                requested,
                available,
            } => write!(
                f,
                "account budget: requested {requested}, available {available}"
            ),
            Self::SellableQuantity {
                strategy_id,
                instrument_id,
                requested,
                available,
            } => write!(
                f,
                "sellable quantity for {strategy_id} on {instrument_id}: requested {requested}, available {available}"
            ),
            Self::UnknownTentative(id) => write!(f, "unknown tentative reservation {:?}", id.0),
        }
    }
}

/// One tentative or committed hold.
#[derive(Debug, Clone)]
enum Hold {
    /// Buy notional held against a strategy (and the account total).
    BuyNotional {
        strategy_id: String,
        notional: Decimal,
    },
    /// Sell quantity held against a strategy's holdings.
    SellQuantity {
        strategy_id: String,
        instrument_id: String,
        quantity: Decimal,
    },
}

/// Per-strategy budgets, account total, and sellable-quantity ledger.
#[derive(Debug)]
pub struct QuotaLedger {
    strategy_budgets: HashMap<String, Decimal>,
    /// Account-wide buy-notional cap; defaults to effectively unlimited
    /// until configured (deployment parameter per the risk contract).
    account_budget: Decimal,
    /// Confirmed buy reservations by strategy.
    committed_buy: HashMap<String, Decimal>,
    /// Confirmed sell reservations by (strategy, instrument).
    committed_sell: HashMap<(String, String), Decimal>,
    /// Strategy holdings (sellable base) by (strategy, instrument).
    positions: HashMap<(String, String), Decimal>,
    /// In-flight holds awaiting durable admission commit.
    tentative: HashMap<TentativeId, Hold>,
    next_tentative: u64,
}

impl Default for QuotaLedger {
    fn default() -> Self {
        Self {
            strategy_budgets: HashMap::new(),
            account_budget: Decimal::MAX,
            committed_buy: HashMap::new(),
            committed_sell: HashMap::new(),
            positions: HashMap::new(),
            tentative: HashMap::new(),
            next_tentative: 0,
        }
    }
}

impl QuotaLedger {
    /// Creates an empty ledger (unlimited account budget until configured).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the buy-notional budget for a strategy.
    pub fn set_budget(&mut self, strategy_id: &str, budget: Decimal) {
        self.strategy_budgets
            .insert(strategy_id.to_string(), budget);
    }

    /// Sets the account-wide buy-notional total cap.
    pub fn set_account_budget(&mut self, budget: Decimal) {
        self.account_budget = budget;
    }

    /// Records strategy holdings (the sellable base) from confirmed fills.
    pub fn add_position(&mut self, strategy_id: &str, instrument_id: &str, quantity: Decimal) {
        let key = (strategy_id.to_string(), instrument_id.to_string());
        *self.positions.entry(key).or_default() += quantity;
    }

    /// Strategy buy notional still available, counting tentative holds.
    #[must_use]
    pub fn available(&self, strategy_id: &str) -> Decimal {
        let budget = self
            .strategy_budgets
            .get(strategy_id)
            .copied()
            .unwrap_or_default();
        let committed = self
            .committed_buy
            .get(strategy_id)
            .copied()
            .unwrap_or_default();
        let tentative = self.tentative_hold_notional(strategy_id);
        (budget - committed - tentative).max(Decimal::ZERO)
    }

    /// Account buy notional still available across all strategies.
    #[must_use]
    pub fn account_available(&self) -> Decimal {
        let committed: Decimal = self.committed_buy.values().copied().sum();
        let tentative: Decimal = self
            .tentative
            .values()
            .map(|hold| match hold {
                Hold::BuyNotional { notional, .. } => *notional,
                Hold::SellQuantity { .. } => Decimal::ZERO,
            })
            .sum();
        (self.account_budget - committed - tentative).max(Decimal::ZERO)
    }

    /// Sellable quantity for a strategy on an instrument, counting holds.
    #[must_use]
    pub fn sellable(&self, strategy_id: &str, instrument_id: &str) -> Decimal {
        let key = &(strategy_id.to_string(), instrument_id.to_string());
        let position = self.positions.get(key).copied().unwrap_or_default();
        let committed = self.committed_sell.get(key).copied().unwrap_or_default();
        let tentative: Decimal = self
            .tentative
            .values()
            .map(|hold| match hold {
                Hold::SellQuantity {
                    strategy_id: sid,
                    instrument_id: iid,
                    quantity,
                } if sid == strategy_id && iid == instrument_id => *quantity,
                _ => Decimal::ZERO,
            })
            .sum();
        (position - committed - tentative).max(Decimal::ZERO)
    }

    /// Atomically checks and tentatively reserves buy notional.
    ///
    /// Both the strategy budget and the account total must cover the
    /// request; either failing refuses the whole hold (no partial holds).
    pub fn try_tentative_buy(
        &mut self,
        strategy_id: &str,
        notional: Decimal,
    ) -> Result<TentativeId, QuotaError> {
        let strategy_available = self.available(strategy_id);
        if notional > strategy_available {
            return Err(QuotaError::StrategyBudget {
                strategy_id: strategy_id.to_string(),
                requested: notional,
                available: strategy_available,
            });
        }
        let account_available = self.account_available();
        if notional > account_available {
            return Err(QuotaError::AccountBudget {
                requested: notional,
                available: account_available,
            });
        }
        self.insert_tentative(Hold::BuyNotional {
            strategy_id: strategy_id.to_string(),
            notional,
        })
    }

    /// Atomically checks and tentatively reserves sellable quantity.
    pub fn try_tentative_sell(
        &mut self,
        strategy_id: &str,
        instrument_id: &str,
        quantity: Decimal,
    ) -> Result<TentativeId, QuotaError> {
        let available = self.sellable(strategy_id, instrument_id);
        if quantity > available {
            return Err(QuotaError::SellableQuantity {
                strategy_id: strategy_id.to_string(),
                instrument_id: instrument_id.to_string(),
                requested: quantity,
                available,
            });
        }
        self.insert_tentative(Hold::SellQuantity {
            strategy_id: strategy_id.to_string(),
            instrument_id: instrument_id.to_string(),
            quantity,
        })
    }

    /// Converts a tentative hold into a confirmed reservation.
    pub fn commit(&mut self, id: TentativeId) -> Result<(), QuotaError> {
        let hold = self
            .tentative
            .remove(&id)
            .ok_or(QuotaError::UnknownTentative(id))?;
        match hold {
            Hold::BuyNotional {
                strategy_id,
                notional,
            } => {
                *self.committed_buy.entry(strategy_id).or_default() += notional;
            }
            Hold::SellQuantity {
                strategy_id,
                instrument_id,
                quantity,
            } => {
                *self
                    .committed_sell
                    .entry((strategy_id, instrument_id))
                    .or_default() += quantity;
            }
        }
        Ok(())
    }

    /// Releases a tentative hold without committing it (pre-commit failure).
    pub fn release_tentative(&mut self, id: TentativeId) -> Result<(), QuotaError> {
        self.tentative
            .remove(&id)
            .map(|_| ())
            .ok_or(QuotaError::UnknownTentative(id))
    }

    /// Releases confirmed buy notional (terminal rejection or cancel).
    pub fn release_committed(&mut self, strategy_id: &str, amount: Decimal) {
        if let Some(entry) = self.committed_buy.get_mut(strategy_id) {
            *entry = (*entry - amount).max(Decimal::ZERO);
        }
    }

    fn tentative_hold_notional(&self, strategy_id: &str) -> Decimal {
        self.tentative
            .values()
            .map(|hold| match hold {
                Hold::BuyNotional {
                    strategy_id: sid,
                    notional,
                } if sid == strategy_id => *notional,
                _ => Decimal::ZERO,
            })
            .sum()
    }

    #[allow(clippy::unnecessary_wraps, reason = "symmetric fallible API surface")]
    fn insert_tentative(&mut self, hold: Hold) -> Result<TentativeId, QuotaError> {
        self.next_tentative = self
            .next_tentative
            .checked_add(1)
            .expect("tentative id overflow");
        let id = TentativeId(self.next_tentative);
        self.tentative.insert(id, hold);
        Ok(id)
    }
}
