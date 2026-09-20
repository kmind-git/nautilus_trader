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

//! Basic submitter quota ledger (M1 scope).
//!
//! Minimal implementation of the reservations contract: per-strategy buy
//! notional budget with the tentative -> committed -> released lifecycle.
//! The core-thread "check + tentatively reserve" is serialized by the caller
//! holding `&mut self`, so two requests competing for the same remaining
//! budget cannot both pass (acceptance scenario R01).
//!
//! Out of M1 scope (per the risk contract): account-level aggregation,
//! multi-currency, sellable-quantity ledger, mark-to-market exposure. The
//! ledger intentionally holds only exact decimals; floats are forbidden.

use std::collections::HashMap;

use rust_decimal::Decimal;

/// A unique tentative reservation token.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TentativeId(u64);

/// Why a tentative reservation was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuotaError {
    /// The strategy's remaining budget cannot cover the request.
    InsufficientBudget {
        /// Strategy whose budget was exceeded.
        strategy_id: String,
        /// Notional the request wanted to reserve.
        requested: Decimal,
        /// Notional still available including tentative holds.
        available: Decimal,
    },
    /// A tentative reservation token was not found at commit/release time.
    UnknownTentative(TentativeId),
}

impl std::error::Error for QuotaError {}

impl std::fmt::Display for QuotaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InsufficientBudget {
                strategy_id,
                requested,
                available,
            } => write!(
                f,
                "insufficient budget for {strategy_id}: requested {requested}, available {available}"
            ),
            Self::UnknownTentative(id) => write!(f, "unknown tentative reservation {:?}", id.0),
        }
    }
}

/// Per-strategy buy-notional budget with tentative holds.
#[derive(Debug, Default)]
pub struct QuotaLedger {
    budgets: HashMap<String, Decimal>,
    /// Confirmed reservations (admitted, unfilled orders).
    committed: HashMap<String, Decimal>,
    /// In-flight reservations awaiting durable admission commit.
    tentative: HashMap<TentativeId, (String, Decimal)>,
    next_tentative: u64,
}

impl QuotaLedger {
    /// Creates an empty ledger.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the total buy-notional budget for a strategy.
    pub fn set_budget(&mut self, strategy_id: &str, budget: Decimal) {
        self.budgets.insert(strategy_id.to_string(), budget);
    }

    /// Notional still available to a strategy, counting tentative holds.
    #[must_use]
    pub fn available(&self, strategy_id: &str) -> Decimal {
        let budget = self.budgets.get(strategy_id).copied().unwrap_or_default();
        let committed = self.committed.get(strategy_id).copied().unwrap_or_default();
        let tentative: Decimal = self
            .tentative
            .values()
            .filter(|(sid, _)| sid == strategy_id)
            .map(|(_, amount)| amount)
            .sum();
        (budget - committed - tentative).max(Decimal::ZERO)
    }

    /// Atomically checks and tentatively reserves budget.
    ///
    /// `check + hold` happens under the caller's exclusive borrow, so
    /// concurrent admissions competing for one remaining budget resolve
    /// strictly serially (at most one succeeds).
    pub fn try_tentative(
        &mut self,
        strategy_id: &str,
        notional: Decimal,
    ) -> Result<TentativeId, QuotaError> {
        let available = self.available(strategy_id);
        if notional > available {
            return Err(QuotaError::InsufficientBudget {
                strategy_id: strategy_id.to_string(),
                requested: notional,
                available,
            });
        }
        self.next_tentative = self
            .next_tentative
            .checked_add(1)
            .expect("tentative id overflow");
        let id = TentativeId(self.next_tentative);
        self.tentative
            .insert(id, (strategy_id.to_string(), notional));
        Ok(id)
    }

    /// Converts a tentative hold into a confirmed reservation.
    ///
    /// Called after the durable admission commit succeeds, per the contract
    /// ordering: tentative during the disk commit window, committed after.
    pub fn commit(&mut self, id: TentativeId) -> Result<(), QuotaError> {
        let (strategy_id, amount) = self
            .tentative
            .remove(&id)
            .ok_or(QuotaError::UnknownTentative(id))?;
        let entry = self.committed.entry(strategy_id).or_default();
        *entry += amount;
        Ok(())
    }

    /// Releases a tentative hold without committing it (pre-commit failure).
    pub fn release_tentative(&mut self, id: TentativeId) -> Result<(), QuotaError> {
        self.tentative
            .remove(&id)
            .map(|_| ())
            .ok_or(QuotaError::UnknownTentative(id))
    }

    /// Releases part or all of a confirmed reservation (terminal rejection,
    /// cancel, or fill conversion; full conversion is M2 scope).
    pub fn release_committed(&mut self, strategy_id: &str, amount: Decimal) {
        if let Some(entry) = self.committed.get_mut(strategy_id) {
            *entry = (*entry - amount).max(Decimal::ZERO);
        }
    }
}
