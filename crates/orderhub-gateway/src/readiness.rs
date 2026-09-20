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

//! Business readiness state (M2).
//!
//! OrderHub control-plane lifecycle per the recovery contract:
//! `BOOTING -> RESTORING -> READY`, any state -> `HALTED`, and
//! `READY -> STOPPED` on drain. New order admission is only permitted in
//! `READY`; queries remain available with a `stale` marker while restoring.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

/// Readiness states of the order hub.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Readiness {
    /// Process starting, nothing available yet.
    Booting = 0,
    /// Restoring journal and reconciling state; queries stale, no admission.
    Restoring = 1,
    /// All gates satisfied; admission open.
    Ready = 2,
    /// Unrecoverable or operator-initiated stop; admission closed.
    Halted = 3,
}

impl Readiness {
    /// Parses the internal representation.
    ///
    /// # Errors
    ///
    /// Returns an error for unknown values.
    pub fn from_u8(value: u8) -> Result<Self, u8> {
        match value {
            0 => Ok(Self::Booting),
            1 => Ok(Self::Restoring),
            2 => Ok(Self::Ready),
            3 => Ok(Self::Halted),
            other => Err(other),
        }
    }

    /// Lowercase wire name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Booting => "BOOTING",
            Self::Restoring => "RESTORING",
            Self::Ready => "READY",
            Self::Halted => "HALTED",
        }
    }
}

/// Shared readiness gate; admission checks are lock-free.
#[derive(Debug, Default)]
pub struct ReadinessGate {
    state: AtomicU8,
    halted_reason: std::sync::Mutex<String>,
}

impl ReadinessGate {
    /// Creates a gate in [`Readiness::Booting`].
    ///
    /// # Panics
    ///
    /// Panics if the mutex-based halt-reason storage cannot allocate.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            state: AtomicU8::new(Readiness::Booting as u8),
            halted_reason: std::sync::Mutex::new(String::new()),
        })
    }

    /// Current readiness.
    #[must_use]
    pub fn get(&self) -> Readiness {
        Readiness::from_u8(self.state.load(Ordering::Acquire)).unwrap_or(Readiness::Halted)
    }

    /// Whether new order admission is permitted.
    #[must_use]
    pub fn is_admissible(&self) -> bool {
        self.get() == Readiness::Ready
    }

    /// Transitions the gate; `Halted` records the reason.
    pub fn set(&self, state: Readiness, reason: &str) {
        self.state.store(state as u8, Ordering::Release);
        if state == Readiness::Halted
            && let Ok(mut guard) = self.halted_reason.lock()
        {
            *guard = reason.to_string();
        }
    }

    /// The recorded halt reason, if halted.
    #[must_use]
    pub fn halt_reason(&self) -> String {
        self.halted_reason
            .lock()
            .map(|guard| guard.clone())
            .unwrap_or_default()
    }
}
