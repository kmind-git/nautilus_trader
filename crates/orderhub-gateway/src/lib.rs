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

//! OrderHub gateway (M0 prototype scope).
//!
//! This crate implements the two M0 validation targets from the OrderHub design:
//!
//! 1. [`bridge`] - the admission bridge which replicates the kernel's native
//!    strategy submission path (order construction, `Cache` registration,
//!    `OrderInitialized` publication, and `SubmitOrder` dispatch to the risk
//!    engine endpoint). It validates that an external submission can enter the
//!    native `RiskEngine -> ExecutionEngine` chain with correct attribution.
//! 2. [`journal`] - a local append-only business journal backed by `redb`
//!    which durably records request admissions, idempotency keys, and order
//!    outcomes. It validates crash-recovery reads and durable write cost.
//!
//! M0 scope notes (per the design documents):
//! - The journal commit is synchronous in this prototype. The bounded-channel
//!   persistence worker described in the admission contract is M1 work.
//! - Order outcome records are appended by whoever observes the corresponding
//!   event; wiring an automatic message-bus subscription is M1 work.
//! - The gRPC transport (`orderhub-proto` and the tonic server) is out of M0
//!   scope entirely.

pub mod bridge;
pub mod grpc;
pub mod journal;
pub mod quota;
pub mod sandbox;
pub mod worker;
