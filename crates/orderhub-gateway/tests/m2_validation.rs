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

//! M2 acceptance: per-submitter authorization (R03), account-level quota
//! (R02), sellable-quantity ledger, readiness gating, and journal recovery
//! (A03 core). Quota and readiness tests run multi-threaded; none touch the
//! process-global message bus.

use rust_decimal_macros::dec;

use orderhub_gateway::auth::{SubmitterCredentials, SubmitterRegistry};
use orderhub_gateway::journal::{BusinessJournal, JournalOutcome};
use orderhub_gateway::quota::{QuotaError, QuotaLedger};
use orderhub_gateway::readiness::{Readiness, ReadinessGate};

#[test]
fn m2_auth_r03_authorization_scopes() {
    let registry = SubmitterRegistry::new(vec![
        SubmitterCredentials {
            token: "token-alpha".to_string(),
            submitter_id: "trader-alpha".to_string(),
            strategies: vec!["ORDERHUB-S001".to_string()],
        },
        SubmitterCredentials {
            token: "token-beta".to_string(),
            submitter_id: "trader-beta".to_string(),
            strategies: vec!["ORDERHUB-S002".to_string(), "ORDERHUB-S003".to_string()],
        },
    ]);

    // Valid token + own strategy
    let alpha = registry
        .authenticate_header("Bearer token-alpha")
        .expect("alpha authenticated");
    assert!(SubmitterRegistry::is_authorized(alpha, "ORDERHUB-S001"));

    // Cross-strategy escalation is refused (R03)
    assert!(!SubmitterRegistry::is_authorized(alpha, "ORDERHUB-S002"));
    let beta = registry
        .authenticate_header("Bearer token-beta")
        .expect("beta authenticated");
    assert!(!SubmitterRegistry::is_authorized(beta, "ORDERHUB-S001"));

    // Unknown tokens and malformed headers fail closed
    assert!(registry.authenticate_header("Bearer nope").is_none());
    assert!(registry.authenticate_header("token-alpha").is_none());
    assert!(registry.authenticate_header("").is_none());
}

// Duplicate tokens are a configuration error and must fail fast rather than
// silently granting another submitter's identity.
#[test]
#[should_panic(expected = "duplicate submitter token")]
fn m2_auth_duplicate_tokens_fail_fast() {
    let _duplicate = SubmitterRegistry::new(vec![
        SubmitterCredentials {
            token: "shared".to_string(),
            submitter_id: "first".to_string(),
            strategies: vec!["S-A".to_string()],
        },
        SubmitterCredentials {
            token: "shared".to_string(),
            submitter_id: "second".to_string(),
            strategies: vec!["S-B".to_string()],
        },
    ]);
}

#[test]
fn m2_quota_r02_account_total_binds_all_strategies() {
    let mut ledger = QuotaLedger::new();
    ledger.set_budget("S-A", dec!(100));
    ledger.set_budget("S-B", dec!(100));
    // Account total 150 while each strategy alone could spend 100.
    ledger.set_account_budget(dec!(150));

    // Strategy A spends 100 (within its own budget and the account total).
    let a = ledger.try_tentative_buy("S-A", dec!(100)).unwrap();
    ledger.commit(a).unwrap();

    // Strategy B has its own budget but the account is exhausted (R02).
    let err = ledger.try_tentative_buy("S-B", dec!(60)).unwrap_err();
    assert!(matches!(err, QuotaError::AccountBudget { .. }));
    assert_eq!(ledger.account_available(), dec!(50));

    // Releasing A's committed reservation restores account capacity.
    ledger.release_committed("S-A", dec!(100));
    let b = ledger.try_tentative_buy("S-B", dec!(50)).unwrap();
    ledger.commit(b).unwrap();
}

#[test]
fn m2_quota_sell_reserves_holdings_not_cash() {
    let mut ledger = QuotaLedger::new();
    ledger.set_budget("S-A", dec!(1000));
    ledger.add_position("S-A", "AAPL.XNAS", dec!(10));

    // Selling without holdings is refused regardless of cash budget.
    let err = ledger
        .try_tentative_sell("S-A", "MSFT.XNAS", dec!(1))
        .unwrap_err();
    assert!(matches!(err, QuotaError::SellableQuantity { .. }));

    // Selling within holdings reserves quantity, not cash.
    let hold = ledger
        .try_tentative_sell("S-A", "AAPL.XNAS", dec!(6))
        .unwrap();
    assert_eq!(ledger.sellable("S-A", "AAPL.XNAS"), dec!(4));
    assert_eq!(
        ledger.available("S-A"),
        dec!(1000),
        "sell holds do not consume buy budget"
    );

    // A second sell beyond the remainder is refused while the first is held.
    assert!(
        ledger
            .try_tentative_sell("S-A", "AAPL.XNAS", dec!(5))
            .is_err()
    );

    // Commit converts the hold; a release path frees quantity again.
    ledger.commit(hold).unwrap();
    assert_eq!(ledger.sellable("S-A", "AAPL.XNAS"), dec!(4));
}

#[test]
fn m2_ready_gate_blocks_and_allows_admission() {
    let gate = ReadinessGate::new();
    assert_eq!(gate.get(), Readiness::Booting);
    assert!(!gate.is_admissible());

    gate.set(Readiness::Restoring, "");
    assert!(!gate.is_admissible());

    gate.set(Readiness::Ready, "");
    assert!(gate.is_admissible());

    gate.set(Readiness::Halted, "storage failure");
    assert!(!gate.is_admissible());
    assert_eq!(gate.halt_reason(), "storage failure");
}

#[test]
fn m2_restore_a03_recovery_returns_original_results() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("journal.redb");

    // Phase 1: two admissions committed, then a "crash" (handle dropped).
    let record = |seq: u64| orderhub_gateway::journal::AdmissionRecord {
        journal_epoch: 1,
        seq,
        submitter_id: "trader-1".to_string(),
        request_id: format!("req-{seq}"),
        request_kind: "SubmitOrder".to_string(),
        request_fingerprint: format!("fp-{seq}"),
        strategy_id: "ORDERHUB-S001".to_string(),
        instrument_id: "AAPL.XNAS".to_string(),
        order_side: "BUY".to_string(),
        quantity: "10".to_string(),
        price: "150.00".to_string(),
        time_in_force: "GTC".to_string(),
        post_only: false,
        reduce_only: false,
        submit_before_ns: None,
        ts_init_ns: 1,
    };
    let journal = BusinessJournal::open(&path).unwrap();
    let first = journal.append_admission(record(1)).unwrap();
    let second = journal.append_admission(record(2)).unwrap();
    journal
        .append_outcome(
            second.seq,
            JournalOutcome::Filled {
                client_order_id: format!("O-1-{:012}", second.seq),
                filled_qty: "10".to_string(),
            },
        )
        .unwrap();
    drop(journal);

    // Phase 2: restart and restore.
    let gate = ReadinessGate::new();
    gate.set(Readiness::Restoring, "");
    let restored = BusinessJournal::open(&path).expect("journal reopens");
    let entries = restored.scan_all().unwrap();
    assert_eq!(entries.len(), 3, "all committed entries survive");
    assert_eq!(restored.last_seq().unwrap(), 3, "high-water preserved");

    // A03 core: same-key retry recovers the ORIGINAL result, including the
    // fill outcome recorded before the crash; no second order is created.
    let recovered = restored
        .find_request("trader-1", "SubmitOrder", "req-2")
        .unwrap()
        .expect("idempotency index survives");
    assert_eq!(recovered.admission.seq, second.seq);
    assert_eq!(
        recovered.admission.request_fingerprint,
        second.request_fingerprint
    );
    assert!(matches!(
        recovered.outcome,
        Some(JournalOutcome::Filled { .. })
    ));

    // Sequences continue without reuse; the gate opens only after restore.
    let third = restored.append_admission(record(3)).unwrap();
    assert_eq!(third.seq, 4);
    assert_ne!(first.seq, second.seq);
    gate.set(Readiness::Ready, "");
    assert!(gate.is_admissible());
}
