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

//! M1 validation tests: group-commit throughput, event tail semantics, and
//! the quota ledger. These do not touch the process-global message bus and
//! may run multi-threaded.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rust_decimal_macros::dec;

use orderhub_gateway::journal::{BusinessJournal, JournalEntry, JournalOutcome};
use orderhub_gateway::quota::QuotaLedger;
use orderhub_gateway::worker::PersistenceWorker;

fn bench_record(seq: u64) -> orderhub_gateway::journal::AdmissionRecord {
    orderhub_gateway::journal::AdmissionRecord {
        journal_epoch: 1,
        seq,
        submitter_id: "bench".to_string(),
        request_id: format!("req-{seq}"),
        request_kind: "SubmitOrder".to_string(),
        request_fingerprint: "fp".to_string(),
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
    }
}

#[test]
fn m1_w1_group_commit_meets_admission_target() {
    // Concurrency depth must exceed commit_time x target rate (~6ms x 2000/s
    // ~ 12 in flight) for group commit to demonstrate capacity; sequential
    // round-trip submitters alone cap at submitters/commit_time.
    const THREADS: usize = 24;
    const PER_THREAD: u64 = 84; // 2000 total, matching the admission target

    let dir = tempfile::tempdir().unwrap();
    let journal = Arc::new(BusinessJournal::open(&dir.path().join("journal.redb")).unwrap());
    let worker = PersistenceWorker::start(Arc::clone(&journal));
    let start = Instant::now();
    let seqs = Arc::new(Mutex::new(Vec::new()));
    std::thread::scope(|scope| {
        for t in 0..THREADS {
            let worker = worker.clone();
            let seqs = Arc::clone(&seqs);
            scope.spawn(move || {
                let mut local = Vec::with_capacity(PER_THREAD as usize);
                for i in 0..PER_THREAD {
                    let record = bench_record(u64::try_from(t).unwrap() * 100_000 + i);
                    let admitted = worker.submit_admission(record).unwrap();
                    local.push(admitted.seq);
                }
                seqs.lock().unwrap().extend(local);
            });
        }
    });
    let elapsed = start.elapsed();

    let mut all = seqs.lock().unwrap().clone();
    all.sort_unstable();
    all.dedup();
    assert_eq!(all.len(), THREADS * PER_THREAD as usize, "unique sequences");
    assert_eq!(all[0], 1);
    assert_eq!(
        all[all.len() - 1],
        (THREADS as u64) * PER_THREAD,
        "contiguous numbering"
    );
    assert_eq!(worker.high_water(), (THREADS as u64) * PER_THREAD);
    assert_eq!(
        journal.scan_all().unwrap().len(),
        (THREADS as u64) as usize * PER_THREAD as usize
    );

    let total_us = elapsed.as_micros() as f64;
    let ops_per_s = 1_000_000.0 / (total_us / (THREADS as u64 * PER_THREAD) as f64);
    println!(
        "M1 group-commit: {} admissions from {THREADS} threads in {total_us:.0} us \
         (~{ops_per_s:.0} durable admissions/s)",
        THREADS as u64 * PER_THREAD
    );
    // The admission target is 2000 durable admissions/s; M0 measured the
    // synchronous path at only ~230-450/s. Group commit must clear the target
    // with margin on this machine.
    assert!(
        ops_per_s >= 2_000.0,
        "group commit {ops_per_s:.0}/s below the 2000/s admission target"
    );
}

#[test]
fn m1_w2_outcomes_via_worker_are_queryable() {
    let dir = tempfile::tempdir().unwrap();
    let journal = Arc::new(BusinessJournal::open(&dir.path().join("journal.redb")).unwrap());
    let worker = PersistenceWorker::start(Arc::clone(&journal));

    let admitted = worker
        .submit_admission(bench_record(1).clone_into_request("req-out-1"))
        .unwrap();
    worker
        .submit_outcome(
            admitted.seq,
            JournalOutcome::DispatchStarted {
                client_order_id: format!("O-1-{:012}", admitted.seq),
            },
        )
        .unwrap();

    let record = journal
        .find_request("bench", "SubmitOrder", "req-out-1")
        .unwrap()
        .expect("admission present");
    assert!(matches!(
        record.outcome,
        Some(JournalOutcome::DispatchStarted { .. })
    ));
    assert!(worker.high_water() >= 2);
}

#[test]
fn m1_t1_tail_scan_after_has_no_gaps_and_follows() {
    let dir = tempfile::tempdir().unwrap();
    let journal = Arc::new(BusinessJournal::open(&dir.path().join("journal.redb")).unwrap());
    let worker = PersistenceWorker::start(Arc::clone(&journal));

    // Phase 1: 10 admissions committed
    for i in 0..10 {
        worker
            .submit_admission(bench_record(i).clone_into_request(&format!("req-tail-{i}")))
            .unwrap();
    }
    let snapshot_cursor = worker.high_water();
    assert_eq!(snapshot_cursor, 10);

    // Entries after cursor 0 are complete and contiguous
    let all = journal.scan_after(0).unwrap();
    assert_eq!(all.len(), 10);
    for (idx, (seq, _)) in all.iter().enumerate() {
        assert_eq!(*seq, u64::try_from(idx + 1).unwrap());
    }
    // Scan after the snapshot cursor is empty for now
    assert!(journal.scan_after(snapshot_cursor).unwrap().is_empty());

    // Phase 2: concurrent commits advance the tail; the follower catches up
    std::thread::scope(|scope| {
        let worker = worker.clone();
        scope.spawn(move || {
            for i in 10..15 {
                worker
                    .submit_admission(bench_record(i).clone_into_request(&format!("req-tail-{i}")))
                    .unwrap();
            }
        });
    });

    let deadline = Instant::now() + Duration::from_secs(5);
    let mut caught_up = Vec::new();
    while caught_up.len() < 5 {
        assert!(Instant::now() < deadline, "tail never caught up");
        caught_up = journal.scan_after(snapshot_cursor).unwrap();
        if caught_up.len() < 5 {
            std::thread::sleep(Duration::from_millis(5));
        }
    }
    // No gap between snapshot cursor and the tail, entries in order
    assert_eq!(caught_up[0].0, snapshot_cursor + 1);
    assert_eq!(caught_up[4].0, snapshot_cursor + 5);
    assert!(
        caught_up
            .iter()
            .all(|(_, e)| matches!(e, JournalEntry::Admission(_)))
    );
}

#[test]
fn m1_q1_competing_requests_for_one_budget_admit_exactly_one() {
    // Acceptance scenario R01: budget 100, two concurrent buys of 60 each.
    let ledger = Arc::new(Mutex::new(QuotaLedger::new()));
    ledger
        .lock()
        .unwrap()
        .set_budget("ORDERHUB-S001", dec!(100));

    let results = Arc::new(Mutex::new(Vec::new()));
    std::thread::scope(|scope| {
        for _ in 0..2 {
            let ledger = Arc::clone(&ledger);
            let results = Arc::clone(&results);
            scope.spawn(move || {
                let outcome = ledger
                    .lock()
                    .unwrap()
                    .try_tentative("ORDERHUB-S001", dec!(60));
                results.lock().unwrap().push(outcome.is_ok());
            });
        }
    });

    let outcomes = results.lock().unwrap();
    let admitted = outcomes.iter().filter(|ok| **ok).count();
    assert_eq!(admitted, 1, "exactly one competitor may pass");
    assert_eq!(
        ledger.lock().unwrap().available("ORDERHUB-S001"),
        dec!(40),
        "tentative hold counts against availability"
    );
}

#[test]
fn m1_q2_tentative_commit_and_release_lifecycle() {
    let mut ledger = QuotaLedger::new();
    ledger.set_budget("ORDERHUB-S001", dec!(100));

    let t1 = ledger.try_tentative("ORDERHUB-S001", dec!(60)).unwrap();
    assert_eq!(ledger.available("ORDERHUB-S001"), dec!(40));
    assert_eq!(
        ledger.try_tentative("ORDERHUB-S001", dec!(50)).unwrap_err(),
        orderhub_gateway::quota::QuotaError::InsufficientBudget {
            strategy_id: "ORDERHUB-S001".to_string(),
            requested: dec!(50),
            available: dec!(40),
        }
    );

    // Pre-commit failure releases the hold
    let t2 = ledger.try_tentative("ORDERHUB-S001", dec!(40)).unwrap();
    ledger.release_tentative(t2).unwrap();
    assert_eq!(ledger.available("ORDERHUB-S001"), dec!(40));

    // Durable commit converts tentative to committed
    ledger.commit(t1).unwrap();
    assert_eq!(ledger.available("ORDERHUB-S001"), dec!(40));
    assert!(ledger.release_tentative(t1).is_err(), "already committed");

    // Terminal rejection releases the confirmed reservation
    ledger.release_committed("ORDERHUB-S001", dec!(60));
    assert_eq!(ledger.available("ORDERHUB-S001"), dec!(100));
}

// Test helper: re-key a bench record for a distinct idempotency key.
trait CloneIntoRequest {
    fn clone_into_request(&self, request_id: &str) -> orderhub_gateway::journal::AdmissionRecord;
}

impl CloneIntoRequest for orderhub_gateway::journal::AdmissionRecord {
    fn clone_into_request(&self, request_id: &str) -> Self {
        let mut record = self.clone();
        record.request_id = request_id.to_string();
        record
    }
}
