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

//! M1 acceptance: mixed durable-write throughput of the group-commit journal.
//! Order lifecycle coverage against the real exchange process lives in
//! `m2_local_e2e` and `m1_grpc` (the sandbox matching lane was removed with
//! the sandbox adapter).

use std::sync::Arc;
use std::time::Instant;

use orderhub_gateway::journal::{BusinessJournal, JournalOutcome};
use orderhub_gateway::worker::PersistenceWorker;

#[test]
fn m1_c1_mixed_submit_and_cancel_durable_throughput() {
    const SUBMITTERS: usize = 24;
    const PER_SUBMITTER: usize = 41;

    let dir = tempfile::tempdir().unwrap();
    let journal = Arc::new(BusinessJournal::open(&dir.path().join("journal.redb")).unwrap());
    let worker = PersistenceWorker::start(Arc::clone(&journal));

    // 1000 admissions (submits) + 1000 outcomes (cancel intents on prior
    // admissions) concurrently: the 1000 + 1000 mixed durable-write target.
    let start = Instant::now();
    std::thread::scope(|scope| {
        for t in 0..SUBMITTERS {
            let worker = worker.clone();
            scope.spawn(move || {
                for i in 0..PER_SUBMITTER {
                    let n = u64::try_from(t * PER_SUBMITTER + i).unwrap();
                    let record = crate::bench_admission(n);
                    let admitted = worker.submit_admission(record).unwrap();
                    worker
                        .submit_outcome(
                            admitted.seq,
                            JournalOutcome::Canceled {
                                client_order_id: format!("O-1-{:012}", admitted.seq),
                            },
                        )
                        .unwrap();
                }
            });
        }
    });
    let elapsed = start.elapsed();

    let total = SUBMITTERS * PER_SUBMITTER * 2; // admissions + outcomes
    let ops_per_s = total as f64 / elapsed.as_secs_f64();
    println!(
        "M1 mixed durable throughput: {total} writes in {:.3}s (~{ops_per_s:.0} writes/s)",
        elapsed.as_secs_f64()
    );
    assert!(
        ops_per_s >= 2_000.0,
        "mixed durable throughput {ops_per_s:.0}/s below 2000/s"
    );
    assert_eq!(
        journal.scan_all().unwrap().len(),
        total,
        "all durable writes committed"
    );
}

pub(crate) fn bench_admission(seq: u64) -> orderhub_gateway::journal::AdmissionRecord {
    orderhub_gateway::journal::AdmissionRecord {
        journal_epoch: 1,
        seq,
        submitter_id: "bench".to_string(),
        request_id: format!("req-mixed-{seq}"),
        request_kind: "SubmitOrder".to_string(),
        request_fingerprint: "fp".to_string(),
        strategy_id: "ORDERHUB-S001".to_string(),
        instrument_id: "AAPL.GOX".to_string(),
        order_side: "BUY".to_string(),
        quantity: "10".to_string(),
        price: "150.00".to_string(),
        time_in_force: "DAY".to_string(),
        post_only: false,
        reduce_only: false,
        submit_before_ns: None,
        ts_init_ns: 1,
    }
}
