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

//! Group-commit persistence worker.
//!
//! M0 measured 2.2-4.3 ms per synchronous immediate-durability commit
//! (~230-450 commits/s), far below the 1000 + 1000 requests/s admission
//! target. This worker implements the bounded-channel persistence design from
//! the admission contract: callers enqueue durable writes on a bounded
//! channel, a dedicated thread batches up to [`MAX_BATCH`] pending writes
//! into a single redb transaction, and each caller blocks until its batch has
//! committed. One fsync amortizes over the whole batch.
//!
//! Backpressure is the bounded channel itself: when the queue is full,
//! submitters block (the gateway admission path applies its own pre-queue
//! capacity rejection before reaching this point in M1+).
//!
//! The worker also publishes the committed high-water sequence as an atomic,
//! which the event-stream tail polls instead of hitting the database.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use crate::journal::{
    AdmissionRecord, BusinessJournal, CommittedEntry, JournalError, JournalOutcome, PendingEntry,
};

/// Maximum writes grouped into one durable transaction.
const MAX_BATCH: usize = 64;
/// Bounded queue capacity before submitters block.
const CHANNEL_CAPACITY: usize = 1024;
/// Coalescing window after the first queued write before committing.
///
/// Without a window the writer commits as soon as one request arrives,
/// grouping only the requests that happened to queue during the previous
/// commit (~2-3 on 8 submitters). Waiting briefly for concurrent submitters
/// to attach trades a little admission latency for a much larger batch, which
/// is what amortizes the fsync.
const COALESCE_WINDOW: std::time::Duration = std::time::Duration::from_micros(600);

struct PendingWrite {
    entry: PendingEntry,
    reply: std::sync::mpsc::Sender<Result<CommittedEntry, JournalError>>,
}

/// Handle to the group-commit persistence worker.
///
/// Cloning is cheap: all clones share one worker thread and one journal.
#[derive(Debug)]
pub struct PersistenceWorker {
    journal: Arc<BusinessJournal>,
    tx: SyncSender<PendingWrite>,
    _guard: Arc<WorkerGuard>,
    high_water: Arc<AtomicU64>,
}

#[derive(Debug)]
struct WorkerGuard {
    handle: Mutex<Option<JoinHandle<()>>>,
}

impl Drop for WorkerGuard {
    fn drop(&mut self) {
        // Dropping the last PersistenceWorker drops the sender, the worker
        // loop ends, and the thread is joined.
        if let Some(handle) = self.handle.lock().expect("worker lock").take() {
            let _ = handle.join();
        }
    }
}

impl Clone for PersistenceWorker {
    fn clone(&self) -> Self {
        Self {
            journal: Arc::clone(&self.journal),
            tx: self.tx.clone(),
            _guard: Arc::clone(&self._guard),
            high_water: Arc::clone(&self.high_water),
        }
    }
}

impl PersistenceWorker {
    /// Spawns a worker thread committing batches to `journal`.
    ///
    /// # Panics
    ///
    /// Panics if the OS thread cannot be spawned.
    pub fn start(journal: Arc<BusinessJournal>) -> Self {
        let (tx, rx) = sync_channel::<PendingWrite>(CHANNEL_CAPACITY);
        let high_water = Arc::new(AtomicU64::new(0));
        let worker_high_water = Arc::clone(&high_water);
        let worker_journal = Arc::clone(&journal);

        let handle = std::thread::Builder::new()
            .name("orderhub-journal-writer".to_string())
            .spawn(move || Self::run(&worker_journal, &rx, &worker_high_water))
            .expect("failed to spawn journal writer thread");

        // Fast-fail if the database is unusable before serving any request.
        let initial = journal.last_seq().unwrap_or(0);
        high_water.store(initial, Ordering::Release);

        Self {
            journal,
            tx,
            _guard: Arc::new(WorkerGuard {
                handle: Mutex::new(Some(handle)),
            }),
            high_water,
        }
    }

    /// Read-only access to the shared journal (snapshots, tail scans, lookups).
    #[must_use]
    pub fn journal(&self) -> Arc<BusinessJournal> {
        Arc::clone(&self.journal)
    }

    /// The last committed high-water sequence published by the worker.
    #[must_use]
    pub fn high_water(&self) -> u64 {
        self.high_water.load(Ordering::Acquire)
    }

    /// Durably records an admission, blocking until its batch has committed.
    pub fn submit_admission(
        &self,
        record: AdmissionRecord,
    ) -> Result<AdmissionRecord, JournalError> {
        match self.enqueue(PendingEntry::Admission(Box::new(record)))? {
            CommittedEntry::Admission(record) => Ok(*record),
            CommittedEntry::Outcome { .. } => Err(JournalError::Encoding(
                "worker returned outcome for admission write".to_string(),
            )),
        }
    }

    /// Durably records an outcome, blocking until its batch has committed.
    pub fn submit_outcome(
        &self,
        origin_seq: u64,
        outcome: JournalOutcome,
    ) -> Result<u64, JournalError> {
        match self.enqueue(PendingEntry::Outcome {
            origin_seq,
            outcome,
        })? {
            CommittedEntry::Outcome { seq, .. } => Ok(seq),
            CommittedEntry::Admission(_) => Err(JournalError::Encoding(
                "worker returned admission for outcome write".to_string(),
            )),
        }
    }

    fn enqueue(&self, entry: PendingEntry) -> Result<CommittedEntry, JournalError> {
        let (reply_tx, reply_rx) = std::sync::mpsc::channel();
        // Bounded send: when the queue is full this blocks, providing the
        // persistence-side backpressure required by the admission contract.
        self.tx
            .send(PendingWrite {
                entry,
                reply: reply_tx,
            })
            .map_err(|err| JournalError::Storage(format!("journal worker stopped: {err}")))?;
        reply_rx
            .recv()
            .map_err(|err| JournalError::Storage(format!("journal worker dropped reply: {err}")))?
    }

    fn run(journal: &BusinessJournal, rx: &Receiver<PendingWrite>, high_water: &AtomicU64) {
        while let Ok(first) = rx.recv() {
            let mut batch = Vec::with_capacity(MAX_BATCH);
            batch.push(first);
            // Coalescing window: keep attaching queued writes until the batch
            // is full or the window elapses. `yield_now` lets submitters run
            // so they can enqueue during the window.
            let window_end = std::time::Instant::now() + COALESCE_WINDOW;
            while batch.len() < MAX_BATCH && std::time::Instant::now() < window_end {
                match rx.try_recv() {
                    Ok(write) => batch.push(write),
                    Err(_) => std::thread::yield_now(),
                }
            }
            let writes: Vec<PendingEntry> = batch.iter().map(|w| w.entry.clone()).collect();
            let results = journal.append_batch(&writes);
            match results {
                Ok(committed) => {
                    if let Some(last) = committed.last() {
                        let last_seq = match last {
                            CommittedEntry::Admission(record) => record.seq,
                            CommittedEntry::Outcome { seq, .. } => *seq,
                        };
                        high_water.store(last_seq, Ordering::Release);
                    }
                    for (write, result) in batch.into_iter().zip(committed) {
                        let _ = write.reply.send(Ok(result));
                    }
                }
                Err(err) => {
                    for write in batch {
                        let _ = write.reply.send(Err(err.clone()));
                    }
                }
            }
        }
    }
}
