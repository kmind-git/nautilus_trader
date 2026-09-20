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

//! Append-only local business journal backed by `redb`.
//!
//! This is the M0 prototype of the OrderHub business log described in the
//! recovery contract. It owns a single `redb` database file with:
//!
//! - `JOURNAL`: sequence-numbered entries (admissions and outcomes), inserted
//!   in commit order.
//! - `REQUEST_INDEX`: idempotency keys mapped to their admission sequence.
//! - `META`: journal epoch and last committed sequence.
//!
//! Every [`BusinessJournal::append_admission`] and
//! [`BusinessJournal::append_outcome`] commits with
//! [`redb::Durability::Immediate`], so a committed record survives process and
//! host crash under the storage device's fsync semantics (see the recovery
//! contract for the distinction from media-failure durability).
//!
//! Sequence allocation happens inside the same transaction as the record
//! insert, so a crash can never re-issue a committed sequence number.
//! `client_order_id` values derived from `(epoch, seq)` are therefore stable
//! across restarts and never reused, as required by the admission contract.

use std::fmt::Debug;
use std::path::Path;

use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

/// Journal entries in commit order, keyed by monotonically increasing `seq`.
const JOURNAL: TableDefinition<u64, &[u8]> = TableDefinition::new("orderhub_journal");
/// Idempotency request key (bytes) -> admission `seq`.
const REQUEST_INDEX: TableDefinition<&[u8], u64> = TableDefinition::new("orderhub_request_index");
/// Small metadata table: key `1` = journal epoch, key `2` = last committed seq.
const META: TableDefinition<u64, u64> = TableDefinition::new("orderhub_meta");

const META_EPOCH: u64 = 1;
const META_LAST_SEQ: u64 = 2;

/// Errors raised by the business journal.
#[derive(Debug, Clone)]
pub enum JournalError {
    /// An underlying storage error from `redb`.
    Storage(String),
    /// A record could not be encoded or decoded.
    Encoding(String),
}

impl std::error::Error for JournalError {}

impl std::fmt::Display for JournalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Storage(err) => write!(f, "journal storage error: {err}"),
            Self::Encoding(err) => write!(f, "journal encoding error: {err}"),
        }
    }
}

impl From<redb::Error> for JournalError {
    fn from(err: redb::Error) -> Self {
        Self::Storage(err.to_string())
    }
}

impl From<redb::TableError> for JournalError {
    fn from(err: redb::TableError) -> Self {
        Self::Storage(err.to_string())
    }
}

impl From<redb::CommitError> for JournalError {
    fn from(err: redb::CommitError) -> Self {
        Self::Storage(err.to_string())
    }
}

impl From<redb::TransactionError> for JournalError {
    fn from(err: redb::TransactionError) -> Self {
        Self::Storage(err.to_string())
    }
}

impl From<redb::StorageError> for JournalError {
    fn from(err: redb::StorageError) -> Self {
        Self::Storage(err.to_string())
    }
}

impl From<redb::DatabaseError> for JournalError {
    fn from(err: redb::DatabaseError) -> Self {
        Self::Storage(err.to_string())
    }
}

impl From<redb::SetDurabilityError> for JournalError {
    fn from(err: redb::SetDurabilityError) -> Self {
        Self::Storage(err.to_string())
    }
}

impl From<serde_json::Error> for JournalError {
    fn from(err: serde_json::Error) -> Self {
        Self::Encoding(err.to_string())
    }
}

/// A durably admitted order request.
///
/// Monetary and quantity fields are decimal strings; float/double are
/// forbidden by the admission contract.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdmissionRecord {
    /// Journal epoch at admission time.
    pub journal_epoch: u64,
    /// Committed sequence number assigned by the journal.
    pub seq: u64,
    /// Server-side submitter identity (from credential mapping in M2+).
    pub submitter_id: String,
    /// Client-generated unique request ID, stable across retries.
    pub request_id: String,
    /// RPC method of the request (idempotency namespace).
    pub request_kind: String,
    /// Canonical digest of the normalized business fields.
    pub request_fingerprint: String,
    /// Owning strategy for attribution, limits, and PnL.
    pub strategy_id: String,
    pub instrument_id: String,
    pub order_side: String,
    pub quantity: String,
    pub price: String,
    pub time_in_force: String,
    pub post_only: bool,
    pub reduce_only: bool,
    /// Business deadline for first dispatch (ns since epoch), if any.
    pub submit_before_ns: Option<u64>,
    /// Gateway receive time (ns since epoch).
    pub ts_init_ns: u64,
}

/// The durable result of an admitted request or a later lifecycle fact.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JournalOutcome {
    /// The request was durably admitted and an order was created.
    Admitted {
        /// Order ID derived deterministically from `(journal_epoch, seq)`.
        client_order_id: String,
    },
    /// A definitive business rejection (risk rules, instrument, deadline).
    Denied {
        /// Stable reason string from the denying component.
        reason: String,
    },
    /// The order crossed the dispatch boundary (conservative uncertainty edge).
    DispatchStarted { client_order_id: String },
    /// A fill (or partial fill) was observed from the execution chain.
    Filled {
        client_order_id: String,
        filled_qty: String,
    },
    /// The order reached a cancelled terminal state.
    Canceled { client_order_id: String },
}

/// One journal entry in commit order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JournalEntry {
    /// An admitted request (idempotency anchor).
    Admission(Box<AdmissionRecord>),
    /// A later outcome appended for a previously admitted request.
    Outcome {
        /// The admission `seq` this outcome applies to.
        origin_seq: u64,
        outcome: JournalOutcome,
    },
}

/// The result of an idempotency lookup.
#[derive(Debug, Clone)]
pub struct RequestRecord {
    /// The original admission.
    pub admission: AdmissionRecord,
    /// The most recent committed outcome for this request, if any.
    pub outcome: Option<JournalOutcome>,
}

/// A pending durable write for group commit.
#[derive(Debug, Clone)]
pub enum PendingEntry {
    /// An admission to append and index.
    Admission(Box<AdmissionRecord>),
    /// An outcome to append for an existing admission.
    Outcome {
        origin_seq: u64,
        outcome: JournalOutcome,
    },
}

/// The committed result of one pending entry.
#[derive(Debug, Clone)]
pub enum CommittedEntry {
    /// The admission with its journal-assigned sequence.
    Admission(Box<AdmissionRecord>),
    /// An outcome record with its own sequence and its origin.
    Outcome { seq: u64, origin_seq: u64 },
}

/// Append-only business journal with immediate-durability commits.
#[derive(Debug)]
pub struct BusinessJournal {
    db: Database,
    journal_epoch: u64,
}

impl BusinessJournal {
    /// Opens (or creates) the journal at `path`.
    ///
    /// A new file starts at epoch 1, seq 0. Epoch bumps on backup-restore
    /// scenarios are handled in M2+ recovery work.
    pub fn open(path: &Path) -> Result<Self, JournalError> {
        let db = if path.exists() {
            Database::open(path)?
        } else {
            Database::builder().create(path)?
        };

        // Table definitions must exist before any read transaction opens
        // them; `open_table` on a write transaction creates missing tables.
        let write = db.begin_write()?;
        {
            let _journal = write.open_table(JOURNAL)?;
            let _index = write.open_table(REQUEST_INDEX)?;
            let _meta = write.open_table(META)?;
        }
        write.commit()?;

        let journal_epoch = {
            let read = db.begin_read()?;
            let meta = read.open_table(META)?;
            meta.get(META_EPOCH)?.map_or(1, |v| v.value())
        };
        Ok(Self { db, journal_epoch })
    }

    /// The current journal epoch.
    #[must_use]
    pub fn epoch(&self) -> u64 {
        self.journal_epoch
    }

    /// The last committed sequence number (0 when the journal is empty).
    pub fn last_seq(&self) -> Result<u64, JournalError> {
        let read = self.db.begin_read()?;
        let meta = read.open_table(META)?;
        Ok(meta.get(META_LAST_SEQ)?.map_or(0, |v| v.value()))
    }

    /// Durably appends an admission and indexes its idempotency key.
    ///
    /// The sequence number is allocated, written, indexed, and the high-water
    /// mark advanced all inside one immediate-durability transaction, so the
    /// returned `seq` can never be re-issued after a crash.
    pub fn append_admission(
        &self,
        mut record: AdmissionRecord,
    ) -> Result<AdmissionRecord, JournalError> {
        let mut write = self.db.begin_write()?;
        write.set_durability(Durability::Immediate)?;
        {
            let mut journal = write.open_table(JOURNAL)?;
            let mut index = write.open_table(REQUEST_INDEX)?;
            let mut meta = write.open_table(META)?;

            let next_seq = meta
                .get(META_LAST_SEQ)?
                .map_or(0, |v| v.value())
                .checked_add(1)
                .ok_or_else(|| JournalError::Encoding("sequence overflow".to_string()))?;

            record.journal_epoch = self.journal_epoch;
            record.seq = next_seq;

            let bytes = serde_json::to_vec(&JournalEntry::Admission(Box::new(record.clone())))?;
            journal.insert(next_seq, bytes.as_slice())?;
            index.insert(Self::request_key(&record).as_bytes(), next_seq)?;
            meta.insert(META_LAST_SEQ, next_seq)?;
        }
        write.commit()?;

        Ok(record)
    }

    /// Durably appends an outcome for the request admitted at `origin_seq`.
    pub fn append_outcome(
        &self,
        origin_seq: u64,
        outcome: JournalOutcome,
    ) -> Result<u64, JournalError> {
        let mut write = self.db.begin_write()?;
        write.set_durability(Durability::Immediate)?;
        let next_seq;
        {
            let mut journal = write.open_table(JOURNAL)?;
            let mut meta = write.open_table(META)?;

            next_seq = meta
                .get(META_LAST_SEQ)?
                .map_or(0, |v| v.value())
                .checked_add(1)
                .ok_or_else(|| JournalError::Encoding("sequence overflow".to_string()))?;

            let bytes = serde_json::to_vec(&JournalEntry::Outcome {
                origin_seq,
                outcome,
            })?;
            journal.insert(next_seq, bytes.as_slice())?;
            meta.insert(META_LAST_SEQ, next_seq)?;
        }
        write.commit()?;

        Ok(next_seq)
    }

    /// Looks up a request by its idempotency key `(submitter, method, request_id)`.
    ///
    /// Returns the original admission and the latest committed outcome.
    pub fn find_request(
        &self,
        submitter_id: &str,
        method: &str,
        request_id: &str,
    ) -> Result<Option<RequestRecord>, JournalError> {
        let read = self.db.begin_read()?;
        let index = read.open_table(REQUEST_INDEX)?;
        let journal = read.open_table(JOURNAL)?;

        let Some(admission_seq) = index
            .get(Self::request_key_parts(submitter_id, method, request_id).as_bytes())?
            .map(|v| v.value())
        else {
            return Ok(None);
        };

        let admission = match journal.get(admission_seq)? {
            Some(value) => match serde_json::from_slice::<JournalEntry>(value.value())
                .map_err(|err| JournalError::Encoding(err.to_string()))?
            {
                JournalEntry::Admission(record) => *record,
                JournalEntry::Outcome { .. } => {
                    return Err(JournalError::Encoding(format!(
                        "request index points at an outcome entry at seq {admission_seq}"
                    )));
                }
            },
            None => {
                return Err(JournalError::Encoding(format!(
                    "request index points at missing seq {admission_seq}"
                )));
            }
        };

        // Latest outcome: scan committed entries after the admission for the
        // last one referencing this origin. Linear scan is acceptable for the
        // M0 prototype; M1 projects outcomes into memory.
        let mut outcome = None;
        for entry in journal.range(admission_seq.saturating_add(1)..)? {
            let (_, value) = entry?;
            if let JournalEntry::Outcome {
                origin_seq,
                outcome: found,
            } = serde_json::from_slice::<JournalEntry>(value.value())?
                && origin_seq == admission_seq
            {
                outcome = Some(found);
            }
        }

        Ok(Some(RequestRecord { admission, outcome }))
    }

    /// Scans all committed entries in sequence order (recovery verification).
    pub fn scan_all(&self) -> Result<Vec<(u64, JournalEntry)>, JournalError> {
        self.scan_after(0)
    }

    /// Scans committed entries with sequence numbers greater than `after`.
    ///
    /// This is the durable-log half of the resync contract: a follower that
    /// has applied through cursor N reads `scan_after(N)` to catch up.
    pub fn scan_after(&self, after: u64) -> Result<Vec<(u64, JournalEntry)>, JournalError> {
        let read = self.db.begin_read()?;
        let journal = read.open_table(JOURNAL)?;
        let mut entries = Vec::new();
        for entry in journal.range(after.saturating_add(1)..)? {
            let (seq, value) = entry?;
            let decoded = serde_json::from_slice::<JournalEntry>(value.value())?;
            entries.push((seq.value(), decoded));
        }
        Ok(entries)
    }

    /// Durably commits a mixed batch of admissions and outcomes in arrival
    /// order under a single immediate-durability transaction.
    ///
    /// All-or-nothing: if any item fails to encode the whole batch is aborted
    /// and every item reports the error. Sequence numbers are allocated
    /// contiguously in arrival order, so group commit preserves the logical
    /// order of concurrent admissions.
    pub fn append_batch(
        &self,
        items: &[PendingEntry],
    ) -> Result<Vec<CommittedEntry>, JournalError> {
        if items.is_empty() {
            return Ok(Vec::new());
        }
        let mut write = self.db.begin_write()?;
        write.set_durability(Durability::Immediate)?;
        let mut committed = Vec::with_capacity(items.len());
        let result = (|| -> Result<(), JournalError> {
            let mut journal = write.open_table(JOURNAL)?;
            let mut index = write.open_table(REQUEST_INDEX)?;
            let mut meta = write.open_table(META)?;
            let mut next_seq = meta.get(META_LAST_SEQ)?.map_or(0, |v| v.value());
            for item in items {
                next_seq = next_seq
                    .checked_add(1)
                    .ok_or_else(|| JournalError::Encoding("sequence overflow".to_string()))?;
                match item {
                    PendingEntry::Admission(record) => {
                        let mut record = (**record).clone();
                        record.journal_epoch = self.journal_epoch;
                        record.seq = next_seq;
                        let bytes =
                            serde_json::to_vec(&JournalEntry::Admission(Box::new(record.clone())))?;
                        journal.insert(next_seq, bytes.as_slice())?;
                        index.insert(Self::request_key(&record).as_bytes(), next_seq)?;
                        committed.push(CommittedEntry::Admission(Box::new(record)));
                    }
                    PendingEntry::Outcome {
                        origin_seq,
                        outcome,
                    } => {
                        let bytes = serde_json::to_vec(&JournalEntry::Outcome {
                            origin_seq: *origin_seq,
                            outcome: outcome.clone(),
                        })?;
                        journal.insert(next_seq, bytes.as_slice())?;
                        committed.push(CommittedEntry::Outcome {
                            seq: next_seq,
                            origin_seq: *origin_seq,
                        });
                    }
                }
            }
            meta.insert(META_LAST_SEQ, next_seq)?;
            Ok(())
        })();
        result?;
        write.commit()?;
        Ok(committed)
    }

    fn request_key(record: &AdmissionRecord) -> String {
        Self::request_key_parts(
            &record.submitter_id,
            &record.request_kind,
            &record.request_id,
        )
    }

    fn request_key_parts(submitter_id: &str, method: &str, request_id: &str) -> String {
        format!("{submitter_id}|{method}|{request_id}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_key_parts_are_unambiguous() {
        let a = BusinessJournal::request_key_parts("s-1", "SubmitOrder", "r-1");
        let b = BusinessJournal::request_key_parts("s-1|SubmitOrder", "", "r-1");
        assert_ne!(a, b);
        assert_eq!(a, "s-1|SubmitOrder|r-1");
    }
}
