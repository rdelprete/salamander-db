//! First-class stream and append-batch contracts.

use std::collections::HashMap;

use crate::format::{
    BatchId, BranchId, EventId, EventType, Metadata, OwnedStoredRecord, StreamId, StreamRevision,
};
use crate::{Result, SalamanderError};

/// The user-facing name of a stream within a branch (UTF-8, non-empty, no
/// NUL bytes, at most [`StreamName::MAX_BYTES`]). The engine maps it to a
/// stable compact `StreamId` internally.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamName(String);

impl StreamName {
    /// Maximum length of a stream name, in bytes.
    pub const MAX_BYTES: usize = 1024;

    /// Validates and constructs a stream name, rejecting empty, oversized,
    /// or NUL-containing input.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.is_empty() {
            return Err(SalamanderError::InvalidArgument(
                "stream name must not be empty".into(),
            ));
        }
        if value.len() > Self::MAX_BYTES {
            return Err(SalamanderError::ResourceLimit {
                resource: "stream name bytes",
                actual: value.len() as u64,
                maximum: Self::MAX_BYTES as u64,
            });
        }
        if value.as_bytes().contains(&0) {
            return Err(SalamanderError::InvalidArgument(
                "stream name must not contain NUL".into(),
            ));
        }
        Ok(Self(value))
    }

    /// The name as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for StreamName {
    type Error = SalamanderError;

    fn try_from(value: &str) -> Result<Self> {
        Self::new(value)
    }
}

/// Optimistic-concurrency expectation checked against a stream's current
/// revision, in the same critical section that assigns positions. A failed
/// expectation appends nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpectedRevision {
    /// Append unconditionally.
    Any,
    /// Require that the stream does not yet exist.
    NoStream,
    /// Require the stream's current revision to equal this value.
    Exact(StreamRevision),
}

/// A caller-supplied key identifying an append command, scoped to a
/// database and branch, so that a retry with identical content is
/// idempotent (non-empty, at most [`IdempotencyKey::MAX_BYTES`]).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IdempotencyKey(Vec<u8>);

impl IdempotencyKey {
    /// Maximum length of an idempotency key, in bytes.
    pub const MAX_BYTES: usize = 1024;

    /// Validates and constructs an idempotency key, rejecting empty or
    /// oversized input.
    pub fn new(value: impl Into<Vec<u8>>) -> Result<Self> {
        let value = value.into();
        if value.is_empty() {
            return Err(SalamanderError::InvalidArgument(
                "idempotency key must not be empty".into(),
            ));
        }
        if value.len() > Self::MAX_BYTES {
            return Err(SalamanderError::ResourceLimit {
                resource: "idempotency key bytes",
                actual: value.len() as u64,
                maximum: Self::MAX_BYTES as u64,
            });
        }
        Ok(Self(value))
    }

    /// The key as a byte slice.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// The durability level requested for an append. A stronger level includes
/// the guarantees of the weaker ones; see the crate's durability contract
/// for the per-platform survival matrix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// Return after bytes are accepted into engine-managed memory; no
    /// promise of surviving process or power loss.
    Buffered,
    /// Submit all bytes to the operating system before returning; not
    /// promised to survive OS or power loss.
    Flush,
    /// Invoke the strongest supported file-data synchronization before
    /// returning; survives abrupt process termination under the documented
    /// model.
    Sync,
}

/// The durability actually achieved for an append, as reported on the
/// [`AppendReceipt`]. The receipt states this rather than leaving the
/// caller to infer it from the method used.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ReceiptDurability {
    /// Accepted into memory only.
    Buffered,
    /// Handed to the operating system.
    Flushed,
    /// Synchronized to durable storage.
    Synced,
}

/// One event to be appended, carrying a typed payload `B`.
#[derive(Debug, Clone)]
pub struct NewEvent<B> {
    /// Caller-supplied event identity; if `None`, the engine generates one.
    pub event_id: Option<EventId>,
    /// The event's type tag.
    pub event_type: EventType,
    /// Schema version of the payload under `event_type`.
    pub schema_version: u32,
    /// Engine metadata attached to the event (reserved keys are prefixed
    /// `salamander.`).
    pub metadata: Metadata,
    /// The application payload.
    pub body: B,
}

impl<B> NewEvent<B> {
    /// Creates an event with a generated ID, schema version 1, and no
    /// metadata.
    pub fn new(event_type: EventType, body: B) -> Self {
        Self {
            event_id: None,
            event_type,
            schema_version: 1,
            metadata: Metadata::new(),
            body,
        }
    }
}

/// One append command: a batch of events for a single stream, appended
/// atomically (all-or-nothing) under an optimistic-concurrency expectation
/// and a durability level.
#[derive(Debug, Clone)]
pub struct AppendRequest<B> {
    /// Branch to append to.
    pub branch: BranchId,
    /// Target stream within the branch.
    pub stream: StreamName,
    /// Optimistic-concurrency expectation validated before any write.
    pub expected: ExpectedRevision,
    /// Optional key making a retry of this command idempotent.
    pub idempotency_key: Option<IdempotencyKey>,
    /// The events, in order; must be non-empty and at most
    /// [`AppendRequest::MAX_EVENTS`].
    pub events: Vec<NewEvent<B>>,
    /// Durability level to satisfy before returning the receipt.
    pub durability: Durability,
}

impl<B> AppendRequest<B> {
    /// Maximum number of events in a single batch.
    pub const MAX_EVENTS: usize = 4096;

    /// Checks the batch is non-empty and within [`Self::MAX_EVENTS`].
    pub fn validate(&self) -> Result<()> {
        if self.events.is_empty() {
            return Err(SalamanderError::InvalidArgument(
                "append batch must contain at least one event".into(),
            ));
        }
        if self.events.len() > Self::MAX_EVENTS {
            return Err(SalamanderError::ResourceLimit {
                resource: "batch event count",
                actual: self.events.len() as u64,
                maximum: Self::MAX_EVENTS as u64,
            });
        }
        Ok(())
    }
}

/// The result of a successful append: the assigned positions, resulting
/// stream revision, and the durability that was achieved.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AppendReceipt {
    /// Stable identity of the appended batch.
    pub batch_id: BatchId,
    /// Global position of the first event in the batch.
    pub first_position: u64,
    /// Global position of the last event in the batch.
    pub last_position: u64,
    /// Stable engine identity of the target stream.
    pub stream_id: StreamId,
    /// The stream's revision before this batch, or `None` if it was new.
    pub previous_revision: Option<StreamRevision>,
    /// The stream's revision after this batch.
    pub current_revision: StreamRevision,
    /// Durability guaranteed at the time the receipt was returned.
    pub durability: ReceiptDurability,
}

#[derive(Default, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct StreamCatalog {
    streams: HashMap<(BranchId, String), StreamEntry>,
    event_ids: HashMap<EventId, (u32, BatchId)>,
    idempotency: HashMap<(BranchId, Vec<u8>), IdempotencyEntry>,
    receipts: HashMap<BatchId, AppendReceipt>,
}

#[derive(Clone, Copy, serde::Serialize, serde::Deserialize)]
struct StreamEntry {
    id: StreamId,
    last_revision: StreamRevision,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct IdempotencyEntry {
    digest: u32,
    receipt: AppendReceipt,
}

impl StreamCatalog {
    pub(crate) fn rebuild(
        records: impl Iterator<Item = Result<OwnedStoredRecord>>,
    ) -> Result<Self> {
        let mut catalog = Self::default();
        let mut batches: HashMap<BatchId, AppendReceipt> = HashMap::new();
        for item in records {
            let record = item?;
            let name = stream_name(&record)?;
            let key = (record.envelope.branch_id, name.clone());
            let entry = catalog.streams.entry(key).or_insert(StreamEntry {
                id: record.envelope.stream_id,
                last_revision: record.envelope.stream_revision,
            });
            if entry.id != record.envelope.stream_id {
                return Err(SalamanderError::Corrupt {
                    offset: record.position,
                    reason: "stream name maps to multiple stream IDs".into(),
                });
            }
            if record.envelope.stream_revision > entry.last_revision {
                entry.last_revision = record.envelope.stream_revision;
            }
            let event_digest = event_fingerprint(&record);
            if let Some(previous) = catalog.event_ids.insert(
                record.envelope.event_id,
                (event_digest, record.envelope.batch_id),
            ) {
                if previous.0 != event_digest {
                    return Err(SalamanderError::EventIdConflict);
                }
            }

            let receipt =
                batches
                    .entry(record.envelope.batch_id)
                    .or_insert_with(|| AppendReceipt {
                        batch_id: record.envelope.batch_id,
                        first_position: record.position,
                        last_position: record.position,
                        stream_id: record.envelope.stream_id,
                        previous_revision: record
                            .envelope
                            .stream_revision
                            .0
                            .checked_sub(1)
                            .map(StreamRevision),
                        current_revision: record.envelope.stream_revision,
                        durability: ReceiptDurability::Buffered,
                    });
            receipt.last_position = record.position;
            receipt.current_revision = record.envelope.stream_revision;

            if let (Some(idempotency_key), Some(digest_bytes)) = (
                record.envelope.metadata.get("salamander.idempotency_key"),
                record.envelope.metadata.get("salamander.request_digest"),
            ) {
                if digest_bytes.len() == 4 {
                    let digest = u32::from_le_bytes(digest_bytes.as_slice().try_into().unwrap());
                    catalog.idempotency.insert(
                        (record.envelope.branch_id, idempotency_key.clone()),
                        IdempotencyEntry {
                            digest,
                            receipt: receipt.clone(),
                        },
                    );
                }
            }
        }
        catalog.receipts = batches;
        Ok(catalog)
    }

    pub(crate) fn revision(&self, branch: BranchId, stream: &StreamName) -> Option<StreamRevision> {
        self.streams
            .get(&(branch, stream.as_str().to_string()))
            .map(|entry| entry.last_revision)
    }

    pub(crate) fn stream_id(&self, branch: BranchId, stream: &StreamName) -> Option<StreamId> {
        self.streams
            .get(&(branch, stream.as_str().to_string()))
            .map(|entry| entry.id)
    }

    pub(crate) fn event_digest(&self, id: EventId) -> Option<u32> {
        self.event_ids.get(&id).map(|entry| entry.0)
    }

    pub(crate) fn event_receipt(&self, id: EventId) -> Option<(u32, AppendReceipt)> {
        let (digest, batch) = self.event_ids.get(&id)?;
        Some((*digest, self.receipts.get(batch)?.clone()))
    }

    pub(crate) fn batch_receipt(&self, id: BatchId) -> Option<AppendReceipt> {
        self.receipts.get(&id).cloned()
    }

    pub(crate) fn idempotent(
        &self,
        branch: BranchId,
        key: &IdempotencyKey,
    ) -> Option<(u32, AppendReceipt)> {
        self.idempotency
            .get(&(branch, key.as_bytes().to_vec()))
            .map(|entry| (entry.digest, entry.receipt.clone()))
    }

    pub(crate) fn record_batch(
        &mut self,
        branch: BranchId,
        stream: &StreamName,
        stream_id: StreamId,
        event_digests: impl IntoIterator<Item = (EventId, u32)>,
        idempotency: Option<(&IdempotencyKey, u32)>,
        receipt: AppendReceipt,
    ) {
        self.streams.insert(
            (branch, stream.as_str().to_string()),
            StreamEntry {
                id: stream_id,
                last_revision: receipt.current_revision,
            },
        );
        self.event_ids.extend(
            event_digests
                .into_iter()
                .map(|(id, digest)| (id, (digest, receipt.batch_id))),
        );
        self.receipts.insert(receipt.batch_id, receipt.clone());
        if let Some((key, digest)) = idempotency {
            self.idempotency.insert(
                (branch, key.as_bytes().to_vec()),
                IdempotencyEntry { digest, receipt },
            );
        }
    }
}

fn stream_name(record: &OwnedStoredRecord) -> Result<String> {
    let bytes = record
        .envelope
        .metadata
        .get("salamander.stream_name")
        .ok_or_else(|| SalamanderError::Corrupt {
            offset: record.position,
            reason: "event is missing stream name metadata".into(),
        })?;
    std::str::from_utf8(bytes)
        .map(str::to_owned)
        .map_err(|_| SalamanderError::Corrupt {
            offset: record.position,
            reason: "stream name metadata is not UTF-8".into(),
        })
}

pub(crate) fn event_fingerprint(record: &OwnedStoredRecord) -> u32 {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(record.envelope.event_type.as_str().as_bytes());
    bytes.extend_from_slice(&record.envelope.schema_version.to_le_bytes());
    for (key, value) in &record.envelope.metadata {
        if !key.starts_with("salamander.") {
            bytes.extend_from_slice(key.as_bytes());
            bytes.extend_from_slice(value);
        }
    }
    bytes.extend_from_slice(&record.payload);
    crc32c::crc32c(&bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_and_idempotency_keys_validate_before_storage() {
        assert!(StreamName::new("").is_err());
        assert!(StreamName::new("a\0b").is_err());
        assert!(StreamName::new("a".repeat(StreamName::MAX_BYTES + 1)).is_err());
        assert!(IdempotencyKey::new(Vec::<u8>::new()).is_err());
        assert!(IdempotencyKey::new(vec![0; IdempotencyKey::MAX_BYTES + 1]).is_err());
    }

    #[test]
    fn empty_and_oversized_batches_are_rejected() {
        let request: AppendRequest<()> = AppendRequest {
            branch: BranchId::ZERO,
            stream: StreamName::new("s").unwrap(),
            expected: ExpectedRevision::Any,
            idempotency_key: None,
            events: Vec::new(),
            durability: Durability::Buffered,
        };
        assert!(request.validate().is_err());

        let mut request = request;
        request.events = (0..=AppendRequest::<()>::MAX_EVENTS)
            .map(|_| NewEvent::new(EventType::new("test").unwrap(), ()))
            .collect();
        assert!(request.validate().is_err());
    }
}
