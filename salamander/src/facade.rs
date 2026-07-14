//! Thread-safe, non-generic service boundary for native-language bindings.
//!
//! This is a `#[doc(hidden)]` layer: the DTOs and handles here are the
//! FFI/binding substrate and the current home of the committed-batch feed,
//! not the stable typed Rust API. `missing_docs` is intentionally relaxed
//! for this module.
#![allow(missing_docs)]

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::ops::Bound;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::{
    AppendReceipt, AppendRequest, BatchId, BranchId, BranchInfo, BranchName, BranchStatus, CodecId,
    CommitPolicy, Durability, EventId, EventType, ExpectedRevision, IdempotencyKey, Metadata,
    NewEvent, OwnedStoredRecord, ReceiptDurability, RecordEnvelopeV2, RecordReader, ReplayEnd,
    ReplayPlan, Salamander, SalamanderError, StreamId, StreamName, StreamRevision, StreamSelector,
};

pub const MAX_FACADE_PAYLOAD_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_FACADE_BATCH_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_REPLAY_PAGE_EVENTS: u32 = 4096;
pub const MAX_REPLAY_PAGE_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCategory {
    InvalidArgument,
    Conflict,
    NotFound,
    Locked,
    Corruption,
    UnsupportedFormat,
    Codec,
    Io,
    Cancelled,
    ResourceLimit,
    Internal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineError {
    pub category: ErrorCategory,
    pub code: &'static str,
    pub message: String,
}

impl EngineError {
    fn closed() -> Self {
        Self {
            category: ErrorCategory::Cancelled,
            code: "engine_closed",
            message: "engine handle is closed".into(),
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            category: ErrorCategory::Internal,
            code: "internal",
            message: message.into(),
        }
    }
}

impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for EngineError {}

impl From<SalamanderError> for EngineError {
    fn from(error: SalamanderError) -> Self {
        use ErrorCategory as C;
        let (category, code) = match &error {
            SalamanderError::InvalidArgument(_) | SalamanderError::NotBatchBoundary(_) => {
                (C::InvalidArgument, "invalid_argument")
            }
            SalamanderError::RevisionConflict { .. }
            | SalamanderError::EventIdConflict
            | SalamanderError::IdempotencyConflict
            | SalamanderError::BatchIdConflict
            | SalamanderError::BranchExists(_)
            | SalamanderError::NamespaceExists(_) => (C::Conflict, "conflict"),
            SalamanderError::BranchNotFound(_) => (C::NotFound, "not_found"),
            SalamanderError::Locked(_) => (C::Locked, "locked"),
            SalamanderError::Corrupt { .. }
            | SalamanderError::Manifest(_)
            | SalamanderError::InvalidFormat(_)
            | SalamanderError::InvalidSegmentName(_)
            | SalamanderError::InvalidBranchAncestry(_) => (C::Corruption, "corruption"),
            SalamanderError::UnsupportedFormat { .. }
            | SalamanderError::UnsupportedStorageFormat { .. } => {
                (C::UnsupportedFormat, "unsupported_format")
            }
            SalamanderError::Codec(_) | SalamanderError::Serialization(_) => (C::Codec, "codec"),
            SalamanderError::Io(_) => (C::Io, "io"),
            SalamanderError::ResourceLimit { .. } => (C::ResourceLimit, "resource_limit"),
            SalamanderError::OffsetBeyondHead(_) => (C::InvalidArgument, "offset_beyond_head"),
            SalamanderError::BranchArchived(_) => (C::Conflict, "branch_archived"),
            SalamanderError::Migration(_) | SalamanderError::MigrationIncomplete(_) => {
                (C::Internal, "migration")
            }
        };
        Self {
            category,
            code,
            message: error.to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PayloadCodec {
    Bytes,
    Json,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EngineEvent {
    codec: PayloadCodec,
    bytes: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct EngineOptions {
    pub path: PathBuf,
    pub commit_every_bytes: Option<u64>,
    pub commit_every_count: Option<u64>,
    pub commit_every_millis: Option<u64>,
    pub snapshot_every_events: Option<u64>,
    pub snapshot_every_bytes: Option<u64>,
    pub snapshot_every_millis: Option<u64>,
}

impl EngineOptions {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            commit_every_bytes: None,
            commit_every_count: None,
            commit_every_millis: None,
            snapshot_every_events: None,
            snapshot_every_bytes: None,
            snapshot_every_millis: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpectedRevisionDto {
    Any,
    NoStream,
    Exact(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurabilityDto {
    Buffered,
    Flush,
    Sync,
}

#[derive(Debug, Clone)]
pub struct EventData {
    pub event_id: Option<[u8; 16]>,
    pub event_type: String,
    pub schema_version: u32,
    pub metadata: BTreeMap<String, Vec<u8>>,
    pub codec: PayloadCodec,
    pub payload: Vec<u8>,
}

impl EventData {
    pub fn json(payload: Vec<u8>) -> Self {
        Self {
            event_id: None,
            event_type: "application.json".into(),
            schema_version: 1,
            metadata: Metadata::new(),
            codec: PayloadCodec::Json,
            payload,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AppendBatch {
    pub branch_id: [u8; 16],
    pub stream: String,
    pub expected: ExpectedRevisionDto,
    pub idempotency_key: Option<Vec<u8>>,
    pub events: Vec<EventData>,
    pub durability: DurabilityDto,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendReceiptDto {
    pub batch_id: [u8; 16],
    pub first_position: u64,
    pub last_position: u64,
    pub stream_id: [u8; 16],
    pub previous_revision: Option<u64>,
    pub current_revision: u64,
    pub durability: DurabilityDto,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchDto {
    pub id: [u8; 16],
    pub name: String,
    pub parent_id: Option<[u8; 16]>,
    pub fork_position: Option<u64>,
    pub created_at_unix_nanos: i64,
    pub metadata: BTreeMap<String, Vec<u8>>,
    pub archived: bool,
}

#[derive(Debug, Clone)]
pub struct ReplayRequest {
    pub branch_id: [u8; 16],
    pub stream: Option<String>,
    pub from: u64,
    pub until: Option<u64>,
    pub page_events: u32,
    pub page_bytes: usize,
}

impl Default for ReplayRequest {
    fn default() -> Self {
        Self {
            branch_id: [0; 16],
            stream: None,
            from: 0,
            until: None,
            page_events: 256,
            page_bytes: 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReaderHandle(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct QueryHandle(pub u64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordDto {
    pub database_id: [u8; 16],
    pub batch_id: [u8; 16],
    pub batch_index: u32,
    pub position: u64,
    pub timestamp_unix_nanos: i64,
    pub event_id: [u8; 16],
    pub branch_id: [u8; 16],
    pub stream_id: [u8; 16],
    pub stream_revision: u64,
    pub event_type: String,
    pub schema_version: u32,
    pub metadata: BTreeMap<String, Vec<u8>>,
    pub codec: PayloadCodec,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayPage {
    pub records: Vec<RecordDto>,
    pub continuation: u64,
    pub done: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
/// Selects committed batches without changing their original boundaries.
pub struct FeedFilter {
    pub branches: Vec<[u8; 16]>,
    pub streams: Vec<[u8; 16]>,
    pub event_types: Vec<String>,
}

#[derive(Debug, Clone)]
/// Configuration for a bounded durable-batch feed.
pub struct FeedRequest {
    pub from: Option<u64>,
    pub consumer_id: Option<String>,
    pub filter: FeedFilter,
    pub page_batches: u32,
    pub page_bytes: usize,
}

impl Default for FeedRequest {
    fn default() -> Self {
        Self {
            from: Some(0),
            consumer_id: None,
            filter: FeedFilter::default(),
            page_batches: 128,
            page_bytes: 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
/// Sequencer-owned identifier for an open feed.
pub struct FeedHandle(pub u64);

#[derive(Debug, Clone, PartialEq, Eq)]
/// One immutable batch as it appeared in the source database.
pub struct CommittedBatch {
    pub database_id: [u8; 16],
    pub batch_id: [u8; 16],
    pub first_position: u64,
    pub last_position: u64,
    pub branch_id: [u8; 16],
    pub stream_ids: Vec<[u8; 16]>,
    pub events: Vec<RecordDto>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// A bounded feed response and its exclusive resume position.
pub struct FeedPage {
    pub batches: Vec<CommittedBatch>,
    pub continuation: u64,
    pub durable_head: u64,
    pub timed_out: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ConsumerCheckpoint {
    consumer_id: String,
    position: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryDefinition {
    pub key_field: String,
    pub indexes: BTreeMap<String, String>,
    pub filter: Option<(String, Vec<u8>)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryOperation {
    Get(String),
    By { index: String, key: Vec<u8> },
    Range { start: String, end: String },
    Prefix(String),
    Len,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryResult {
    pub rows: Vec<Vec<u8>>,
    pub len: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputType {
    pub event_type: String,
    pub min_schema_version: u32,
    pub max_schema_version: u32,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionScope {
    pub branch_id: [u8; 16],
    pub stream: Option<String>,
}

/// Versioned envelope-only routing for independently recoverable projection state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionScheme {
    pub scheme_id: String,
    pub version: u32,
    pub partition_count: u32,
}

impl Default for PartitionScheme {
    fn default() -> Self {
        Self {
            scheme_id: "stream-id-modulo".into(),
            version: 1,
            partition_count: 1,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionDescriptor {
    pub name: String,
    pub definition_id: [u8; 16],
    pub definition_version: u32,
    pub input_types: Vec<InputType>,
    pub state_codec: u32,
    pub state_codec_version: u32,
    pub scope: ProjectionScope,
    #[serde(default)]
    pub partition_scheme: PartitionScheme,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionCursor {
    pub database_id: [u8; 16],
    pub branch_id: [u8; 16],
    pub position: u64,
    pub descriptor_fingerprint: [u8; 16],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionFailure {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StaleReason {
    DescriptorChanged,
    BehindHead { head: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectionStatus {
    Building {
        cursor: ProjectionCursor,
    },
    Ready {
        cursor: ProjectionCursor,
    },
    Stale {
        cursor: ProjectionCursor,
        reason: StaleReason,
    },
    Failed {
        cursor: ProjectionCursor,
        error: ProjectionFailure,
    },
    Dropping,
}

/// Recovery state for one deterministic projection partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionStatus {
    Cold {
        cursor: ProjectionCursor,
    },
    Healing {
        cursor: ProjectionCursor,
    },
    Ready {
        cursor: ProjectionCursor,
    },
    Stale {
        cursor: ProjectionCursor,
        reason: StaleReason,
    },
    Failed {
        cursor: ProjectionCursor,
        error: ProjectionFailure,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryConsistency {
    RequireHead,
    AllowStale,
    WaitFor(u64),
}

pub trait ProjectionRuntime: Send {
    fn reset(&mut self) -> Result<(), ProjectionFailure>;
    fn apply(&mut self, record: &RecordDto) -> Result<(), ProjectionFailure>;
    fn query(&self, operation: QueryOperation) -> Result<QueryResult, ProjectionFailure>;
    fn checkpoint(&self) -> Result<Vec<u8>, ProjectionFailure> {
        Err(projection_failure(
            "checkpoint_unsupported",
            "runtime has no checkpoint codec",
        ))
    }
    fn restore_checkpoint(&mut self, _state: &[u8]) -> Result<(), ProjectionFailure> {
        Err(projection_failure(
            "checkpoint_unsupported",
            "runtime has no checkpoint codec",
        ))
    }
    fn checkpoint_partition(
        &self,
        partition: u32,
        partition_count: u32,
    ) -> Result<Vec<u8>, ProjectionFailure> {
        if partition == 0 && partition_count == 1 {
            self.checkpoint()
        } else {
            Err(projection_failure(
                "partition_unsupported",
                "runtime does not implement partition checkpoints",
            ))
        }
    }
    fn restore_partition(
        &mut self,
        partition: u32,
        partition_count: u32,
        state: &[u8],
    ) -> Result<(), ProjectionFailure> {
        if partition == 0 && partition_count == 1 {
            self.restore_checkpoint(state)
        } else {
            Err(projection_failure(
                "partition_unsupported",
                "runtime does not implement partition checkpoints",
            ))
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DurableProjectionRegistration {
    descriptor: ProjectionDescriptor,
    definition: Option<QueryDefinition>,
}

#[derive(Clone)]
pub struct Engine {
    inner: Arc<EngineInner>,
}

struct EngineInner {
    sender: mpsc::Sender<Command>,
    closed: AtomicBool,
    join: Mutex<Option<JoinHandle<()>>>,
    feed_signal: Arc<FeedSignal>,
}

struct FeedSignal {
    durable_head: AtomicU64,
    generation: Mutex<u64>,
    changed: Condvar,
}

impl FeedSignal {
    fn publish(&self, durable_head: u64) {
        let previous = self.durable_head.swap(durable_head, Ordering::AcqRel);
        if previous == durable_head {
            return;
        }
        if let Ok(mut generation) = self.generation.lock() {
            *generation = generation.wrapping_add(1);
            self.changed.notify_all();
        }
    }

    fn notify(&self) {
        if let Ok(mut generation) = self.generation.lock() {
            *generation = generation.wrapping_add(1);
            self.changed.notify_all();
        }
    }
}

impl Drop for EngineInner {
    fn drop(&mut self) {
        self.feed_signal.notify();
        let _ = self.sender.send(Command::Shutdown(None));
        if let Some(join) = self.join.get_mut().ok().and_then(Option::take) {
            let _ = join.join();
        }
    }
}

impl Engine {
    pub fn open(options: EngineOptions) -> Result<Self, EngineError> {
        let (tx, rx) = mpsc::channel();
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let feed_signal = Arc::new(FeedSignal {
            durable_head: AtomicU64::new(0),
            generation: Mutex::new(0),
            changed: Condvar::new(),
        });
        let sequencer_signal = feed_signal.clone();
        let join = thread::Builder::new()
            .name("salamander-sequencer".into())
            .spawn(move || sequencer(options, rx, ready_tx, sequencer_signal))
            .map_err(|error| EngineError::internal(error.to_string()))?;
        ready_rx
            .recv()
            .map_err(|_| EngineError::internal("sequencer stopped during open"))??;
        Ok(Self {
            inner: Arc::new(EngineInner {
                sender: tx,
                closed: AtomicBool::new(false),
                join: Mutex::new(Some(join)),
                feed_signal,
            }),
        })
    }

    fn call<T>(
        &self,
        make: impl FnOnce(mpsc::SyncSender<Result<T, EngineError>>) -> Command,
    ) -> Result<T, EngineError> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(EngineError::closed());
        }
        let (tx, rx) = mpsc::sync_channel(1);
        self.inner
            .sender
            .send(make(tx))
            .map_err(|_| EngineError::closed())?;
        rx.recv().map_err(|_| EngineError::closed())?
    }

    pub fn append(&self, request: AppendBatch) -> Result<AppendReceiptDto, EngineError> {
        self.call(|reply| Command::Append(request, reply))
    }

    pub fn commit(&self) -> Result<u64, EngineError> {
        self.call(Command::Commit)
    }

    pub fn head(&self) -> Result<u64, EngineError> {
        self.call(Command::Head)
    }

    pub fn durable_head(&self) -> Result<u64, EngineError> {
        self.call(Command::DurableHead)
    }

    pub fn uncommitted_count(&self) -> Result<u64, EngineError> {
        self.call(Command::UncommittedCount)
    }

    pub fn fork(
        &self,
        parent: [u8; 16],
        at: u64,
        name: String,
        metadata: BTreeMap<String, Vec<u8>>,
    ) -> Result<BranchDto, EngineError> {
        self.call(|reply| Command::Fork {
            parent,
            at,
            name,
            metadata,
            reply,
        })
    }

    pub fn branch_named(&self, name: String) -> Result<BranchDto, EngineError> {
        self.call(|reply| Command::BranchNamed(name, reply))
    }

    pub fn ancestry(&self, id: [u8; 16]) -> Result<Vec<BranchDto>, EngineError> {
        self.call(|reply| Command::Ancestry(id, reply))
    }

    pub fn archive(&self, id: [u8; 16]) -> Result<BranchDto, EngineError> {
        self.call(|reply| Command::Archive(id, reply))
    }

    pub fn open_reader(&self, request: ReplayRequest) -> Result<ReaderHandle, EngineError> {
        self.call(|reply| Command::OpenReader(request, reply))
    }

    pub fn next_page(&self, handle: ReaderHandle) -> Result<ReplayPage, EngineError> {
        self.call(|reply| Command::NextPage(handle, reply))
    }

    pub fn cancel_reader(&self, handle: ReaderHandle) -> Result<(), EngineError> {
        self.call(|reply| Command::CancelReader(handle, reply))
    }

    pub fn close_reader(&self, handle: ReaderHandle) -> Result<(), EngineError> {
        self.call(|reply| Command::CloseReader(handle, reply))
    }

    pub fn open_feed(&self, request: FeedRequest) -> Result<FeedHandle, EngineError> {
        self.call(|reply| Command::OpenFeed(request, reply))
    }

    pub fn next_feed_page(
        &self,
        handle: FeedHandle,
        wait_millis: Option<u64>,
    ) -> Result<FeedPage, EngineError> {
        let mut page = self.call(|reply| Command::NextFeedPage(handle, reply))?;
        let Some(wait_millis) = wait_millis else {
            return Ok(page);
        };
        if !page.batches.is_empty() || wait_millis == 0 {
            return Ok(page);
        }
        let guard = self
            .inner
            .feed_signal
            .generation
            .lock()
            .map_err(|_| EngineError::internal("feed wait lock poisoned"))?;
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(EngineError::closed());
        }
        if self.inner.feed_signal.durable_head.load(Ordering::Acquire) <= page.continuation {
            let (_guard, timeout) = self
                .inner
                .feed_signal
                .changed
                .wait_timeout(guard, Duration::from_millis(wait_millis))
                .map_err(|_| EngineError::internal("feed wait lock poisoned"))?;
            if timeout.timed_out() {
                page.timed_out = true;
                return Ok(page);
            }
        }
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(EngineError::closed());
        }
        self.call(|reply| Command::NextFeedPage(handle, reply))
    }

    pub fn acknowledge_feed(&self, handle: FeedHandle) -> Result<u64, EngineError> {
        self.call(|reply| Command::AcknowledgeFeed(handle, reply))
    }

    pub fn cancel_feed(&self, handle: FeedHandle) -> Result<(), EngineError> {
        let result = self.call(|reply| Command::CancelFeed(handle, reply));
        self.inner.feed_signal.notify();
        result
    }

    pub fn close_feed(&self, handle: FeedHandle) -> Result<(), EngineError> {
        let result = self.call(|reply| Command::CloseFeed(handle, reply));
        self.inner.feed_signal.notify();
        result
    }

    pub fn clear_consumer_checkpoint(&self, consumer_id: String) -> Result<(), EngineError> {
        self.call(|reply| Command::ClearConsumerCheckpoint(consumer_id, reply))
    }

    pub fn ingest_batch(&self, batch: CommittedBatch) -> Result<AppendReceiptDto, EngineError> {
        self.call(|reply| Command::IngestBatch(batch, reply))
    }

    pub fn register_query(
        &self,
        name: String,
        definition: QueryDefinition,
    ) -> Result<QueryHandle, EngineError> {
        self.call(|reply| Command::RegisterQuery(name, definition, reply))
    }

    pub fn register_partitioned_query(
        &self,
        name: String,
        definition: QueryDefinition,
        partition_count: u32,
    ) -> Result<QueryHandle, EngineError> {
        self.call(|reply| {
            Command::RegisterPartitionedQuery(name, definition, partition_count, reply)
        })
    }

    pub fn register_runtime(
        &self,
        descriptor: ProjectionDescriptor,
        runtime: Box<dyn ProjectionRuntime>,
    ) -> Result<QueryHandle, EngineError> {
        self.call(|reply| Command::RegisterRuntime(descriptor, runtime, reply))
    }

    pub fn remove_query(&self, name: String) -> Result<bool, EngineError> {
        self.call(|reply| Command::RemoveQuery(name, reply))
    }

    pub fn query_named(&self, name: String) -> Result<QueryHandle, EngineError> {
        self.call(|reply| Command::QueryNamed(name, reply))
    }

    pub fn query(
        &self,
        handle: QueryHandle,
        operation: QueryOperation,
    ) -> Result<QueryResult, EngineError> {
        self.query_consistent(handle, operation, QueryConsistency::RequireHead)
    }

    pub fn query_consistent(
        &self,
        handle: QueryHandle,
        operation: QueryOperation,
        consistency: QueryConsistency,
    ) -> Result<QueryResult, EngineError> {
        self.call(|reply| Command::Query(handle, operation, consistency, reply))
    }

    /// Query after healing only the named partitions. Callers must route the
    /// operation correctly; unrouteable scans should use `query`.
    pub fn query_partitions(
        &self,
        handle: QueryHandle,
        partitions: Vec<u32>,
        operation: QueryOperation,
        consistency: QueryConsistency,
    ) -> Result<QueryResult, EngineError> {
        self.call(|reply| {
            Command::QueryPartitions(handle, partitions, operation, consistency, reply)
        })
    }

    pub fn projection_status(&self, handle: QueryHandle) -> Result<ProjectionStatus, EngineError> {
        self.call(|reply| Command::ProjectionStatus(handle, reply))
    }

    pub fn partition_status(
        &self,
        handle: QueryHandle,
    ) -> Result<Vec<PartitionStatus>, EngineError> {
        self.call(|reply| Command::PartitionStatus(handle, reply))
    }

    pub fn create_snapshot(&self, handle: QueryHandle) -> Result<crate::SnapshotInfo, EngineError> {
        self.call(|reply| Command::CreateSnapshot(handle, reply))
    }

    pub fn create_partition_snapshot(
        &self,
        handle: QueryHandle,
        partition: u32,
    ) -> Result<crate::SnapshotInfo, EngineError> {
        self.call(|reply| Command::CreatePartitionSnapshot(handle, partition, reply))
    }

    pub fn list_snapshots(
        &self,
        handle: QueryHandle,
    ) -> Result<Vec<crate::SnapshotInfo>, EngineError> {
        self.call(|reply| Command::ListSnapshots(handle, reply))
    }

    pub fn verify_snapshot(&self, id: String) -> Result<crate::SnapshotInfo, EngineError> {
        self.call(|reply| Command::VerifySnapshot(id, reply))
    }

    pub fn delete_snapshot(&self, id: String) -> Result<bool, EngineError> {
        self.call(|reply| Command::DeleteSnapshot(id, reply))
    }

    pub fn delete_all_derived_state(&self) -> Result<(), EngineError> {
        self.call(Command::DeleteAllDerived)
    }

    pub fn rebuild_projection(&self, handle: QueryHandle) -> Result<(), EngineError> {
        self.call(|reply| Command::RebuildProjection(handle, reply))
    }

    pub fn close(&self) -> Result<(), EngineError> {
        if self.inner.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.inner.feed_signal.notify();
        let (tx, rx) = mpsc::sync_channel(1);
        self.inner
            .sender
            .send(Command::Shutdown(Some(tx)))
            .map_err(|_| EngineError::closed())?;
        rx.recv().map_err(|_| EngineError::closed())?;
        if let Some(join) = self
            .inner
            .join
            .lock()
            .map_err(|_| EngineError::internal("join lock poisoned"))?
            .take()
        {
            join.join()
                .map_err(|_| EngineError::internal("sequencer panicked"))?;
        }
        Ok(())
    }
}

enum Command {
    Append(
        AppendBatch,
        mpsc::SyncSender<Result<AppendReceiptDto, EngineError>>,
    ),
    Commit(mpsc::SyncSender<Result<u64, EngineError>>),
    Head(mpsc::SyncSender<Result<u64, EngineError>>),
    DurableHead(mpsc::SyncSender<Result<u64, EngineError>>),
    UncommittedCount(mpsc::SyncSender<Result<u64, EngineError>>),
    Fork {
        parent: [u8; 16],
        at: u64,
        name: String,
        metadata: BTreeMap<String, Vec<u8>>,
        reply: mpsc::SyncSender<Result<BranchDto, EngineError>>,
    },
    BranchNamed(String, mpsc::SyncSender<Result<BranchDto, EngineError>>),
    Ancestry(
        [u8; 16],
        mpsc::SyncSender<Result<Vec<BranchDto>, EngineError>>,
    ),
    Archive([u8; 16], mpsc::SyncSender<Result<BranchDto, EngineError>>),
    OpenReader(
        ReplayRequest,
        mpsc::SyncSender<Result<ReaderHandle, EngineError>>,
    ),
    NextPage(
        ReaderHandle,
        mpsc::SyncSender<Result<ReplayPage, EngineError>>,
    ),
    CancelReader(ReaderHandle, mpsc::SyncSender<Result<(), EngineError>>),
    CloseReader(ReaderHandle, mpsc::SyncSender<Result<(), EngineError>>),
    OpenFeed(
        FeedRequest,
        mpsc::SyncSender<Result<FeedHandle, EngineError>>,
    ),
    NextFeedPage(FeedHandle, mpsc::SyncSender<Result<FeedPage, EngineError>>),
    AcknowledgeFeed(FeedHandle, mpsc::SyncSender<Result<u64, EngineError>>),
    CancelFeed(FeedHandle, mpsc::SyncSender<Result<(), EngineError>>),
    CloseFeed(FeedHandle, mpsc::SyncSender<Result<(), EngineError>>),
    ClearConsumerCheckpoint(String, mpsc::SyncSender<Result<(), EngineError>>),
    IngestBatch(
        CommittedBatch,
        mpsc::SyncSender<Result<AppendReceiptDto, EngineError>>,
    ),
    RegisterQuery(
        String,
        QueryDefinition,
        mpsc::SyncSender<Result<QueryHandle, EngineError>>,
    ),
    RegisterPartitionedQuery(
        String,
        QueryDefinition,
        u32,
        mpsc::SyncSender<Result<QueryHandle, EngineError>>,
    ),
    RegisterRuntime(
        ProjectionDescriptor,
        Box<dyn ProjectionRuntime>,
        mpsc::SyncSender<Result<QueryHandle, EngineError>>,
    ),
    RemoveQuery(String, mpsc::SyncSender<Result<bool, EngineError>>),
    QueryNamed(String, mpsc::SyncSender<Result<QueryHandle, EngineError>>),
    Query(
        QueryHandle,
        QueryOperation,
        QueryConsistency,
        mpsc::SyncSender<Result<QueryResult, EngineError>>,
    ),
    QueryPartitions(
        QueryHandle,
        Vec<u32>,
        QueryOperation,
        QueryConsistency,
        mpsc::SyncSender<Result<QueryResult, EngineError>>,
    ),
    ProjectionStatus(
        QueryHandle,
        mpsc::SyncSender<Result<ProjectionStatus, EngineError>>,
    ),
    PartitionStatus(
        QueryHandle,
        mpsc::SyncSender<Result<Vec<PartitionStatus>, EngineError>>,
    ),
    CreateSnapshot(
        QueryHandle,
        mpsc::SyncSender<Result<crate::SnapshotInfo, EngineError>>,
    ),
    CreatePartitionSnapshot(
        QueryHandle,
        u32,
        mpsc::SyncSender<Result<crate::SnapshotInfo, EngineError>>,
    ),
    ListSnapshots(
        QueryHandle,
        mpsc::SyncSender<Result<Vec<crate::SnapshotInfo>, EngineError>>,
    ),
    VerifySnapshot(
        String,
        mpsc::SyncSender<Result<crate::SnapshotInfo, EngineError>>,
    ),
    DeleteSnapshot(String, mpsc::SyncSender<Result<bool, EngineError>>),
    DeleteAllDerived(mpsc::SyncSender<Result<(), EngineError>>),
    RebuildProjection(QueryHandle, mpsc::SyncSender<Result<(), EngineError>>),
    Shutdown(Option<mpsc::SyncSender<()>>),
}

struct ReaderState {
    request: ReplayRequest,
    continuation: u64,
    cancelled: bool,
}
struct FeedState {
    request: FeedRequest,
    continuation: u64,
    cancelled: bool,
}
struct QueryState {
    descriptor: ProjectionDescriptor,
    status: ProjectionStatus,
    runtime: Box<dyn ProjectionRuntime>,
    partitions: Vec<PartitionStatus>,
}

type ProjectionRegistry = (
    HashMap<QueryHandle, QueryState>,
    HashMap<String, QueryHandle>,
);

struct JsonIndexRuntime {
    definition: QueryDefinition,
    rows: BTreeMap<String, Vec<u8>>,
    row_streams: BTreeMap<String, [u8; 16]>,
}

impl JsonIndexRuntime {
    fn new(definition: QueryDefinition) -> Self {
        Self {
            definition,
            rows: BTreeMap::new(),
            row_streams: BTreeMap::new(),
        }
    }
}

impl ProjectionRuntime for JsonIndexRuntime {
    fn reset(&mut self) -> Result<(), ProjectionFailure> {
        self.rows.clear();
        self.row_streams.clear();
        Ok(())
    }

    fn apply(&mut self, record: &RecordDto) -> Result<(), ProjectionFailure> {
        if record.codec != PayloadCodec::Json {
            return Ok(());
        }
        let value: serde_json::Value = serde_json::from_slice(&record.payload)
            .map_err(|error| projection_failure("invalid_json", error.to_string()))?;
        if let Some((field, expected)) = &self.definition.filter {
            let expected: serde_json::Value = serde_json::from_slice(expected)
                .map_err(|error| projection_failure("invalid_filter", error.to_string()))?;
            if value.get(field) != Some(&expected) {
                return Ok(());
            }
        }
        if let Some(key) = value
            .get(&self.definition.key_field)
            .and_then(serde_json::Value::as_str)
        {
            self.rows.insert(key.to_string(), record.payload.clone());
            self.row_streams.insert(key.to_string(), record.stream_id);
        }
        Ok(())
    }

    fn query(&self, operation: QueryOperation) -> Result<QueryResult, ProjectionFailure> {
        let selected: Vec<Vec<u8>> = match operation {
            QueryOperation::Get(key) => self.rows.get(&key).cloned().into_iter().collect(),
            QueryOperation::Range { start, end } => self
                .rows
                .range(start..end)
                .map(|(_, value)| value.clone())
                .collect(),
            QueryOperation::Prefix(prefix) => self
                .rows
                .range(prefix.clone()..)
                .take_while(|(key, _)| key.starts_with(&prefix))
                .map(|(_, value)| value.clone())
                .collect(),
            QueryOperation::By { index, key } => {
                let field = self
                    .definition
                    .indexes
                    .get(&index)
                    .ok_or_else(|| projection_failure("index_not_found", index))?;
                self.rows
                    .values()
                    .filter(|payload| {
                        serde_json::from_slice::<serde_json::Value>(payload)
                            .ok()
                            .and_then(|value| value.get(field).cloned())
                            .is_some_and(|value| index_key(&value) == key)
                    })
                    .cloned()
                    .collect()
            }
            QueryOperation::Len => Vec::new(),
        };
        Ok(QueryResult {
            len: self.rows.len() as u64,
            rows: selected,
        })
    }

    fn checkpoint(&self) -> Result<Vec<u8>, ProjectionFailure> {
        serde_json::to_vec(&self.rows)
            .map_err(|error| projection_failure("checkpoint_encode", error.to_string()))
    }

    fn restore_checkpoint(&mut self, state: &[u8]) -> Result<(), ProjectionFailure> {
        self.rows = serde_json::from_slice(state)
            .map_err(|error| projection_failure("checkpoint_decode", error.to_string()))?;
        Ok(())
    }

    fn checkpoint_partition(
        &self,
        partition: u32,
        partition_count: u32,
    ) -> Result<Vec<u8>, ProjectionFailure> {
        let rows = self
            .rows
            .iter()
            .filter(|(key, _)| {
                self.row_streams.get(*key).is_some_and(|id| {
                    crate::partition_of(StreamId::from_bytes(*id), partition_count) == partition
                })
            })
            .map(|(key, value)| (key.clone(), (self.row_streams[key], value.clone())))
            .collect::<BTreeMap<_, _>>();
        serde_json::to_vec(&rows)
            .map_err(|error| projection_failure("checkpoint_encode", error.to_string()))
    }

    fn restore_partition(
        &mut self,
        _partition: u32,
        _partition_count: u32,
        state: &[u8],
    ) -> Result<(), ProjectionFailure> {
        let rows: BTreeMap<String, ([u8; 16], Vec<u8>)> = serde_json::from_slice(state)
            .map_err(|error| projection_failure("checkpoint_decode", error.to_string()))?;
        for (key, (stream, value)) in rows {
            self.row_streams.insert(key.clone(), stream);
            self.rows.insert(key, value);
        }
        Ok(())
    }
}

struct MissingRuntime;

impl ProjectionRuntime for MissingRuntime {
    fn reset(&mut self) -> Result<(), ProjectionFailure> {
        Ok(())
    }
    fn apply(&mut self, _record: &RecordDto) -> Result<(), ProjectionFailure> {
        Err(projection_failure(
            "runtime_missing",
            "native runtime must be re-registered",
        ))
    }
    fn query(&self, _operation: QueryOperation) -> Result<QueryResult, ProjectionFailure> {
        Err(projection_failure(
            "runtime_missing",
            "native runtime must be re-registered",
        ))
    }
}

fn sequencer(
    options: EngineOptions,
    rx: mpsc::Receiver<Command>,
    ready: mpsc::SyncSender<Result<(), EngineError>>,
    feed_signal: Arc<FeedSignal>,
) {
    let root = options.path.clone();
    let snapshot_every_events = options.snapshot_every_events;
    let snapshot_every_bytes = options.snapshot_every_bytes;
    let snapshot_every_millis = options.snapshot_every_millis;
    let mut snapshot_events = 0u64;
    let mut snapshot_bytes = 0u64;
    let mut last_snapshot = Instant::now();
    let mut policy = CommitPolicy::manual();
    if let Some(value) = options.commit_every_bytes {
        policy = policy.and_bytes(value);
    }
    if let Some(value) = options.commit_every_count {
        policy = policy.and_count(value);
    }
    if let Some(value) = options.commit_every_millis {
        policy = policy.and_millis(value);
    }
    let mut db: Salamander<EngineEvent> = match Salamander::open_with_policy(options.path, policy) {
        Ok(db) => db,
        Err(error) => {
            let _ = ready.send(Err(error.into()));
            return;
        }
    };
    let mut readers = HashMap::new();
    let mut feeds = HashMap::new();
    let mut consumer_checkpoints = restore_consumer_checkpoints(&db).unwrap_or_default();
    let mut next_handle = 1u64;
    let (mut queries, mut query_names) = match restore_projections(&db, &mut next_handle) {
        Ok(registry) => registry,
        Err(error) => {
            let _ = ready.send(Err(error));
            return;
        }
    };
    // WP-09: open restores registration metadata only. Snapshot bytes and
    // event payloads are touched by the first query, never by open.
    feed_signal.publish(db.durable_head());
    let _ = ready.send(Ok(()));
    while let Ok(command) = rx.recv() {
        match command {
            Command::Append(request, reply) => {
                let appended_events = request.events.len() as u64;
                let appended_bytes = request
                    .events
                    .iter()
                    .map(|event| event.payload.len() as u64)
                    .sum::<u64>();
                let result = append(&mut db, request);
                if result.is_ok() {
                    drive_all(&db, &mut queries);
                    snapshot_events = snapshot_events.saturating_add(appended_events);
                    snapshot_bytes = snapshot_bytes.saturating_add(appended_bytes);
                    let due = snapshot_every_events
                        .is_some_and(|limit| snapshot_events >= limit.max(1))
                        || snapshot_every_bytes.is_some_and(|limit| snapshot_bytes >= limit.max(1))
                        || snapshot_every_millis.is_some_and(|limit| {
                            last_snapshot.elapsed() >= Duration::from_millis(limit.max(1))
                        });
                    if due {
                        snapshot_ready(&root, &db, &queries);
                        snapshot_events = 0;
                        snapshot_bytes = 0;
                        last_snapshot = Instant::now();
                    }
                }
                feed_signal.publish(db.durable_head());
                let _ = reply.send(result);
            }
            Command::Commit(reply) => {
                let result = db.commit().map_err(Into::into);
                if result.is_ok() {
                    feed_signal.publish(db.durable_head());
                }
                let _ = reply.send(result);
            }
            Command::Head(reply) => {
                let _ = reply.send(Ok(db.head()));
            }
            Command::DurableHead(reply) => {
                let _ = reply.send(Ok(db.durable_head()));
            }
            Command::UncommittedCount(reply) => {
                let _ = reply.send(Ok(db.uncommitted_count()));
            }
            Command::Fork {
                parent,
                at,
                name,
                metadata,
                reply,
            } => {
                let result = BranchName::new(name)
                    .map_err(EngineError::from)
                    .and_then(|name| {
                        db.fork_branch(BranchId::from_bytes(parent), at, name, metadata)
                            .map(branch_dto)
                            .map_err(Into::into)
                    });
                let _ = reply.send(result);
            }
            Command::BranchNamed(name, reply) => {
                let result = db
                    .branch_named(&name)
                    .cloned()
                    .map(branch_dto)
                    .ok_or_else(|| not_found("branch"));
                let _ = reply.send(result);
            }
            Command::Ancestry(id, reply) => {
                let result = db
                    .branch_ancestry(BranchId::from_bytes(id))
                    .map(|items| items.into_iter().map(branch_dto).collect())
                    .map_err(Into::into);
                let _ = reply.send(result);
            }
            Command::Archive(id, reply) => {
                let result = db
                    .archive_branch(BranchId::from_bytes(id))
                    .map(branch_dto)
                    .map_err(Into::into);
                let _ = reply.send(result);
            }
            Command::OpenReader(request, reply) => {
                let mut request = request;
                if request.until.is_none() {
                    request.until = Some(db.head());
                }
                let result = validate_replay(&db, &request).map(|_| {
                    let handle = ReaderHandle(next_handle);
                    next_handle += 1;
                    readers.insert(
                        handle,
                        ReaderState {
                            continuation: request.from,
                            request,
                            cancelled: false,
                        },
                    );
                    handle
                });
                let _ = reply.send(result);
            }
            Command::NextPage(handle, reply) => {
                let result = readers
                    .get_mut(&handle)
                    .ok_or_else(|| not_found("reader"))
                    .and_then(|state| next_page(&db, state));
                let _ = reply.send(result);
            }
            Command::CancelReader(handle, reply) => {
                let result = readers
                    .get_mut(&handle)
                    .ok_or_else(|| not_found("reader"))
                    .map(|state| state.cancelled = true);
                let _ = reply.send(result);
            }
            Command::CloseReader(handle, reply) => {
                let result = readers
                    .remove(&handle)
                    .map(|_| ())
                    .ok_or_else(|| not_found("reader"));
                let _ = reply.send(result);
            }
            Command::OpenFeed(mut request, reply) => {
                let result = validate_feed(&request, db.durable_head()).map(|_| {
                    let continuation = request.from.unwrap_or_else(|| {
                        request
                            .consumer_id
                            .as_ref()
                            .and_then(|id| consumer_checkpoints.get(id))
                            .copied()
                            .unwrap_or(0)
                    });
                    request.from = Some(continuation);
                    let handle = FeedHandle(next_handle);
                    next_handle += 1;
                    feeds.insert(
                        handle,
                        FeedState {
                            request,
                            continuation,
                            cancelled: false,
                        },
                    );
                    handle
                });
                let _ = reply.send(result);
            }
            Command::NextFeedPage(handle, reply) => {
                let result = feeds
                    .get_mut(&handle)
                    .ok_or_else(|| not_found("feed"))
                    .and_then(|state| feed_page(&db, state));
                let _ = reply.send(result);
            }
            Command::AcknowledgeFeed(handle, reply) => {
                let result = feeds
                    .get(&handle)
                    .ok_or_else(|| not_found("feed"))
                    .and_then(|state| {
                        if let Some(id) = &state.request.consumer_id {
                            persist_consumer_checkpoint(&mut db, id, state.continuation)?;
                            consumer_checkpoints.insert(id.clone(), state.continuation);
                        }
                        Ok(state.continuation)
                    });
                let _ = reply.send(result);
            }
            Command::CancelFeed(handle, reply) => {
                let result = feeds
                    .get_mut(&handle)
                    .ok_or_else(|| not_found("feed"))
                    .map(|state| state.cancelled = true);
                let _ = reply.send(result);
            }
            Command::CloseFeed(handle, reply) => {
                let result = feeds
                    .remove(&handle)
                    .map(|_| ())
                    .ok_or_else(|| not_found("feed"));
                let _ = reply.send(result);
            }
            Command::ClearConsumerCheckpoint(id, reply) => {
                let result = clear_consumer_checkpoint(&mut db, &id).map(|_| {
                    consumer_checkpoints.remove(&id);
                });
                let _ = reply.send(result);
            }
            Command::IngestBatch(batch, reply) => {
                let result = ingest_batch(&mut db, batch);
                if result.is_ok() {
                    drive_all(&db, &mut queries);
                    feed_signal.publish(db.durable_head());
                }
                let _ = reply.send(result);
            }
            Command::RegisterQuery(name, definition, reply) => {
                let result = register_query(
                    &mut db,
                    &mut queries,
                    &mut query_names,
                    &mut next_handle,
                    name,
                    definition,
                );
                let _ = reply.send(result);
            }
            Command::RegisterPartitionedQuery(name, definition, count, reply) => {
                let result = register_query_with_partitions(
                    &mut db,
                    &mut queries,
                    &mut query_names,
                    &mut next_handle,
                    name,
                    definition,
                    count,
                );
                let _ = reply.send(result);
            }
            Command::RegisterRuntime(descriptor, runtime, reply) => {
                let result = register_runtime(
                    &mut db,
                    &mut queries,
                    &mut query_names,
                    &mut next_handle,
                    descriptor,
                    runtime,
                );
                let _ = reply.send(result);
            }
            Command::RemoveQuery(name, reply) => {
                let result = remove_query(&mut db, &mut queries, &mut query_names, &name);
                let _ = reply.send(result);
            }
            Command::QueryNamed(name, reply) => {
                let result = query_names
                    .get(&name)
                    .copied()
                    .ok_or_else(|| not_found("query"));
                let _ = reply.send(result);
            }
            Command::Query(handle, operation, consistency, reply) => {
                let result = queries
                    .get_mut(&handle)
                    .ok_or_else(|| not_found("query"))
                    .and_then(|state| {
                        let partitions = (0..state.partitions.len() as u32).collect::<Vec<_>>();
                        heal_partitions(Some(&root), &db, state, &partitions, consistency);
                        query_projection(state, operation, consistency, db.head())
                    });
                let _ = reply.send(result);
            }
            Command::QueryPartitions(handle, partitions, operation, consistency, reply) => {
                let result = queries
                    .get_mut(&handle)
                    .ok_or_else(|| not_found("query"))
                    .and_then(|state| {
                        validate_partitions(state, &partitions)?;
                        heal_partitions(Some(&root), &db, state, &partitions, consistency);
                        query_touched_partitions(
                            state,
                            operation,
                            consistency,
                            db.head(),
                            &partitions,
                        )
                    });
                let _ = reply.send(result);
            }
            Command::ProjectionStatus(handle, reply) => {
                let result = queries
                    .get(&handle)
                    .map(|state| state.status.clone())
                    .ok_or_else(|| not_found("query"));
                let _ = reply.send(result);
            }
            Command::PartitionStatus(handle, reply) => {
                let result = queries
                    .get(&handle)
                    .map(|state| state.partitions.clone())
                    .ok_or_else(|| not_found("query"));
                let _ = reply.send(result);
            }
            Command::CreateSnapshot(handle, reply) => {
                let result = queries
                    .get_mut(&handle)
                    .ok_or_else(|| not_found("query"))
                    .and_then(|state| {
                        let partitions = (0..state.partitions.len() as u32).collect::<Vec<_>>();
                        heal_partitions(
                            Some(&root),
                            &db,
                            state,
                            &partitions,
                            QueryConsistency::RequireHead,
                        );
                        create_snapshot(&root, &db, state)
                    });
                let _ = reply.send(result);
            }
            Command::CreatePartitionSnapshot(handle, partition, reply) => {
                let result = queries
                    .get_mut(&handle)
                    .ok_or_else(|| not_found("query"))
                    .and_then(|state| {
                        validate_partitions(state, &[partition])?;
                        heal_partitions(
                            Some(&root),
                            &db,
                            state,
                            &[partition],
                            QueryConsistency::RequireHead,
                        );
                        create_one_partition_snapshot(&root, &db, state, partition)
                    });
                let _ = reply.send(result);
            }
            Command::ListSnapshots(handle, reply) => {
                let result = queries
                    .get(&handle)
                    .map(|state| {
                        crate::snapshot::list(&root, descriptor_fingerprint(&state.descriptor))
                    })
                    .ok_or_else(|| not_found("query"));
                let _ = reply.send(result);
            }
            Command::VerifySnapshot(id, reply) => {
                let _ = reply.send(crate::snapshot::verify(&root, &id));
            }
            Command::DeleteSnapshot(id, reply) => {
                let _ = reply.send(crate::snapshot::delete(&root, &id));
            }
            Command::DeleteAllDerived(reply) => {
                let _ = reply.send(crate::snapshot::delete_all(&root));
            }
            Command::RebuildProjection(handle, reply) => {
                let result = queries
                    .get_mut(&handle)
                    .ok_or_else(|| not_found("query"))
                    .and_then(|state| {
                        crate::snapshot::delete_projection(
                            &root,
                            descriptor_fingerprint(&state.descriptor),
                        )?;
                        state.runtime.reset().map_err(projection_error)?;
                        state.status = ProjectionStatus::Building {
                            cursor: initial_cursor(&db, &state.descriptor),
                        };
                        drive_projection(&db, state);
                        Ok(())
                    });
                let _ = reply.send(result);
            }
            Command::Shutdown(reply) => {
                if let Some(reply) = reply {
                    let _ = reply.send(());
                }
                break;
            }
        }
    }
}

fn append(
    db: &mut Salamander<EngineEvent>,
    request: AppendBatch,
) -> Result<AppendReceiptDto, EngineError> {
    let batch_bytes = request
        .events
        .iter()
        .map(|event| event.payload.len())
        .sum::<usize>();
    if batch_bytes > MAX_FACADE_BATCH_BYTES {
        return Err(resource(
            "batch payload",
            batch_bytes,
            MAX_FACADE_BATCH_BYTES,
        ));
    }
    for event in &request.events {
        if event.payload.len() > MAX_FACADE_PAYLOAD_BYTES {
            return Err(resource(
                "payload",
                event.payload.len(),
                MAX_FACADE_PAYLOAD_BYTES,
            ));
        }
        if event.codec == PayloadCodec::Json {
            serde_json::from_slice::<serde_json::Value>(&event.payload).map_err(|e| {
                EngineError {
                    category: ErrorCategory::Codec,
                    code: "invalid_json",
                    message: e.to_string(),
                }
            })?;
        }
    }
    let events = request
        .events
        .into_iter()
        .map(|event| {
            Ok(NewEvent {
                event_id: event.event_id.map(EventId::from_bytes),
                event_type: EventType::new(event.event_type).map_err(EngineError::from)?,
                schema_version: event.schema_version,
                metadata: event.metadata,
                body: EngineEvent {
                    codec: event.codec,
                    bytes: event.payload,
                },
            })
        })
        .collect::<Result<Vec<_>, EngineError>>()?;
    let receipt = db
        .append_batch(AppendRequest {
            branch: BranchId::from_bytes(request.branch_id),
            stream: StreamName::new(request.stream).map_err(EngineError::from)?,
            expected: match request.expected {
                ExpectedRevisionDto::Any => ExpectedRevision::Any,
                ExpectedRevisionDto::NoStream => ExpectedRevision::NoStream,
                ExpectedRevisionDto::Exact(value) => ExpectedRevision::Exact(StreamRevision(value)),
            },
            idempotency_key: request
                .idempotency_key
                .map(IdempotencyKey::new)
                .transpose()
                .map_err(EngineError::from)?,
            events,
            durability: match request.durability {
                DurabilityDto::Buffered => Durability::Buffered,
                DurabilityDto::Flush => Durability::Flush,
                DurabilityDto::Sync => Durability::Sync,
            },
        })
        .map_err(EngineError::from)?;
    Ok(receipt_dto(receipt))
}

fn validate_replay(
    db: &Salamander<EngineEvent>,
    request: &ReplayRequest,
) -> Result<(), EngineError> {
    if request.page_events == 0 || request.page_events > MAX_REPLAY_PAGE_EVENTS {
        return Err(resource(
            "page events",
            request.page_events as usize,
            MAX_REPLAY_PAGE_EVENTS as usize,
        ));
    }
    if request.page_bytes == 0 || request.page_bytes > MAX_REPLAY_PAGE_BYTES {
        return Err(resource(
            "page bytes",
            request.page_bytes,
            MAX_REPLAY_PAGE_BYTES,
        ));
    }
    let _ = db
        .read(ReplayPlan {
            branch: BranchId::from_bytes(request.branch_id),
            from: Bound::Included(request.from),
            until: request.until.map_or(ReplayEnd::Head, ReplayEnd::At),
            ..ReplayPlan::default()
        })
        .map_err(EngineError::from)?;
    Ok(())
}

fn next_page(
    db: &Salamander<EngineEvent>,
    state: &mut ReaderState,
) -> Result<ReplayPage, EngineError> {
    if state.cancelled {
        return Err(EngineError {
            category: ErrorCategory::Cancelled,
            code: "cancelled",
            message: "reader was cancelled".into(),
        });
    }
    let mut reader = db
        .read(ReplayPlan {
            branch: BranchId::from_bytes(state.request.branch_id),
            from: Bound::Included(state.continuation),
            until: state.request.until.map_or(ReplayEnd::Head, ReplayEnd::At),
            ..ReplayPlan::default()
        })
        .map_err(EngineError::from)?;
    let mut records = Vec::new();
    let mut bytes = 0usize;
    let mut continuation = state.continuation;
    loop {
        let Some(record) = reader.next_owned().map_err(EngineError::from)? else {
            // Exhausted scan: adopt the reader's continuation, which has
            // advanced past records its filters skipped (e.g. another
            // branch's events at the tail). Leaving `continuation` at the
            // last *yielded* record would keep `done` false forever and
            // livelock paging loops.
            continuation = continuation.max(reader.continuation());
            break;
        };
        let stream = record
            .envelope
            .metadata
            .get("salamander.stream_name")
            .and_then(|v| std::str::from_utf8(v).ok());
        if state
            .request
            .stream
            .as_deref()
            .is_some_and(|wanted| stream != Some(wanted))
        {
            continuation = reader.continuation();
            continue;
        }
        let dto = record_dto(record)?;
        let size = dto.payload.len() + dto.metadata.values().map(Vec::len).sum::<usize>();
        if !records.is_empty()
            && (records.len() >= state.request.page_events as usize
                || bytes + size > state.request.page_bytes)
        {
            continuation = dto.position;
            break;
        }
        bytes += size;
        continuation = reader.continuation();
        records.push(dto);
        if records.len() >= state.request.page_events as usize {
            break;
        }
    }
    state.continuation = continuation;
    let end = state.request.until.unwrap_or_else(|| db.head());
    Ok(ReplayPage {
        records,
        continuation,
        done: continuation >= end,
    })
}

fn record_dto(record: OwnedStoredRecord) -> Result<RecordDto, EngineError> {
    let event: EngineEvent = bincode::deserialize(&record.payload).map_err(|e| EngineError {
        category: ErrorCategory::Codec,
        code: "codec",
        message: e.to_string(),
    })?;
    Ok(RecordDto {
        database_id: record.envelope.database_id.into_bytes(),
        batch_id: record.envelope.batch_id.into_bytes(),
        batch_index: record.envelope.batch_index,
        position: record.position,
        timestamp_unix_nanos: record.envelope.timestamp_unix_nanos,
        event_id: record.envelope.event_id.into_bytes(),
        branch_id: record.envelope.branch_id.into_bytes(),
        stream_id: record.envelope.stream_id.into_bytes(),
        stream_revision: record.envelope.stream_revision.0,
        event_type: record.envelope.event_type.as_str().to_string(),
        schema_version: record.envelope.schema_version,
        metadata: record.envelope.metadata,
        codec: event.codec,
        payload: event.bytes,
    })
}

fn validate_feed(request: &FeedRequest, durable_head: u64) -> Result<(), EngineError> {
    if request.page_batches == 0 || request.page_batches > MAX_REPLAY_PAGE_EVENTS {
        return Err(resource(
            "feed page batches",
            request.page_batches as usize,
            MAX_REPLAY_PAGE_EVENTS as usize,
        ));
    }
    if request.page_bytes == 0 || request.page_bytes > MAX_REPLAY_PAGE_BYTES {
        return Err(resource(
            "feed page bytes",
            request.page_bytes,
            MAX_REPLAY_PAGE_BYTES,
        ));
    }
    if request.from.is_some_and(|position| position > durable_head) {
        return Err(EngineError {
            category: ErrorCategory::InvalidArgument,
            code: "position_unavailable",
            message: format!("feed position is beyond durable head {durable_head}"),
        });
    }
    if request
        .consumer_id
        .as_ref()
        .is_some_and(|id| id.is_empty() || id.len() > 1024)
    {
        return Err(invalid("consumer ID must contain 1 to 1024 bytes"));
    }
    Ok(())
}

fn feed_page(db: &Salamander<EngineEvent>, state: &mut FeedState) -> Result<FeedPage, EngineError> {
    if state.cancelled {
        return Err(EngineError {
            category: ErrorCategory::Cancelled,
            code: "cancelled",
            message: "feed was cancelled".into(),
        });
    }
    let durable_head = db.durable_head();
    let mut batches = Vec::new();
    let mut page_bytes = 0usize;
    let mut current: Vec<RecordDto> = Vec::new();
    let mut current_batch = None;
    let mut continuation = state.continuation;
    for item in db.log.records_from(state.continuation) {
        let record = item.map_err(EngineError::from)?;
        if record.position >= durable_head {
            break;
        }
        if current_batch.is_some_and(|id| id != record.envelope.batch_id.into_bytes()) {
            if !finish_feed_batch(
                &state.request.filter,
                &mut batches,
                &mut page_bytes,
                &current,
                state.request.page_batches as usize,
                state.request.page_bytes,
            ) {
                continuation = current.first().map_or(continuation, |event| event.position);
                state.continuation = continuation;
                return Ok(FeedPage {
                    batches,
                    continuation,
                    durable_head,
                    timed_out: false,
                });
            }
            continuation = current
                .last()
                .map_or(continuation, |event| event.position + 1);
            current.clear();
        }
        current_batch = Some(record.envelope.batch_id.into_bytes());
        current.push(record_dto(record)?);
    }
    if !current.is_empty() {
        if finish_feed_batch(
            &state.request.filter,
            &mut batches,
            &mut page_bytes,
            &current,
            state.request.page_batches as usize,
            state.request.page_bytes,
        ) {
            continuation = current.last().unwrap().position + 1;
        } else {
            continuation = current[0].position;
        }
    } else if continuation < durable_head {
        continuation = durable_head;
    }
    state.continuation = continuation;
    Ok(FeedPage {
        batches,
        continuation,
        durable_head,
        timed_out: false,
    })
}

fn finish_feed_batch(
    filter: &FeedFilter,
    output: &mut Vec<CommittedBatch>,
    page_bytes: &mut usize,
    events: &[RecordDto],
    maximum_batches: usize,
    maximum_bytes: usize,
) -> bool {
    let first = &events[0];
    let selected = (filter.branches.is_empty() || filter.branches.contains(&first.branch_id))
        && (filter.streams.is_empty()
            || events
                .iter()
                .any(|event| filter.streams.contains(&event.stream_id)))
        && (filter.event_types.is_empty()
            || events
                .iter()
                .any(|event| filter.event_types.contains(&event.event_type)));
    if !selected {
        return true;
    }
    let bytes = events
        .iter()
        .map(|event| event.payload.len() + event.metadata.values().map(Vec::len).sum::<usize>())
        .sum::<usize>();
    if !output.is_empty()
        && (output.len() >= maximum_batches || page_bytes.saturating_add(bytes) > maximum_bytes)
    {
        return false;
    }
    let mut streams = events
        .iter()
        .map(|event| event.stream_id)
        .collect::<Vec<_>>();
    streams.sort_unstable();
    streams.dedup();
    output.push(CommittedBatch {
        database_id: first.database_id,
        batch_id: first.batch_id,
        first_position: first.position,
        last_position: events.last().unwrap().position,
        branch_id: first.branch_id,
        stream_ids: streams,
        events: events.to_vec(),
    });
    *page_bytes = page_bytes.saturating_add(bytes);
    true
}

fn restore_consumer_checkpoints(
    db: &Salamander<EngineEvent>,
) -> Result<HashMap<String, u64>, EngineError> {
    let mut checkpoints = HashMap::new();
    for item in db.log.system_records() {
        let record = item.map_err(EngineError::from)?;
        match record.envelope.event_type.as_str() {
            "salamander.consumer.checkpoint" => {
                let checkpoint: ConsumerCheckpoint = serde_json::from_slice(&record.payload)
                    .map_err(|error| EngineError::internal(error.to_string()))?;
                checkpoints.insert(checkpoint.consumer_id, checkpoint.position);
            }
            "salamander.consumer.cleared" => {
                if let Ok(id) = std::str::from_utf8(&record.payload) {
                    checkpoints.remove(id);
                }
            }
            _ => {}
        }
    }
    Ok(checkpoints)
}

fn persist_consumer_checkpoint(
    db: &mut Salamander<EngineEvent>,
    id: &str,
    position: u64,
) -> Result<(), EngineError> {
    let payload = serde_json::to_vec(&ConsumerCheckpoint {
        consumer_id: id.to_string(),
        position,
    })
    .map_err(|error| EngineError::internal(error.to_string()))?;
    append_projection_system(db, "salamander.consumer.checkpoint", &payload)
}

fn clear_consumer_checkpoint(
    db: &mut Salamander<EngineEvent>,
    id: &str,
) -> Result<(), EngineError> {
    append_projection_system(db, "salamander.consumer.cleared", id.as_bytes())
}

fn ingest_batch(
    db: &mut Salamander<EngineEvent>,
    batch: CommittedBatch,
) -> Result<AppendReceiptDto, EngineError> {
    if batch.events.is_empty() {
        return Err(invalid("replicated batch has no events"));
    }
    if batch.first_position > batch.last_position
        || batch.events.len() as u64 != batch.last_position - batch.first_position + 1
    {
        return Err(invalid("replicated batch position range is invalid"));
    }
    let stream = batch.events[0]
        .metadata
        .get("salamander.stream_name")
        .and_then(|value| std::str::from_utf8(value).ok())
        .ok_or_else(|| invalid("replicated event has no stream name"))?
        .to_string();
    for (index, event) in batch.events.iter().enumerate() {
        if event.database_id != batch.database_id
            || event.batch_id != batch.batch_id
            || event.batch_index != index as u32
            || event.position != batch.first_position + index as u64
            || event.branch_id != batch.branch_id
            || !batch.stream_ids.contains(&event.stream_id)
        {
            return Err(invalid(
                "replicated event envelope does not match its batch",
            ));
        }
        let event_stream = event
            .metadata
            .get("salamander.stream_name")
            .and_then(|value| std::str::from_utf8(value).ok());
        if event_stream != Some(&stream) {
            return Err(invalid("replicated batch spans multiple stream names"));
        }
    }
    let events = batch
        .events
        .into_iter()
        .map(|event| {
            Ok(NewEvent {
                event_id: Some(EventId::from_bytes(event.event_id)),
                event_type: EventType::new(event.event_type).map_err(EngineError::from)?,
                schema_version: event.schema_version,
                metadata: event.metadata,
                body: EngineEvent {
                    codec: event.codec,
                    bytes: event.payload,
                },
            })
        })
        .collect::<Result<Vec<_>, EngineError>>()?;
    let receipt = db
        .append_batch_with_id(
            AppendRequest {
                branch: BranchId::from_bytes(batch.branch_id),
                stream: StreamName::new(stream).map_err(EngineError::from)?,
                expected: ExpectedRevision::Any,
                idempotency_key: None,
                events,
                durability: Durability::Sync,
            },
            Some(BatchId::from_bytes(batch.batch_id)),
        )
        .map_err(EngineError::from)?;
    Ok(receipt_dto(receipt))
}

fn validate_query(definition: &QueryDefinition) -> Result<(), EngineError> {
    if definition.key_field.is_empty() {
        Err(invalid("query key field is empty"))
    } else {
        Ok(())
    }
}

fn register_query(
    db: &mut Salamander<EngineEvent>,
    queries: &mut HashMap<QueryHandle, QueryState>,
    names: &mut HashMap<String, QueryHandle>,
    next_handle: &mut u64,
    name: String,
    definition: QueryDefinition,
) -> Result<QueryHandle, EngineError> {
    register_query_with_partitions(db, queries, names, next_handle, name, definition, 1)
}

fn register_query_with_partitions(
    db: &mut Salamander<EngineEvent>,
    queries: &mut HashMap<QueryHandle, QueryState>,
    names: &mut HashMap<String, QueryHandle>,
    next_handle: &mut u64,
    name: String,
    definition: QueryDefinition,
    partition_count: u32,
) -> Result<QueryHandle, EngineError> {
    validate_query(&definition)?;
    if partition_count == 0 || partition_count > 4096 {
        return Err(invalid("partition count must be between 1 and 4096"));
    }
    let mut descriptor = descriptor_for_query(&name, &definition);
    descriptor.partition_scheme.partition_count = partition_count;
    if let Some(handle) = names.get(&name).copied() {
        let state = queries.get_mut(&handle).ok_or_else(|| not_found("query"))?;
        if state.descriptor == descriptor {
            if matches!(
                state.status,
                ProjectionStatus::Failed { .. } | ProjectionStatus::Stale { .. }
            ) {
                state.runtime.reset().map_err(projection_error)?;
                state.status = ProjectionStatus::Building {
                    cursor: initial_cursor(db, &descriptor),
                };
                drive_projection(db, state);
            }
            return Ok(handle);
        }
        state.status = ProjectionStatus::Stale {
            cursor: status_cursor(&state.status),
            reason: StaleReason::DescriptorChanged,
        };
    }

    let registration = DurableProjectionRegistration {
        descriptor: descriptor.clone(),
        definition: Some(definition.clone()),
    };
    append_projection_system(
        db,
        "salamander.projection.registered",
        &serde_json::to_vec(&registration)
            .map_err(|error| EngineError::internal(error.to_string()))?,
    )?;
    let handle = names.get(&name).copied().unwrap_or_else(|| {
        let handle = QueryHandle(*next_handle);
        *next_handle += 1;
        handle
    });
    let state = QueryState {
        descriptor: descriptor.clone(),
        status: ProjectionStatus::Building {
            cursor: initial_cursor(db, &descriptor),
        },
        runtime: Box::new(JsonIndexRuntime::new(definition)),
        partitions: cold_partitions(db, &descriptor),
    };
    queries.insert(handle, state);
    names.insert(name, handle);
    Ok(handle)
}

fn register_runtime(
    db: &mut Salamander<EngineEvent>,
    queries: &mut HashMap<QueryHandle, QueryState>,
    names: &mut HashMap<String, QueryHandle>,
    next_handle: &mut u64,
    descriptor: ProjectionDescriptor,
    mut runtime: Box<dyn ProjectionRuntime>,
) -> Result<QueryHandle, EngineError> {
    if descriptor.name.is_empty() {
        return Err(invalid("projection name is empty"));
    }
    runtime.reset().map_err(projection_error)?;
    let registration = DurableProjectionRegistration {
        descriptor: descriptor.clone(),
        definition: None,
    };
    append_projection_system(
        db,
        "salamander.projection.registered",
        &serde_json::to_vec(&registration)
            .map_err(|error| EngineError::internal(error.to_string()))?,
    )?;
    let handle = names.get(&descriptor.name).copied().unwrap_or_else(|| {
        let handle = QueryHandle(*next_handle);
        *next_handle += 1;
        handle
    });
    let state = QueryState {
        status: ProjectionStatus::Building {
            cursor: initial_cursor(db, &descriptor),
        },
        descriptor: descriptor.clone(),
        runtime,
        partitions: cold_partitions(db, &descriptor),
    };
    names.insert(descriptor.name.clone(), handle);
    queries.insert(handle, state);
    Ok(handle)
}

fn remove_query(
    db: &mut Salamander<EngineEvent>,
    queries: &mut HashMap<QueryHandle, QueryState>,
    names: &mut HashMap<String, QueryHandle>,
    name: &str,
) -> Result<bool, EngineError> {
    let Some(handle) = names.remove(name) else {
        return Ok(false);
    };
    if let Some(state) = queries.get_mut(&handle) {
        state.status = ProjectionStatus::Dropping;
    }
    append_projection_system(db, "salamander.projection.dropped", name.as_bytes())?;
    queries.remove(&handle);
    Ok(true)
}

fn restore_projections(
    db: &Salamander<EngineEvent>,
    next_handle: &mut u64,
) -> Result<ProjectionRegistry, EngineError> {
    let mut registrations: BTreeMap<String, DurableProjectionRegistration> = BTreeMap::new();
    for item in db.log.system_records() {
        let record = item.map_err(EngineError::from)?;
        match record.envelope.event_type.as_str() {
            "salamander.projection.registered" => {
                let registration: DurableProjectionRegistration =
                    serde_json::from_slice(&record.payload).map_err(|error| {
                        EngineError::internal(format!("projection descriptor: {error}"))
                    })?;
                registrations.insert(registration.descriptor.name.clone(), registration);
            }
            "salamander.projection.dropped" => {
                if let Ok(name) = std::str::from_utf8(&record.payload) {
                    registrations.remove(name);
                }
            }
            _ => {}
        }
    }
    let mut queries = HashMap::new();
    let mut names = HashMap::new();
    for (name, registration) in registrations {
        let handle = QueryHandle(*next_handle);
        *next_handle += 1;
        let cursor = initial_cursor(db, &registration.descriptor);
        let (status, runtime): (ProjectionStatus, Box<dyn ProjectionRuntime>) =
            match registration.definition {
                Some(definition) => (
                    ProjectionStatus::Building { cursor },
                    Box::new(JsonIndexRuntime::new(definition)),
                ),
                None => (
                    ProjectionStatus::Stale {
                        cursor,
                        reason: StaleReason::DescriptorChanged,
                    },
                    Box::new(MissingRuntime),
                ),
            };
        queries.insert(
            handle,
            QueryState {
                partitions: cold_partitions(db, &registration.descriptor),
                descriptor: registration.descriptor,
                status,
                runtime,
            },
        );
        names.insert(name, handle);
    }
    Ok((queries, names))
}

fn drive_all(db: &Salamander<EngineEvent>, queries: &mut HashMap<QueryHandle, QueryState>) {
    for state in queries.values_mut() {
        let ready = state
            .partitions
            .iter()
            .enumerate()
            .filter_map(|(index, status)| {
                matches!(status, PartitionStatus::Ready { .. }).then_some(index as u32)
            })
            .collect::<Vec<_>>();
        if !ready.is_empty() {
            heal_partitions(None, db, state, &ready, QueryConsistency::RequireHead);
        }
    }
}

fn restore_partition_snapshot(
    root: &std::path::Path,
    db: &Salamander<EngineEvent>,
    state: &mut QueryState,
    partition: u32,
) -> Option<ProjectionCursor> {
    let expected = snapshot_expectation(
        db,
        &state.descriptor,
        db.head(),
        (state.partitions.len() > 1).then_some(partition),
    );
    for (info, bytes) in crate::snapshot::load_candidates(root, &expected) {
        let restored = if state.partitions.len() == 1 {
            state.runtime.restore_checkpoint(&bytes)
        } else {
            state
                .runtime
                .restore_partition(partition, state.partitions.len() as u32, &bytes)
        };
        if restored.is_ok() {
            return Some(info.manifest.cursor);
        }
    }
    None
}

fn create_snapshot(
    root: &std::path::Path,
    db: &Salamander<EngineEvent>,
    state: &QueryState,
) -> Result<crate::SnapshotInfo, EngineError> {
    let cursor = match &state.status {
        ProjectionStatus::Ready { cursor } => cursor.clone(),
        _ => {
            return Err(EngineError {
                category: ErrorCategory::Conflict,
                code: "projection_not_ready",
                message: "only a ready projection can be snapshotted".into(),
            });
        }
    };
    let count = state.descriptor.partition_scheme.partition_count;
    if count > 1 {
        let mut published = None;
        for partition in 0..count {
            let cursor = partition_cursor(&state.partitions[partition as usize]);
            let bytes = state
                .runtime
                .checkpoint_partition(partition, count)
                .map_err(projection_error)?;
            if bytes.len() > crate::MAX_SNAPSHOT_STATE_BYTES {
                return Err(resource(
                    "snapshot state",
                    bytes.len(),
                    crate::MAX_SNAPSHOT_STATE_BYTES,
                ));
            }
            let manifest = crate::SnapshotManifest {
                format_version: 2,
                database_id: db.log.database_id().into_bytes(),
                projection_name: state.descriptor.name.clone(),
                descriptor_fingerprint: descriptor_fingerprint(&state.descriptor),
                definition_id: state.descriptor.definition_id,
                definition_version: state.descriptor.definition_version,
                branch_id: state.descriptor.scope.branch_id,
                branch_lineage_fingerprint: lineage_fingerprint(
                    db,
                    state.descriptor.scope.branch_id,
                ),
                cursor,
                state_codec: state.descriptor.state_codec,
                state_codec_version: state.descriptor.state_codec_version,
                created_at_unix_nanos: crate::snapshot::created_now(),
                uncompressed_len: bytes.len() as u64,
                checksum: crc32c::crc32c(&bytes),
                partition: Some(partition),
                partition_scheme_id: Some(state.descriptor.partition_scheme.scheme_id.clone()),
                partition_scheme_version: Some(state.descriptor.partition_scheme.version),
                partition_count: Some(count),
            };
            published = Some(crate::snapshot::publish(root, manifest, &bytes)?);
        }
        return published
            .ok_or_else(|| EngineError::internal("partition scheme has no partitions"));
    }
    let bytes = state.runtime.checkpoint().map_err(projection_error)?;
    if bytes.len() > crate::MAX_SNAPSHOT_STATE_BYTES {
        return Err(resource(
            "snapshot state",
            bytes.len(),
            crate::MAX_SNAPSHOT_STATE_BYTES,
        ));
    }
    let manifest = crate::SnapshotManifest {
        format_version: 1,
        database_id: db.log.database_id().into_bytes(),
        projection_name: state.descriptor.name.clone(),
        descriptor_fingerprint: descriptor_fingerprint(&state.descriptor),
        definition_id: state.descriptor.definition_id,
        definition_version: state.descriptor.definition_version,
        branch_id: state.descriptor.scope.branch_id,
        branch_lineage_fingerprint: lineage_fingerprint(db, state.descriptor.scope.branch_id),
        cursor,
        state_codec: state.descriptor.state_codec,
        state_codec_version: state.descriptor.state_codec_version,
        created_at_unix_nanos: crate::snapshot::created_now(),
        uncompressed_len: bytes.len() as u64,
        checksum: crc32c::crc32c(&bytes),
        partition: None,
        partition_scheme_id: None,
        partition_scheme_version: None,
        partition_count: None,
    };
    crate::snapshot::publish(root, manifest, &bytes)
}

fn create_one_partition_snapshot(
    root: &std::path::Path,
    db: &Salamander<EngineEvent>,
    state: &QueryState,
    partition: u32,
) -> Result<crate::SnapshotInfo, EngineError> {
    let count = state.partitions.len() as u32;
    let cursor = match &state.partitions[partition as usize] {
        PartitionStatus::Ready { cursor } => cursor.clone(),
        _ => {
            return Err(EngineError {
                category: ErrorCategory::Conflict,
                code: "partition_not_ready",
                message: "only a ready partition can be snapshotted".into(),
            })
        }
    };
    let bytes = state
        .runtime
        .checkpoint_partition(partition, count)
        .map_err(projection_error)?;
    if bytes.len() > crate::MAX_SNAPSHOT_STATE_BYTES {
        return Err(resource(
            "snapshot state",
            bytes.len(),
            crate::MAX_SNAPSHOT_STATE_BYTES,
        ));
    }
    let manifest = crate::SnapshotManifest {
        format_version: 2,
        database_id: db.log.database_id().into_bytes(),
        projection_name: state.descriptor.name.clone(),
        descriptor_fingerprint: descriptor_fingerprint(&state.descriptor),
        definition_id: state.descriptor.definition_id,
        definition_version: state.descriptor.definition_version,
        branch_id: state.descriptor.scope.branch_id,
        branch_lineage_fingerprint: lineage_fingerprint(db, state.descriptor.scope.branch_id),
        cursor,
        state_codec: state.descriptor.state_codec,
        state_codec_version: state.descriptor.state_codec_version,
        created_at_unix_nanos: crate::snapshot::created_now(),
        uncompressed_len: bytes.len() as u64,
        checksum: crc32c::crc32c(&bytes),
        partition: Some(partition),
        partition_scheme_id: Some(state.descriptor.partition_scheme.scheme_id.clone()),
        partition_scheme_version: Some(state.descriptor.partition_scheme.version),
        partition_count: Some(count),
    };
    crate::snapshot::publish(root, manifest, &bytes)
}

fn snapshot_ready(
    root: &std::path::Path,
    db: &Salamander<EngineEvent>,
    queries: &HashMap<QueryHandle, QueryState>,
) {
    for state in queries.values() {
        let ProjectionStatus::Ready { .. } = &state.status else {
            continue;
        };
        let _ = create_snapshot(root, db, state);
    }
}

fn snapshot_expectation<'a>(
    db: &Salamander<EngineEvent>,
    descriptor: &'a ProjectionDescriptor,
    maximum_cursor: u64,
    partition: Option<u32>,
) -> crate::snapshot::SnapshotExpectation<'a> {
    crate::snapshot::SnapshotExpectation {
        database_id: db.log.database_id().into_bytes(),
        descriptor,
        descriptor_fingerprint: descriptor_fingerprint(descriptor),
        lineage_fingerprint: lineage_fingerprint(db, descriptor.scope.branch_id),
        maximum_cursor,
        partition,
    }
}

fn lineage_fingerprint(db: &Salamander<EngineEvent>, branch: [u8; 16]) -> [u8; 16] {
    let mut bytes = Vec::new();
    if let Ok(ancestry) = db.branch_ancestry(BranchId::from_bytes(branch)) {
        for item in ancestry {
            bytes.extend_from_slice(item.id.as_bytes());
            bytes.extend_from_slice(&item.fork_position.unwrap_or(u64::MAX).to_le_bytes());
        }
    }
    fingerprint(bytes)
}

fn drive_projection(db: &Salamander<EngineEvent>, state: &mut QueryState) {
    let start = status_cursor(&state.status).position;
    let head = db.head();
    if start >= head {
        state.status = ProjectionStatus::Ready {
            cursor: cursor_at(db, &state.descriptor, head),
        };
        return;
    }
    state.status = ProjectionStatus::Building {
        cursor: cursor_at(db, &state.descriptor, start),
    };
    let mut reader = match db.read(ReplayPlan {
        branch: BranchId::from_bytes(state.descriptor.scope.branch_id),
        from: Bound::Included(start),
        until: ReplayEnd::At(head),
        ..ReplayPlan::default()
    }) {
        Ok(reader) => reader,
        Err(error) => {
            state.status = ProjectionStatus::Failed {
                cursor: cursor_at(db, &state.descriptor, start),
                error: projection_failure("read", error.to_string()),
            };
            return;
        }
    };
    loop {
        let record = match reader.next_owned() {
            Ok(Some(record)) => record,
            Ok(None) => break,
            Err(error) => {
                let cursor = status_cursor(&state.status);
                state.status = ProjectionStatus::Failed {
                    cursor,
                    error: projection_failure("read", error.to_string()),
                };
                return;
            }
        };
        let position = record.position;
        let dto = match record_dto(record) {
            Ok(dto) => dto,
            Err(error) => {
                state.status = ProjectionStatus::Failed {
                    cursor: cursor_at(db, &state.descriptor, position),
                    error: projection_failure("decode", error.to_string()),
                };
                return;
            }
        };
        if !projection_selects(&state.descriptor, &dto) {
            continue;
        }
        let applied =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| state.runtime.apply(&dto)));
        match applied {
            Ok(Ok(())) => {
                state.status = ProjectionStatus::Building {
                    cursor: cursor_at(db, &state.descriptor, position + 1),
                }
            }
            Ok(Err(error)) => {
                state.status = ProjectionStatus::Failed {
                    cursor: cursor_at(db, &state.descriptor, position),
                    error,
                };
                return;
            }
            Err(_) => {
                state.status = ProjectionStatus::Failed {
                    cursor: cursor_at(db, &state.descriptor, position),
                    error: projection_failure("panic", "projection panicked"),
                };
                return;
            }
        }
    }
    state.status = ProjectionStatus::Ready {
        cursor: cursor_at(db, &state.descriptor, head),
    };
}

fn query_projection(
    state: &QueryState,
    operation: QueryOperation,
    consistency: QueryConsistency,
    head: u64,
) -> Result<QueryResult, EngineError> {
    let cursor = status_cursor(&state.status);
    let acceptable = match consistency {
        QueryConsistency::AllowStale => !matches!(state.status, ProjectionStatus::Dropping),
        QueryConsistency::RequireHead => {
            matches!(state.status, ProjectionStatus::Ready { .. }) && cursor.position >= head
        }
        QueryConsistency::WaitFor(position) => {
            !matches!(
                state.status,
                ProjectionStatus::Failed { .. } | ProjectionStatus::Dropping
            ) && cursor.position >= position
        }
    };
    if !acceptable {
        return Err(EngineError {
            category: ErrorCategory::Conflict,
            code: "projection_not_ready",
            message: format!("projection status is {:?}", state.status),
        });
    }
    state.runtime.query(operation).map_err(projection_error)
}

fn projection_selects(descriptor: &ProjectionDescriptor, record: &RecordDto) -> bool {
    if descriptor.scope.stream.as_deref().is_some_and(|stream| {
        record
            .metadata
            .get("salamander.stream_name")
            .and_then(|value| std::str::from_utf8(value).ok())
            != Some(stream)
    }) {
        return false;
    }
    descriptor.input_types.is_empty()
        || descriptor.input_types.iter().any(|input| {
            input.event_type == record.event_type
                && (input.min_schema_version..=input.max_schema_version)
                    .contains(&record.schema_version)
        })
}

fn descriptor_for_query(name: &str, definition: &QueryDefinition) -> ProjectionDescriptor {
    let bytes = serde_json::to_vec(definition).unwrap_or_default();
    ProjectionDescriptor {
        name: name.to_string(),
        definition_id: fingerprint(name.as_bytes().iter().chain(bytes.iter()).copied()),
        definition_version: 1,
        input_types: Vec::new(),
        state_codec: CodecId::JSON_UTF8.0,
        state_codec_version: 1,
        scope: ProjectionScope::default(),
        partition_scheme: PartitionScheme::default(),
    }
}

fn cold_partitions(
    db: &Salamander<EngineEvent>,
    descriptor: &ProjectionDescriptor,
) -> Vec<PartitionStatus> {
    (0..descriptor.partition_scheme.partition_count.max(1))
        .map(|_| PartitionStatus::Cold {
            cursor: initial_cursor(db, descriptor),
        })
        .collect()
}

fn validate_partitions(state: &QueryState, partitions: &[u32]) -> Result<(), EngineError> {
    if partitions.is_empty()
        || partitions
            .iter()
            .any(|partition| *partition as usize >= state.partitions.len())
    {
        return Err(invalid("query partition set is empty or out of range"));
    }
    Ok(())
}

fn heal_partitions(
    root: Option<&std::path::Path>,
    db: &Salamander<EngineEvent>,
    state: &mut QueryState,
    partitions: &[u32],
    consistency: QueryConsistency,
) {
    let head = db.head();
    let target = match consistency {
        QueryConsistency::RequireHead | QueryConsistency::AllowStale => head,
        QueryConsistency::WaitFor(position) => position.min(head),
    };
    for &partition in partitions {
        let slot = partition as usize;
        if matches!(state.partitions[slot], PartitionStatus::Cold { .. }) {
            if let Some(cursor) =
                root.and_then(|root| restore_partition_snapshot(root, db, state, partition))
            {
                state.partitions[slot] = PartitionStatus::Ready { cursor };
            }
        }
        let start = partition_cursor(&state.partitions[slot]).position;
        if matches!(
            state.partitions[slot],
            PartitionStatus::Failed { .. } | PartitionStatus::Stale { .. }
        ) {
            continue;
        }
        if start >= target {
            state.partitions[slot] = PartitionStatus::Ready {
                cursor: cursor_at(db, &state.descriptor, target),
            };
            continue;
        }
        state.partitions[slot] = PartitionStatus::Healing {
            cursor: cursor_at(db, &state.descriptor, start),
        };
        let selector = state
            .descriptor
            .scope
            .stream
            .as_deref()
            .and_then(|name| StreamName::new(name).ok())
            .and_then(|name| {
                db.stream_id(
                    BranchId::from_bytes(state.descriptor.scope.branch_id),
                    &name,
                )
            })
            .map_or(
                StreamSelector::PartitionClass {
                    count: state.partitions.len() as u32,
                    index: partition,
                },
                |stream| {
                    if crate::partition_of(stream, state.partitions.len() as u32) == partition {
                        StreamSelector::Streams(vec![stream])
                    } else {
                        StreamSelector::Streams(Vec::new())
                    }
                },
            );
        let mut reader = match db.read(ReplayPlan {
            branch: BranchId::from_bytes(state.descriptor.scope.branch_id),
            streams: selector,
            from: Bound::Included(start),
            until: ReplayEnd::At(target),
            ..ReplayPlan::default()
        }) {
            Ok(reader) => reader,
            Err(error) => {
                state.partitions[slot] = PartitionStatus::Failed {
                    cursor: cursor_at(db, &state.descriptor, start),
                    error: projection_failure("read", error.to_string()),
                };
                continue;
            }
        };
        let mut failed = None;
        loop {
            let record = match reader.next_owned() {
                Ok(Some(record)) => record,
                Ok(None) => break,
                Err(error) => {
                    failed = Some(projection_failure("read", error.to_string()));
                    break;
                }
            };
            let dto = match record_dto(record) {
                Ok(dto) => dto,
                Err(error) => {
                    failed = Some(projection_failure("decode", error.to_string()));
                    break;
                }
            };
            if projection_selects(&state.descriptor, &dto) {
                let applied = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    state.runtime.apply(&dto)
                }));
                match applied {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        failed = Some(error);
                        break;
                    }
                    Err(_) => {
                        failed = Some(projection_failure("panic", "projection panicked"));
                        break;
                    }
                }
            }
        }
        state.partitions[slot] = if let Some(error) = failed {
            PartitionStatus::Failed {
                cursor: cursor_at(db, &state.descriptor, start),
                error,
            }
        } else {
            PartitionStatus::Ready {
                cursor: cursor_at(db, &state.descriptor, target),
            }
        };
    }
    aggregate_partition_status(state, db);
}

fn partition_cursor(status: &PartitionStatus) -> ProjectionCursor {
    match status {
        PartitionStatus::Cold { cursor }
        | PartitionStatus::Healing { cursor }
        | PartitionStatus::Ready { cursor }
        | PartitionStatus::Stale { cursor, .. }
        | PartitionStatus::Failed { cursor, .. } => cursor.clone(),
    }
}

fn aggregate_partition_status(state: &mut QueryState, db: &Salamander<EngineEvent>) {
    if let Some((cursor, error)) = state.partitions.iter().find_map(|status| match status {
        PartitionStatus::Failed { cursor, error } => Some((cursor.clone(), error.clone())),
        _ => None,
    }) {
        state.status = ProjectionStatus::Failed { cursor, error };
        return;
    }
    if let Some((cursor, reason)) = state.partitions.iter().find_map(|status| match status {
        PartitionStatus::Stale { cursor, reason } => Some((cursor.clone(), reason.clone())),
        _ => None,
    }) {
        state.status = ProjectionStatus::Stale { cursor, reason };
        return;
    }
    let cursor = state
        .partitions
        .iter()
        .map(partition_cursor)
        .min_by_key(|cursor| cursor.position)
        .unwrap_or_else(|| initial_cursor(db, &state.descriptor));
    state.status = if state
        .partitions
        .iter()
        .all(|status| matches!(status, PartitionStatus::Ready { .. }))
    {
        ProjectionStatus::Ready { cursor }
    } else {
        ProjectionStatus::Building { cursor }
    };
}

fn query_touched_partitions(
    state: &QueryState,
    operation: QueryOperation,
    consistency: QueryConsistency,
    head: u64,
    partitions: &[u32],
) -> Result<QueryResult, EngineError> {
    let target = match consistency {
        QueryConsistency::RequireHead => head,
        QueryConsistency::AllowStale => 0,
        QueryConsistency::WaitFor(position) => position,
    };
    if partitions.iter().any(|partition| {
        let status = &state.partitions[*partition as usize];
        matches!(
            status,
            PartitionStatus::Failed { .. } | PartitionStatus::Stale { .. }
        ) || (!matches!(consistency, QueryConsistency::AllowStale)
            && partition_cursor(status).position < target)
    }) {
        return Err(EngineError {
            category: ErrorCategory::Conflict,
            code: "projection_not_ready",
            message: "one or more requested partitions are not ready".into(),
        });
    }
    state.runtime.query(operation).map_err(projection_error)
}

fn initial_cursor(
    db: &Salamander<EngineEvent>,
    descriptor: &ProjectionDescriptor,
) -> ProjectionCursor {
    cursor_at(db, descriptor, 0)
}
fn cursor_at(
    db: &Salamander<EngineEvent>,
    descriptor: &ProjectionDescriptor,
    position: u64,
) -> ProjectionCursor {
    ProjectionCursor {
        database_id: db.log.database_id().into_bytes(),
        branch_id: descriptor.scope.branch_id,
        position,
        descriptor_fingerprint: descriptor_fingerprint(descriptor),
    }
}
fn status_cursor(status: &ProjectionStatus) -> ProjectionCursor {
    match status {
        ProjectionStatus::Building { cursor }
        | ProjectionStatus::Ready { cursor }
        | ProjectionStatus::Stale { cursor, .. }
        | ProjectionStatus::Failed { cursor, .. } => cursor.clone(),
        ProjectionStatus::Dropping => ProjectionCursor {
            database_id: [0; 16],
            branch_id: [0; 16],
            position: 0,
            descriptor_fingerprint: [0; 16],
        },
    }
}
fn descriptor_fingerprint(descriptor: &ProjectionDescriptor) -> [u8; 16] {
    fingerprint(serde_json::to_vec(descriptor).unwrap_or_default())
}
fn fingerprint(bytes: impl IntoIterator<Item = u8>) -> [u8; 16] {
    let mut a = 0xcbf29ce484222325u64;
    let mut b = 0x84222325cbf29ce4u64;
    for byte in bytes {
        a = (a ^ u64::from(byte)).wrapping_mul(0x100000001b3);
        b = (b ^ u64::from(byte).rotate_left(1)).wrapping_mul(0x9e3779b185ebca87);
    }
    let mut out = [0; 16];
    out[..8].copy_from_slice(&a.to_le_bytes());
    out[8..].copy_from_slice(&b.to_le_bytes());
    out
}

fn append_projection_system(
    db: &mut Salamander<EngineEvent>,
    event_type: &str,
    payload: &[u8],
) -> Result<(), EngineError> {
    let id = crate::format::generate_id_bytes();
    let envelope = RecordEnvelopeV2 {
        event_id: EventId::from_bytes(id),
        database_id: db.log.database_id(),
        branch_id: BranchId::ZERO,
        stream_id: StreamId::ZERO,
        stream_revision: StreamRevision(0),
        timestamp_unix_nanos: 0,
        event_type: EventType::new(event_type).map_err(EngineError::from)?,
        schema_version: 1,
        codec: CodecId::JSON_UTF8,
        batch_id: BatchId::from_bytes(id),
        batch_index: 0,
        metadata: Metadata::new(),
    };
    db.log
        .append_system(&envelope, payload)
        .map_err(EngineError::from)?;
    db.commit().map_err(EngineError::from)?;
    Ok(())
}

fn projection_failure(code: impl Into<String>, message: impl Into<String>) -> ProjectionFailure {
    ProjectionFailure {
        code: code.into(),
        message: message.into(),
    }
}
fn projection_error(error: ProjectionFailure) -> EngineError {
    EngineError {
        category: ErrorCategory::Internal,
        code: "projection",
        message: format!("{}: {}", error.code, error.message),
    }
}

fn index_key(value: &serde_json::Value) -> Vec<u8> {
    value
        .as_str()
        .map_or_else(|| value.to_string().into_bytes(), |v| v.as_bytes().to_vec())
}
fn receipt_dto(value: AppendReceipt) -> AppendReceiptDto {
    AppendReceiptDto {
        batch_id: value.batch_id.into_bytes(),
        first_position: value.first_position,
        last_position: value.last_position,
        stream_id: value.stream_id.into_bytes(),
        previous_revision: value.previous_revision.map(|v| v.0),
        current_revision: value.current_revision.0,
        durability: match value.durability {
            ReceiptDurability::Buffered => DurabilityDto::Buffered,
            ReceiptDurability::Flushed => DurabilityDto::Flush,
            ReceiptDurability::Synced => DurabilityDto::Sync,
        },
    }
}
fn branch_dto(value: BranchInfo) -> BranchDto {
    BranchDto {
        id: value.id.into_bytes(),
        name: value.name.as_str().to_string(),
        parent_id: value.parent.map(BranchId::into_bytes),
        fork_position: value.fork_position,
        created_at_unix_nanos: value.created_at_unix_nanos,
        metadata: value.metadata,
        archived: value.status == BranchStatus::Archived,
    }
}
fn invalid(message: impl Into<String>) -> EngineError {
    EngineError {
        category: ErrorCategory::InvalidArgument,
        code: "invalid_argument",
        message: message.into(),
    }
}
fn not_found(kind: &str) -> EngineError {
    EngineError {
        category: ErrorCategory::NotFound,
        code: "not_found",
        message: format!("{kind} handle was not found"),
    }
}
fn resource(name: &'static str, actual: usize, maximum: usize) -> EngineError {
    EngineError {
        category: ErrorCategory::ResourceLimit,
        code: "resource_limit",
        message: format!("{name} is {actual}, maximum is {maximum}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_core_error_maps_to_a_stable_category() {
        let cases = [
            (
                SalamanderError::InvalidArgument("x".into()),
                ErrorCategory::InvalidArgument,
            ),
            (SalamanderError::EventIdConflict, ErrorCategory::Conflict),
            (
                SalamanderError::BranchNotFound("x".into()),
                ErrorCategory::NotFound,
            ),
            (SalamanderError::Locked("x".into()), ErrorCategory::Locked),
            (
                SalamanderError::Corrupt {
                    offset: 0,
                    reason: "x".into(),
                },
                ErrorCategory::Corruption,
            ),
            (
                SalamanderError::UnsupportedFormat {
                    found: 9,
                    supported: 1,
                },
                ErrorCategory::UnsupportedFormat,
            ),
            (SalamanderError::Codec("x".into()), ErrorCategory::Codec),
            (
                SalamanderError::Io(std::io::Error::other("x")),
                ErrorCategory::Io,
            ),
            (
                SalamanderError::ResourceLimit {
                    resource: "x",
                    actual: 2,
                    maximum: 1,
                },
                ErrorCategory::ResourceLimit,
            ),
            (
                SalamanderError::Migration("x".into()),
                ErrorCategory::Internal,
            ),
        ];
        for (error, expected) in cases {
            let mapped = EngineError::from(error);
            assert_eq!(mapped.category, expected);
            assert!(!mapped.code.is_empty());
        }
    }
}
