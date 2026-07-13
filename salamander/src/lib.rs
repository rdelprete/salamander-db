//! SalamanderDB — an embedded event-sourcing engine with instant recovery.
//!
//! DESIGN.md §1 — the append-only log is the only durable structure;
//! everything else is a rebuildable projection.
//!
//! The engine is generic over its payload type: [`Salamander<B>`] frames,
//! orders, and persists bodies of any [`Body`] type but never interprets
//! them (P1 — "general engine underneath, agent memory as a labeled
//! beachhead"). The agent vocabulary — [`agent::EventBody`],
//! [`agent::SessionProjection`], `session_view`, `fork` — is a *provided
//! module* over that engine, in [`agent`]. Agent users open an
//! [`AgentDb`] and never see the type parameter.
//!
//! Phase 1 scope: log core, in-memory projections, full replay on open,
//! time-travel, fork. See DESIGN.md and IMPLEMENTATION.md at the repo root.
//!
//! # Custom payloads
//!
//! Any serde-serializable, `Clone`, `'static` type is a valid payload —
//! the agent vocabulary is just one choice. Define your own events and
//! project them however you like:
//!
//! ```
//! use salamander::{Event, Salamander};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Clone, Serialize, Deserialize)]
//! enum Metric {
//!     Cpu(f64),
//!     Mem(u64),
//! }
//!
//! # fn main() -> salamander::Result<()> {
//! let dir = tempfile::tempdir().unwrap();
//!
//! let mut db: Salamander<Metric> = Salamander::open(dir.path())?;
//! db.append("host-1", Metric::Cpu(0.7))?;
//! db.append("host-1", Metric::Mem(2048))?;
//! db.commit()?;
//! drop(db); // release the single-writer lock before reopening
//!
//! // Reopen from disk and read the custom payloads straight back out.
//! let db: Salamander<Metric> = Salamander::open(dir.path())?;
//! let mut count = 0;
//! db.replay("host-1", 0..db.head(), |_e: &Event<Metric>| count += 1)?;
//! assert_eq!(count, 2);
//! # Ok(())
//! # }
//! ```

mod branch;
mod commit;
mod error;
mod event;
mod facade;
mod log;
mod migration;
mod projection;
mod snapshot;
mod stream;

pub mod agent;
pub mod format;
pub mod introspect;
pub mod json;
pub mod view;

mod db;

pub use branch::{BranchInfo, BranchName, BranchStatus, DEFAULT_BRANCH_NAME, MAX_LINEAGE_DEPTH};
pub use commit::CommitPolicy;
pub use error::{Result, SalamanderError};
pub use event::{Body, Event};
pub use facade::{
    AppendBatch as EngineAppendBatch, AppendReceiptDto as EngineAppendReceipt, BranchDto,
    CommittedBatch, DurabilityDto, Engine, EngineError, EngineOptions, ErrorCategory, EventData,
    ExpectedRevisionDto, FeedFilter, FeedHandle, FeedPage, FeedRequest, InputType, PartitionScheme,
    PartitionStatus, PayloadCodec, ProjectionCursor, ProjectionDescriptor, ProjectionFailure,
    ProjectionRuntime, ProjectionScope, ProjectionStatus, QueryConsistency, QueryDefinition,
    QueryHandle, QueryOperation, QueryResult, ReaderHandle, RecordDto, ReplayPage, ReplayRequest,
    StaleReason, MAX_FACADE_BATCH_BYTES, MAX_FACADE_PAYLOAD_BYTES, MAX_REPLAY_PAGE_BYTES,
    MAX_REPLAY_PAGE_EVENTS,
};
pub use format::{
    BatchId, BincodeCodec, BranchId, CodecId, DatabaseId, EventId, EventType, FormatLimits,
    FrameKind, JsonCodec, Metadata, OwnedStoredRecord, RecordEnvelopeV2, StoredRecord, StreamId,
    StreamRevision, TypedCodec,
};
pub use log::{
    partition_of, LogReader, RecordReader, ReplayEnd, ReplayPlan, StreamSelector, VerificationMode,
};
pub use projection::{NamespaceScoped, Projection};
pub use view::{Change, IndexKey, IndexedView, View};

pub use agent::AgentDb;
pub use db::Salamander;
pub use json::{Json, JsonDb};
pub use migration::{migrate_legacy_branches, migrate_v1, BranchMigrationReport, MigrationReport};
pub use snapshot::{SnapshotInfo, SnapshotManifest, MAX_SNAPSHOT_STATE_BYTES};
pub use stream::{
    AppendReceipt, AppendRequest, Durability, ExpectedRevision, IdempotencyKey, NewEvent,
    ReceiptDurability, StreamName,
};
