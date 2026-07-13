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

#![warn(missing_docs)]

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
/// Low-level engine framing and codec types. Most users need only the
/// types re-exported at the crate root (`BranchId`, `StreamId`,
/// `EventType`, `CodecId`, …); the framing internals here are not a stable
/// API.
#[doc(hidden)]
pub mod format;
pub mod json;
pub mod view;

mod db;
mod introspect;

pub use branch::{BranchInfo, BranchName, BranchStatus, DEFAULT_BRANCH_NAME, MAX_LINEAGE_DEPTH};
pub use commit::CommitPolicy;
pub use error::{Result, SalamanderError};
pub use event::{Body, Event};
// The non-generic engine facade: the language-neutral boundary the
// language bindings bind to, and the current home of the committed-batch
// feed. Reachable (examples and `salamander-py` use these root paths) but
// hidden from the documented surface — it is an advanced/plumbing layer,
// not the stable typed Rust API, which is `Salamander`/`AgentDb`/`JsonDb`.
#[doc(hidden)]
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
    LogReader, RecordReader, ReplayEnd, ReplayPlan, StreamSelector, VerificationMode,
};
#[doc(hidden)]
pub use log::partition_of;
pub use projection::{NamespaceScoped, Projection};
pub use view::{Change, IndexKey, IndexedView, View};

pub use agent::AgentDb;
pub use db::Salamander;
pub use json::{Json, JsonDb};
pub use migration::{migrate_legacy_branches, migrate_v1, BranchMigrationReport, MigrationReport};
// Snapshot descriptors are surfaced only through the engine facade
// (snapshot management is not on the typed `Salamander` API — instant
// recovery uses snapshots internally). Reachable for bindings, hidden from
// the documented surface.
#[doc(hidden)]
pub use snapshot::{SnapshotInfo, SnapshotManifest, MAX_SNAPSHOT_STATE_BYTES};
pub use stream::{
    AppendReceipt, AppendRequest, Durability, ExpectedRevision, IdempotencyKey, NewEvent,
    ReceiptDurability, StreamName,
};
