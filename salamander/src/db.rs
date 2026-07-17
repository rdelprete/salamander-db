//! DESIGN.md §7 — the public API surface: open, append, commit, projection
//! access, and `view_at`.
//!
//! `Salamander<B>` is the payload-generic engine (P1). It frames and
//! persists bodies of any [`Body`] type without interpreting them; the
//! agent-specific `session_view` / `fork` operations live in
//! [`crate::agent`] as an `impl Salamander<agent::EventBody>` block.

use std::collections::HashMap;
use std::io::Write;
use std::ops::{Bound, Range};
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crate::branch::{BranchCatalog, BranchInfo, BranchName, BranchStatus};
use crate::commit::CommitPolicy;
use crate::event::{Body, Event};
use crate::format::{
    derive_stream_id, generate_id_bytes, BatchId, BranchId, CodecId, EventId, EventType, Metadata,
    OwnedStoredRecord, RecordEnvelopeV2, StreamId, StreamRevision,
};
use crate::log::reader::{FrameFilter, ResolvedFilter};
use crate::log::{Log, LogReader, RecordReader, ReplayEnd, ReplayPlan, StreamSelector};
use crate::projection::{decode_stored_event, replay_into, NamespaceScoped, Projection};
use crate::stream::{event_fingerprint, StreamCatalog};
use crate::view::{catch_up, View};
use crate::{
    AppendReceipt, AppendRequest, Durability, ExpectedRevision, ReceiptDurability, Result,
    SalamanderError, StreamName,
};

/// A whole closed segment that a retention plan could reclaim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionSegment {
    /// Original base position encoded in the segment filename.
    pub base_position: u64,
    /// Current on-disk length of the segment.
    pub bytes: u64,
}

/// A condition that prevents a retention plan from being applied safely.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetentionBlocker {
    /// Authoritative engine-core retention anchors are not implemented yet.
    EngineAnchorUnavailable,
    /// A branch depends on history below the proposed effective floor.
    BranchRequiresBootstrap {
        /// User-facing branch name.
        branch: BranchName,
        /// Position at which the branch inherits its parent.
        fork_position: u64,
    },
    /// A registered live view has no authoritative checkpoint at the floor.
    ProjectionRequiresBootstrap {
        /// Registered projection or live-view name.
        name: String,
    },
    /// A durable consumer checkpoint would be stranded below the floor.
    ConsumerRequiresBootstrap {
        /// Stable consumer identifier.
        consumer_id: String,
        /// Last acknowledged exclusive continuation.
        position: u64,
    },
    /// Maintenance cannot apply while bounded readers or feeds are open.
    MaintenanceHandlesOpen {
        /// Number of currently open replay readers.
        readers: usize,
        /// Number of currently open committed-batch feeds.
        feeds: usize,
    },
}

/// Read-only result of planning `KeepFrom(position)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionPlan {
    /// Opaque identifier used to reject stale apply requests.
    pub plan_id: [u8; 16],
    /// Retention generation observed while planning.
    pub generation: u64,
    /// Position requested by the operator.
    pub requested_floor: u64,
    /// Whole-segment boundary the initial compactor could actually use.
    pub effective_floor: u64,
    /// Floor already committed in the manifest.
    pub current_floor: u64,
    /// Durable head against which this plan was produced.
    pub durable_head: u64,
    /// Closed segments strictly below the effective floor.
    pub reclaimable_segments: Vec<RetentionSegment>,
    /// Sum of the current segment file lengths.
    pub reclaimable_bytes: u64,
    /// Conditions that prevent safe application of this plan.
    pub blockers: Vec<RetentionBlocker>,
}

/// Result of committing a retention generation and attempting cleanup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionApplyResult {
    /// Newly committed retention generation.
    pub generation: u64,
    /// Newly committed global floor.
    pub floor: u64,
    /// Bytes successfully reclaimed during this attempt.
    pub reclaimed_bytes: u64,
}

/// Physical cleanup still pending below the committed retention floor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionCleanupStatus {
    /// Closed segment files that remain below the committed floor.
    pub pending_segments: Vec<RetentionSegment>,
    /// Sum of the remaining obsolete segment lengths.
    pub pending_bytes: u64,
}

/// A deterministic selector for an explicit retention planning position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetentionPolicy {
    /// Keep user events at and after this position.
    KeepFrom(u64),
    /// Keep at most the latest number of user events.
    KeepLatestEvents(u64),
    /// Keep every event whose envelope timestamp is at or after this cutoff.
    KeepNewerThan(i64),
    /// Choose the earliest whole-segment boundary whose retained suffix fits.
    TargetLogBytes(u64),
}

/// Read-only policy resolution plus the normal blocker-aware retention plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionPolicyPreview {
    /// Policy that was evaluated.
    pub policy: RetentionPolicy,
    /// Exact position selected before whole-segment rounding.
    pub selected_floor: u64,
    /// False when an indivisible active segment exceeds a byte target.
    pub target_satisfied: bool,
    /// Stable human-readable explanation of the selection.
    pub explanation: String,
    /// Existing explicit-floor safety plan produced from the selection.
    pub plan: RetentionPlan,
}

/// The embedded event-sourcing engine, generic over its payload type `B`.
///
/// Frames, orders, and persists events of any [`Body`] type without
/// interpreting them; derived state is produced by [`Projection`]s and
/// live [`View`](crate::View)s folded from the log. See
/// [`AgentDb`](crate::AgentDb) and [`JsonDb`](crate::JsonDb) for ready-made
/// payload vocabularies.
pub struct Salamander<B> {
    pub(crate) log: Log,
    /// Live views owned by the DB, driven type-erased and kept at head by
    /// the fan-out in `append` (query-layer design §4). Registered by name;
    /// `B` is used here, so no `PhantomData` marker is needed.
    views: HashMap<String, Box<dyn View<B>>>,
    /// When `append` should auto-commit on the caller's behalf (WP-4). The
    /// counters below track what has accumulated since the last commit.
    policy: CommitPolicy,
    pending_bytes: u64,
    pending_count: u64,
    last_commit: Instant,
    catalog: StreamCatalog,
    branches: BranchCatalog,
    retention_anchor: Option<crate::RetentionAnchorInfo>,
    retention_projection_coverage: Vec<crate::RetentionProjectionCoverage>,
    retention_branch_bootstraps: Vec<crate::RetentionBranchBootstrap>,
    retention_consumer_bootstraps: Vec<crate::RetentionConsumerBootstrap>,
    retention_system_records: Vec<crate::retention::AnchoredSystemRecord>,
    durable_head: u64,
    root: PathBuf,
}

fn load_core_catalog(root: &Path, database_id: [u8; 16], head: u64) -> Option<StreamCatalog> {
    let bytes = std::fs::read(root.join("core-catalog.bin")).ok()?;
    if bytes.len() > 256 * 1024 * 1024 {
        return None;
    }
    let checkpoint: CoreCatalogCheckpoint = bincode::deserialize(&bytes).ok()?;
    if checkpoint.database_id != database_id || checkpoint.head != head {
        return None;
    }
    let payload =
        bincode::serialize(&(checkpoint.database_id, checkpoint.head, &checkpoint.catalog)).ok()?;
    (crc32c::crc32c(&payload) == checkpoint.checksum).then_some(checkpoint.catalog)
}

fn persist_core_catalog(
    root: &Path,
    database_id: [u8; 16],
    head: u64,
    catalog: &StreamCatalog,
) -> Result<()> {
    let payload = bincode::serialize(&(database_id, head, catalog))
        .map_err(|error| SalamanderError::Serialization(error.to_string()))?;
    let checkpoint = CoreCatalogCheckpoint {
        database_id,
        head,
        catalog: catalog.clone(),
        checksum: crc32c::crc32c(&payload),
    };
    let bytes = bincode::serialize(&checkpoint)
        .map_err(|error| SalamanderError::Serialization(error.to_string()))?;
    let temporary = root.join("core-catalog.tmp");
    let final_path = root.join("core-catalog.bin");
    {
        let mut file = std::fs::File::create(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
    }
    std::fs::rename(temporary, final_path)?;
    Ok(())
}

#[derive(serde::Serialize, serde::Deserialize)]
struct CoreCatalogCheckpoint {
    database_id: [u8; 16],
    head: u64,
    catalog: StreamCatalog,
    checksum: u32,
}

impl<B: Body> Salamander<B> {
    /// Opens instantly in Phase 2; Phase 1 replays fully and measures it
    /// (DESIGN.md §7). Phase 1 doesn't cache any projection state on
    /// `Salamander` itself — every accessor below rebuilds from the log on
    /// demand ("no snapshot cleverness," IMPLEMENTATION.md Step 5), so
    /// there's nothing to warm up here beyond recovering the log itself.
    /// Registered views (query layer) start empty and are caught up on
    /// `register`. Opens with the default `Manual` commit policy — the
    /// caller drives durability; see [`open_with_policy`](Self::open_with_policy).
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_policy(dir, CommitPolicy::default())
    }

    /// Like [`open`](Self::open), but with a group-commit policy active from
    /// the start (WP-4). The policy can also be changed later with
    /// [`set_commit_policy`](Self::set_commit_policy).
    pub fn open_with_policy(dir: impl AsRef<Path>, policy: CommitPolicy) -> Result<Self> {
        Self::open_internal(dir.as_ref(), policy, None)
    }

    pub(crate) fn open_with_policy_and_segment_max(
        dir: impl AsRef<Path>,
        policy: CommitPolicy,
        segment_max_bytes: u64,
    ) -> Result<Self> {
        Self::open_internal(dir.as_ref(), policy, Some(segment_max_bytes))
    }

    fn open_internal(
        dir: &Path,
        policy: CommitPolicy,
        segment_max_bytes: Option<u64>,
    ) -> Result<Self> {
        let root = dir.to_path_buf();
        let log = match segment_max_bytes {
            Some(maximum) => Log::open_with_segment_max_bytes(&root, maximum)?,
            None => Log::open(&root)?,
        };
        let database_id = log.database_id().into_bytes();
        let authoritative_anchor = log.retention_anchor_checksum().is_some();
        let loaded_anchor = match crate::retention::load(&root) {
            Ok(anchor) => anchor,
            Err(error) if log.has_complete_prefix() && !authoritative_anchor => {
                eprintln!("salamander: ignoring non-authoritative retention anchor: {error}");
                None
            }
            Err(error) => return Err(error),
        };
        let exact_anchor = loaded_anchor.and_then(|(anchor, info)| {
            (crate::retention::validate_identity(
                &anchor,
                database_id,
                log.retention_floor(),
                log.head(),
            )
            .is_ok()
                && log
                    .retention_anchor_checksum()
                    .is_none_or(|checksum| checksum == info.checksum))
            .then_some((anchor, info))
        });
        let (
            catalog,
            branches,
            retention_anchor,
            retention_projection_coverage,
            retention_branch_bootstraps,
            retention_consumer_bootstraps,
            retention_system_records,
        ) = if let Some((anchor, info)) = exact_anchor {
            (
                anchor.stream_catalog,
                anchor.branch_catalog,
                Some(info),
                anchor.projection_coverage,
                anchor.branch_bootstraps,
                anchor.consumer_bootstraps,
                anchor.system_records,
            )
        } else if authoritative_anchor {
            return Err(SalamanderError::InvalidFormat(
                "manifest has no matching authoritative retention anchor".into(),
            ));
        } else if log.has_complete_prefix() {
            let catalog = match load_core_catalog(&root, database_id, log.head()) {
                Some(catalog) => catalog,
                None => {
                    let catalog = StreamCatalog::rebuild(log.records_from(0))?;
                    // Best effort: cache failure cannot make an otherwise valid
                    // log unavailable, and the next commit retries publication.
                    let _ = persist_core_catalog(&root, database_id, log.head(), &catalog);
                    catalog
                }
            };
            let branches = BranchCatalog::rebuild(log.system_records())?;
            (
                catalog,
                branches,
                None,
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
            )
        } else {
            return Err(SalamanderError::InvalidFormat(
                "retained database has no compatible authoritative core anchor".into(),
            ));
        };
        let durable_head = log.head();
        Ok(Salamander {
            log,
            views: HashMap::new(),
            policy,
            pending_bytes: 0,
            pending_count: 0,
            last_commit: Instant::now(),
            catalog,
            branches,
            retention_anchor,
            retention_projection_coverage,
            retention_branch_bootstraps,
            retention_consumer_bootstraps,
            retention_system_records,
            durable_head,
            root,
        })
    }

    /// Replace the group-commit policy. Takes effect on the next append; the
    /// uncommitted counters carry over unchanged (a smaller threshold may
    /// therefore fire on the very next append).
    pub fn set_commit_policy(&mut self, policy: CommitPolicy) {
        self.policy = policy;
    }

    /// The active group-commit policy.
    pub fn commit_policy(&self) -> CommitPolicy {
        self.policy
    }

    /// Lowest position accepted by public historical reads.
    pub fn retention_floor(&self) -> u64 {
        self.log.retention_floor()
    }

    /// Plans a non-destructive global `KeepFrom(position)` retention boundary.
    ///
    /// The effective floor rounds down to a segment base, so planning never
    /// promises partial active-segment rewriting. This method never mutates
    /// metadata or deletes files.
    pub fn plan_retention(&self, requested_floor: u64) -> Result<RetentionPlan> {
        let head = self.durable_head();
        if requested_floor > head {
            return Err(SalamanderError::OffsetBeyondHead(requested_floor));
        }
        if requested_floor < self.retention_floor() {
            return Err(SalamanderError::InvalidArgument(format!(
                "retention floor cannot move backward from {} to {requested_floor}",
                self.retention_floor()
            )));
        }
        let (effective_floor, segments) = self.log.retention_boundary(requested_floor);
        let reclaimable_segments: Vec<_> = segments
            .into_iter()
            .map(|(base_position, bytes)| RetentionSegment {
                base_position,
                bytes,
            })
            .collect();
        let anchor_ready = self.retention_anchor.as_ref().is_some_and(|anchor| {
            anchor.database_id == self.log.database_id().into_bytes()
                && anchor.floor == effective_floor
                && anchor.head == head
        });
        let mut blockers = Vec::new();
        if !anchor_ready {
            blockers.push(RetentionBlocker::EngineAnchorUnavailable);
        }
        blockers.extend(self.branches.all().filter_map(|branch| {
            let fork_position = branch.fork_position?;
            (fork_position < effective_floor
                && !self.has_retention_branch_bootstrap(branch.id.into_bytes(), effective_floor))
            .then(|| RetentionBlocker::BranchRequiresBootstrap {
                branch: branch.name.clone(),
                fork_position,
            })
        }));
        blockers.extend(
            self.views
                .keys()
                .cloned()
                .map(|name| RetentionBlocker::ProjectionRequiresBootstrap { name }),
        );
        Ok(RetentionPlan {
            plan_id: generate_id_bytes(),
            generation: self.log.retention_generation(),
            requested_floor,
            effective_floor,
            current_floor: self.retention_floor(),
            durable_head: head,
            reclaimable_bytes: reclaimable_segments
                .iter()
                .map(|segment| segment.bytes)
                .sum(),
            reclaimable_segments,
            blockers,
        })
    }

    /// Resolves a policy into the same explicit-floor plan used by manual retention.
    ///
    /// This operation is read-only. Event timestamps need not be monotonic: the
    /// age selector conservatively chooses the earliest matching event, so it
    /// never removes a newer event that appears behind an older timestamp.
    pub fn preview_retention_policy(
        &self,
        policy: RetentionPolicy,
    ) -> Result<RetentionPolicyPreview> {
        let current = self.retention_floor();
        let head = self.durable_head();
        let (selected_floor, target_satisfied, explanation) = match policy {
            RetentionPolicy::KeepFrom(position) => (
                position,
                true,
                format!("selected explicit position {position}"),
            ),
            RetentionPolicy::KeepLatestEvents(count) => {
                let available = head.saturating_sub(current);
                let selected = head.saturating_sub(count.min(available)).max(current);
                (
                    selected,
                    true,
                    format!("selected position {selected} to keep the latest {count} event(s)"),
                )
            }
            RetentionPolicy::KeepNewerThan(cutoff) => {
                let mut selected = head;
                for item in self.log.records_from(current) {
                    let record = item?;
                    if record.envelope.timestamp_unix_nanos >= cutoff {
                        selected = selected.min(record.position);
                    }
                }
                (
                    selected,
                    true,
                    format!("selected earliest event at or after unix-nanosecond cutoff {cutoff}"),
                )
            }
            RetentionPolicy::TargetLogBytes(bytes) => {
                let (selected, retained, satisfied) = self.log.retention_floor_for_bytes(bytes)?;
                (
                    selected,
                    satisfied,
                    format!(
                        "selected position {selected} with {retained} retained segment byte(s) for target {bytes}"
                    ),
                )
            }
        };
        let plan = self.plan_retention(selected_floor)?;
        Ok(RetentionPolicyPreview {
            policy,
            selected_floor,
            target_satisfied,
            explanation,
            plan,
        })
    }

    pub(crate) fn apply_retention_prevalidated(
        &mut self,
        effective_floor: u64,
        durable_head: u64,
    ) -> Result<RetentionApplyResult> {
        if self.durable_head() != durable_head {
            return Err(SalamanderError::InvalidArgument(
                "retention plan is stale because durable head changed".into(),
            ));
        }
        let anchor = self.retention_anchor.as_ref().ok_or_else(|| {
            SalamanderError::InvalidArgument("retention plan has no verified anchor".into())
        })?;
        if anchor.floor != effective_floor || anchor.head != durable_head {
            return Err(SalamanderError::InvalidArgument(
                "retention plan anchor identity is stale".into(),
            ));
        }
        crate::retention::crash_point("before_manifest_switch");
        self.log
            .activate_retention(effective_floor, anchor.checksum)?;
        crate::retention::crash_point("after_manifest_switch");
        let reclaimed_bytes = self.log.reclaim_below_retention_floor();
        crate::retention::crash_point("after_cleanup");
        Ok(RetentionApplyResult {
            generation: self.log.retention_generation(),
            floor: self.retention_floor(),
            reclaimed_bytes,
        })
    }

    /// Rebuilds engine catalogs from verified log truth and publishes a
    /// checksummed core retention anchor for a planned floor.
    ///
    /// This does not advance the manifest floor or delete any segment.
    pub fn create_retention_anchor(
        &mut self,
        requested_floor: u64,
    ) -> Result<crate::RetentionAnchorInfo> {
        self.create_retention_anchor_with_coverage(requested_floor, Vec::new())
    }

    pub(crate) fn create_retention_anchor_with_coverage(
        &mut self,
        requested_floor: u64,
        projection_coverage: Vec<crate::RetentionProjectionCoverage>,
    ) -> Result<crate::RetentionAnchorInfo> {
        self.create_retention_anchor_with_all_coverage(
            requested_floor,
            projection_coverage,
            Vec::new(),
            Vec::new(),
        )
    }

    pub(crate) fn create_retention_anchor_with_all_coverage(
        &mut self,
        requested_floor: u64,
        projection_coverage: Vec<crate::RetentionProjectionCoverage>,
        branch_bootstraps: Vec<crate::RetentionBranchBootstrap>,
        consumer_bootstraps: Vec<crate::RetentionConsumerBootstrap>,
    ) -> Result<crate::RetentionAnchorInfo> {
        self.commit()?;
        let plan = self.plan_retention(requested_floor)?;
        let catalog = StreamCatalog::rebuild(self.log.records_from(0))?;
        let branches = BranchCatalog::rebuild(self.log.system_records())?;
        let system_records = self
            .log
            .system_records()
            .map(|item| {
                item.map(|record| crate::retention::AnchoredSystemRecord {
                    event_type: record.envelope.event_type.as_str().to_string(),
                    payload: record.payload,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let info = crate::retention::publish(
            &self.root,
            crate::retention::CoreRetentionAnchor {
                format_version: crate::retention::FORMAT_VERSION,
                database_id: self.log.database_id().into_bytes(),
                floor: plan.effective_floor,
                head: self.durable_head(),
                stream_catalog: catalog.clone(),
                branch_catalog: branches.clone(),
                projection_coverage: projection_coverage.clone(),
                branch_bootstraps: branch_bootstraps.clone(),
                consumer_bootstraps: consumer_bootstraps.clone(),
                system_records: system_records.clone(),
            },
        )?;
        self.catalog = catalog;
        self.branches = branches;
        self.retention_anchor = Some(info.clone());
        self.retention_projection_coverage = projection_coverage;
        self.retention_branch_bootstraps = branch_bootstraps;
        self.retention_consumer_bootstraps = consumer_bootstraps;
        self.retention_system_records = system_records;
        Ok(info)
    }

    pub(crate) fn has_retention_branch_bootstrap(&self, branch_id: [u8; 16], floor: u64) -> bool {
        self.retention_branch_bootstraps.iter().any(|item| {
            item.branch_id == branch_id
                && item.floor == floor
                && crc32c::crc32c(&item.checkpoint) == item.checksum
        })
    }

    pub(crate) fn has_retention_consumer_bootstrap(&self, consumer_id: &str, floor: u64) -> bool {
        self.retention_consumer_bootstraps.iter().any(|item| {
            item.consumer_id == consumer_id
                && item.floor == floor
                && crc32c::crc32c(&item.checkpoint) == item.checksum
        })
    }

    pub(crate) fn retention_branch_bootstrap(&self, branch_id: [u8; 16]) -> Option<Vec<u8>> {
        self.retention_branch_bootstraps
            .iter()
            .find(|item| {
                item.branch_id == branch_id && crc32c::crc32c(&item.checkpoint) == item.checksum
            })
            .map(|item| item.checkpoint.clone())
    }

    pub(crate) fn retention_consumer_bootstrap(&self, consumer_id: &str) -> Option<Vec<u8>> {
        self.retention_consumer_bootstraps
            .iter()
            .find(|item| {
                item.consumer_id == consumer_id && crc32c::crc32c(&item.checkpoint) == item.checksum
            })
            .map(|item| item.checkpoint.clone())
    }

    pub(crate) fn retention_consumer_bootstrap_info(
        &self,
        consumer_id: &str,
    ) -> Option<crate::RetentionConsumerBootstrap> {
        self.retention_consumer_bootstraps
            .iter()
            .find(|item| {
                item.consumer_id == consumer_id && crc32c::crc32c(&item.checkpoint) == item.checksum
            })
            .cloned()
    }

    pub(crate) fn retention_identity(&self) -> ([u8; 16], u64) {
        (
            self.log.database_id().into_bytes(),
            self.log.retention_generation(),
        )
    }

    pub(crate) fn retention_cleanup_status(&self) -> RetentionCleanupStatus {
        let (_, segments) = self.log.retention_boundary(self.retention_floor());
        let pending_segments = segments
            .into_iter()
            .map(|(base_position, bytes)| RetentionSegment {
                base_position,
                bytes,
            })
            .collect::<Vec<_>>();
        let pending_bytes = pending_segments.iter().map(|segment| segment.bytes).sum();
        RetentionCleanupStatus {
            pending_segments,
            pending_bytes,
        }
    }

    pub(crate) fn system_metadata(&self) -> Result<Vec<(String, Vec<u8>)>> {
        let mut records = self
            .retention_system_records
            .iter()
            .map(|record| (record.event_type.clone(), record.payload.clone()))
            .collect::<Vec<_>>();
        for item in self.log.system_records() {
            let record = item?;
            records.push((
                record.envelope.event_type.as_str().to_string(),
                record.payload,
            ));
        }
        Ok(records)
    }

    pub(crate) fn has_retention_projection_coverage(
        &self,
        name: &str,
        descriptor_fingerprint: [u8; 16],
        branch_id: [u8; 16],
        partitions: u32,
    ) -> bool {
        self.retention_anchor.as_ref().is_some_and(|anchor| {
            self.retention_projection_coverage.iter().any(|coverage| {
                let mut covered_partitions = std::collections::BTreeSet::new();
                coverage.name == name
                    && coverage.descriptor_fingerprint == descriptor_fingerprint
                    && coverage.branch_id == branch_id
                    && coverage.cursor == anchor.head
                    && coverage.snapshot_ids.len() == partitions as usize
                    && coverage.snapshot_ids.iter().all(|id| {
                        crate::snapshot::verify(&self.root, id).is_ok_and(|info| {
                            info.manifest.projection_name == name
                                && info.manifest.descriptor_fingerprint == descriptor_fingerprint
                                && info.manifest.branch_id == branch_id
                                && info.manifest.cursor.position == anchor.head
                                && info.manifest.partition_count.unwrap_or(1) == partitions
                                && covered_partitions.insert(info.manifest.partition.unwrap_or(0))
                        })
                    })
                    && covered_partitions.len() == partitions as usize
            })
        })
    }

    /// Appends a single `body` to `namespace` on the default branch and
    /// returns its position. A convenience wrapper over
    /// [`append_batch`](Self::append_batch) with buffered durability.
    pub fn append(&mut self, namespace: &str, body: B) -> Result<u64> {
        let request = AppendRequest {
            branch: BranchId::ZERO,
            stream: crate::StreamName::new(namespace)?,
            expected: ExpectedRevision::Any,
            idempotency_key: None,
            events: vec![crate::NewEvent::new(
                EventType::new(std::any::type_name::<B>())?,
                body,
            )],
            durability: Durability::Buffered,
        };
        Ok(self.append_batch(request)?.first_position)
    }

    /// Like [`append`](Self::append), but targets a specific branch.
    pub fn append_on_branch(&mut self, branch: BranchId, namespace: &str, body: B) -> Result<u64> {
        let request = AppendRequest {
            branch,
            stream: crate::StreamName::new(namespace)?,
            expected: ExpectedRevision::Any,
            idempotency_key: None,
            events: vec![crate::NewEvent::new(
                EventType::new(std::any::type_name::<B>())?,
                body,
            )],
            durability: Durability::Buffered,
        };
        Ok(self.append_batch(request)?.first_position)
    }

    #[allow(dead_code)]
    fn append_wp01_compat(&mut self, namespace: &str, body: B) -> Result<u64> {
        let timestamp_ms = current_timestamp_ms();
        let mut event = Event {
            offset: 0,
            timestamp_ms,
            namespace: namespace.to_string(),
            body,
        };
        let bytes = bincode::serialize(&event.body)
            .map_err(|e| SalamanderError::Serialization(e.to_string()))?;
        let database_id = self.log.database_id();
        let branch_id = BranchId::ZERO;
        let id_bytes = generate_id_bytes();
        let mut metadata = Metadata::new();
        metadata.insert(
            "salamander.stream_name".to_string(),
            namespace.as_bytes().to_vec(),
        );
        let envelope = RecordEnvelopeV2 {
            event_id: EventId::from_bytes(id_bytes),
            database_id,
            branch_id,
            stream_id: derive_stream_id(database_id, branch_id, namespace),
            // WP-02 replaces this placeholder with catalog-owned per-stream
            // revisions. It remains deterministic and monotonic for v2 data
            // written during WP-01.
            stream_revision: StreamRevision(self.log.head()),
            timestamp_unix_nanos: (timestamp_ms as i64).saturating_mul(1_000_000),
            event_type: EventType::new(std::any::type_name::<B>())?,
            schema_version: 1,
            codec: CodecId::RUST_BINCODE_V1,
            batch_id: BatchId::from_bytes(id_bytes),
            batch_index: 0,
            metadata,
        };
        let (offset, last) = self.log.append_batch(&[(envelope, bytes.clone())])?;
        debug_assert_eq!(offset, last);

        // Fan out to every registered view synchronously, with the real
        // log-assigned offset stamped in — so a view is always at head
        // before `append` returns (INV-2). Runs before `commit`, so views
        // track *visible* state, mirroring the log's visible/durable split
        // (query-layer design §4.4). Empty when no view is registered, so
        // the common path pays nothing.
        event.offset = offset;
        for view in self.views.values_mut() {
            view.apply(&event);
        }

        // Group commit (WP-4): tally what's now uncommitted and let the
        // policy decide whether to fsync. `commit` resets the counters.
        self.pending_bytes += bytes.len() as u64;
        self.pending_count += 1;
        if self.policy.should_commit(
            self.pending_bytes,
            self.pending_count,
            self.last_commit.elapsed(),
        ) {
            self.commit()?;
        }

        self.catalog = StreamCatalog::rebuild(self.log.records_from(0))?;
        if self.durable_head == self.log.head() {
            persist_core_catalog(
                &self.root,
                self.log.database_id().into_bytes(),
                self.durable_head,
                &self.catalog,
            )?;
        }
        Ok(offset)
    }

    /// Appends a batch of events atomically, validating the
    /// optimistic-concurrency expectation and idempotency key in the
    /// writer-critical section, and returns the [`AppendReceipt`].
    pub fn append_batch(&mut self, request: AppendRequest<B>) -> Result<AppendReceipt> {
        self.append_batch_with_id(request, None)
    }

    pub(crate) fn append_batch_with_id(
        &mut self,
        request: AppendRequest<B>,
        supplied_batch_id: Option<BatchId>,
    ) -> Result<AppendReceipt> {
        request.validate()?;
        let branch = self
            .branches
            .get(request.branch)
            .ok_or_else(|| SalamanderError::BranchNotFound(format!("{:?}", request.branch)))?;
        if branch.status == BranchStatus::Archived {
            return Err(SalamanderError::BranchArchived(
                branch.name.as_str().to_string(),
            ));
        }
        let previous = self.catalog.revision(request.branch, &request.stream);
        let serialized: Vec<Vec<u8>> = request
            .events
            .iter()
            .map(|event| {
                bincode::serialize(&event.body)
                    .map_err(|error| SalamanderError::Serialization(error.to_string()))
            })
            .collect::<Result<_>>()?;
        let request_digest = request_fingerprint(&request, &serialized);
        if let Some(key) = &request.idempotency_key {
            if let Some((digest, mut receipt)) = self.catalog.idempotent(request.branch, key) {
                if digest != request_digest {
                    return Err(SalamanderError::IdempotencyConflict);
                }
                if request.durability == Durability::Sync
                    && receipt.durability != ReceiptDurability::Synced
                {
                    self.commit()?;
                    receipt.durability = ReceiptDurability::Synced;
                }
                return Ok(receipt);
            }
        }
        validate_expected(request.expected, previous)?;

        let database_id = self.log.database_id();
        let stream_id = self
            .catalog
            .stream_id(request.branch, &request.stream)
            .unwrap_or_else(|| {
                derive_stream_id(database_id, request.branch, request.stream.as_str())
            });
        let batch_id =
            supplied_batch_id.unwrap_or_else(|| BatchId::from_bytes(generate_id_bytes()));
        let first_revision = previous.map_or(0, |revision| revision.0 + 1);
        let timestamp_ms = current_timestamp_ms();
        let mut stored = Vec::with_capacity(request.events.len());
        let mut digests = Vec::with_capacity(request.events.len());

        for (index, (event, body)) in request.events.iter().zip(&serialized).enumerate() {
            let event_id = event
                .event_id
                .unwrap_or_else(|| EventId::from_bytes(generate_id_bytes()));
            let mut metadata = event.metadata.clone();
            metadata.insert(
                "salamander.stream_name".into(),
                request.stream.as_str().as_bytes().to_vec(),
            );
            if let Some(key) = &request.idempotency_key {
                metadata.insert("salamander.idempotency_key".into(), key.as_bytes().to_vec());
                metadata.insert(
                    "salamander.request_digest".into(),
                    request_digest.to_le_bytes().to_vec(),
                );
            }
            let envelope = RecordEnvelopeV2 {
                event_id,
                database_id,
                branch_id: request.branch,
                stream_id,
                stream_revision: StreamRevision(first_revision + index as u64),
                timestamp_unix_nanos: (timestamp_ms as i64).saturating_mul(1_000_000),
                event_type: event.event_type.clone(),
                schema_version: event.schema_version,
                codec: CodecId::RUST_BINCODE_V1,
                batch_id,
                batch_index: index as u32,
                metadata,
            };
            let record = crate::format::OwnedStoredRecord {
                kind: crate::format::FrameKind::Event,
                flags: 0,
                position: self.log.head() + index as u64,
                envelope: envelope.clone(),
                payload: body.clone(),
            };
            let digest = event_fingerprint(&record);
            if self
                .catalog
                .event_digest(event_id)
                .is_some_and(|old| old != digest)
                || digests
                    .iter()
                    .any(|(id, old)| *id == event_id && *old != digest)
            {
                return Err(SalamanderError::EventIdConflict);
            }
            digests.push((event_id, digest));
            stored.push((envelope, body.clone()));
        }

        let supplied_ids: Vec<_> = request.events.iter().map(|event| event.event_id).collect();
        if supplied_ids
            .iter()
            .flatten()
            .any(|id| self.catalog.event_receipt(*id).is_some())
        {
            let mut original: Option<AppendReceipt> = None;
            for (supplied, (_, digest)) in supplied_ids.iter().zip(&digests) {
                let Some(id) = supplied else {
                    return Err(SalamanderError::EventIdConflict);
                };
                let Some((stored_digest, receipt)) = self.catalog.event_receipt(*id) else {
                    return Err(SalamanderError::EventIdConflict);
                };
                if stored_digest != *digest
                    || original
                        .as_ref()
                        .is_some_and(|existing| existing.batch_id != receipt.batch_id)
                {
                    return Err(SalamanderError::EventIdConflict);
                }
                original = Some(receipt);
            }
            let mut original = original.ok_or(SalamanderError::EventIdConflict)?;
            if supplied_batch_id.is_some_and(|id| id != original.batch_id) {
                return Err(SalamanderError::BatchIdConflict);
            }
            if request.durability == Durability::Sync
                && original.durability != ReceiptDurability::Synced
            {
                self.commit()?;
                original.durability = ReceiptDurability::Synced;
            }
            return Ok(original);
        }

        if self.catalog.batch_receipt(batch_id).is_some() {
            return Err(SalamanderError::BatchIdConflict);
        }

        let (first_position, last_position) = self.log.append_batch(&stored)?;
        for (index, event) in request.events.iter().enumerate() {
            let runtime = Event {
                offset: first_position + index as u64,
                timestamp_ms,
                namespace: request.stream.as_str().to_string(),
                body: event.body.clone(),
            };
            for view in self.views.values_mut() {
                view.apply(&runtime);
            }
        }

        self.pending_bytes += serialized.iter().map(Vec::len).sum::<usize>() as u64;
        self.pending_count += request.events.len() as u64;
        let sync = request.durability == Durability::Sync
            || self.policy.should_commit(
                self.pending_bytes,
                self.pending_count,
                self.last_commit.elapsed(),
            );
        if sync {
            self.commit()?;
        }
        let receipt = AppendReceipt {
            batch_id,
            first_position,
            last_position,
            stream_id,
            previous_revision: previous,
            current_revision: StreamRevision(first_revision + request.events.len() as u64 - 1),
            durability: if sync {
                ReceiptDurability::Synced
            } else if request.durability == Durability::Flush {
                ReceiptDurability::Flushed
            } else {
                ReceiptDurability::Buffered
            },
        };
        self.catalog.record_batch(
            request.branch,
            &request.stream,
            stream_id,
            digests,
            request
                .idempotency_key
                .as_ref()
                .map(|key| (key, request_digest)),
            receipt.clone(),
        );
        if sync {
            persist_core_catalog(
                &self.root,
                self.log.database_id().into_bytes(),
                self.durable_head,
                &self.catalog,
            )?;
        }
        Ok(receipt)
    }

    /// Metadata for the branch with `id`, or `None` if it does not exist.
    pub fn branch(&self, id: BranchId) -> Option<&BranchInfo> {
        self.branches.get(id)
    }

    /// Metadata for the branch with the given name, or `None`.
    pub fn branch_named(&self, name: &str) -> Option<&BranchInfo> {
        self.branches.named(name)
    }

    /// The branch's ancestry, root first, ending with `id`.
    pub fn branch_ancestry(&self, id: BranchId) -> Result<Vec<BranchInfo>> {
        self.branches.ancestry(id)
    }

    /// The direct child branches of `id`.
    pub fn branch_children(&self, id: BranchId) -> Vec<BranchInfo> {
        self.branches.children(id)
    }

    /// The nearest common ancestor of two branches.
    pub fn branch_common_ancestor(&self, left: BranchId, right: BranchId) -> Result<BranchInfo> {
        self.branches.common_ancestor(left, right)
    }

    /// The divergence of two timelines as an engine operation — a
    /// position plus three replay plans, computed from the branch catalog
    /// alone (`docs/specs/first-class-diff.md`). No record is read or
    /// compared: two timelines are identical below the divergence position
    /// by construction, because inherited replay is positional (DIFF-1).
    /// Feed the returned plans to [`read`](Self::read) to enumerate the
    /// shared prefix or either divergent suffix; computing the diff itself
    /// performs no log I/O and writes nothing (DIFF-5).
    pub fn diff(&self, request: DiffRequest) -> Result<TimelineDiff> {
        request.streams.validate()?;
        let head = self.log.head();
        let resolve = |end: ReplayEnd| match end {
            ReplayEnd::Head => Ok(head),
            ReplayEnd::At(position) if position > head => {
                Err(SalamanderError::OffsetBeyondHead(position))
            }
            ReplayEnd::At(position) => Ok(position),
        };
        let left_until = resolve(request.left_until)?;
        let right_until = resolve(request.right_until)?;
        let branch = |id: BranchId| {
            self.branches
                .get(id)
                .cloned()
                .ok_or_else(|| SalamanderError::BranchNotFound(format!("{id:?}")))
        };
        let left = branch(request.left)?;
        let right = branch(request.right)?;
        let (common_ancestor, divergence) =
            self.branches
                .divergence(request.left, left_until, request.right, right_until)?;
        let floor = self.retention_floor();
        if divergence < floor {
            return Err(self.position_unavailable(divergence));
        }
        let plan = |branch: BranchId, from: u64, until: u64| ReplayPlan {
            branch,
            streams: request.streams.clone(),
            from: Bound::Included(from),
            until: ReplayEnd::At(until),
            ..ReplayPlan::default()
        };
        Ok(TimelineDiff {
            shared: plan(common_ancestor.id, floor, divergence),
            common_ancestor,
            divergence,
            left: DiffSide {
                suffix: plan(left.id, divergence, left_until),
                branch: left,
                until: left_until,
            },
            right: DiffSide {
                suffix: plan(right.id, divergence, right_until),
                branch: right,
                until: right_until,
            },
        })
    }

    /// Build a bounded-memory streaming reader for `plan` (WP-04). The
    /// plan's branch is resolved to its flattened ancestry scopes, so
    /// inherited parent history is visible through the fork point; every
    /// other selection (streams, position window, time, max events) is
    /// applied by the reader from envelope data alone.
    pub fn read(&self, plan: ReplayPlan) -> Result<LogReader<'_>> {
        plan.streams.validate()?;
        let head = self.log.head();
        let until = match plan.until {
            ReplayEnd::Head => head,
            ReplayEnd::At(position) => {
                if position > head {
                    return Err(SalamanderError::OffsetBeyondHead(position));
                }
                position
            }
        };
        let from = match plan.from {
            Bound::Unbounded => 0,
            Bound::Included(position) => position,
            Bound::Excluded(position) => position.saturating_add(1),
        };
        if until < self.retention_floor() {
            return Err(self.position_unavailable(until));
        }
        if from < self.retention_floor() {
            return Err(self.position_unavailable(from));
        }
        let scopes = self.branches.replay_scopes(plan.branch, until)?;
        Ok(self.log.plan_reader(ResolvedFilter {
            from,
            until,
            selector: plan.streams,
            scopes: Some(scopes),
            time: plan.time,
            kinds: FrameFilter::UserEvents,
            max_events: plan.max_events,
            verification: plan.verification,
        }))
    }

    /// Replays the events of `namespace` visible on `branch` within
    /// `range`, in order, invoking `f` on each — inherited parent history
    /// is included through the fork point.
    pub fn replay_branch(
        &self,
        branch: BranchId,
        namespace: &str,
        range: Range<u64>,
        mut f: impl FnMut(&Event<B>),
    ) -> Result<()> {
        let mut reader = self.read(ReplayPlan {
            branch,
            from: Bound::Included(range.start),
            until: ReplayEnd::At(range.end),
            ..ReplayPlan::default()
        })?;
        while let Some(record) = reader.next()? {
            let record = OwnedStoredRecord::from(record);
            let event = decode_stored_event::<B>(&record)?;
            if event.namespace == namespace {
                f(&event);
            }
        }
        Ok(())
    }

    /// Creates a branch forked from `parent` at position `at`, which must
    /// be a committed batch boundary visible in the parent. The child
    /// inherits parent history up to `at` and then diverges; the parent is
    /// unaffected.
    pub fn fork_branch(
        &mut self,
        parent: BranchId,
        at: u64,
        name: BranchName,
        metadata: Metadata,
    ) -> Result<BranchInfo> {
        if self.branches.get(parent).is_none() {
            return Err(SalamanderError::BranchNotFound(format!("{parent:?}")));
        }
        if at > self.head() {
            return Err(SalamanderError::OffsetBeyondHead(at));
        }
        if at < self.retention_floor() {
            return Err(self.position_unavailable(at));
        }
        if !self.is_batch_boundary(at)? {
            return Err(SalamanderError::NotBatchBoundary(at));
        }
        let id = BranchId::from_bytes(generate_id_bytes());
        let info = BranchInfo {
            id,
            name,
            parent: Some(parent),
            fork_position: Some(at),
            created_at_unix_nanos: (current_timestamp_ms() as i64).saturating_mul(1_000_000),
            metadata,
            status: BranchStatus::Active,
        };
        // Validate on a candidate catalog before writing; publish the new
        // in-memory graph only after the system frame is accepted.
        let mut updated_branches = self.branches.clone();
        updated_branches.insert(info.clone())?;
        let payload = serde_json::to_vec(&info)
            .map_err(|error| SalamanderError::Serialization(error.to_string()))?;
        let event_bytes = generate_id_bytes();
        let envelope = RecordEnvelopeV2 {
            event_id: EventId::from_bytes(event_bytes),
            database_id: self.log.database_id(),
            branch_id: id,
            stream_id: crate::StreamId::ZERO,
            stream_revision: StreamRevision(0),
            timestamp_unix_nanos: info.created_at_unix_nanos,
            event_type: EventType::new("salamander.branch.created")?,
            schema_version: 1,
            codec: CodecId::JSON_UTF8,
            batch_id: BatchId::from_bytes(event_bytes),
            batch_index: 0,
            metadata: Metadata::new(),
        };
        self.log.append_system(&envelope, &payload)?;
        if let Err(error) = self.commit() {
            self.branches = BranchCatalog::rebuild(self.log.system_records())?;
            return Err(error);
        }
        self.branches = updated_branches;
        Ok(info)
    }

    /// Archives a branch: it keeps its readable history but rejects new
    /// writes. The default branch cannot be archived.
    pub fn archive_branch(&mut self, id: BranchId) -> Result<BranchInfo> {
        let mut info = self
            .branches
            .get(id)
            .cloned()
            .ok_or_else(|| SalamanderError::BranchNotFound(format!("{id:?}")))?;
        if id == BranchId::ZERO {
            return Err(SalamanderError::InvalidArgument(
                "the default branch cannot be archived".into(),
            ));
        }
        if info.status == BranchStatus::Archived {
            return Ok(info);
        }
        info.status = BranchStatus::Archived;
        let mut updated_branches = self.branches.clone();
        updated_branches.archive(info.clone())?;
        let payload = serde_json::to_vec(&info)
            .map_err(|error| SalamanderError::Serialization(error.to_string()))?;
        let event_bytes = generate_id_bytes();
        let envelope = RecordEnvelopeV2 {
            event_id: EventId::from_bytes(event_bytes),
            database_id: self.log.database_id(),
            branch_id: id,
            stream_id: crate::StreamId::ZERO,
            stream_revision: StreamRevision(0),
            timestamp_unix_nanos: (current_timestamp_ms() as i64).saturating_mul(1_000_000),
            event_type: EventType::new("salamander.branch.archived")?,
            schema_version: 1,
            codec: CodecId::JSON_UTF8,
            batch_id: BatchId::from_bytes(event_bytes),
            batch_index: 0,
            metadata: Metadata::new(),
        };
        self.log.append_system(&envelope, &payload)?;
        if let Err(error) = self.commit() {
            self.branches = BranchCatalog::rebuild(self.log.system_records())?;
            return Err(error);
        }
        self.branches = updated_branches;
        Ok(info)
    }

    fn is_batch_boundary(&self, at: u64) -> Result<bool> {
        if at == 0 || at == self.head() {
            return Ok(true);
        }
        // Stream just the two records either side of the boundary; the
        // reader stops after them instead of materializing the log tail.
        let mut before = None;
        let mut after = None;
        for item in self.log.records_from(at - 1) {
            let record = item?;
            if record.position == at - 1 {
                before = Some(record.envelope.batch_id);
            } else if record.position >= at {
                after = (record.position == at).then_some(record.envelope.batch_id);
                break;
            }
        }
        Ok(matches!((before, after), (Some(left), Some(right)) if left != right))
    }

    fn position_unavailable(&self, requested: u64) -> SalamanderError {
        SalamanderError::PositionUnavailable {
            requested,
            floor: self.retention_floor(),
            head: self.head(),
            bootstrap_available: false,
        }
    }

    /// fsync the log and return the durable head (DESIGN.md §3.3). Always
    /// available regardless of the commit policy; resets the group-commit
    /// counters so the next auto-commit measures from here.
    pub fn commit(&mut self) -> Result<u64> {
        let head = self.log.commit()?;
        self.durable_head = head;
        self.pending_bytes = 0;
        self.pending_count = 0;
        self.last_commit = Instant::now();
        persist_core_catalog(
            &self.root,
            self.log.database_id().into_bytes(),
            head,
            &self.catalog,
        )?;
        Ok(head)
    }

    /// Payload bytes appended but not yet committed (fsynced). Reset to 0 by
    /// `commit()` and by any auto-commit the policy triggers.
    pub fn uncommitted_bytes(&self) -> u64 {
        self.pending_bytes
    }

    /// Events appended but not yet committed (fsynced).
    pub fn uncommitted_count(&self) -> u64 {
        self.pending_count
    }

    /// Full rebuild: a fresh `P`, replayed to `head()`. The projection's
    /// `Body` must match this engine's payload type `B` — you can't fold a
    /// log of one payload type with a projection written for another.
    pub fn projection<P: Projection<Body = B> + Default>(&self) -> Result<P> {
        self.require_full_history()?;
        let mut p = P::default();
        replay_into(&mut p, &self.log, self.log.head())?;
        Ok(p)
    }

    /// Full rebuild of a namespace-scoped projection, replayed to
    /// `head()`. This is the plain, non-stitched view: for the agent
    /// `SessionProjection` specifically, prefer [`crate::agent`]'s
    /// `session_view` if `namespace` might be a fork (see its doc comment
    /// for why).
    pub fn projection_for<P: NamespaceScoped<Body = B>>(&self, namespace: &str) -> Result<P> {
        self.require_full_history()?;
        let mut p = P::new_for(namespace);
        replay_into(&mut p, &self.log, self.log.head())?;
        Ok(p)
    }

    /// Read-only projection as of offset `n` (DESIGN.md §5, time-travel).
    /// For views that can't be `Default`-constructed (e.g. `IndexedView`,
    /// which owns closures), use [`replay_to`](Self::replay_to) instead.
    pub fn view_at<P: Projection<Body = B> + Default>(&self, n: u64) -> Result<P> {
        if n > self.log.head() {
            return Err(SalamanderError::OffsetBeyondHead(n));
        }
        self.require_full_history()?;
        let mut p = P::default();
        replay_into(&mut p, &self.log, n)?;
        Ok(p)
    }

    // ── query layer: live registered views (query-layer design §4) ───────

    /// Register a live view under `name`, catching it up from its cursor to
    /// head before it starts receiving fan-out (so it's immediately at
    /// head, INV-2). Re-registering a name replaces the previous view.
    pub fn register(&mut self, name: &str, mut view: Box<dyn View<B>>) -> Result<()> {
        self.require_full_history()?;
        catch_up(view.as_mut(), &self.log, self.log.head())?;
        self.views.insert(name.to_string(), view);
        Ok(())
    }

    /// Remove and return a registered view (query-layer design OQ-Q2 — a
    /// long-lived host must be able to reclaim view memory). `None` if no
    /// view is registered under `name`.
    pub fn deregister(&mut self, name: &str) -> Option<Box<dyn View<B>>> {
        self.views.remove(name)
    }

    /// Typed, read-only access to a registered view: downcast the erased
    /// `dyn View<B>` back to the concrete `T` the query methods live on.
    /// `None` if `name` isn't registered or the type doesn't match.
    ///
    /// The borrow checker enforces the one correctness rule for free: this
    /// takes `&self`, `append` takes `&mut self`, so a query reference can
    /// never be held across an append — you can't query a half-updated view.
    pub fn view<T: View<B>>(&self, name: &str) -> Option<&T> {
        self.views.get(name)?.as_any().downcast_ref::<T>()
    }

    /// Time-travel for a caller-constructed view: hand in a fresh, empty
    /// view/projection and get it back replayed to offset `n`. This is the
    /// historical counterpart to `register` (which replays to head) and the
    /// path for non-`Default` views like `IndexedView` (query-layer design
    /// §4.3 — "one view type, two modes").
    pub fn replay_to<P: Projection<Body = B>>(&self, mut view: P, n: u64) -> Result<P> {
        if n > self.log.head() {
            return Err(SalamanderError::OffsetBeyondHead(n));
        }
        self.require_full_history()?;
        replay_into(&mut view, &self.log, n)?;
        Ok(view)
    }

    /// Next offset to be assigned.
    pub fn head(&self) -> u64 {
        self.log.head()
    }

    /// Exclusive upper position proven durable by the latest successful sync.
    pub fn durable_head(&self) -> u64 {
        self.durable_head
    }

    pub(crate) fn stream_id(&self, branch: BranchId, stream: &StreamName) -> Option<StreamId> {
        self.catalog.stream_id(branch, stream)
    }

    /// Raw event iteration over `namespace` within `range` (DESIGN.md §7).
    pub fn replay(
        &self,
        namespace: &str,
        range: Range<u64>,
        f: impl FnMut(&Event<B>),
    ) -> Result<()> {
        if range.start < self.retention_floor() {
            return Err(self.position_unavailable(range.start));
        }
        crate::introspect::replay(&self.log, namespace, range, f)
    }

    fn require_full_history(&self) -> Result<()> {
        if self.retention_floor() > 0 {
            return Err(self.position_unavailable(0));
        }
        Ok(())
    }
}

/// What to diff: two timelines, each a branch bounded by an exclusive
/// until, plus a stream selector scoped onto the emitted plans. See
/// [`Salamander::diff`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiffRequest {
    /// The left timeline's branch.
    pub left: BranchId,
    /// The right timeline's branch.
    pub right: BranchId,
    /// Exclusive upper bound of the left timeline (default: head).
    pub left_until: ReplayEnd,
    /// Exclusive upper bound of the right timeline (default: head).
    pub right_until: ReplayEnd,
    /// Which streams the emitted replay plans select. The divergence
    /// position itself is positional and stream-independent.
    pub streams: StreamSelector,
}

impl DiffRequest {
    /// A whole-timeline diff of two branches at head, all streams.
    pub fn new(left: BranchId, right: BranchId) -> Self {
        Self {
            left,
            right,
            left_until: ReplayEnd::Head,
            right_until: ReplayEnd::Head,
            streams: StreamSelector::All,
        }
    }
}

/// One side of a [`TimelineDiff`]: the branch, its resolved until, and the
/// replay plan for its divergent suffix `[divergence, until)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiffSide {
    /// The branch this side describes.
    pub branch: BranchInfo,
    /// The resolved exclusive upper bound of this timeline.
    pub until: u64,
    /// Replay plan for this timeline's records past the divergence.
    pub suffix: ReplayPlan,
}

/// The result of [`Salamander::diff`]: where two timelines share history
/// and what each says after that — a position plus three replay plans.
/// Both timelines replay identically below [`divergence`](Self::divergence)
/// by construction; no record comparison is involved (DIFF-1, DIFF-6).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimelineDiff {
    /// The deepest branch node the two ancestries share.
    pub common_ancestor: BranchInfo,
    /// Exclusive upper bound of the shared history.
    pub divergence: u64,
    /// Replay plan for the shared prefix `[0, divergence)`, on the common
    /// ancestor's timeline — resolving it against either side yields the
    /// same records.
    pub shared: ReplayPlan,
    /// The left timeline's branch, until, and suffix plan.
    pub left: DiffSide,
    /// The right timeline's branch, until, and suffix plan.
    pub right: DiffSide,
}

fn current_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn validate_expected(expected: ExpectedRevision, actual: Option<StreamRevision>) -> Result<()> {
    let matches = match expected {
        ExpectedRevision::Any => true,
        ExpectedRevision::NoStream => actual.is_none(),
        ExpectedRevision::Exact(expected) => actual == Some(expected),
    };
    if matches {
        return Ok(());
    }
    Err(SalamanderError::RevisionConflict {
        expected: format!("{expected:?}"),
        actual: format!("{actual:?}"),
    })
}

fn request_fingerprint<B>(request: &AppendRequest<B>, bodies: &[Vec<u8>]) -> u32 {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(request.branch.as_bytes());
    bytes.extend_from_slice(request.stream.as_str().as_bytes());
    bytes.extend_from_slice(format!("{:?}", request.expected).as_bytes());
    for (event, body) in request.events.iter().zip(bodies) {
        bytes.extend_from_slice(event.event_id.unwrap_or(EventId::ZERO).as_bytes());
        bytes.extend_from_slice(event.event_type.as_str().as_bytes());
        bytes.extend_from_slice(&event.schema_version.to_le_bytes());
        for (key, value) in &event.metadata {
            bytes.extend_from_slice(key.as_bytes());
            bytes.extend_from_slice(value);
        }
        bytes.extend_from_slice(body);
    }
    crc32c::crc32c(&bytes)
}
