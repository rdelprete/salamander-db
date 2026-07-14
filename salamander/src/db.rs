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
use crate::log::{Log, LogReader, RecordReader, ReplayEnd, ReplayPlan};
use crate::projection::{decode_stored_event, replay_into, NamespaceScoped, Projection};
use crate::stream::{event_fingerprint, StreamCatalog};
use crate::view::{catch_up, View};
use crate::{
    AppendReceipt, AppendRequest, Durability, ExpectedRevision, ReceiptDurability, Result,
    SalamanderError, StreamName,
};

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
        let root = dir.as_ref().to_path_buf();
        let log = Log::open(&root)?;
        let catalog = match load_core_catalog(&root, log.database_id().into_bytes(), log.head()) {
            Some(catalog) => catalog,
            None => {
                let catalog = StreamCatalog::rebuild(log.records_from(0))?;
                // Best effort: cache failure cannot make an otherwise valid
                // log unavailable, and the next commit retries publication.
                let _ = persist_core_catalog(
                    &root,
                    log.database_id().into_bytes(),
                    log.head(),
                    &catalog,
                );
                catalog
            }
        };
        let branches = BranchCatalog::rebuild(log.system_records())?;
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
        let mut p = P::default();
        replay_into(&mut p, &self.log, n)?;
        Ok(p)
    }

    // ── query layer: live registered views (query-layer design §4) ───────

    /// Register a live view under `name`, catching it up from its cursor to
    /// head before it starts receiving fan-out (so it's immediately at
    /// head, INV-2). Re-registering a name replaces the previous view.
    pub fn register(&mut self, name: &str, mut view: Box<dyn View<B>>) -> Result<()> {
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
        crate::introspect::replay(&self.log, namespace, range, f)
    }
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
