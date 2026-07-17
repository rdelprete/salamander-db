//! DESIGN.md §3 — the log: the only durable structure. This module knows
//! nothing above it — no Event, no projections, only `&[u8]` payloads at
//! offsets (IMPLEMENTATION.md §1, layout principles).

mod index;
mod lock;
mod manifest;
pub(crate) mod reader;
mod record;
mod segment;

pub use manifest::Manifest;
pub use reader::{
    partition_of, LogReader, RecordReader, ReplayEnd, ReplayPlan, StreamSelector, VerificationMode,
};
pub use segment::{Segment, SegmentMeta};

use std::cell::RefCell;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::format::{generate_id_bytes, DatabaseId, OwnedStoredRecord, RecordEnvelopeV2};
use crate::{Result, SalamanderError};
use lock::DirLock;
use reader::{FrameFilter, PlannedSegment, ResolvedFilter, SegmentSource};

/// DESIGN.md §3.1: a segment is closed when it reaches this size.
const DEFAULT_SEGMENT_MAX_BYTES: u64 = 64 * 1024 * 1024;
pub(crate) const MIGRATION_IN_PROGRESS: &str = "migration.in-progress.json";

pub struct Log {
    dir: PathBuf,
    closed: Vec<SegmentMeta>,
    active: Segment,
    manifest: Manifest,
    segment_max_bytes: u64,
    /// Lazily loaded/built sidecars for closed segments, keyed by base
    /// offset. Closed segments are immutable, so a cached sidecar never
    /// goes stale. `RefCell` because readers only hold `&Log`; the engine
    /// is single-writer and this cache is not shared across threads.
    sidecars: RefCell<HashMap<u64, Arc<index::Sidecar>>>,
    /// Held for the lifetime of the open log; its `Drop` releases the
    /// single-writer lock (review M-2). Never read directly.
    _lock: DirLock,
}

/// Owned-record adapter over the streaming reader — the `Iterator` face
/// used by catalog rebuilds and replay helpers. Streams through the
/// bounded reader; it never materializes segments.
pub struct StoredLogIter<'log> {
    reader: LogReader<'log>,
    failed: bool,
}

impl Iterator for StoredLogIter<'_> {
    type Item = Result<OwnedStoredRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        match self.reader.next_owned() {
            Ok(Some(record)) => Some(Ok(record)),
            Ok(None) => None,
            Err(error) => {
                self.failed = true;
                Some(Err(error))
            }
        }
    }
}

impl Log {
    /// Open (or create) the log directory, recovering per DESIGN.md §6
    /// cases C1 (torn active-segment tail) and C3 (interrupted roll).
    pub fn open(dir: &Path) -> Result<Self> {
        Self::open_internal(dir, DEFAULT_SEGMENT_MAX_BYTES, false)
    }

    #[allow(dead_code)]
    pub(crate) fn open_with_segment_max_bytes(dir: &Path, segment_max_bytes: u64) -> Result<Self> {
        Self::open_internal(dir, segment_max_bytes, false)
    }

    pub(crate) fn open_for_migration(dir: &Path) -> Result<Self> {
        Self::open_internal(dir, DEFAULT_SEGMENT_MAX_BYTES, true)
    }

    fn open_internal(dir: &Path, segment_max_bytes: u64, allow_migration: bool) -> Result<Self> {
        std::fs::create_dir_all(dir)?;

        if !allow_migration && dir.join(MIGRATION_IN_PROGRESS).exists() {
            return Err(SalamanderError::MigrationIncomplete(
                dir.display().to_string(),
            ));
        }

        // Acquire the single-writer lock before touching anything else, so
        // a second process bails out immediately rather than racing on the
        // manifest and active segment (review M-2). The guard lives in a
        // local until it's moved into `Log` at the end — if any `?` below
        // early-returns, dropping the local releases the lock.
        let dir_lock = DirLock::acquire(dir)?;

        let segs_dir = segments_dir(dir);
        std::fs::create_dir_all(&segs_dir)?;

        let manifest = match Manifest::read(dir) {
            Ok(m) => m,
            Err(SalamanderError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
                bootstrap_manifest(dir, &segs_dir)?
            }
            Err(other) => return Err(other),
        };

        if manifest.storage_format_version != manifest::STORAGE_FORMAT_VERSION {
            return Err(SalamanderError::UnsupportedStorageFormat {
                found: manifest.storage_format_version,
                supported: manifest::STORAGE_FORMAT_VERSION,
            });
        }

        // Refuse a dir whose payload encoding this build doesn't know,
        // *before* any record gets fed to bincode — a newer, unknown
        // encoding would otherwise deserialize into plausible-looking
        // garbage rather than a clear error (OQ-2b). Bootstrapped manifests
        // always carry the current version, so this only ever fires for a
        // dir written by a different build. The lock acquired above is
        // released when `dir_lock` drops on this early return.
        if manifest.payload_format_version != manifest::PAYLOAD_FORMAT_VERSION {
            return Err(SalamanderError::UnsupportedFormat {
                found: manifest.payload_format_version,
                supported: manifest::PAYLOAD_FORMAT_VERSION,
            });
        }

        let discovered = discover_segments(&segs_dir)?;

        let mut closed = Vec::new();
        let mut active = None;

        for (base, path) in discovered {
            match base.cmp(&manifest.active_segment_base) {
                std::cmp::Ordering::Equal => {
                    active = Some(Segment::open(&path)?); // C1 torn-tail recovery
                }
                std::cmp::Ordering::Less => {
                    closed.push(SegmentMeta {
                        base_offset: base,
                        path,
                    });
                }
                std::cmp::Ordering::Greater => {
                    // DESIGN.md §6 case C3: an orphan from a roll that
                    // crashed after creating the file but before the
                    // manifest update. It can only ever be empty — nothing
                    // could have been durably appended to it before the
                    // manifest pointed at it, so it's always safe to
                    // delete rather than adopt.
                    let len = std::fs::metadata(&path)?.len();
                    if len != 0 {
                        return Err(SalamanderError::Manifest(format!(
                            "orphaned segment {path:?} beyond the active one is non-empty ({len} bytes) — refusing to delete"
                        )));
                    }
                    std::fs::remove_file(&path)?;
                }
            }
        }

        let active = active.ok_or_else(|| {
            SalamanderError::Manifest(format!(
                "manifest points at missing active segment base {}",
                manifest.active_segment_base
            ))
        })?;

        // The active-segment scan is authoritative for the next offset;
        // the persisted manifest value is only advisory (see Manifest's
        // doc comment, review M-4). Overwrite it with the scanned truth.
        let mut manifest = manifest;
        manifest.next_offset = active.next_offset();
        if manifest.retention_floor > manifest.next_offset {
            return Err(SalamanderError::Manifest(format!(
                "retention floor {} is beyond head {}",
                manifest.retention_floor, manifest.next_offset
            )));
        }

        Ok(Log {
            dir: dir.to_path_buf(),
            closed,
            active,
            manifest,
            segment_max_bytes,
            sidecars: RefCell::new(HashMap::new()),
            _lock: dir_lock,
        })
    }

    #[allow(dead_code)]
    pub fn append(&mut self, payload: &[u8]) -> Result<u64> {
        if self.active.len_bytes() >= self.segment_max_bytes {
            self.roll()?;
        }
        self.active.append(payload)
    }

    pub fn append_enveloped(&mut self, envelope: &RecordEnvelopeV2, payload: &[u8]) -> Result<u64> {
        if self.active.len_bytes() >= self.segment_max_bytes {
            self.roll()?;
        }
        self.active.append_enveloped(envelope, payload)
    }

    pub fn append_batch(&mut self, events: &[(RecordEnvelopeV2, Vec<u8>)]) -> Result<(u64, u64)> {
        if self.active.len_bytes() >= self.segment_max_bytes {
            self.roll()?;
        }
        self.active.append_batch(events)
    }

    pub fn append_system(&mut self, envelope: &RecordEnvelopeV2, payload: &[u8]) -> Result<()> {
        if self.active.len_bytes() >= self.segment_max_bytes {
            self.roll()?;
        }
        self.active.append_system(envelope, payload)
    }

    /// fsync; returns the durable head (DESIGN.md §3.3).
    pub fn commit(&mut self) -> Result<u64> {
        self.active.sync()?;
        Ok(self.head())
    }

    pub fn head(&self) -> u64 {
        self.active.next_offset()
    }

    pub fn retention_floor(&self) -> u64 {
        self.manifest.retention_floor
    }

    pub(crate) fn retention_generation(&self) -> u64 {
        self.manifest.retention_generation
    }

    pub(crate) fn retention_anchor_checksum(&self) -> Option<u32> {
        self.manifest.retention_anchor_checksum
    }

    pub(crate) fn activate_retention(&mut self, floor: u64, checksum: u32) -> Result<()> {
        if floor < self.retention_floor() || floor > self.head() {
            return Err(SalamanderError::InvalidArgument(format!(
                "invalid retention floor {floor} for current floor {} and head {}",
                self.retention_floor(),
                self.head()
            )));
        }
        let mut manifest = self.manifest.clone();
        manifest.retention_floor = floor;
        manifest.retention_generation = manifest.retention_generation.saturating_add(1);
        manifest.retention_anchor_checksum = Some(checksum);
        manifest.next_offset = self.head();
        manifest.write(&self.dir)?;
        self.manifest = manifest;
        Ok(())
    }

    pub(crate) fn reclaim_below_retention_floor(&mut self) -> u64 {
        let floor = self.retention_floor();
        let mut reclaimed = 0u64;
        self.closed.retain(|segment| {
            if segment.base_offset >= floor {
                return true;
            }
            let bytes = std::fs::metadata(&segment.path)
                .map(|metadata| metadata.len())
                .unwrap_or(0);
            match std::fs::remove_file(&segment.path) {
                Ok(()) => {
                    reclaimed = reclaimed.saturating_add(bytes);
                    crate::retention::crash_point("during_cleanup");
                    false
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
                Err(_) => true,
            }
        });
        self.sidecars.borrow_mut().retain(|base, _| *base >= floor);
        reclaimed
    }

    pub(crate) fn has_complete_prefix(&self) -> bool {
        self.closed.first().map_or_else(
            || self.active.base_offset() == 0,
            |segment| segment.base_offset == 0,
        )
    }

    pub(crate) fn retention_boundary(&self, requested: u64) -> (u64, Vec<(u64, u64)>) {
        let mut bases: Vec<u64> = self
            .closed
            .iter()
            .map(|segment| segment.base_offset)
            .collect();
        bases.push(self.active.base_offset());
        let effective = bases
            .iter()
            .copied()
            .filter(|base| *base <= requested)
            .max()
            .unwrap_or(self.retention_floor())
            .max(self.retention_floor());
        let reclaimable = self
            .closed
            .iter()
            .filter(|segment| segment.base_offset < effective)
            .filter_map(|segment| {
                std::fs::metadata(&segment.path)
                    .ok()
                    .map(|metadata| (segment.base_offset, metadata.len()))
            })
            .collect();
        (effective, reclaimable)
    }

    pub(crate) fn retention_floor_for_bytes(&self, target_bytes: u64) -> Result<(u64, u64, bool)> {
        let mut segments = Vec::new();
        for segment in self
            .closed
            .iter()
            .filter(|segment| segment.base_offset >= self.retention_floor())
        {
            segments.push((segment.base_offset, std::fs::metadata(&segment.path)?.len()));
        }
        let active_bytes = self.active.len_bytes();
        segments.push((self.active.base_offset(), active_bytes));
        segments.sort_unstable_by_key(|(base, _)| *base);
        let mut retained = segments.iter().map(|(_, bytes)| *bytes).sum::<u64>();
        for (base, bytes) in &segments {
            if retained <= target_bytes {
                return Ok((*base, retained, true));
            }
            if *base != self.active.base_offset() {
                retained = retained.saturating_sub(*bytes);
            }
        }
        Ok((
            self.active.base_offset(),
            active_bytes,
            active_bytes <= target_bytes,
        ))
    }

    pub fn database_id(&self) -> DatabaseId {
        DatabaseId::from_bytes(self.manifest.database_id)
    }

    /// Stream user-event records with position >= `offset`, oldest first.
    /// Backed by the WP-04 bounded-memory reader: segments below `offset`
    /// are pruned by binary search and never opened.
    pub fn records_from(&self, offset: u64) -> StoredLogIter<'_> {
        let filter = ResolvedFilter::raw(offset, self.head(), FrameFilter::UserEvents);
        StoredLogIter {
            reader: self.plan_reader(filter),
            failed: false,
        }
    }

    /// Stream every system frame in the log. System frames carry no
    /// position window of their own, so all segments are considered;
    /// sidecar system-frame counts let closed segments without any be
    /// skipped unopened.
    pub fn system_records(&self) -> StoredLogIter<'_> {
        let filter = ResolvedFilter::raw(0, u64::MAX, FrameFilter::SystemOnly);
        StoredLogIter {
            reader: self.plan_reader(filter),
            failed: false,
        }
    }

    /// Build a [`LogReader`] for a resolved filter: prune segments to the
    /// plan's position window (metadata arithmetic only — no file I/O
    /// here) and hand the reader the ordered slice it may need to open.
    pub(crate) fn plan_reader(&self, filter: ResolvedFilter) -> LogReader<'_> {
        let active_base = self.active.base_offset();
        let head = self.head();
        let mut segments = Vec::new();

        let closed_end = |i: usize| -> u64 {
            self.closed
                .get(i + 1)
                .map_or(active_base, |m| m.base_offset)
        };

        match filter.kinds {
            // System frames do not advance positions, so the position
            // window cannot prune segments for a system read.
            FrameFilter::SystemOnly => {
                for (i, meta) in self.closed.iter().enumerate() {
                    segments.push(PlannedSegment {
                        base: meta.base_offset,
                        end: closed_end(i),
                        source: SegmentSource::Closed(meta.path.clone()),
                    });
                }
                segments.push(PlannedSegment {
                    base: active_base,
                    end: head,
                    source: SegmentSource::Active,
                });
            }
            FrameFilter::UserEvents => {
                let bases: Vec<u64> = self
                    .closed
                    .iter()
                    .map(|m| m.base_offset)
                    .chain([active_base])
                    .collect();
                for i in reader::intersecting_range(&bases, head, filter.from, filter.until) {
                    if i < self.closed.len() {
                        segments.push(PlannedSegment {
                            base: self.closed[i].base_offset,
                            end: closed_end(i),
                            source: SegmentSource::Closed(self.closed[i].path.clone()),
                        });
                    } else {
                        segments.push(PlannedSegment {
                            base: active_base,
                            end: head,
                            source: SegmentSource::Active,
                        });
                    }
                }
            }
        }
        LogReader::new(self, filter, segments)
    }

    /// A cloned handle of the active segment for a reader to stream from.
    pub(crate) fn active_handle(&self) -> Result<std::fs::File> {
        self.active.clone_handle()
    }

    /// The validated sidecar for a closed segment, from cache, disk, or a
    /// fresh build (persisted best-effort). `None` means "no acceleration
    /// available" — the caller scans the segment; genuine damage surfaces
    /// there, never here.
    pub(crate) fn sidecar_for(
        &self,
        path: &Path,
        base: u64,
        end: u64,
    ) -> Option<Arc<index::Sidecar>> {
        if let Some(sidecar) = self.sidecars.borrow().get(&base) {
            return Some(sidecar.clone());
        }
        let sidecar = index::load(path, base, end).or_else(|| {
            let built = reader::build_sidecar(path, base).ok()?;
            // A layout mismatch means our metadata and the file disagree;
            // refuse the sidecar and let the scan report the damage.
            if built.end != end {
                return None;
            }
            if let Err(error) = index::persist(path, &built) {
                eprintln!(
                    "salamander: could not persist sidecar for {}: {error}",
                    path.display()
                );
            }
            Some(built)
        })?;
        let sidecar = Arc::new(sidecar);
        self.sidecars.borrow_mut().insert(base, sidecar.clone());
        Some(sidecar)
    }

    /// Ordering, in full:
    ///   1. fsync the retiring segment (DESIGN.md §3.3 — "fsync on segment
    ///      close"). It's about to become immutable, and `commit()` only
    ///      ever syncs the *active* segment; without this, records buffered
    ///      in the old segment could still be in the page cache when the
    ///      caller was told (via a later commit's returned head) they were
    ///      durable. Sync before the manifest flips so the manifest never
    ///      points past data that isn't durable (review C-1).
    ///   2. create the new segment, fsync the directory (durable existence).
    ///   3. update + write the manifest (DESIGN.md §6 case C3). A crash
    ///      between 2 and 3 leaves an orphaned empty segment, which `open`
    ///      deletes on the next recovery — the manifest can never end up
    ///      pointing at a segment that doesn't exist.
    fn roll(&mut self) -> Result<()> {
        let new_base = self.active.next_offset();
        let segs_dir = segments_dir(&self.dir);

        self.active.sync()?;

        let new_active = Segment::create(&segs_dir, new_base)?;
        sync_dir(&segs_dir)?;

        let old_active = std::mem::replace(&mut self.active, new_active);
        let retired_base = old_active.base_offset();
        let retired_path = segment::segment_path(&segs_dir, retired_base);
        self.closed.push(SegmentMeta {
            base_offset: retired_base,
            path: retired_path.clone(),
        });

        self.manifest.active_segment_base = new_base;
        self.manifest.next_offset = new_base;
        self.manifest.write(&self.dir)?;

        // Build the retired segment's sidecar eagerly (WP-04): the segment
        // is now immutable, its bytes are still warm in the page cache,
        // and readers get skip/seek acceleration from the start. Strictly
        // best-effort — a failure means readers rebuild lazily instead.
        match reader::build_sidecar(&retired_path, retired_base) {
            Ok(sidecar) => {
                if let Err(error) = index::persist(&retired_path, &sidecar) {
                    eprintln!(
                        "salamander: could not persist sidecar for {}: {error}",
                        retired_path.display()
                    );
                }
                self.sidecars
                    .borrow_mut()
                    .insert(retired_base, Arc::new(sidecar));
            }
            Err(error) => eprintln!(
                "salamander: could not index retired segment {}: {error}",
                retired_path.display()
            ),
        }

        Ok(())
    }
}

fn bootstrap_manifest(dir: &Path, segs_dir: &Path) -> Result<Manifest> {
    let discovered = discover_segments(segs_dir)?;
    if discovered.len() > 1 {
        return Err(SalamanderError::Manifest(
            "multiple segments on disk but no manifest — unexpected state".into(),
        ));
    }

    let active_base = match discovered.first() {
        // A lone segment with no manifest: a crashed first-ever open()
        // that created the segment but never got to write the manifest
        // (DESIGN.md §6 case C3, applied to bootstrap). Unlike a mid-roll
        // orphan, there's no prior valid manifest to fall back to, so this
        // one gets adopted rather than deleted.
        Some((base, _)) => *base,
        None => {
            Segment::create(segs_dir, 0)?;
            sync_dir(segs_dir)?;
            0
        }
    };

    let manifest = Manifest {
        format_version: manifest::MANIFEST_FORMAT_VERSION,
        storage_format_version: manifest::STORAGE_FORMAT_VERSION,
        database_id: generate_id_bytes(),
        payload_format_version: manifest::PAYLOAD_FORMAT_VERSION,
        active_segment_base: active_base,
        retention_floor: 0,
        retention_generation: 0,
        retention_anchor_checksum: None,
        next_offset: active_base,
    };
    manifest.write(dir)?;
    Ok(manifest)
}

fn discover_segments(segs_dir: &Path) -> Result<Vec<(u64, PathBuf)>> {
    let mut found = Vec::new();
    for entry in std::fs::read_dir(segs_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("seg") {
            continue;
        }
        // A `.seg` file whose stem isn't a valid base offset (e.g. a
        // stray `backup.seg`) shouldn't make the whole DB unopenable —
        // warn and skip it rather than aborting `open` (review M-5).
        // `parse_base_offset` stays an error for `Segment::open`/`scan`
        // called directly on a bad path, which is a genuine caller bug.
        match segment::parse_base_offset(&path) {
            Ok(base) => found.push((base, path)),
            Err(_) => {
                eprintln!(
                    "salamander: ignoring non-segment file in log dir: {}",
                    path.display()
                );
            }
        }
    }
    found.sort_by_key(|(base, _)| *base);
    Ok(found)
}

fn segments_dir(dir: &Path) -> PathBuf {
    dir.join("log")
}

#[cfg(unix)]
fn sync_dir(dir: &Path) -> Result<()> {
    std::fs::File::open(dir)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_dir(_dir: &Path) -> Result<()> {
    // Windows: std::fs can't open a directory handle the way POSIX
    // fsync-the-parent needs, and NTFS's own metadata journaling gives
    // adequate durability for Phase 1's single-writer, single-machine
    // scope. Revisit if this ever needs to be bulletproof across
    // power-loss specifically on Windows.
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    /// `(position, payload)` pairs streamed through the WP-04 reader —
    /// the shape the old vector-backed `iter_from` used to return.
    fn payloads_from(log: &Log, offset: u64) -> Vec<(u64, Vec<u8>)> {
        log.records_from(offset)
            .map(|item| item.map(|record| (record.position, record.payload)))
            .collect::<Result<_>>()
            .unwrap()
    }

    #[test]
    fn fresh_open_creates_segment_zero_and_manifest() {
        let dir = tempdir().unwrap();
        let log = Log::open(dir.path()).unwrap();
        assert_eq!(log.head(), 0);
        assert!(dir.path().join("manifest.json").exists());
        assert!(dir
            .path()
            .join("log")
            .join("00000000000000000000.seg")
            .exists());
    }

    #[test]
    fn append_commit_reopen_round_trip() {
        let dir = tempdir().unwrap();
        {
            let mut log = Log::open(dir.path()).unwrap();
            log.append(b"a").unwrap();
            log.append(b"bb").unwrap();
            log.append(b"ccc").unwrap();
            log.commit().unwrap();
        }

        let log = Log::open(dir.path()).unwrap();
        assert_eq!(log.head(), 3);
        let records = payloads_from(&log, 0);
        assert_eq!(
            records,
            vec![
                (0, b"a".to_vec()),
                (1, b"bb".to_vec()),
                (2, b"ccc".to_vec())
            ]
        );
    }

    #[test]
    fn records_from_skips_earlier_offsets() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path()).unwrap();
        for i in 0..5u8 {
            log.append(&[i]).unwrap();
        }
        let records = payloads_from(&log, 3);
        assert_eq!(records, vec![(3, vec![3]), (4, vec![4])]);
    }

    #[test]
    fn segment_rolls_when_max_bytes_exceeded() {
        let dir = tempdir().unwrap();
        let mut log = Log::open_with_segment_max_bytes(dir.path(), 64).unwrap();
        for i in 0..20u32 {
            log.append(format!("record-{i:03}").as_bytes()).unwrap();
        }
        log.commit().unwrap();

        assert!(!log.closed.is_empty(), "expected at least one segment roll");

        let records = payloads_from(&log, 0);
        assert_eq!(records.len(), 20);
        for (i, (offset, payload)) in records.iter().enumerate() {
            assert_eq!(*offset, i as u64);
            assert_eq!(payload, &format!("record-{i:03}").into_bytes());
        }
    }

    #[test]
    fn reopen_after_roll_recovers_closed_and_active_segments() {
        let dir = tempdir().unwrap();
        {
            let mut log = Log::open_with_segment_max_bytes(dir.path(), 64).unwrap();
            for i in 0..20u32 {
                log.append(format!("record-{i:03}").as_bytes()).unwrap();
            }
            log.commit().unwrap();
        }

        let log = Log::open(dir.path()).unwrap();
        assert!(!log.closed.is_empty());
        assert_eq!(log.head(), 20);

        let records = payloads_from(&log, 0);
        assert_eq!(records.len(), 20);
    }

    #[test]
    fn reopen_after_roll_without_commit_keeps_pre_roll_records() {
        // review C-1: rolling must fsync the retiring segment, so records
        // written before a roll survive even if the caller never calls
        // commit() afterward. NOTE: this pins the code path but does not
        // *prove* the fsync happened — the OS page cache survives an
        // ordinary process exit, so a reopen in the same OS session reads
        // the bytes regardless. An honest durability proof needs a
        // fault-injection filesystem that drops un-fsynced writes, which
        // Phase 1 doesn't have. The assertion still guards against the
        // records being lost in-process (e.g. a future refactor dropping
        // the old segment before its data is flushed to the page cache).
        let dir = tempdir().unwrap();
        {
            let mut log = Log::open_with_segment_max_bytes(dir.path(), 64).unwrap();
            for i in 0..20u32 {
                log.append(format!("record-{i:03}").as_bytes()).unwrap();
            }
            // Deliberately NO commit() here.
            assert!(!log.closed.is_empty(), "expected at least one segment roll");
        }

        let log = Log::open(dir.path()).unwrap();
        let records = payloads_from(&log, 0);
        assert_eq!(records.len(), 20);
        for (i, (offset, payload)) in records.iter().enumerate() {
            assert_eq!(*offset, i as u64);
            assert_eq!(payload, &format!("record-{i:03}").into_bytes());
        }
    }

    #[test]
    fn crashed_bootstrap_adopts_orphaned_segment_zero_on_reopen() {
        let dir = tempdir().unwrap();
        let segs_dir = dir.path().join("log");
        std::fs::create_dir_all(&segs_dir).unwrap();
        Segment::create(&segs_dir, 0).unwrap();
        assert!(!dir.path().join("manifest.json").exists());

        let log = Log::open(dir.path()).unwrap();
        assert_eq!(log.head(), 0);
        assert!(dir.path().join("manifest.json").exists());
    }

    #[test]
    fn crashed_roll_deletes_orphaned_empty_segment_on_reopen() {
        let dir = tempdir().unwrap();
        {
            let mut log = Log::open(dir.path()).unwrap();
            log.append(b"first").unwrap();
            log.commit().unwrap();
        }

        // Simulate a crash between "create the new segment file" and
        // "write the updated manifest" during a roll: the new segment
        // exists on disk, but manifest.json still points at segment 0.
        let segs_dir = dir.path().join("log");
        Segment::create(&segs_dir, 1).unwrap();
        let orphan_path = segs_dir.join("00000000000000000001.seg");
        assert!(orphan_path.exists());

        let log = Log::open(dir.path()).unwrap();
        assert_eq!(log.head(), 1); // still segment 0's recovered state
        assert!(
            !orphan_path.exists(),
            "orphaned empty segment should be deleted"
        );

        let records = payloads_from(&log, 0);
        assert_eq!(records, vec![(0, b"first".to_vec())]);
    }

    #[test]
    fn second_open_of_locked_dir_fails() {
        let dir = tempdir().unwrap();
        let _held = Log::open(dir.path()).unwrap();
        // A second open while the first is still live must be refused
        // (review M-2 — single-writer enforcement).
        assert!(matches!(
            Log::open(dir.path()),
            Err(SalamanderError::Locked(_))
        ));
    }

    #[test]
    fn open_succeeds_after_previous_log_is_dropped() {
        let dir = tempdir().unwrap();
        {
            let log = Log::open(dir.path()).unwrap();
            assert!(dir.path().join("LOCK").exists());
            drop(log);
        }
        // Dropping the first Log releases the lock, so a fresh open works
        // and the LOCK file is gone.
        assert!(!dir.path().join("LOCK").exists());
        let _reopened = Log::open(dir.path()).unwrap();
    }

    #[test]
    fn fresh_manifest_declares_current_versions() {
        let dir = tempdir().unwrap();
        let log = Log::open(dir.path()).unwrap();
        assert_eq!(
            log.manifest.format_version,
            manifest::MANIFEST_FORMAT_VERSION
        );
        assert_eq!(
            log.manifest.payload_format_version,
            manifest::PAYLOAD_FORMAT_VERSION
        );
    }

    /// Overwrite `manifest.json` after mutating it as loose JSON — how the
    /// two format-version tests fake a manifest written by a different
    /// build without needing that build.
    fn edit_manifest_json(
        dir: &Path,
        edit: impl FnOnce(&mut serde_json::Map<String, serde_json::Value>),
    ) {
        let path = dir.join("manifest.json");
        let raw = std::fs::read(&path).unwrap();
        let mut val: serde_json::Value = serde_json::from_slice(&raw).unwrap();
        edit(val.as_object_mut().unwrap());
        std::fs::write(&path, serde_json::to_vec_pretty(&val).unwrap()).unwrap();
    }

    #[test]
    fn pre_wp2_manifest_without_payload_version_is_adopted_as_v1() {
        let dir = tempdir().unwrap();
        {
            let mut log = Log::open(dir.path()).unwrap();
            log.append(b"a").unwrap();
            log.commit().unwrap();
        }

        // Simulate a manifest written before WP-2: format v1, and with no
        // `payload_format_version` field at all.
        edit_manifest_json(dir.path(), |m| {
            m.insert("format_version".into(), serde_json::json!(1));
            m.remove("payload_format_version");
        });

        // The missing field defaults to payload v1, so the reopen succeeds
        // and reads the record back — no migration needed.
        let log = Log::open(dir.path()).unwrap();
        assert_eq!(log.head(), 1);
        assert_eq!(log.manifest.payload_format_version, 1);
        let records = payloads_from(&log, 0);
        assert_eq!(records, vec![(0, b"a".to_vec())]);
    }

    #[test]
    fn manifest_with_unknown_payload_version_is_rejected() {
        let dir = tempdir().unwrap();
        {
            let mut log = Log::open(dir.path()).unwrap();
            log.append(b"a").unwrap();
            log.commit().unwrap();
        }

        // A payload version from the future: this build must refuse rather
        // than feed unknown bytes to bincode (OQ-2b).
        edit_manifest_json(dir.path(), |m| {
            m.insert("payload_format_version".into(), serde_json::json!(99));
        });

        assert!(matches!(
            Log::open(dir.path()),
            Err(SalamanderError::UnsupportedFormat {
                found: 99,
                supported: 1
            })
        ));
    }

    /// WP-04 reader semantics that need crate-internal control over
    /// segment sizes, envelopes, and file surgery. Public-API behavior is
    /// covered by `tests/streaming_reader.rs`.
    mod reader_behavior {
        use super::*;
        use crate::format::{FormatLimits, FrameKind, StreamId};
        use crate::log::reader::{
            build_sidecar, FrameFilter, RecordReader, ResolvedFilter, StreamSelector,
            VerificationMode,
        };

        fn env_for(stream_byte: u8, ts: i64) -> RecordEnvelopeV2 {
            let mut envelope = record::raw_test_envelope();
            envelope.stream_id = StreamId::from_bytes([stream_byte; 16]);
            envelope.timestamp_unix_nanos = ts;
            envelope
        }

        /// Encoded size of one test record, measured rather than guessed so
        /// segment-count expectations don't drift with envelope changes.
        fn record_size() -> u64 {
            let mut buf = Vec::new();
            record::encode_kind(FrameKind::Event, 0, &env_for(0, 0), &[0], &mut buf).unwrap();
            buf.len() as u64
        }

        /// A log whose segments hold exactly `per_segment` one-byte-payload
        /// records each, with `streams[i]` naming record i's stream and a
        /// timestamp of `10 * position`.
        fn build_log(dir: &Path, per_segment: usize, streams: &[u8]) -> Log {
            let seg_max = per_segment as u64 * record_size();
            let mut log = Log::open_with_segment_max_bytes(dir, seg_max).unwrap();
            for (i, stream) in streams.iter().enumerate() {
                log.append_enveloped(&env_for(*stream, 10 * i as i64), &[*stream])
                    .unwrap();
            }
            log.commit().unwrap();
            log
        }

        fn positions(log: &Log, filter: ResolvedFilter) -> Result<Vec<u64>> {
            let mut reader = log.plan_reader(filter);
            let mut out = Vec::new();
            while let Some(record) = reader.next()? {
                out.push(record.position);
            }
            Ok(out)
        }

        fn user_filter(from: u64, until: u64) -> ResolvedFilter {
            ResolvedFilter::raw(from, until, FrameFilter::UserEvents)
        }

        #[test]
        fn seek_at_every_boundary_matches_the_position_window() {
            let dir = tempdir().unwrap();
            let streams: Vec<u8> = (0..20).map(|i| i % 3).collect();
            let log = build_log(dir.path(), 5, &streams);
            assert!(log.closed.len() >= 2, "need multiple segments");

            for from in 0..=log.head() + 3 {
                for until in 0..=log.head() + 3 {
                    let got = positions(&log, user_filter(from, until.min(log.head()))).unwrap();
                    let want: Vec<u64> = (from..until.min(log.head())).collect();
                    assert_eq!(got, want, "window {from}..{until}");
                }
            }
        }

        #[test]
        fn stream_and_time_and_max_filters_select_exactly() {
            let dir = tempdir().unwrap();
            let streams: Vec<u8> = (0..18).map(|i| i % 3).collect();
            let log = build_log(dir.path(), 6, &streams);

            let mut filter = user_filter(0, log.head());
            filter.selector = StreamSelector::Streams(vec![StreamId::from_bytes([1; 16])]);
            let want: Vec<u64> = (0..18).filter(|p| p % 3 == 1).collect();
            assert_eq!(positions(&log, filter).unwrap(), want);

            let mut filter = user_filter(0, log.head());
            filter.time = Some(30..90); // ts = 10 * position
            assert_eq!(positions(&log, filter).unwrap(), vec![3, 4, 5, 6, 7, 8]);

            let mut filter = user_filter(4, log.head());
            filter.max_events = Some(3);
            let mut reader = log.plan_reader(filter);
            let mut got = Vec::new();
            while let Some(record) = reader.next().unwrap() {
                got.push(record.position);
            }
            assert_eq!(got, vec![4, 5, 6]);
            assert_eq!(reader.continuation(), 7);
        }

        #[test]
        fn continuation_pages_have_no_gaps_or_duplicates() {
            let dir = tempdir().unwrap();
            let streams: Vec<u8> = (0..23).map(|_| 0).collect();
            let log = build_log(dir.path(), 4, &streams);

            let mut collected = Vec::new();
            let mut from = 0;
            loop {
                let mut filter = user_filter(from, log.head());
                filter.max_events = Some(4);
                let mut reader = log.plan_reader(filter);
                let mut page = Vec::new();
                while let Some(record) = reader.next().unwrap() {
                    page.push(record.position);
                }
                if page.is_empty() {
                    break;
                }
                collected.extend(page);
                from = reader.continuation();
            }
            assert_eq!(collected, (0..23).collect::<Vec<u64>>());
        }

        #[test]
        fn selective_read_skips_a_corrupt_unselected_segment_without_payload_io() {
            let dir = tempdir().unwrap();
            // ~90-byte records, 100-byte max: every segment holds one
            // record. Segment 0 holds stream 7; later ones hold stream 1.
            let log = build_log(dir.path(), 1, &[7, 1, 1]);
            assert_eq!(log.closed.len(), 2);

            // Roll built the sidecars; now vandalize segment 0's payload.
            let seg0 = &log.closed[0].path;
            let mut bytes = std::fs::read(seg0).unwrap();
            let last = bytes.len() - 1;
            bytes[last] ^= 0xFF;
            std::fs::write(seg0, &bytes).unwrap();

            // A stream-1 read proves segment 0 irrelevant from its sidecar
            // postings and never touches the damaged bytes...
            let mut filter = user_filter(0, log.head());
            filter.selector = StreamSelector::Streams(vec![StreamId::from_bytes([1; 16])]);
            assert_eq!(positions(&log, filter).unwrap(), vec![1, 2]);

            // ...while a full read must traverse it and report the damage.
            let error = positions(&log, user_filter(0, log.head())).unwrap_err();
            assert!(matches!(error, SalamanderError::Corrupt { offset: 0, .. }));
        }

        #[test]
        fn missing_or_corrupt_sidecars_change_io_only_never_answers() {
            let dir = tempdir().unwrap();
            let streams: Vec<u8> = (0..12).map(|i| i % 2).collect();
            let log = build_log(dir.path(), 3, &streams);
            let mut filter = user_filter(2, log.head());
            filter.selector = StreamSelector::Streams(vec![StreamId::from_bytes([0; 16])]);
            let want = positions(&log, filter.clone()).unwrap();

            // Corrupt one sidecar, delete another; a fresh Log (empty
            // cache) must fall back to scanning and answer identically.
            drop(log);
            let segs: Vec<_> = std::fs::read_dir(dir.path().join("log"))
                .unwrap()
                .map(|e| e.unwrap().path())
                .filter(|p| p.extension().is_some_and(|e| e == "sidx"))
                .collect();
            assert!(segs.len() >= 2, "expected sidecars from rolls");
            let mut bytes = std::fs::read(&segs[0]).unwrap();
            bytes[10] ^= 0xFF;
            std::fs::write(&segs[0], &bytes).unwrap();
            std::fs::remove_file(&segs[1]).unwrap();

            let log = Log::open(dir.path()).unwrap();
            assert_eq!(positions(&log, filter).unwrap(), want);
        }

        #[test]
        fn reader_buffer_stays_bounded_across_many_records() {
            let dir = tempdir().unwrap();
            let streams: Vec<u8> = (0..600u32).map(|i| (i % 4) as u8).collect();
            let log = build_log(dir.path(), 100, &streams);

            let mut reader = log.plan_reader(user_filter(0, log.head()));
            let mut count = 0u64;
            while reader.next().unwrap().is_some() {
                count += 1;
            }
            assert_eq!(count, 600);
            // One read chunk of readahead plus compaction slack — never
            // O(result count) or O(segment size).
            assert!(
                reader.max_buffer_bytes() <= 3 * 128 * 1024,
                "buffer grew to {} bytes",
                reader.max_buffer_bytes()
            );
        }

        #[test]
        fn batch_digest_mode_detects_a_swapped_event_frame() {
            let dir = tempdir().unwrap();
            let mut log = Log::open(dir.path()).unwrap();
            let batch: Vec<(RecordEnvelopeV2, Vec<u8>)> = (0..2)
                .map(|i| {
                    let mut envelope = env_for(1, 0);
                    envelope.batch_id = crate::format::BatchId::from_bytes([9; 16]);
                    envelope.batch_index = i;
                    (envelope, vec![i as u8; 8])
                })
                .collect();
            log.append_batch(&batch).unwrap();
            log.commit().unwrap();

            // Frame surgery: re-encode event #1 with a different payload.
            // Every frame CRC and position stays valid — only the batch
            // digest can catch it.
            let seg_path = segment::segment_path(&segments_dir(dir.path()), 0);
            let bytes = std::fs::read(&seg_path).unwrap();
            let limits = FormatLimits::default();
            let mut out = Vec::new();
            let mut at = 0;
            while let Some((frame, consumed)) = crate::format::decode(&bytes[at..], limits).unwrap()
            {
                if frame.kind == FrameKind::Event && frame.envelope.batch_index == 1 {
                    record::encode_kind(
                        FrameKind::Event,
                        frame.position,
                        &frame.envelope,
                        b"tampered",
                        &mut out,
                    )
                    .unwrap();
                } else {
                    out.extend_from_slice(&bytes[at..at + consumed]);
                }
                at += consumed;
            }
            std::fs::write(&seg_path, &out).unwrap();

            // Frame-CRC mode streams it happily (every frame is valid)...
            let got = positions(&log, user_filter(0, log.head())).unwrap();
            assert_eq!(got, vec![0, 1]);

            // ...digest mode refuses at the commit frame.
            let mut filter = user_filter(0, log.head());
            filter.verification = VerificationMode::BatchDigests;
            let error = positions(&log, filter).unwrap_err();
            assert!(matches!(error, SalamanderError::Corrupt { .. }));
        }

        #[test]
        fn system_reader_skips_segments_without_system_frames() {
            let dir = tempdir().unwrap();
            let mut log = Log::open_with_segment_max_bytes(dir.path(), 200).unwrap();
            let mut system_env = env_for(0, 0);
            system_env.event_type =
                crate::format::EventType::new("salamander.branch.created").unwrap();
            log.append_system(&system_env, b"{}").unwrap();
            for i in 0..6u8 {
                log.append_enveloped(&env_for(i % 2, 0), &[i]).unwrap();
            }
            log.append_system(&system_env, b"{}").unwrap();
            log.commit().unwrap();

            let system: Vec<_> = log.system_records().collect::<Result<_>>().unwrap();
            assert_eq!(system.len(), 2);
            assert!(system.iter().all(|record| record.kind == FrameKind::System));

            // Sidecars know which closed segments hold system frames.
            let with_system: usize = log
                .closed
                .iter()
                .enumerate()
                .filter_map(|(i, meta)| {
                    let end = log
                        .closed
                        .get(i + 1)
                        .map_or(log.active.base_offset(), |m| m.base_offset);
                    log.sidecar_for(&meta.path, meta.base_offset, end)
                })
                .filter(|sidecar| sidecar.system_frames > 0)
                .count();
            assert!(with_system >= 1);
        }

        #[test]
        fn built_sidecar_matches_segment_contents() {
            let dir = tempdir().unwrap();
            let streams: Vec<u8> = (0..10).map(|i| i % 2).collect();
            let log = build_log(dir.path(), 5, &streams);
            let meta = &log.closed[0];
            let expected_end = log
                .closed
                .get(1)
                .map_or(log.active.base_offset(), |m| m.base_offset);
            let sidecar = build_sidecar(&meta.path, meta.base_offset).unwrap();
            assert_eq!(sidecar.base, 0);
            assert_eq!(sidecar.end, expected_end);
            // Streams alternate 0,1 from position 0, so both appear as
            // soon as the segment holds two records.
            assert_eq!(
                sidecar.postings.len(),
                if expected_end >= 2 { 2 } else { 1 }
            );
            let total: u64 = sidecar.postings.iter().map(|p| p.count).sum();
            assert_eq!(total, expected_end);
            assert_eq!(sidecar.seek_points.first(), Some(&(0, 0)));
        }

        mod proptests {
            use super::*;
            use proptest::prelude::*;

            proptest! {
                /// The streaming reader must agree with an independent
                /// model (built during append) for arbitrary segment
                /// layouts, windows, and selectors.
                #[test]
                fn reader_matches_independent_model(
                    streams in prop::collection::vec(0u8..4, 1..50),
                    seg_max in 64u64..2048,
                    from in 0u64..60,
                    until in 0u64..60,
                    select in 0u8..4,
                ) {
                    let dir = tempdir().unwrap();
                    let mut log = Log::open_with_segment_max_bytes(dir.path(), seg_max).unwrap();
                    let mut model: Vec<(u64, u8)> = Vec::new();
                    for (i, stream) in streams.iter().enumerate() {
                        let position = log
                            .append_enveloped(&env_for(*stream, i as i64), &[*stream])
                            .unwrap();
                        model.push((position, *stream));
                    }
                    log.commit().unwrap();

                    let until = until.min(log.head());
                    let selector = match select {
                        0 => StreamSelector::All,
                        1 => StreamSelector::Streams(vec![StreamId::from_bytes([1; 16])]),
                        2 => StreamSelector::PartitionClass { count: 3, index: 0 },
                        _ => StreamSelector::PartitionClass { count: 3, index: 2 },
                    };
                    let mut filter = user_filter(from, until);
                    filter.selector = selector.clone();
                    let got = positions(&log, filter).unwrap();
                    let want: Vec<u64> = model
                        .iter()
                        .filter(|(p, s)| {
                            *p >= from
                                && *p < until
                                && selector.matches(StreamId::from_bytes([*s; 16]))
                        })
                        .map(|(p, _)| *p)
                        .collect();
                    prop_assert_eq!(got, want);
                }
            }
        }
    }

    #[test]
    fn stray_non_segment_file_is_ignored_not_fatal() {
        let dir = tempdir().unwrap();
        {
            let mut log = Log::open(dir.path()).unwrap();
            log.append(b"real").unwrap();
            log.commit().unwrap();
        }

        // A stray `.seg` file with a non-numeric stem (e.g. a leftover
        // backup) must not make the whole DB unopenable (review M-5).
        let segs_dir = dir.path().join("log");
        std::fs::write(segs_dir.join("backup.seg"), b"not a real segment").unwrap();

        let log = Log::open(dir.path()).unwrap();
        assert_eq!(log.head(), 1);
        let records = payloads_from(&log, 0);
        assert_eq!(records, vec![(0, b"real".to_vec())]);
    }

    #[test]
    fn retention_planning_rounds_down_and_reports_only_closed_segments() {
        let dir = tempdir().unwrap();
        let mut log = Log::open_with_segment_max_bytes(dir.path(), 64).unwrap();
        log.append(&[1; 128]).unwrap();
        log.append(&[2; 128]).unwrap();
        log.append(&[3; 128]).unwrap();
        log.commit().unwrap();

        let (effective, segments) = log.retention_boundary(2);
        assert_eq!(effective, 2);
        assert_eq!(
            segments
                .iter()
                .map(|(base, _bytes)| *base)
                .collect::<Vec<_>>(),
            [0, 1]
        );
        assert!(segments.iter().all(|(_, bytes)| *bytes > 0));

        // A position inside the current segment never makes that active
        // segment reclaimable.
        let (effective, segments) = log.retention_boundary(log.head());
        assert_eq!(effective, 2);
        assert_eq!(segments.len(), 2);
    }
}
