//! WP-04 — the bounded-memory streaming reader.
//!
//! Replaces the materializing scans: a [`LogReader`] walks closed segments
//! and a snapshot handle of the active segment through a fixed-size refill
//! buffer, decoding one frame at a time. Peak memory is bounded by the
//! buffer (one read chunk plus the largest single frame), never by result
//! count or segment size.
//!
//! Selection happens at three altitudes, cheapest first:
//! 1. **segment pruning** — binary search over `[base, end)` ranges picks
//!    only segments intersecting the plan's position window; sidecar
//!    postings/timestamps prove whole segments irrelevant to a stream or
//!    time selector without opening them (the WP-09 skip guarantee);
//! 2. **in-segment seek** — sparse sidecar seek points land the cursor
//!    near `from` instead of at byte 0;
//! 3. **per-frame filters** — position window, frame kind, stream
//!    selector, branch scopes, and time bounds are applied after envelope
//!    decode; unselected payloads are never copied out of the buffer, and
//!    payload bytes are never interpreted (INV-9).
//!
//! Every traversed frame is CRC-verified (that is `format::decode`'s
//! contract); [`VerificationMode::BatchDigests`] additionally re-verifies
//! batch begin/commit digests while streaming. Digest verification detects
//! damage and fails the read at the commit frame — it does not buffer
//! whole batches to suppress already-yielded events, which would break the
//! memory bound (recorded as a WP-04 design decision).

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::ops::{Bound, Range};

use crate::format::{
    self, BranchId, FormatLimits, FrameKind, OwnedStoredRecord, StoredRecord, StreamId,
};
use crate::{Result, SalamanderError};

use super::index::{PostingEntry, Sidecar, SEEK_POINT_SPACING};
use super::segment::parse_batch_control;

/// Refill granularity for the segment cursor. The buffer holds at most one
/// chunk of read-ahead plus the largest single frame it has ever needed.
const READ_CHUNK: usize = 128 * 1024;

/// Where a replay stops (exclusive), resolved against the log head when
/// the reader is constructed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReplayEnd {
    /// The head observed at reader construction; later appends are not
    /// yielded by this reader.
    #[default]
    Head,
    /// A fixed exclusive position. Beyond-head values are rejected at
    /// construction with `OffsetBeyondHead`.
    At(u64),
}

/// Which streams a plan selects. Evaluated per record from the envelope's
/// `StreamId` — never from the stream catalog — so streams created after
/// any derived state was built still route correctly.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum StreamSelector {
    #[default]
    All,
    /// An explicit set of stream IDs (deduplicated and sorted internally).
    Streams(Vec<StreamId>),
    /// Every stream whose ID hashes into partition `index` of `count`
    /// equal hash classes — WP-09's healing unit. The hash is the first
    /// eight little-endian bytes of the `StreamId` modulo `count`, which
    /// is uniform because stream IDs are themselves hash-derived.
    PartitionClass { count: u32, index: u32 },
}

impl StreamSelector {
    pub(crate) fn validate(&self) -> Result<()> {
        if let StreamSelector::PartitionClass { count, index } = self {
            if *count == 0 || index >= count {
                return Err(SalamanderError::InvalidArgument(format!(
                    "partition class {index} of {count} is not a valid partition"
                )));
            }
        }
        Ok(())
    }

    pub(crate) fn matches(&self, stream: StreamId) -> bool {
        match self {
            StreamSelector::All => true,
            StreamSelector::Streams(ids) => ids.binary_search(&stream).is_ok(),
            StreamSelector::PartitionClass { count, index } => {
                partition_of(stream, *count) == *index
            }
        }
    }

    /// True when the postings prove **no** stream in a segment matches —
    /// the segment-skip proof required by WP-04 for WP-09. `All` never
    /// skips; an explicit set probes each ID by binary search; a partition
    /// class tests every posted stream (postings are per distinct stream,
    /// so this is small).
    pub(crate) fn disjoint_from(&self, postings: &[PostingEntry]) -> bool {
        match self {
            StreamSelector::All => false,
            StreamSelector::Streams(ids) => !ids
                .iter()
                .any(|id| postings.binary_search_by_key(id, |p| p.stream).is_ok()),
            StreamSelector::PartitionClass { count, index } => !postings
                .iter()
                .any(|p| partition_of(p.stream, *count) == *index),
        }
    }
}

/// The stable partition-routing hash: first eight little-endian bytes of
/// the stream ID modulo the partition count. WP-09 versions this scheme;
/// changing it invalidates partitioned derived state, so it must never be
/// silently altered.
pub fn partition_of(stream: StreamId, count: u32) -> u32 {
    let head = u64::from_le_bytes(stream.as_bytes()[..8].try_into().unwrap());
    (head % u64::from(count)) as u32
}

/// How much re-verification the reader performs beyond per-frame CRCs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VerificationMode {
    /// Every traversed frame's CRC is checked (always on). Batch framing
    /// is tracked for position continuity only.
    #[default]
    FrameCrc,
    /// Additionally re-verify batch begin/commit control digests over the
    /// raw event-frame bytes, streaming (no batch buffering). A mismatch
    /// fails the read with `Corrupt` at the commit frame.
    BatchDigests,
}

/// A declarative replay request (spec/04). Resolved against the log and
/// branch catalog by `Salamander::read`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayPlan {
    pub branch: BranchId,
    pub streams: StreamSelector,
    pub from: Bound<u64>,
    pub until: ReplayEnd,
    /// Half-open envelope-timestamp filter (unix nanos). Timestamps are
    /// not monotonic in the log, so this is an exact per-record *filter*;
    /// sidecar min/max ranges are used only as a segment-skip hint.
    pub time: Option<Range<i64>>,
    pub max_events: Option<u64>,
    pub verification: VerificationMode,
}

impl Default for ReplayPlan {
    fn default() -> Self {
        ReplayPlan {
            branch: BranchId::ZERO,
            streams: StreamSelector::All,
            from: Bound::Unbounded,
            until: ReplayEnd::Head,
            time: None,
            max_events: None,
            verification: VerificationMode::FrameCrc,
        }
    }
}

/// The object-safe pull interface over any record reader. `next` lends a
/// record borrowing the reader's internal buffer; callers that need to
/// hold records across calls use `next_owned`.
pub trait RecordReader {
    fn next(&mut self) -> Result<Option<StoredRecord<'_>>>;

    /// The resumable continuation: the first position this reader has not
    /// yet yielded or skipped past. Feeding it back as `from` in a new
    /// plan resumes without gaps or duplicates.
    fn continuation(&self) -> u64;

    fn next_owned(&mut self) -> Result<Option<OwnedStoredRecord>> {
        Ok(self.next()?.map(OwnedStoredRecord::from))
    }
}

/// Which frame kinds a reader yields. User replay yields `Event` frames;
/// catalog rebuilds read `System` frames.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FrameFilter {
    UserEvents,
    SystemOnly,
}

/// A plan resolved against a concrete log: position window fixed, branch
/// scopes flattened. Everything the log layer filters on is engine
/// envelope data — payload bytes stay opaque (INV-9).
#[derive(Debug, Clone)]
pub(crate) struct ResolvedFilter {
    pub from: u64,
    pub until: u64,
    pub selector: StreamSelector,
    /// Flattened branch visibility: `(branch, exclusive upper bound)`
    /// pairs from `BranchCatalog::replay_scopes`. `None` disables branch
    /// filtering (raw log-level reads).
    pub scopes: Option<Vec<(BranchId, u64)>>,
    pub time: Option<Range<i64>>,
    pub kinds: FrameFilter,
    pub max_events: Option<u64>,
    pub verification: VerificationMode,
}

impl ResolvedFilter {
    pub(crate) fn raw(from: u64, until: u64, kinds: FrameFilter) -> Self {
        ResolvedFilter {
            from,
            until,
            selector: StreamSelector::All,
            scopes: None,
            time: None,
            kinds,
            max_events: None,
            verification: VerificationMode::FrameCrc,
        }
    }
}

/// One segment the planner selected, in log order.
#[derive(Debug)]
pub(crate) struct PlannedSegment {
    pub base: u64,
    /// Exclusive position bound; the next segment's base. Derived from the
    /// manifest layout, not from any sidecar.
    pub end: u64,
    pub source: SegmentSource,
}

#[derive(Debug)]
pub(crate) enum SegmentSource {
    Closed(std::path::PathBuf),
    /// The active segment, read through a cloned handle taken when the
    /// reader enters it.
    Active,
}

/// Prune `[base, end)` segment ranges to those intersecting
/// `[from, until)`. Pure index arithmetic over metadata — O(log n) binary
/// search plus the intersecting range — so planning cost is independent of
/// record counts (the 100M-synthetic-records planning test drives this
/// directly).
pub(crate) fn intersecting_range(
    bases: &[u64],
    overall_end: u64,
    from: u64,
    until: u64,
) -> Range<usize> {
    if bases.is_empty() || from >= overall_end.min(until) {
        return 0..0;
    }
    // Segments are contiguous: segment i spans [bases[i], bases[i+1]) with
    // the last ending at overall_end. A position `from` below overall_end
    // therefore lives in the segment with the greatest base <= from.
    let start = bases.partition_point(|&b| b <= from).saturating_sub(1);
    // First segment starting at or past `until` is the exclusive stop.
    let stop = bases.partition_point(|&b| b < until);
    start..stop.max(start)
}

struct SegmentCursor {
    file: File,
    buf: Vec<u8>,
    /// First unconsumed byte in `buf`.
    start: usize,
    /// Bytes of `buf` holding file content.
    valid: usize,
    eof: bool,
}

impl SegmentCursor {
    fn new(mut file: File, seek_byte: u64) -> Result<Self> {
        file.seek(SeekFrom::Start(seek_byte))?;
        Ok(SegmentCursor {
            file,
            buf: Vec::new(),
            start: 0,
            valid: 0,
            eof: false,
        })
    }

    fn available(&self) -> &[u8] {
        &self.buf[self.start..self.valid]
    }

    /// Ensure at least `need` unconsumed bytes are buffered or EOF has
    /// been reached. Compacts before growing so the buffer stays bounded
    /// by one chunk of read-ahead plus the largest single frame.
    fn fill(&mut self, need: usize) -> Result<()> {
        while self.valid - self.start < need && !self.eof {
            if self.start > 0 {
                self.buf.copy_within(self.start..self.valid, 0);
                self.valid -= self.start;
                self.start = 0;
            }
            let target = (self.valid + READ_CHUNK).max(need);
            if self.buf.len() < target {
                self.buf.resize(target, 0);
            }
            let read = self.file.read(&mut self.buf[self.valid..])?;
            if read == 0 {
                self.eof = true;
            }
            self.valid += read;
        }
        Ok(())
    }

    fn consume(&mut self, n: usize) {
        debug_assert!(self.start + n <= self.valid);
        self.start += n;
    }
}

/// Streaming batch-integrity state (see `VerificationMode`).
struct BatchTrack {
    first: u64,
    batch_id: crate::format::BatchId,
    seen: u32,
    /// Populated only in `BatchDigests` mode.
    check: Option<(u32, u32, u32)>, // (count, expected digest, running crc)
}

/// The concrete bounded-memory reader over one log.
pub struct LogReader<'log> {
    log: &'log super::Log,
    limits: FormatLimits,
    filter: ResolvedFilter,
    segments: Vec<PlannedSegment>,
    seg_idx: usize,
    cursor: Option<SegmentCursor>,
    /// Continuity: the position the next non-batch frame must carry.
    expected: u64,
    batch: Option<BatchTrack>,
    yielded: u64,
    continuation: u64,
    finished: bool,
    max_buf: usize,
}

/// A matched frame located in the cursor buffer: everything `next` needs
/// to assemble a `StoredRecord` without re-borrowing during the search.
struct Located {
    kind: FrameKind,
    flags: u8,
    position: u64,
    envelope: crate::format::RecordEnvelopeV2,
    payload_start: usize,
    payload_len: usize,
}

impl<'log> LogReader<'log> {
    pub(crate) fn new(
        log: &'log super::Log,
        filter: ResolvedFilter,
        segments: Vec<PlannedSegment>,
    ) -> Self {
        let continuation = filter.from;
        LogReader {
            log,
            limits: FormatLimits::default(),
            filter,
            segments,
            seg_idx: 0,
            cursor: None,
            expected: 0,
            batch: None,
            yielded: 0,
            continuation,
            finished: false,
            max_buf: 0,
        }
    }

    /// Largest cursor buffer observed, for the peak-memory tests. This is
    /// the reader's entire record-dependent allocation.
    pub fn max_buffer_bytes(&self) -> usize {
        self.max_buf
    }

    /// Enter the next planned segment, consulting its sidecar to skip it
    /// outright or to seek within it. Returns false when no segments
    /// remain.
    fn enter_next_segment(&mut self) -> Result<bool> {
        while self.seg_idx < self.segments.len() {
            let seg = &self.segments[self.seg_idx];
            let (base, end) = (seg.base, seg.end);
            let mut seek = (base, 0u64);
            match &seg.source {
                SegmentSource::Closed(path) => {
                    let sidecar = self.log.sidecar_for(path, base, end);
                    if let Some(sidecar) = sidecar {
                        if self.skippable(&sidecar) {
                            self.seg_idx += 1;
                            continue;
                        }
                        if self.filter.from > base {
                            if let Some(point) = sidecar.seek_point_before(self.filter.from) {
                                seek = point;
                            }
                        }
                    }
                    let file = File::open(path)?;
                    self.cursor = Some(SegmentCursor::new(file, seek.1)?);
                }
                SegmentSource::Active => {
                    let file = self.log.active_handle()?;
                    self.cursor = Some(SegmentCursor::new(file, 0)?);
                }
            }
            self.expected = seek.0;
            self.batch = None;
            self.seg_idx += 1;
            return Ok(true);
        }
        Ok(false)
    }

    /// Whole-segment skip proof from derived metadata alone: no selected
    /// stream, no system frames for a system read, or a provably disjoint
    /// timestamp range. Never consults payload bytes.
    fn skippable(&self, sidecar: &Sidecar) -> bool {
        match self.filter.kinds {
            FrameFilter::SystemOnly => sidecar.system_frames == 0,
            FrameFilter::UserEvents => {
                self.filter.selector.disjoint_from(&sidecar.postings)
                    || self
                        .filter
                        .time
                        .as_ref()
                        .is_some_and(|range| sidecar.time_disjoint(range))
            }
        }
    }

    /// Walk frames until one passes every filter; maintain continuity and
    /// batch state along the way. `Ok(None)` is exhaustion.
    fn advance_to_match(&mut self) -> Result<Option<Located>> {
        'segments: loop {
            if self.finished {
                return Ok(None);
            }
            if self
                .filter
                .max_events
                .is_some_and(|max| self.yielded >= max)
            {
                self.finished = true;
                return Ok(None);
            }
            if self.cursor.is_none() && !self.enter_next_segment()? {
                self.finished = true;
                self.continuation = self
                    .continuation
                    .max(self.filter.until.min(self.log.head()));
                return Ok(None);
            }
            let closed = !matches!(
                self.segments[self.seg_idx - 1].source,
                SegmentSource::Active
            );
            let seg_end = self.segments[self.seg_idx - 1].end;

            loop {
                let cursor = self.cursor.as_mut().expect("cursor set above");
                cursor.fill(format::FRAME_HEADER_LEN)?;
                self.max_buf = self.max_buf.max(cursor.buf.len());
                if cursor.available().len() < format::FRAME_HEADER_LEN {
                    let leftover = cursor.available().len();
                    if leftover != 0 && closed {
                        return Err(SalamanderError::Corrupt {
                            offset: self.expected,
                            reason: format!("closed segment has {leftover} trailing byte(s)"),
                        });
                    }
                    // Active tail: bytes past the last complete frame are
                    // an in-flight append; recovery would truncate them.
                    if closed && self.expected < seg_end {
                        return Err(SalamanderError::Corrupt {
                            offset: self.expected,
                            reason: format!(
                                "closed segment ended at position {} before its recorded end {seg_end}",
                                self.expected
                            ),
                        });
                    }
                    self.cursor = None;
                    continue 'segments;
                }
                let total = match format::frame_total_len(cursor.available(), self.limits)? {
                    Some(total) => total,
                    None => unreachable!("header length ensured by fill"),
                };
                cursor.fill(total)?;
                self.max_buf = self.max_buf.max(cursor.buf.len());
                if cursor.available().len() < total {
                    if closed {
                        return Err(SalamanderError::Corrupt {
                            offset: self.expected,
                            reason: "closed segment ends mid-frame".into(),
                        });
                    }
                    self.cursor = None;
                    continue 'segments;
                }

                let frame_start = cursor.start;
                let (record, consumed) = format::decode(cursor.available(), self.limits)?
                    .expect("full frame ensured by fill");
                debug_assert_eq!(consumed, total);
                let kind = record.kind;
                let flags = record.flags;
                let position = record.position;
                let payload_len = record.payload.len();
                let envelope = record.envelope;
                let payload_start = frame_start + total - payload_len;

                // --- continuity + batch machine (mirrors recovery's
                // scan_records; any deviation is damage, not a tail) ---
                match kind {
                    FrameKind::BatchBegin => {
                        if self.batch.is_some() || position != self.expected {
                            return Err(corrupt_sequence(position, self.expected));
                        }
                        let check = if self.filter.verification == VerificationMode::BatchDigests {
                            let payload = &cursor.buf[payload_start..payload_start + payload_len];
                            let (count, digest) = parse_batch_control(payload)?;
                            Some((count, digest, 0u32))
                        } else {
                            None
                        };
                        self.batch = Some(BatchTrack {
                            first: position,
                            batch_id: envelope.batch_id,
                            seen: 0,
                            check,
                        });
                        cursor.consume(total);
                        continue;
                    }
                    FrameKind::BatchCommit => {
                        let Some(batch) = self.batch.take() else {
                            return Err(SalamanderError::Corrupt {
                                offset: position,
                                reason: "batch commit without begin".into(),
                            });
                        };
                        if position != batch.first || envelope.batch_id != batch.batch_id {
                            return Err(SalamanderError::Corrupt {
                                offset: position,
                                reason: "batch commit does not match its begin".into(),
                            });
                        }
                        if let Some((count, digest, running)) = batch.check {
                            let payload = &cursor.buf[payload_start..payload_start + payload_len];
                            let (commit_count, commit_digest) = parse_batch_control(payload)?;
                            if commit_count != count
                                || commit_digest != digest
                                || batch.seen != count
                                || running != digest
                            {
                                return Err(SalamanderError::Corrupt {
                                    offset: position,
                                    reason: "batch digest verification failed".into(),
                                });
                            }
                        }
                        if batch.seen == 0 {
                            return Err(SalamanderError::Corrupt {
                                offset: position,
                                reason: "batch committed zero events".into(),
                            });
                        }
                        self.expected = batch.first + u64::from(batch.seen);
                        cursor.consume(total);
                        continue;
                    }
                    FrameKind::System => {
                        if self.batch.is_some() || position != self.expected {
                            return Err(corrupt_sequence(position, self.expected));
                        }
                    }
                    FrameKind::Event => match self.batch.as_mut() {
                        Some(batch) => {
                            let slot = batch.first + u64::from(batch.seen);
                            if position != slot
                                || envelope.batch_id != batch.batch_id
                                || envelope.batch_index != batch.seen
                            {
                                return Err(corrupt_sequence(position, slot));
                            }
                            if let Some((_, _, running)) = batch.check.as_mut() {
                                *running = crc32c::crc32c_append(
                                    *running,
                                    &cursor.buf[frame_start..frame_start + total],
                                );
                            }
                            batch.seen += 1;
                        }
                        None => {
                            if position != self.expected {
                                return Err(corrupt_sequence(position, self.expected));
                            }
                            self.expected += 1;
                        }
                    },
                }

                // --- filters ---
                let is_event = kind == FrameKind::Event;
                if is_event && position >= self.filter.until {
                    self.finished = true;
                    self.continuation = self.continuation.max(self.filter.until);
                    return Ok(None);
                }
                let wanted = match self.filter.kinds {
                    FrameFilter::UserEvents => is_event,
                    FrameFilter::SystemOnly => kind == FrameKind::System,
                };
                let passes = wanted
                    && (!is_event || position >= self.filter.from)
                    && self.filter.selector.matches(envelope.stream_id)
                    && self.filter.scopes.as_ref().map_or(true, |scopes| {
                        scopes.iter().any(|(branch, upper)| {
                            envelope.branch_id == *branch && position < *upper
                        })
                    })
                    && self
                        .filter
                        .time
                        .as_ref()
                        .map_or(true, |range| range.contains(&envelope.timestamp_unix_nanos));

                if is_event {
                    self.continuation = self.continuation.max(position + 1);
                }
                cursor.consume(total);
                if passes {
                    self.yielded += 1;
                    return Ok(Some(Located {
                        kind,
                        flags,
                        position,
                        envelope,
                        payload_start,
                        payload_len,
                    }));
                }
            }
        }
    }
}

impl std::fmt::Debug for LogReader<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogReader")
            .field("from", &self.filter.from)
            .field("until", &self.filter.until)
            .field("selector", &self.filter.selector)
            .field("segments", &self.segments.len())
            .field("continuation", &self.continuation)
            .field("finished", &self.finished)
            .finish_non_exhaustive()
    }
}

fn corrupt_sequence(position: u64, expected: u64) -> SalamanderError {
    SalamanderError::Corrupt {
        offset: position,
        reason: format!("position {position} breaks expected sequence {expected}"),
    }
}

impl RecordReader for LogReader<'_> {
    fn next(&mut self) -> Result<Option<StoredRecord<'_>>> {
        let located = match self.advance_to_match()? {
            Some(located) => located,
            None => return Ok(None),
        };
        let cursor = self.cursor.as_ref().expect("cursor holds matched frame");
        let payload =
            &cursor.buf[located.payload_start..located.payload_start + located.payload_len];
        Ok(Some(StoredRecord {
            kind: located.kind,
            flags: located.flags,
            position: located.position,
            envelope: located.envelope,
            payload,
        }))
    }

    fn continuation(&self) -> u64 {
        self.continuation
    }
}

/// Build a segment's sidecar by walking it once with the same bounded
/// cursor the reader uses (envelope decode only, no payload copies). Any
/// integrity failure aborts the build — a damaged segment gets no sidecar
/// and surfaces its damage on the next scan that traverses it.
pub(crate) fn build_sidecar(path: &std::path::Path, base: u64) -> Result<Sidecar> {
    let file = File::open(path)?;
    let limits = FormatLimits::default();
    let mut cursor = SegmentCursor::new(file, 0)?;
    let mut expected = base;
    let mut in_batch: Option<(u64, u32)> = None; // (first, seen)
    let mut byte_offset = 0u64;
    let mut min_ts = i64::MAX;
    let mut max_ts = i64::MIN;
    let mut system_frames = 0u64;
    let mut seek_points = vec![(base, 0u64)];
    let mut last_seek_byte = 0u64;
    let mut postings: std::collections::BTreeMap<StreamId, PostingEntry> =
        std::collections::BTreeMap::new();

    loop {
        cursor.fill(format::FRAME_HEADER_LEN)?;
        if cursor.available().len() < format::FRAME_HEADER_LEN {
            if !cursor.available().is_empty() {
                return Err(SalamanderError::Corrupt {
                    offset: expected,
                    reason: "segment has trailing bytes".into(),
                });
            }
            break;
        }
        let total =
            format::frame_total_len(cursor.available(), limits)?.expect("header ensured by fill");
        cursor.fill(total)?;
        if cursor.available().len() < total {
            return Err(SalamanderError::Corrupt {
                offset: expected,
                reason: "segment ends mid-frame".into(),
            });
        }
        if in_batch.is_none()
            && byte_offset > 0
            && byte_offset - last_seek_byte >= SEEK_POINT_SPACING
        {
            seek_points.push((expected, byte_offset));
            last_seek_byte = byte_offset;
        }
        let (record, consumed) =
            format::decode(cursor.available(), limits)?.expect("full frame ensured by fill");
        match record.kind {
            FrameKind::BatchBegin => {
                if in_batch.is_some() || record.position != expected {
                    return Err(corrupt_sequence(record.position, expected));
                }
                in_batch = Some((record.position, 0));
            }
            FrameKind::BatchCommit => {
                let Some((first, seen)) = in_batch.take() else {
                    return Err(SalamanderError::Corrupt {
                        offset: record.position,
                        reason: "batch commit without begin".into(),
                    });
                };
                if record.position != first || seen == 0 {
                    return Err(corrupt_sequence(record.position, first));
                }
                expected = first + u64::from(seen);
            }
            FrameKind::System => {
                if in_batch.is_some() || record.position != expected {
                    return Err(corrupt_sequence(record.position, expected));
                }
                system_frames += 1;
            }
            FrameKind::Event => {
                let slot = match in_batch.as_mut() {
                    Some((first, seen)) => {
                        let slot = *first + u64::from(*seen);
                        *seen += 1;
                        slot
                    }
                    None => {
                        let slot = expected;
                        expected += 1;
                        slot
                    }
                };
                if record.position != slot {
                    return Err(corrupt_sequence(record.position, slot));
                }
                min_ts = min_ts.min(record.envelope.timestamp_unix_nanos);
                max_ts = max_ts.max(record.envelope.timestamp_unix_nanos);
                let entry = postings
                    .entry(record.envelope.stream_id)
                    .or_insert(PostingEntry {
                        stream: record.envelope.stream_id,
                        first: record.position,
                        last: record.position,
                        count: 0,
                    });
                entry.last = record.position;
                entry.count += 1;
            }
        }
        cursor.consume(consumed);
        byte_offset += consumed as u64;
    }
    if in_batch.is_some() {
        return Err(SalamanderError::Corrupt {
            offset: expected,
            reason: "segment ends inside an uncommitted batch".into(),
        });
    }
    Ok(Sidecar {
        base,
        end: expected,
        min_ts,
        max_ts,
        system_frames,
        seek_points,
        postings: postings.into_values().collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_of_is_stable_and_in_range() {
        // Pinned values: WP-09 derived state depends on this hash never
        // changing (spec/09 partition scheme versioning).
        let a = StreamId::from_bytes([1, 0, 0, 0, 0, 0, 0, 0, 9, 9, 9, 9, 9, 9, 9, 9]);
        assert_eq!(partition_of(a, 4), 1);
        let b = StreamId::from_bytes([7, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(partition_of(b, 4), (263u64 % 4) as u32);
        for count in [1u32, 2, 3, 16, 1024] {
            assert!(partition_of(a, count) < count);
        }
    }

    #[test]
    fn selector_validation_rejects_bad_partitions() {
        assert!(StreamSelector::PartitionClass { count: 0, index: 0 }
            .validate()
            .is_err());
        assert!(StreamSelector::PartitionClass { count: 4, index: 4 }
            .validate()
            .is_err());
        assert!(StreamSelector::PartitionClass { count: 4, index: 3 }
            .validate()
            .is_ok());
    }

    #[test]
    fn disjoint_from_proves_skips_for_all_selector_shapes() {
        let posted = |bytes: [u8; 16]| PostingEntry {
            stream: StreamId::from_bytes(bytes),
            first: 0,
            last: 0,
            count: 1,
        };
        let mut postings = vec![posted([4; 16]), posted([8; 16])];
        postings.sort_by_key(|p| p.stream);

        assert!(!StreamSelector::All.disjoint_from(&postings));

        let hit = StreamSelector::Streams(vec![StreamId::from_bytes([4; 16])]);
        let miss = StreamSelector::Streams(vec![StreamId::from_bytes([5; 16])]);
        assert!(!hit.disjoint_from(&postings));
        assert!(miss.disjoint_from(&postings));

        // Partition classes: both posted streams land in a computable
        // class; the other classes must be provably disjoint.
        let classes: Vec<u32> = postings.iter().map(|p| partition_of(p.stream, 8)).collect();
        for index in 0..8u32 {
            let selector = StreamSelector::PartitionClass { count: 8, index };
            assert_eq!(
                selector.disjoint_from(&postings),
                !classes.contains(&index),
                "class {index}"
            );
        }
    }

    #[test]
    fn intersecting_range_prunes_synthetic_hundred_million_records() {
        // 10_000 segments of 10_000 records each: 100M records of pure
        // metadata. Planning must be index arithmetic — no per-record or
        // per-segment I/O — and must pick exactly the right window.
        let bases: Vec<u64> = (0..10_000u64).map(|i| i * 10_000).collect();
        let overall_end = 100_000_000u64;

        let range = intersecting_range(&bases, overall_end, 55_554_321, 55_554_322);
        assert_eq!(range, 5_555..5_556);

        let range = intersecting_range(&bases, overall_end, 0, overall_end);
        assert_eq!(range, 0..10_000);

        let range = intersecting_range(&bases, overall_end, 99_999_999, u64::MAX);
        assert_eq!(range, 9_999..10_000);

        assert_eq!(
            intersecting_range(&bases, overall_end, overall_end, u64::MAX),
            0..0
        );
        assert_eq!(intersecting_range(&bases, overall_end, 5, 5), 0..0);
    }
}
