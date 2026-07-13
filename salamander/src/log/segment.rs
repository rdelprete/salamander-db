//! DESIGN.md §3.1, §3.3 — one segment file: append, scan, torn-tail truncate.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use super::record;
use crate::format::{OwnedStoredRecord, RecordEnvelopeV2};
use crate::{Result, SalamanderError};

/// Metadata for a closed segment, tracked by `Log`.
pub struct SegmentMeta {
    // Not read anywhere yet — `Log` currently only needs `path` to reopen
    // a closed segment for iteration. Kept for debugging/introspection and
    // for Phase 2's segment key-range summaries (DESIGN.md §8), which will
    // need to know a segment's offset range without opening it.
    #[allow(dead_code)]
    pub base_offset: u64,
    pub path: PathBuf,
}

pub struct Segment {
    file: File,
    base_offset: u64,
    len_bytes: u64,
    next_offset: u64,
}

impl Segment {
    pub fn create(dir: &Path, base_offset: u64) -> Result<Self> {
        let path = segment_path(dir, base_offset);
        // create_new: refuse to reopen an existing segment for writing —
        // closed segments are immutable (DESIGN.md §3.1).
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;

        Ok(Segment {
            file,
            base_offset,
            len_bytes: 0,
            next_offset: base_offset,
        })
    }

    /// Open the *active* segment for recovery + append. Scans from byte 0,
    /// stops at the first invalid record, and **truncates the file there**
    /// (the torn-tail rule, DESIGN.md §3.2 / §6 case C1). Opens read+write
    /// because recovery may need to truncate; use `scan` for read-only
    /// iteration of a closed segment.
    pub fn open(path: &Path) -> Result<Self> {
        let base_offset = parse_base_offset(path)?;

        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        let scanned = scan_records(&buf, base_offset)?;
        let pos = scanned.valid_bytes;

        if pos < buf.len() {
            // Durable end found short of EOF: truncate, and fsync the
            // truncation itself so a second crash can't resurrect the torn
            // tail (DESIGN.md §6, case C1). Announce it — silently dropping
            // bytes (which may include committed data if the damage is
            // mid-log, not just a torn tail) is hostile to the postmortem
            // story this engine exists for (review M-3).
            eprintln!(
                "salamander: truncating {} at byte {} ({}), dropping {} trailing byte(s)",
                path.display(),
                pos,
                scanned.stop.reason(),
                buf.len() - pos,
            );
            file.set_len(pos as u64)?;
            file.sync_all()?;
        }
        file.seek(SeekFrom::Start(pos as u64))?;

        Ok(Segment {
            file,
            base_offset,
            len_bytes: pos as u64,
            next_offset: scanned.next_offset,
        })
    }

    /// A cloned handle for read-only streaming of the active segment
    /// (WP-04 reader). The clone shares the file description, so bytes
    /// appended through the primary handle are visible to reads through
    /// the clone without any flush.
    pub(crate) fn clone_handle(&self) -> Result<File> {
        Ok(self.file.try_clone()?)
    }

    #[allow(dead_code)]
    pub fn append(&mut self, payload: &[u8]) -> Result<u64> {
        let offset = self.next_offset;
        let mut buf = Vec::new();
        record::encode(offset, payload, &mut buf);
        // No Rust-level buffering here (see guide/02-segment.md): the OS
        // page cache already gives us "written but not yet durable," and
        // it keeps same-process reads (`iter`) consistent with what was
        // just appended without needing an explicit flush.
        self.file.write_all(&buf)?;
        self.len_bytes += buf.len() as u64;
        self.next_offset += 1;
        Ok(offset)
    }

    pub fn append_enveloped(&mut self, envelope: &RecordEnvelopeV2, payload: &[u8]) -> Result<u64> {
        let offset = self.next_offset;
        let mut buf = Vec::new();
        record::encode_enveloped(offset, envelope, payload, &mut buf)?;
        self.file.write_all(&buf)?;
        self.len_bytes += buf.len() as u64;
        self.next_offset += 1;
        Ok(offset)
    }

    pub fn append_batch(&mut self, events: &[(RecordEnvelopeV2, Vec<u8>)]) -> Result<(u64, u64)> {
        if events.is_empty() {
            return Err(SalamanderError::InvalidArgument(
                "storage batch must not be empty".into(),
            ));
        }
        let first = self.next_offset;
        let count = u32::try_from(events.len()).map_err(|_| SalamanderError::ResourceLimit {
            resource: "batch event count",
            actual: events.len() as u64,
            maximum: u32::MAX as u64,
        })?;

        let mut encoded_events = Vec::new();
        for (index, (envelope, payload)) in events.iter().enumerate() {
            let expected_position = first + index as u64;
            if envelope.batch_index != index as u32 {
                return Err(SalamanderError::InvalidFormat(format!(
                    "batch index {} does not match event order {index}",
                    envelope.batch_index
                )));
            }
            record::encode_kind(
                crate::format::FrameKind::Event,
                expected_position,
                envelope,
                payload,
                &mut encoded_events,
            )?;
        }
        let digest = crc32c::crc32c(&encoded_events);
        let control_payload = batch_control_payload(count, digest);
        let control_envelope = batch_control_envelope(&events[0].0, count, digest)?;
        let mut encoded = Vec::new();
        record::encode_kind(
            crate::format::FrameKind::BatchBegin,
            first,
            &control_envelope,
            &control_payload,
            &mut encoded,
        )?;
        encoded.extend_from_slice(&encoded_events);
        record::encode_kind(
            crate::format::FrameKind::BatchCommit,
            first,
            &control_envelope,
            &control_payload,
            &mut encoded,
        )?;
        self.file.write_all(&encoded)?;
        self.len_bytes += encoded.len() as u64;
        self.next_offset += events.len() as u64;
        Ok((first, self.next_offset - 1))
    }

    pub fn append_system(&mut self, envelope: &RecordEnvelopeV2, payload: &[u8]) -> Result<()> {
        let mut encoded = Vec::new();
        record::encode_kind(
            crate::format::FrameKind::System,
            self.next_offset,
            envelope,
            payload,
            &mut encoded,
        )?;
        self.file.write_all(&encoded)?;
        self.len_bytes += encoded.len() as u64;
        Ok(())
    }

    /// fsync (DESIGN.md §3.3 — `append()` buffers, `commit()`/`sync()`
    /// makes durable). `sync_data` (fdatasync) is cheaper than a full
    /// `sync_all` and is still correct here: POSIX only lets it skip
    /// metadata that isn't needed to read the data back correctly, and our
    /// writes change the file's length, which fdatasync does flush.
    pub fn sync(&mut self) -> Result<()> {
        self.file.sync_data()?;
        Ok(())
    }

    /// Test-only convenience: decode the whole segment through the same
    /// scan recovery uses and return `(position, payload)` pairs.
    /// Production reads go through the WP-04 streaming reader.
    #[cfg(test)]
    pub fn iter(&self) -> impl Iterator<Item = Result<(u64, Vec<u8>)>> + '_ {
        let items: Vec<Result<(u64, Vec<u8>)>> = (|| -> Result<Vec<(u64, Vec<u8>)>> {
            let mut clone = self.file.try_clone()?;
            clone.seek(SeekFrom::Start(0))?;
            let mut buf = Vec::new();
            clone.read_to_end(&mut buf)?;
            Ok(scan_records(&buf, self.base_offset)?
                .records
                .into_iter()
                .map(|record| (record.position, record.payload))
                .collect())
        })()
        .map(|records| records.into_iter().map(Ok).collect())
        .unwrap_or_else(|error| vec![Err(error)]);
        items.into_iter()
    }

    pub fn base_offset(&self) -> u64 {
        self.base_offset
    }

    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }

    pub fn len_bytes(&self) -> u64 {
        self.len_bytes
    }
}

/// 20-digit zero-padded base offset, so lexicographic filename order ==
/// offset order (DESIGN.md §3.1). `pub(crate)`: `Log` (Step 3) reuses this
/// to name a just-retired active segment's path without duplicating the
/// format string.
pub(crate) fn segment_path(dir: &Path, base_offset: u64) -> PathBuf {
    dir.join(format!("{base_offset:020}.seg"))
}

pub(crate) fn parse_base_offset(path: &Path) -> Result<u64> {
    path.file_stem()
        .and_then(|s| s.to_str())
        .and_then(|s| s.parse::<u64>().ok())
        .ok_or_else(|| SalamanderError::InvalidSegmentName(path.display().to_string()))
}

/// Why a scan stopped before consuming the whole buffer (or that it
/// reached a clean end). Used only for diagnostics (review M-3).
enum ScanStop {
    /// Reached the end of the buffer exactly — no leftover bytes.
    CleanEof,
    /// `Ok(None)` with bytes still remaining: an incomplete record at the
    /// tail (crash mid-append, DESIGN.md §6 case C1).
    TornTail,
    /// A complete record whose CRC didn't match.
    CrcError,
    /// A complete, CRC-valid record whose stored offset wasn't the next
    /// expected one (DESIGN.md §3.2 torn-write detection).
    OffsetMismatch,
    BatchMismatch,
}

fn batch_control_payload(count: u32, digest: u32) -> [u8; 8] {
    let mut payload = [0; 8];
    payload[..4].copy_from_slice(&count.to_le_bytes());
    payload[4..].copy_from_slice(&digest.to_le_bytes());
    payload
}

/// Parse a `BatchBegin`/`BatchCommit` control payload. `pub(crate)` so the
/// streaming reader (WP-04) verifies digests with the exact same rule the
/// recovery scan uses.
pub(crate) fn parse_batch_control(payload: &[u8]) -> Result<(u32, u32)> {
    if payload.len() != 8 {
        return Err(SalamanderError::InvalidFormat(
            "batch control payload must be eight bytes".into(),
        ));
    }
    let count = u32::from_le_bytes(payload[..4].try_into().unwrap());
    let digest = u32::from_le_bytes(payload[4..].try_into().unwrap());
    if count == 0 {
        return Err(SalamanderError::InvalidFormat(
            "batch control count must be nonzero".into(),
        ));
    }
    Ok((count, digest))
}

fn batch_control_envelope(
    first: &RecordEnvelopeV2,
    count: u32,
    digest: u32,
) -> Result<RecordEnvelopeV2> {
    let mut envelope = first.clone();
    envelope.event_type = crate::format::EventType::new("salamander.batch-control")?;
    envelope.batch_index = count;
    envelope.metadata.insert(
        "salamander.batch_count".into(),
        count.to_le_bytes().to_vec(),
    );
    envelope.metadata.insert(
        "salamander.batch_digest".into(),
        digest.to_le_bytes().to_vec(),
    );
    Ok(envelope)
}

impl ScanStop {
    fn reason(&self) -> &'static str {
        match self {
            ScanStop::CleanEof => "clean eof",
            ScanStop::TornTail => "torn tail: incomplete trailing record",
            ScanStop::CrcError => "crc mismatch",
            ScanStop::OffsetMismatch => "offset-sequence mismatch",
            ScanStop::BatchMismatch => "batch commit mismatch",
        }
    }
}

struct ScanResult {
    // Since WP-04 every production read goes through the streaming
    // reader; recovery (`Segment::open`) consumes only the boundary
    // fields below. The collected records remain for the recovery tests,
    // which assert exactly which records survive a truncation.
    #[cfg_attr(not(test), allow(dead_code))]
    records: Vec<OwnedStoredRecord>,
    #[allow(dead_code)]
    system_records: Vec<OwnedStoredRecord>,
    /// Byte length of the valid prefix — where a torn tail would be
    /// truncated.
    valid_bytes: usize,
    /// The offset the next appended record would take.
    next_offset: u64,
    stop: ScanStop,
}

/// The single record-walk shared by `open` (which then truncates) and
/// `scan` (which never does). Keeping one loop means the two entry points
/// can't drift on what counts as a valid record — the offset-continuity
/// guard, the CRC check, and the torn-tail boundary are defined here once.
fn scan_records(buf: &[u8], base_offset: u64) -> Result<ScanResult> {
    let mut pos = 0usize;
    let mut valid_bytes = 0usize;
    let mut next_offset = base_offset;
    let mut records = Vec::new();
    let mut system_records = Vec::new();
    let mut pending: Option<PendingBatch> = None;

    let stop = loop {
        match record::decode_owned(&buf[pos..]) {
            Ok(Some((rec, consumed))) => {
                let frame_start = pos;
                pos += consumed;
                match rec.kind {
                    crate::format::FrameKind::BatchBegin => {
                        if pending.is_some() || rec.position != next_offset {
                            break ScanStop::OffsetMismatch;
                        }
                        let (count, digest) = parse_batch_control(&rec.payload)?;
                        pending = Some(PendingBatch {
                            start_byte: frame_start,
                            first_position: rec.position,
                            batch_id: rec.envelope.batch_id,
                            count,
                            digest,
                            event_bytes: Vec::new(),
                            events: Vec::new(),
                        });
                    }
                    crate::format::FrameKind::Event => {
                        if let Some(batch) = pending.as_mut() {
                            let index = batch.events.len();
                            if rec.position != batch.first_position + index as u64
                                || rec.envelope.batch_id != batch.batch_id
                                || rec.envelope.batch_index != index as u32
                                || index >= batch.count as usize
                            {
                                pos = batch.start_byte;
                                break ScanStop::BatchMismatch;
                            }
                            batch.event_bytes.extend_from_slice(&buf[frame_start..pos]);
                            batch.events.push(rec);
                        } else if rec.position == next_offset {
                            records.push(rec);
                            next_offset += 1;
                            valid_bytes = pos;
                        } else {
                            break ScanStop::OffsetMismatch;
                        }
                    }
                    crate::format::FrameKind::BatchCommit => {
                        let Some(batch) = pending.take() else {
                            break ScanStop::BatchMismatch;
                        };
                        let (count, digest) = parse_batch_control(&rec.payload)?;
                        let valid = rec.position == batch.first_position
                            && rec.envelope.batch_id == batch.batch_id
                            && count == batch.count
                            && digest == batch.digest
                            && batch.events.len() == batch.count as usize
                            && crc32c::crc32c(&batch.event_bytes) == batch.digest;
                        if !valid {
                            pos = batch.start_byte;
                            break ScanStop::BatchMismatch;
                        }
                        next_offset += u64::from(batch.count);
                        records.extend(batch.events);
                        valid_bytes = pos;
                    }
                    crate::format::FrameKind::System => {
                        if pending.is_some() || rec.position != next_offset {
                            break ScanStop::OffsetMismatch;
                        }
                        system_records.push(rec);
                        valid_bytes = pos;
                    }
                }
            }
            Ok(None) => {
                if let Some(batch) = pending.take() {
                    pos = batch.start_byte;
                }
                break if pos == buf.len() {
                    ScanStop::CleanEof
                } else {
                    ScanStop::TornTail
                };
            }
            Err(SalamanderError::Corrupt { .. }) => {
                if let Some(batch) = pending.take() {
                    pos = batch.start_byte;
                }
                break ScanStop::CrcError;
            }
            Err(other) => return Err(other),
        }
    };

    Ok(ScanResult {
        records,
        system_records,
        valid_bytes: valid_bytes.min(pos),
        next_offset,
        stop,
    })
}

struct PendingBatch {
    start_byte: usize,
    first_position: u64,
    batch_id: crate::format::BatchId,
    count: u32,
    digest: u32,
    event_bytes: Vec<u8>,
    events: Vec<OwnedStoredRecord>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::{BatchId, EventId};
    use tempfile::tempdir;

    /// Test-local read-only scan (the production read path is the WP-04
    /// streaming reader): decode via the recovery walk, mutate nothing.
    fn scan(path: &Path) -> Result<Vec<(u64, Vec<u8>)>> {
        let base_offset = parse_base_offset(path)?;
        let bytes = std::fs::read(path)?;
        Ok(scan_records(&bytes, base_offset)?
            .records
            .into_iter()
            .map(|record| (record.position, record.payload))
            .collect())
    }

    fn batch_events(count: usize) -> Vec<(RecordEnvelopeV2, Vec<u8>)> {
        let batch_id = BatchId::from_bytes([9; 16]);
        (0..count)
            .map(|index| {
                let mut envelope = record::raw_test_envelope();
                envelope.batch_id = batch_id;
                envelope.event_id = EventId::from_bytes([index as u8 + 1; 16]);
                envelope.batch_index = index as u32;
                (envelope, vec![index as u8; index + 1])
            })
            .collect()
    }

    #[test]
    fn every_batch_truncation_recovers_none_or_all() {
        let complete_dir = tempdir().unwrap();
        let path = segment_path(complete_dir.path(), 0);
        {
            let mut segment = Segment::create(complete_dir.path(), 0).unwrap();
            assert_eq!(segment.append_batch(&batch_events(3)).unwrap(), (0, 2));
            segment.sync().unwrap();
        }
        let complete = std::fs::read(path).unwrap();

        for cut in 0..=complete.len() {
            let scan = scan_records(&complete[..cut], 0).unwrap();
            if cut == complete.len() {
                assert_eq!(scan.next_offset, 3, "full batch at cut {cut}");
                assert_eq!(scan.records.len(), 3);
            } else {
                assert_eq!(scan.next_offset, 0, "partial batch at cut {cut}");
                assert!(scan.records.is_empty(), "partial batch leaked at cut {cut}");
                assert_eq!(scan.valid_bytes, 0, "partial batch boundary at cut {cut}");
            }
        }
    }

    #[test]
    fn create_then_reopen_empty_segment() {
        let dir = tempdir().unwrap();
        let path = segment_path(dir.path(), 5);
        {
            let seg = Segment::create(dir.path(), 5).unwrap();
            assert_eq!(seg.base_offset(), 5);
            assert_eq!(seg.next_offset(), 5);
            assert_eq!(seg.len_bytes(), 0);
        }

        let reopened = Segment::open(&path).unwrap();
        assert_eq!(reopened.base_offset(), 5);
        assert_eq!(reopened.next_offset(), 5);
        assert_eq!(reopened.iter().count(), 0);
    }

    #[test]
    fn append_and_reopen_round_trip() {
        let dir = tempdir().unwrap();
        let path = segment_path(dir.path(), 0);
        {
            let mut seg = Segment::create(dir.path(), 0).unwrap();
            seg.append(b"a").unwrap();
            seg.append(b"bb").unwrap();
            seg.append(b"ccc").unwrap();
            seg.sync().unwrap();
        }

        let mut reopened = Segment::open(&path).unwrap();
        let records: Vec<_> = reopened.iter().map(|r| r.unwrap()).collect();
        assert_eq!(
            records,
            vec![
                (0, b"a".to_vec()),
                (1, b"bb".to_vec()),
                (2, b"ccc".to_vec())
            ]
        );
        assert_eq!(reopened.next_offset(), 3);

        let offset = reopened.append(b"ddd").unwrap();
        assert_eq!(offset, 3);
    }

    #[test]
    fn iter_reflects_appends_without_explicit_sync() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), 0).unwrap();
        seg.append(b"no-sync-yet").unwrap();
        // Deliberately no seg.sync() call: this is the property that rules
        // out a Rust-level BufWriter (see the comment on `append`).
        let records: Vec<_> = seg.iter().map(|r| r.unwrap()).collect();
        assert_eq!(records, vec![(0, b"no-sync-yet".to_vec())]);
    }

    #[test]
    fn truncated_tail_record_is_dropped_on_open() {
        let dir = tempdir().unwrap();
        let path = segment_path(dir.path(), 0);
        let valid_len;
        {
            let mut seg = Segment::create(dir.path(), 0).unwrap();
            seg.append(b"first").unwrap();
            seg.append(b"second").unwrap();
            seg.sync().unwrap();
            valid_len = seg.len_bytes();
            seg.append(b"a third record long enough to survive a mid-payload cut")
                .unwrap();
        }

        // Simulate a crash mid-write: cut a few bytes into the third
        // record, leaving the first two fully intact.
        let full_len = std::fs::metadata(&path).unwrap().len();
        let cut_len = valid_len + 5;
        assert!(
            cut_len < full_len,
            "test record too short to exercise this cut"
        );
        {
            let f = OpenOptions::new().write(true).open(&path).unwrap();
            f.set_len(cut_len).unwrap();
        }

        let mut reopened = Segment::open(&path).unwrap();
        let records: Vec<_> = reopened.iter().map(|r| r.unwrap()).collect();
        assert_eq!(
            records,
            vec![(0, b"first".to_vec()), (1, b"second".to_vec())]
        );
        assert_eq!(reopened.next_offset(), 2);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), valid_len);

        // Appends continue at the correct offset after recovery.
        let offset = reopened.append(b"recovered-third").unwrap();
        assert_eq!(offset, 2);
    }

    #[test]
    fn flipped_bit_in_last_record_is_dropped_on_open() {
        let dir = tempdir().unwrap();
        let path = segment_path(dir.path(), 0);
        let valid_len;
        {
            let mut seg = Segment::create(dir.path(), 0).unwrap();
            seg.append(b"first").unwrap();
            valid_len = seg.len_bytes();
            seg.append(b"corrupt-me").unwrap();
            seg.sync().unwrap();
        }

        // Flip the last byte of the file — guaranteed to land inside the
        // second record's payload since it's non-empty.
        let mut bytes = std::fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();

        let mut reopened = Segment::open(&path).unwrap();
        let records: Vec<_> = reopened.iter().map(|r| r.unwrap()).collect();
        assert_eq!(records, vec![(0, b"first".to_vec())]);
        assert_eq!(reopened.next_offset(), 1);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), valid_len);

        let offset = reopened.append(b"recovered-second").unwrap();
        assert_eq!(offset, 1);
    }

    #[test]
    fn offset_mismatch_is_treated_as_corruption() {
        let dir = tempdir().unwrap();
        let path = segment_path(dir.path(), 0);

        let mut buf = Vec::new();
        record::encode(0, b"first", &mut buf);
        let valid_len = buf.len() as u64;
        record::encode(2, b"skipped-one", &mut buf); // should have been offset 1
        std::fs::write(&path, &buf).unwrap();

        let reopened = Segment::open(&path).unwrap();
        let records: Vec<_> = reopened.iter().map(|r| r.unwrap()).collect();
        assert_eq!(records, vec![(0, b"first".to_vec())]);
        assert_eq!(reopened.next_offset(), 1);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), valid_len);
    }

    #[test]
    fn segment_filename_zero_padded_to_twenty_digits() {
        let dir = tempdir().unwrap();
        Segment::create(dir.path(), 104857).unwrap();
        let expected = dir.path().join("00000000000000104857.seg");
        assert!(expected.exists(), "expected {expected:?} to exist");
    }

    #[test]
    fn open_rejects_non_numeric_filename() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("not-a-number.seg");
        std::fs::write(&path, b"").unwrap();
        assert!(matches!(
            Segment::open(&path),
            Err(SalamanderError::InvalidSegmentName(_))
        ));
    }

    #[test]
    fn scan_reads_all_records_without_write_access() {
        let dir = tempdir().unwrap();
        let path = segment_path(dir.path(), 0);
        {
            let mut seg = Segment::create(dir.path(), 0).unwrap();
            seg.append(b"a").unwrap();
            seg.append(b"bb").unwrap();
            seg.sync().unwrap();
        }

        // Mark the closed segment read-only: `scan` must still read it,
        // because iterating a closed segment (or a restored backup on
        // read-only media) must never require write access (review C-2).
        let mut perms = std::fs::metadata(&path).unwrap().permissions();
        perms.set_readonly(true);
        std::fs::set_permissions(&path, perms).unwrap();

        let records = scan(&path).unwrap();
        assert_eq!(records, vec![(0, b"a".to_vec()), (1, b"bb".to_vec())]);

        // Restore write permission so the tempdir can be cleaned up
        // (Windows refuses to delete read-only files). The clippy lint
        // warns that `set_readonly(false)` is world-writable on Unix — that
        // is irrelevant for a throwaway file inside a per-test tempdir that
        // is deleted moments later.
        let mut perms = std::fs::metadata(&path).unwrap().permissions();
        #[allow(clippy::permissions_set_readonly_false)]
        perms.set_readonly(false);
        std::fs::set_permissions(&path, perms).unwrap();
    }

    #[test]
    fn scan_returns_valid_prefix_without_mutating_a_corrupt_tail() {
        let dir = tempdir().unwrap();
        let path = segment_path(dir.path(), 0);
        let valid_len;
        {
            let mut seg = Segment::create(dir.path(), 0).unwrap();
            seg.append(b"first").unwrap();
            valid_len = seg.len_bytes();
            seg.append(b"corrupt-me").unwrap();
            seg.sync().unwrap();
        }

        // Flip the last byte: the second record now fails its CRC.
        let mut bytes = std::fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        let full_len = bytes.len() as u64;
        std::fs::write(&path, &bytes).unwrap();

        // scan returns the valid prefix but, unlike open, leaves the file
        // byte-for-byte unchanged (review C-2: closed segments are
        // immutable, the read path must not truncate).
        let records = scan(&path).unwrap();
        assert_eq!(records, vec![(0, b"first".to_vec())]);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), full_len);
        assert_ne!(valid_len, full_len); // sanity: the corrupt record really was present
    }

    mod proptests {
        use super::*;
        use proptest::prelude::*;

        proptest! {
            /// IMPLEMENTATION.md Step 6: random valid records + a random
            /// truncation point; `Segment::open` must recover exactly the
            /// prefix of records that fit fully within the truncated
            /// length -- no fewer, no more.
            #[test]
            fn scan_recovers_exactly_the_valid_prefix(
                payloads in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..64), 1..12),
                cut_fraction in 0.0f64..=1.0,
            ) {
                let dir = tempdir().unwrap();
                let path = segment_path(dir.path(), 0);

                let mut cumulative_lens = Vec::new();
                {
                    let mut seg = Segment::create(dir.path(), 0).unwrap();
                    for payload in &payloads {
                        seg.append(payload).unwrap();
                        cumulative_lens.push(seg.len_bytes());
                    }
                    seg.sync().unwrap();
                }

                let full_len = *cumulative_lens.last().unwrap();
                let cut_len = (cut_fraction * full_len as f64) as u64;

                {
                    let f = OpenOptions::new().write(true).open(&path).unwrap();
                    f.set_len(cut_len).unwrap();
                }

                let expected_count = cumulative_lens.iter().filter(|&&len| len <= cut_len).count();

                let reopened = Segment::open(&path).unwrap();
                let records: Vec<_> = reopened.iter().map(|r| r.unwrap()).collect();

                prop_assert_eq!(records.len(), expected_count);
                for (i, (offset, payload)) in records.iter().enumerate() {
                    prop_assert_eq!(*offset, i as u64);
                    prop_assert_eq!(payload, &payloads[i]);
                }
                prop_assert_eq!(reopened.next_offset(), expected_count as u64);

                let expected_file_len = if expected_count == 0 {
                    0
                } else {
                    cumulative_lens[expected_count - 1]
                };
                prop_assert_eq!(std::fs::metadata(&path).unwrap().len(), expected_file_len);
            }
        }
    }
}
