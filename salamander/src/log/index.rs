//! WP-04 — derived per-segment sidecar indexes.
//!
//! One sidecar file (`<base>.sidx`) per **closed** segment carries three
//! acceleration structures:
//!
//! - **sparse seek points** `(position, byte_offset)` at frame boundaries,
//!   so a reader can enter a segment near its start position instead of
//!   walking from byte 0;
//! - **stream postings** — per `StreamId`: first/last position and record
//!   count. A reader proves "no stream in this segment matches the
//!   selector" from the postings alone and skips the segment without any
//!   payload I/O (this is load-bearing for WP-09 lazy healing);
//! - a **timestamp range** (`min_ts`, `max_ts`) used strictly as a skip
//!   *hint* — record timestamps are not monotonic, so time bounds are
//!   always re-checked per record by the reader.
//!
//! Sidecars are derived state (spec rule 3): they are rebuilt from the
//! segment on any validation failure and their loss or corruption changes
//! I/O counts only, never answers. They are therefore written best-effort
//! (temp file + rename) and never fsynced — a torn sidecar fails its CRC
//! and is rebuilt.

use std::path::{Path, PathBuf};

use crate::format::StreamId;
use crate::{Result, SalamanderError};

const SIDECAR_MAGIC: [u8; 4] = *b"SDBX";
const SIDECAR_VERSION: u16 = 1;
/// Fixed header: magic 4 + version 2 + reserved 2 + base 8 + end 8 +
/// min_ts 8 + max_ts 8 + system_frames 8 + seek count 4 + posting count 4.
const HEADER_LEN: usize = 56;
const SEEK_ENTRY_LEN: usize = 16;
const POSTING_ENTRY_LEN: usize = 40;

/// Target spacing between sparse seek points, in segment-file bytes.
pub(crate) const SEEK_POINT_SPACING: u64 = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PostingEntry {
    pub stream: StreamId,
    pub first: u64,
    pub last: u64,
    pub count: u64,
}

/// The in-memory form of one segment's sidecar.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Sidecar {
    /// First position in the segment (== the segment's base offset).
    pub base: u64,
    /// One past the last position in the segment (== next segment's base).
    pub end: u64,
    /// Minimum/maximum envelope timestamp over user events. When the
    /// segment holds no user events, `min_ts > max_ts` and the range must
    /// not be used for skipping.
    pub min_ts: i64,
    pub max_ts: i64,
    /// Number of system frames in the segment — lets a system-only read
    /// skip segments that provably contain none.
    pub system_frames: u64,
    /// `(position, byte_offset)` pairs at frame boundaries outside any
    /// batch, sorted by position. A reader may start decoding at any entry
    /// with continuity state `expected == position`.
    pub seek_points: Vec<(u64, u64)>,
    /// Sorted by `stream`; one entry per distinct stream in the segment.
    pub postings: Vec<PostingEntry>,
}

impl Sidecar {
    /// Greatest seek point with `position <= from`, if any.
    pub fn seek_point_before(&self, from: u64) -> Option<(u64, u64)> {
        match self.seek_points.binary_search_by_key(&from, |(p, _)| *p) {
            Ok(i) => Some(self.seek_points[i]),
            Err(0) => None,
            Err(i) => Some(self.seek_points[i - 1]),
        }
    }

    /// True when the timestamp *hint* proves no user event in the segment
    /// can fall inside `range`. Conservative: an empty or absent range
    /// never skips.
    pub fn time_disjoint(&self, range: &std::ops::Range<i64>) -> bool {
        self.min_ts <= self.max_ts && (self.max_ts < range.start || self.min_ts >= range.end)
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(
            HEADER_LEN
                + self.seek_points.len() * SEEK_ENTRY_LEN
                + self.postings.len() * POSTING_ENTRY_LEN
                + 4,
        );
        out.extend_from_slice(&SIDECAR_MAGIC);
        out.extend_from_slice(&SIDECAR_VERSION.to_le_bytes());
        out.extend_from_slice(&0u16.to_le_bytes());
        out.extend_from_slice(&self.base.to_le_bytes());
        out.extend_from_slice(&self.end.to_le_bytes());
        out.extend_from_slice(&self.min_ts.to_le_bytes());
        out.extend_from_slice(&self.max_ts.to_le_bytes());
        out.extend_from_slice(&self.system_frames.to_le_bytes());
        out.extend_from_slice(&(self.seek_points.len() as u32).to_le_bytes());
        out.extend_from_slice(&(self.postings.len() as u32).to_le_bytes());
        for (position, byte_offset) in &self.seek_points {
            out.extend_from_slice(&position.to_le_bytes());
            out.extend_from_slice(&byte_offset.to_le_bytes());
        }
        for entry in &self.postings {
            out.extend_from_slice(entry.stream.as_bytes());
            out.extend_from_slice(&entry.first.to_le_bytes());
            out.extend_from_slice(&entry.last.to_le_bytes());
            out.extend_from_slice(&entry.count.to_le_bytes());
        }
        let crc = crc32c::crc32c(&out);
        out.extend_from_slice(&crc.to_le_bytes());
        out
    }

    /// Strict decode: any deviation is an error — the caller treats every
    /// error identically (discard and rebuild), so there is no partial
    /// acceptance path to get wrong.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let fail = |reason: &str| SalamanderError::InvalidFormat(format!("sidecar: {reason}"));
        if bytes.len() < HEADER_LEN + 4 {
            return Err(fail("too short"));
        }
        let (body, crc_bytes) = bytes.split_at(bytes.len() - 4);
        let stored_crc = u32::from_le_bytes(crc_bytes.try_into().unwrap());
        if crc32c::crc32c(body) != stored_crc {
            return Err(fail("crc mismatch"));
        }
        if body[0..4] != SIDECAR_MAGIC {
            return Err(fail("bad magic"));
        }
        if u16::from_le_bytes(body[4..6].try_into().unwrap()) != SIDECAR_VERSION {
            return Err(fail("unsupported version"));
        }
        let u64_at = |at: usize| u64::from_le_bytes(body[at..at + 8].try_into().unwrap());
        let i64_at = |at: usize| i64::from_le_bytes(body[at..at + 8].try_into().unwrap());
        let base = u64_at(8);
        let end = u64_at(16);
        let min_ts = i64_at(24);
        let max_ts = i64_at(32);
        let system_frames = u64_at(40);
        let seek_count = u32::from_le_bytes(body[48..52].try_into().unwrap()) as usize;
        let posting_count = u32::from_le_bytes(body[52..56].try_into().unwrap()) as usize;
        let expected_len = HEADER_LEN
            .checked_add(
                seek_count
                    .checked_mul(SEEK_ENTRY_LEN)
                    .ok_or_else(|| fail("overflow"))?,
            )
            .and_then(|n| n.checked_add(posting_count * POSTING_ENTRY_LEN))
            .ok_or_else(|| fail("overflow"))?;
        if body.len() != expected_len {
            return Err(fail("length mismatch"));
        }
        let mut at = HEADER_LEN;
        let mut seek_points = Vec::with_capacity(seek_count);
        for _ in 0..seek_count {
            seek_points.push((u64_at(at), u64_at(at + 8)));
            at += SEEK_ENTRY_LEN;
        }
        let mut postings = Vec::with_capacity(posting_count);
        for _ in 0..posting_count {
            let stream = StreamId::from_bytes(body[at..at + 16].try_into().unwrap());
            postings.push(PostingEntry {
                stream,
                first: u64_at(at + 16),
                last: u64_at(at + 24),
                count: u64_at(at + 32),
            });
            at += POSTING_ENTRY_LEN;
        }
        if !seek_points.windows(2).all(|w| w[0].0 <= w[1].0)
            || !postings.windows(2).all(|w| w[0].stream < w[1].stream)
        {
            return Err(fail("unsorted entries"));
        }
        Ok(Sidecar {
            base,
            end,
            min_ts,
            max_ts,
            system_frames,
            seek_points,
            postings,
        })
    }
}

pub(crate) fn sidecar_path(segment_path: &Path) -> PathBuf {
    segment_path.with_extension("sidx")
}

/// Load and validate the sidecar for a segment, checking it against the
/// positions the manifest-derived layout says the segment must span. Any
/// failure returns `None` — the caller rebuilds or scans.
pub(crate) fn load(segment_path: &Path, base: u64, end: u64) -> Option<Sidecar> {
    let bytes = std::fs::read(sidecar_path(segment_path)).ok()?;
    let sidecar = Sidecar::decode(&bytes).ok()?;
    (sidecar.base == base && sidecar.end == end).then_some(sidecar)
}

/// Best-effort persist: temp file + rename, no fsync. A crash mid-write
/// leaves either the previous sidecar or a temp file nobody reads; a torn
/// rename target fails CRC on load and is rebuilt. Failure is reported to
/// the caller only so it can log — it must never fail a read or a roll.
pub(crate) fn persist(segment_path: &Path, sidecar: &Sidecar) -> std::io::Result<()> {
    let final_path = sidecar_path(segment_path);
    let tmp_path = final_path.with_extension("sidx.tmp");
    std::fs::write(&tmp_path, sidecar.encode())?;
    match std::fs::rename(&tmp_path, &final_path) {
        Ok(()) => Ok(()),
        Err(err) => {
            // Windows refuses to rename over an existing file; replace it.
            let _ = std::fs::remove_file(&final_path);
            std::fs::rename(&tmp_path, &final_path).map_err(|_| err)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Sidecar {
        Sidecar {
            base: 100,
            end: 240,
            min_ts: -5,
            max_ts: 1_000,
            system_frames: 2,
            seek_points: vec![(100, 0), (150, 70_000), (200, 140_000)],
            postings: vec![
                PostingEntry {
                    stream: StreamId::from_bytes([1; 16]),
                    first: 100,
                    last: 199,
                    count: 90,
                },
                PostingEntry {
                    stream: StreamId::from_bytes([2; 16]),
                    first: 105,
                    last: 239,
                    count: 50,
                },
            ],
        }
    }

    #[test]
    fn encode_decode_round_trip() {
        let sidecar = sample();
        assert_eq!(Sidecar::decode(&sidecar.encode()).unwrap(), sidecar);
    }

    #[test]
    fn every_single_bit_flip_is_rejected() {
        let bytes = sample().encode();
        for i in 0..bytes.len() {
            let mut mutated = bytes.clone();
            mutated[i] ^= 0x01;
            assert!(
                Sidecar::decode(&mutated).is_err(),
                "bit flip at byte {i} was accepted"
            );
        }
    }

    #[test]
    fn truncation_is_rejected() {
        let bytes = sample().encode();
        for len in 0..bytes.len() {
            assert!(Sidecar::decode(&bytes[..len]).is_err());
        }
    }

    #[test]
    fn seek_point_before_picks_greatest_at_or_below() {
        let sidecar = sample();
        assert_eq!(sidecar.seek_point_before(99), None);
        assert_eq!(sidecar.seek_point_before(100), Some((100, 0)));
        assert_eq!(sidecar.seek_point_before(149), Some((100, 0)));
        assert_eq!(sidecar.seek_point_before(150), Some((150, 70_000)));
        assert_eq!(sidecar.seek_point_before(10_000), Some((200, 140_000)));
    }

    #[test]
    fn time_disjoint_is_conservative_for_empty_segments() {
        let mut sidecar = sample();
        assert!(sidecar.time_disjoint(&(2_000..3_000)));
        assert!(sidecar.time_disjoint(&(i64::MIN..-5)));
        assert!(!sidecar.time_disjoint(&(1_000..1_001)));
        assert!(!sidecar.time_disjoint(&(500..600)));
        // No user events: the hint must never claim disjointness.
        sidecar.min_ts = i64::MAX;
        sidecar.max_ts = i64::MIN;
        assert!(!sidecar.time_disjoint(&(2_000..3_000)));
    }
}
