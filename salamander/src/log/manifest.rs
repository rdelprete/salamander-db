//! DESIGN.md §3.1, §6 (case C3) — manifest.json: active segment, next
//! offset, format version. Written via temp-file + rename (atomic on
//! POSIX). The C3 crash-ordering (create segment, fsync dir, update
//! manifest, fsync manifest) lives in `Log::roll`, not here — this module
//! only owns the on-disk representation.

use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::{Result, SalamanderError};

/// Version of the manifest's *own* on-disk shape. Bumped to 2 in WP-2 when
/// `payload_format_version` was added — see [`Manifest`].
pub(crate) const MANIFEST_FORMAT_VERSION: u32 = 3;
pub(crate) const STORAGE_FORMAT_VERSION: u32 = 2;

/// Version of the *payload* encoding this build reads and writes: v1 means
/// "bincode, fixint, `Event` envelope" (DESIGN.md §3.2). This is the
/// tier of the two-tier stability contract that is allowed to migrate: the
/// record framing (`len|crc|offset|payload`) is stable forever, but how the
/// `payload` bytes are encoded is versioned here. A future migration bumps
/// this and, if it wants, teaches `Log::open` to read older payloads.
pub(crate) const PAYLOAD_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub format_version: u32,
    #[serde(default = "legacy_storage_format_version")]
    pub storage_format_version: u32,
    #[serde(default)]
    pub database_id: [u8; 16],
    /// Which payload encoding the records in this dir use (see
    /// [`PAYLOAD_FORMAT_VERSION`]). Manifests written before WP-2 don't
    /// carry this field; `serde(default)` maps their absence to v1, the
    /// only encoding that ever existed back then (OQ-2b). `Log::open`
    /// refuses to open a dir whose value this build doesn't recognize,
    /// rather than handing garbage bytes to bincode.
    #[serde(default = "default_payload_format_version")]
    pub payload_format_version: u32,
    pub active_segment_base: u64,
    /// Lowest user-event position available through public history APIs.
    /// Databases created before retention support default to zero.
    #[serde(default)]
    pub retention_floor: u64,
    /// Monotonic generation advanced by each committed retention operation.
    #[serde(default)]
    pub retention_generation: u64,
    /// Checksum of the authoritative anchor for `retention_floor`.
    #[serde(default)]
    pub retention_anchor_checksum: Option<u32>,
    /// Advisory / diagnostic only. `Log::open` never trusts this value —
    /// it always re-derives the true next offset by scanning the active
    /// segment, which *must* win: after a torn-tail truncation (DESIGN.md
    /// §6 case C1) the real end of the log can be earlier than whatever
    /// was last persisted here. Kept in the file because it's useful to a
    /// human eyeballing manifest.json, and removing it would churn the
    /// on-disk format for no functional gain (review M-4).
    pub next_offset: u64,
}

fn legacy_storage_format_version() -> u32 {
    1
}

/// The payload version a manifest with no `payload_format_version` field is
/// assumed to use. A missing field means the dir predates WP-2, and those
/// dirs only ever used payload format 1 — so this is a hard `1`, not
/// [`PAYLOAD_FORMAT_VERSION`]. It describes the past and must not drift if
/// the current build's payload version is later bumped.
fn default_payload_format_version() -> u32 {
    1
}

impl Manifest {
    pub fn read(dir: &Path) -> Result<Self> {
        let bytes = std::fs::read(manifest_path(dir))?;
        serde_json::from_slice(&bytes).map_err(|e| SalamanderError::Manifest(e.to_string()))
    }

    /// Temp-file + rename + fsync-the-directory, so the manifest either
    /// fully updates or doesn't change at all — never a half-written file
    /// (DESIGN.md §6, case C3).
    pub fn write(&self, dir: &Path) -> Result<()> {
        let final_path = manifest_path(dir);
        let tmp_path = dir.join("manifest.json.tmp");

        let bytes = serde_json::to_vec_pretty(self)
            .map_err(|e| SalamanderError::Manifest(e.to_string()))?;

        {
            let mut tmp = File::create(&tmp_path)?;
            tmp.write_all(&bytes)?;
            tmp.sync_all()?;
        }

        std::fs::rename(&tmp_path, &final_path)?;
        super::sync_dir(dir)?;

        Ok(())
    }
}

fn manifest_path(dir: &Path) -> PathBuf {
    dir.join("manifest.json")
}
