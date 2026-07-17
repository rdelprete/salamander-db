//! Authoritative engine-core retention anchor publication and validation.

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::branch::BranchCatalog;
use crate::stream::StreamCatalog;
use crate::{Result, SalamanderError};

const MAGIC: &[u8; 8] = b"SLMRETN1";
pub(crate) const FORMAT_VERSION: u32 = 5;
const MAX_ANCHOR_BYTES: usize = 256 * 1024 * 1024;
/// Maximum opaque bootstrap payload accepted for one branch or consumer.
pub const MAX_BOOTSTRAP_BYTES: usize = 64 * 1024 * 1024;

/// Verified metadata for one authoritative engine-core retention anchor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionAnchorInfo {
    /// On-disk anchor format.
    pub format_version: u32,
    /// Database whose core catalogs are captured.
    pub database_id: [u8; 16],
    /// Effective retention floor represented by the anchor.
    pub floor: u64,
    /// Exact durable head represented by the anchor.
    pub head: u64,
    /// Encoded file length.
    pub bytes: u64,
    /// CRC32C of the encoded catalog payload.
    pub checksum: u32,
    /// Projection checkpoints promoted into this anchor.
    pub projection_checkpoints: usize,
    /// Branch bootstrap payloads promoted into this anchor.
    pub branch_bootstraps: usize,
    /// Consumer bootstrap payloads promoted into this anchor.
    pub consumer_bootstraps: usize,
    /// Total opaque bootstrap payload bytes carried by the anchor.
    pub bootstrap_bytes: u64,
    /// Engine system metadata records captured by the anchor.
    pub system_records: usize,
}

/// Verified projection checkpoint coverage promoted by a retention anchor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionProjectionCoverage {
    /// Durable projection registration name.
    pub name: String,
    /// Descriptor fingerprint captured by every referenced checkpoint.
    pub descriptor_fingerprint: [u8; 16],
    /// Branch whose projection state is captured.
    pub branch_id: [u8; 16],
    /// Exclusive projection cursor represented by the checkpoints.
    pub cursor: u64,
    /// Immutable, checksummed snapshot files promoted by the anchor.
    pub snapshot_ids: Vec<String>,
}

/// Opaque application bootstrap for one branch at an effective floor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionBranchBootstrap {
    /// Branch covered by the application checkpoint.
    pub branch_id: [u8; 16],
    /// Effective floor for which the checkpoint was created.
    pub floor: u64,
    /// CRC32C of `checkpoint`.
    pub checksum: u32,
    /// Opaque application bytes; the engine never interprets them.
    pub checkpoint: Vec<u8>,
}

/// Opaque external-consumer bootstrap at an effective floor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionConsumerBootstrap {
    /// Stable durable consumer identifier.
    pub consumer_id: String,
    /// Effective floor for which the checkpoint was created.
    pub floor: u64,
    /// CRC32C of `checkpoint`.
    pub checksum: u32,
    /// Opaque consumer bytes; the engine never interprets them.
    pub checkpoint: Vec<u8>,
    /// Stable identity used when fetching this checkpoint.
    #[serde(default)]
    pub checkpoint_id: [u8; 16],
    /// Exact feed scope for which this checkpoint is valid.
    #[serde(default)]
    pub scope: RetentionFeedScope,
    /// Application-defined checkpoint codec identifier.
    #[serde(default = "default_bootstrap_codec")]
    pub codec: String,
    /// Application-defined checkpoint codec version.
    #[serde(default = "default_bootstrap_codec_version")]
    pub codec_version: u32,
}

fn default_bootstrap_codec() -> String {
    "opaque".into()
}

fn default_bootstrap_codec_version() -> u32 {
    1
}

/// Envelope-only feed scope bound to a consumer bootstrap.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionFeedScope {
    /// Selected branches; empty means all branches.
    pub branches: Vec<[u8; 16]>,
    /// Selected streams; empty means all streams.
    pub streams: Vec<[u8; 16]>,
    /// Selected event types; empty means all event types.
    pub event_types: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct AnchoredSystemRecord {
    pub event_type: String,
    pub payload: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct CoreRetentionAnchor {
    pub format_version: u32,
    pub database_id: [u8; 16],
    pub floor: u64,
    pub head: u64,
    pub stream_catalog: StreamCatalog,
    pub branch_catalog: BranchCatalog,
    #[serde(default)]
    pub projection_coverage: Vec<RetentionProjectionCoverage>,
    #[serde(default)]
    pub branch_bootstraps: Vec<RetentionBranchBootstrap>,
    #[serde(default)]
    pub consumer_bootstraps: Vec<RetentionConsumerBootstrap>,
    #[serde(default)]
    pub system_records: Vec<AnchoredSystemRecord>,
}

pub(crate) fn publish(root: &Path, anchor: CoreRetentionAnchor) -> Result<RetentionAnchorInfo> {
    validate_bootstraps(
        anchor.floor,
        &anchor.branch_bootstraps,
        &anchor.consumer_bootstraps,
    )?;
    if let Ok(Some((existing, info))) = load(root) {
        if validate_identity(&existing, anchor.database_id, anchor.floor, anchor.head).is_ok()
            && existing.projection_coverage == anchor.projection_coverage
            && existing.branch_bootstraps == anchor.branch_bootstraps
            && existing.consumer_bootstraps == anchor.consumer_bootstraps
        {
            return Ok(info);
        }
    }
    let payload = bincode::serialize(&anchor)
        .map_err(|error| SalamanderError::Serialization(error.to_string()))?;
    if payload.len() > MAX_ANCHOR_BYTES {
        return Err(SalamanderError::ResourceLimit {
            resource: "retention anchor bytes",
            actual: payload.len() as u64,
            maximum: MAX_ANCHOR_BYTES as u64,
        });
    }
    let checksum = crc32c::crc32c(&payload);
    let mut encoded = Vec::with_capacity(16 + payload.len());
    encoded.extend_from_slice(MAGIC);
    encoded.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    encoded.extend_from_slice(&checksum.to_le_bytes());
    encoded.extend_from_slice(&payload);

    let dir = anchor_dir(root);
    std::fs::create_dir_all(&dir)?;
    let temporary = dir.join("core.anchor.tmp");
    crash_point("before_anchor_publish");
    {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)?;
        file.write_all(&encoded)?;
        file.sync_all()?;
    }
    crash_point("after_anchor_fsync");
    let (verified, info) = decode(&temporary)?;
    validate_identity(&verified, anchor.database_id, anchor.floor, anchor.head)?;
    if anchor_path(root).exists() {
        std::fs::remove_file(anchor_path(root))?;
    }
    std::fs::rename(&temporary, anchor_path(root))?;
    sync_dir(&dir)?;
    crash_point("after_anchor_publish");
    Ok(info)
}

pub(crate) fn crash_point(name: &str) {
    if std::env::var_os("SALAMANDER_RETENTION_CRASH_AT").is_some_and(|value| value == name) {
        std::process::abort();
    }
}

pub(crate) fn load(root: &Path) -> Result<Option<(CoreRetentionAnchor, RetentionAnchorInfo)>> {
    let path = anchor_path(root);
    match decode(&path) {
        Ok(value) => Ok(Some(value)),
        Err(SalamanderError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

pub(crate) fn validate_identity(
    anchor: &CoreRetentionAnchor,
    database_id: [u8; 16],
    floor: u64,
    head: u64,
) -> Result<()> {
    if anchor.format_version != FORMAT_VERSION {
        return Err(corrupt(format!(
            "unsupported retention anchor format {}",
            anchor.format_version
        )));
    }
    if anchor.database_id != database_id {
        return Err(corrupt("retention anchor belongs to another database"));
    }
    if anchor.floor != floor {
        return Err(corrupt(format!(
            "retention anchor floor {} does not match required floor {floor}",
            anchor.floor
        )));
    }
    if anchor.head != head {
        return Err(corrupt(format!(
            "retention anchor head {} does not match durable head {head}",
            anchor.head
        )));
    }
    Ok(())
}

fn decode(path: &Path) -> Result<(CoreRetentionAnchor, RetentionAnchorInfo)> {
    let length = std::fs::metadata(path)?.len() as usize;
    if !(16..=16 + MAX_ANCHOR_BYTES).contains(&length) {
        return Err(corrupt("retention anchor length is invalid"));
    }
    let mut file = File::open(path)?;
    let mut prefix = [0u8; 16];
    file.read_exact(&mut prefix)?;
    if &prefix[..8] != MAGIC {
        return Err(corrupt("retention anchor magic is invalid"));
    }
    let payload_len = u32::from_le_bytes(prefix[8..12].try_into().unwrap()) as usize;
    if payload_len == 0 || payload_len > MAX_ANCHOR_BYTES || payload_len + 16 != length {
        return Err(corrupt("retention anchor payload length is invalid"));
    }
    let expected_checksum = u32::from_le_bytes(prefix[12..16].try_into().unwrap());
    let mut payload = vec![0; payload_len];
    file.read_exact(&mut payload)?;
    if crc32c::crc32c(&payload) != expected_checksum {
        return Err(corrupt("retention anchor checksum mismatch"));
    }
    let anchor: CoreRetentionAnchor = bincode::deserialize(&payload)
        .map_err(|error| corrupt(format!("retention anchor decode: {error}")))?;
    validate_bootstraps(
        anchor.floor,
        &anchor.branch_bootstraps,
        &anchor.consumer_bootstraps,
    )?;
    let info = RetentionAnchorInfo {
        format_version: anchor.format_version,
        database_id: anchor.database_id,
        floor: anchor.floor,
        head: anchor.head,
        bytes: length as u64,
        checksum: expected_checksum,
        projection_checkpoints: anchor
            .projection_coverage
            .iter()
            .map(|coverage| coverage.snapshot_ids.len())
            .sum(),
        branch_bootstraps: anchor.branch_bootstraps.len(),
        consumer_bootstraps: anchor.consumer_bootstraps.len(),
        bootstrap_bytes: anchor
            .branch_bootstraps
            .iter()
            .map(|item| item.checkpoint.len() as u64)
            .chain(
                anchor
                    .consumer_bootstraps
                    .iter()
                    .map(|item| item.checkpoint.len() as u64),
            )
            .sum(),
        system_records: anchor.system_records.len(),
    };
    Ok((anchor, info))
}

fn validate_bootstraps(
    floor: u64,
    branches: &[RetentionBranchBootstrap],
    consumers: &[RetentionConsumerBootstrap],
) -> Result<()> {
    let mut branch_ids = std::collections::BTreeSet::new();
    for item in branches {
        if item.floor != floor
            || item.checkpoint.len() > MAX_BOOTSTRAP_BYTES
            || crc32c::crc32c(&item.checkpoint) != item.checksum
            || !branch_ids.insert(item.branch_id)
        {
            return Err(corrupt("invalid or duplicate branch bootstrap coverage"));
        }
    }
    let mut consumer_ids = std::collections::BTreeSet::new();
    let mut checkpoint_ids = std::collections::BTreeSet::new();
    for item in consumers {
        if item.floor != floor
            || item.checkpoint.len() > MAX_BOOTSTRAP_BYTES
            || crc32c::crc32c(&item.checkpoint) != item.checksum
            || !consumer_ids.insert(item.consumer_id.as_str())
            || item.checkpoint_id == [0; 16]
            || !checkpoint_ids.insert(item.checkpoint_id)
            || item.codec.is_empty()
            || item.codec.len() > 128
        {
            return Err(corrupt("invalid or duplicate consumer bootstrap coverage"));
        }
    }
    Ok(())
}

fn anchor_dir(root: &Path) -> PathBuf {
    root.join("retention")
}

fn anchor_path(root: &Path) -> PathBuf {
    anchor_dir(root).join("core.anchor")
}

fn corrupt(reason: impl Into<String>) -> SalamanderError {
    SalamanderError::InvalidFormat(reason.into())
}

#[cfg(unix)]
fn sync_dir(dir: &Path) -> Result<()> {
    File::open(dir)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_dir(_dir: &Path) -> Result<()> {
    Ok(())
}
