//! Hostile-input validation and atomic publication for full projection snapshots.
//!
//! The descriptor types here are surfaced only through the `#[doc(hidden)]`
//! engine facade, so `missing_docs` is relaxed for this module.
#![allow(missing_docs)]

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::facade::{ProjectionCursor, ProjectionDescriptor};
use crate::{EngineError, ErrorCategory};

const MAGIC: &[u8; 8] = b"SLMSNAP1";
const MAX_MANIFEST_BYTES: usize = 64 * 1024;
pub const MAX_SNAPSHOT_STATE_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotManifest {
    pub format_version: u32,
    pub database_id: [u8; 16],
    pub projection_name: String,
    pub descriptor_fingerprint: [u8; 16],
    pub definition_id: [u8; 16],
    pub definition_version: u32,
    pub branch_id: [u8; 16],
    pub branch_lineage_fingerprint: [u8; 16],
    pub cursor: ProjectionCursor,
    pub state_codec: u32,
    pub state_codec_version: u32,
    pub created_at_unix_nanos: i64,
    pub uncompressed_len: u64,
    pub checksum: u32,
    #[serde(default)]
    pub partition: Option<u32>,
    #[serde(default)]
    pub partition_scheme_id: Option<String>,
    #[serde(default)]
    pub partition_scheme_version: Option<u32>,
    #[serde(default)]
    pub partition_count: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotInfo {
    pub id: String,
    pub manifest: SnapshotManifest,
}

pub(crate) struct SnapshotExpectation<'a> {
    pub database_id: [u8; 16],
    pub descriptor: &'a ProjectionDescriptor,
    pub descriptor_fingerprint: [u8; 16],
    pub lineage_fingerprint: [u8; 16],
    pub maximum_cursor: u64,
    pub partition: Option<u32>,
}

pub(crate) fn publish(
    root: &Path,
    manifest: SnapshotManifest,
    state: &[u8],
) -> Result<SnapshotInfo, EngineError> {
    publish_inner(root, manifest, state, PublicationFault::None)
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[cfg_attr(not(test), allow(dead_code))]
enum PublicationFault {
    None,
    AfterCreate,
    AfterWrite,
    AfterSync,
    AfterValidate,
    AfterRename,
    AfterCatalog,
}

fn publish_inner(
    root: &Path,
    manifest: SnapshotManifest,
    state: &[u8],
    fault: PublicationFault,
) -> Result<SnapshotInfo, EngineError> {
    if state.len() > MAX_SNAPSHOT_STATE_BYTES {
        return Err(resource(
            "snapshot state",
            state.len(),
            MAX_SNAPSHOT_STATE_BYTES,
        ));
    }
    let dir = snapshot_dir(root);
    std::fs::create_dir_all(&dir).map_err(io_error)?;
    let id = format!(
        "{}-p{:04}-{:020}-{:016x}.snap",
        hex(&manifest.descriptor_fingerprint),
        manifest.partition.unwrap_or(0),
        manifest.cursor.position,
        manifest.created_at_unix_nanos as u64
    );
    let final_path = dir.join(&id);
    let temporary = dir.join(format!(".{id}.tmp"));
    let encoded = encode(&manifest, state)?;
    {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .map_err(io_error)?;
        inject(fault, PublicationFault::AfterCreate)?;
        file.write_all(&encoded).map_err(io_error)?;
        inject(fault, PublicationFault::AfterWrite)?;
        file.sync_all().map_err(io_error)?;
        inject(fault, PublicationFault::AfterSync)?;
    }
    let (validated, _) = decode_file(&temporary)?;
    if validated != manifest {
        return Err(corrupt("snapshot changed during publication"));
    }
    inject(fault, PublicationFault::AfterValidate)?;
    std::fs::rename(&temporary, &final_path).map_err(io_error)?;
    sync_dir(&dir)?;
    inject(fault, PublicationFault::AfterRename)?;
    update_catalog(&dir)?;
    inject(fault, PublicationFault::AfterCatalog)?;
    retain_two(&dir, manifest.descriptor_fingerprint, manifest.partition)?;
    Ok(SnapshotInfo { id, manifest })
}

fn inject(actual: PublicationFault, at: PublicationFault) -> Result<(), EngineError> {
    if actual == at {
        Err(internal("injected snapshot publication fault"))
    } else {
        Ok(())
    }
}

pub(crate) fn list(root: &Path, fingerprint: [u8; 16]) -> Vec<SnapshotInfo> {
    let mut items = scan(root)
        .into_iter()
        .filter(|item| item.manifest.descriptor_fingerprint == fingerprint)
        .collect::<Vec<_>>();
    items.sort_by_key(|item| {
        std::cmp::Reverse((
            item.manifest.cursor.position,
            item.manifest.created_at_unix_nanos,
        ))
    });
    items
}

pub(crate) fn load_candidates(
    root: &Path,
    expected: &SnapshotExpectation<'_>,
) -> Vec<(SnapshotInfo, Vec<u8>)> {
    list(root, expected.descriptor_fingerprint)
        .into_iter()
        .filter_map(|info| {
            let (manifest, state) = decode_file(&snapshot_dir(root).join(&info.id)).ok()?;
            validates(&manifest, expected).then_some((
                SnapshotInfo {
                    id: info.id,
                    manifest,
                },
                state,
            ))
        })
        .collect()
}

pub(crate) fn verify(root: &Path, id: &str) -> Result<SnapshotInfo, EngineError> {
    validate_id(id)?;
    let (manifest, _) = decode_file(&snapshot_dir(root).join(id))?;
    Ok(SnapshotInfo {
        id: id.to_string(),
        manifest,
    })
}

pub(crate) fn delete(root: &Path, id: &str) -> Result<bool, EngineError> {
    validate_id(id)?;
    let path = snapshot_dir(root).join(id);
    match std::fs::remove_file(path) {
        Ok(()) => {
            update_catalog(&snapshot_dir(root))?;
            Ok(true)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(io_error(error)),
    }
}

pub(crate) fn delete_all(root: &Path) -> Result<(), EngineError> {
    let dir = root.join("derived");
    match std::fs::remove_dir_all(dir) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_error(error)),
    }
}

pub(crate) fn delete_projection(root: &Path, fingerprint: [u8; 16]) -> Result<(), EngineError> {
    let dir = snapshot_dir(root);
    for info in list(root, fingerprint) {
        let _ = std::fs::remove_file(dir.join(info.id));
    }
    update_catalog(&dir)
}

fn validates(manifest: &SnapshotManifest, expected: &SnapshotExpectation<'_>) -> bool {
    matches!(manifest.format_version, 1 | 2)
        && manifest.database_id == expected.database_id
        && manifest.projection_name == expected.descriptor.name
        && manifest.descriptor_fingerprint == expected.descriptor_fingerprint
        && manifest.definition_id == expected.descriptor.definition_id
        && manifest.definition_version == expected.descriptor.definition_version
        && manifest.branch_id == expected.descriptor.scope.branch_id
        && manifest.branch_lineage_fingerprint == expected.lineage_fingerprint
        && manifest.cursor.database_id == expected.database_id
        && manifest.cursor.branch_id == expected.descriptor.scope.branch_id
        && manifest.cursor.descriptor_fingerprint == expected.descriptor_fingerprint
        && manifest.cursor.position <= expected.maximum_cursor
        && manifest.state_codec == expected.descriptor.state_codec
        && manifest.state_codec_version == expected.descriptor.state_codec_version
        && manifest.partition == expected.partition
        && (manifest.partition.is_none()
            || (manifest.partition_scheme_id.as_deref()
                == Some(&expected.descriptor.partition_scheme.scheme_id)
                && manifest.partition_scheme_version
                    == Some(expected.descriptor.partition_scheme.version)
                && manifest.partition_count
                    == Some(expected.descriptor.partition_scheme.partition_count)))
}

fn encode(manifest: &SnapshotManifest, state: &[u8]) -> Result<Vec<u8>, EngineError> {
    let bytes = serde_json::to_vec(manifest).map_err(|error| internal(error.to_string()))?;
    if bytes.len() > MAX_MANIFEST_BYTES {
        return Err(resource(
            "snapshot manifest",
            bytes.len(),
            MAX_MANIFEST_BYTES,
        ));
    }
    let mut out = Vec::with_capacity(16 + bytes.len() + state.len());
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(&crc32c::crc32c(&bytes).to_le_bytes());
    out.extend_from_slice(&bytes);
    out.extend_from_slice(state);
    Ok(out)
}

fn decode_file(path: &Path) -> Result<(SnapshotManifest, Vec<u8>), EngineError> {
    let length = std::fs::metadata(path).map_err(io_error)?.len() as usize;
    if !(16..=16 + MAX_MANIFEST_BYTES + MAX_SNAPSHOT_STATE_BYTES).contains(&length) {
        return Err(corrupt("snapshot length is invalid"));
    }
    let mut file = File::open(path).map_err(io_error)?;
    let mut prefix = [0u8; 16];
    file.read_exact(&mut prefix).map_err(io_error)?;
    if &prefix[..8] != MAGIC {
        return Err(corrupt("snapshot magic is invalid"));
    }
    let manifest_len = u32::from_le_bytes(prefix[8..12].try_into().unwrap()) as usize;
    if manifest_len == 0 || manifest_len > MAX_MANIFEST_BYTES || 16 + manifest_len > length {
        return Err(corrupt("snapshot manifest length is invalid"));
    }
    let mut manifest_bytes = vec![0; manifest_len];
    file.read_exact(&mut manifest_bytes).map_err(io_error)?;
    if crc32c::crc32c(&manifest_bytes) != u32::from_le_bytes(prefix[12..16].try_into().unwrap()) {
        return Err(corrupt("snapshot manifest checksum mismatch"));
    }
    let manifest: SnapshotManifest =
        serde_json::from_slice(&manifest_bytes).map_err(|error| corrupt(error.to_string()))?;
    let state_len = length - 16 - manifest_len;
    if state_len != manifest.uncompressed_len as usize || state_len > MAX_SNAPSHOT_STATE_BYTES {
        return Err(corrupt("snapshot state length mismatch"));
    }
    let mut state = vec![0; state_len];
    file.read_exact(&mut state).map_err(io_error)?;
    if crc32c::crc32c(&state) != manifest.checksum {
        return Err(corrupt("snapshot checksum mismatch"));
    }
    Ok((manifest, state))
}

fn scan(root: &Path) -> Vec<SnapshotInfo> {
    let Ok(entries) = std::fs::read_dir(snapshot_dir(root)) else {
        return Vec::new();
    };
    entries
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let id = entry.file_name().to_string_lossy().into_owned();
            if !id.ends_with(".snap") {
                return None;
            }
            decode_file(&entry.path())
                .ok()
                .map(|(manifest, _)| SnapshotInfo { id, manifest })
        })
        .collect()
}

fn update_catalog(dir: &Path) -> Result<(), EngineError> {
    std::fs::create_dir_all(dir).map_err(io_error)?;
    let entries = std::fs::read_dir(dir)
        .map_err(io_error)?
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let name = entry.file_name().to_string_lossy().into_owned();
            name.ends_with(".snap").then_some(name)
        })
        .collect::<Vec<_>>();
    let bytes = serde_json::to_vec(&entries).map_err(|error| internal(error.to_string()))?;
    let temporary = dir.join("catalog.tmp");
    {
        let mut file = File::create(&temporary).map_err(io_error)?;
        file.write_all(&bytes).map_err(io_error)?;
        file.sync_all().map_err(io_error)?;
    }
    std::fs::rename(temporary, dir.join("catalog.json")).map_err(io_error)?;
    sync_dir(dir)
}

fn retain_two(
    dir: &Path,
    fingerprint: [u8; 16],
    partition: Option<u32>,
) -> Result<(), EngineError> {
    let root = dir
        .parent()
        .and_then(Path::parent)
        .ok_or_else(|| internal("snapshot directory has no database root"))?;
    for old in list(root, fingerprint)
        .into_iter()
        .filter(|item| item.manifest.partition == partition)
        .skip(2)
    {
        let _ = std::fs::remove_file(dir.join(old.id));
    }
    update_catalog(dir)
}

fn snapshot_dir(root: &Path) -> PathBuf {
    root.join("derived").join("snapshots")
}
fn validate_id(id: &str) -> Result<(), EngineError> {
    if id.ends_with(".snap") && !id.contains(['/', '\\']) {
        Ok(())
    } else {
        Err(internal("invalid snapshot id"))
    }
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
fn now_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_nanos() as i64)
        .unwrap_or(0)
}
pub(crate) fn created_now() -> i64 {
    now_nanos()
}
fn corrupt(message: impl Into<String>) -> EngineError {
    EngineError {
        category: ErrorCategory::Corruption,
        code: "snapshot_corrupt",
        message: message.into(),
    }
}
fn internal(message: impl Into<String>) -> EngineError {
    EngineError {
        category: ErrorCategory::Internal,
        code: "snapshot_internal",
        message: message.into(),
    }
}
fn resource(name: &str, actual: usize, maximum: usize) -> EngineError {
    EngineError {
        category: ErrorCategory::ResourceLimit,
        code: "resource_limit",
        message: format!("{name} is {actual}, maximum is {maximum}"),
    }
}
fn io_error(error: std::io::Error) -> EngineError {
    EngineError {
        category: ErrorCategory::Io,
        code: "io",
        message: error.to_string(),
    }
}

#[cfg(unix)]
fn sync_dir(path: &Path) -> Result<(), EngineError> {
    File::open(path)
        .map_err(io_error)?
        .sync_all()
        .map_err(io_error)
}
#[cfg(not(unix))]
fn sync_dir(_path: &Path) -> Result<(), EngineError> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::facade::{ProjectionCursor, ProjectionScope};

    fn manifest(cursor: u64, created: i64, state: &[u8]) -> SnapshotManifest {
        SnapshotManifest {
            format_version: 1,
            database_id: [1; 16],
            projection_name: "p".into(),
            descriptor_fingerprint: [2; 16],
            definition_id: [3; 16],
            definition_version: 1,
            branch_id: [0; 16],
            branch_lineage_fingerprint: [4; 16],
            cursor: ProjectionCursor {
                database_id: [1; 16],
                branch_id: [0; 16],
                position: cursor,
                descriptor_fingerprint: [2; 16],
            },
            state_codec: 1,
            state_codec_version: 1,
            created_at_unix_nanos: created,
            uncompressed_len: state.len() as u64,
            checksum: crc32c::crc32c(state),
            partition: None,
            partition_scheme_id: None,
            partition_scheme_version: None,
            partition_count: None,
        }
    }

    #[test]
    fn every_publication_fault_leaves_a_valid_generation_or_full_replay_path() {
        let faults = [
            PublicationFault::AfterCreate,
            PublicationFault::AfterWrite,
            PublicationFault::AfterSync,
            PublicationFault::AfterValidate,
            PublicationFault::AfterRename,
            PublicationFault::AfterCatalog,
        ];
        for (index, fault) in faults.into_iter().enumerate() {
            let dir = tempfile::tempdir().unwrap();
            let prior_state = b"prior";
            publish(dir.path(), manifest(5, 1, prior_state), prior_state).unwrap();
            let next_state = b"next";
            assert!(publish_inner(
                dir.path(),
                manifest(10, 10 + index as i64, next_state),
                next_state,
                fault
            )
            .is_err());
            let candidates = scan(dir.path());
            assert!(!candidates.is_empty());
            for candidate in candidates {
                decode_file(&snapshot_dir(dir.path()).join(candidate.id)).unwrap();
            }
        }
    }

    #[test]
    fn identity_validation_rejects_every_incompatible_field() {
        let state = b"state";
        let base = manifest(5, 1, state);
        let descriptor = ProjectionDescriptor {
            name: "p".into(),
            definition_id: [3; 16],
            definition_version: 1,
            input_types: Vec::new(),
            state_codec: 1,
            state_codec_version: 1,
            scope: ProjectionScope::default(),
            partition_scheme: Default::default(),
        };
        let expected = SnapshotExpectation {
            database_id: [1; 16],
            descriptor: &descriptor,
            descriptor_fingerprint: [2; 16],
            lineage_fingerprint: [4; 16],
            maximum_cursor: 5,
            partition: None,
        };
        assert!(validates(&base, &expected));
        let mut mutations = Vec::new();
        let mut value = base.clone();
        value.database_id = [9; 16];
        mutations.push(value);
        let mut value = base.clone();
        value.branch_id = [9; 16];
        mutations.push(value);
        let mut value = base.clone();
        value.branch_lineage_fingerprint = [9; 16];
        mutations.push(value);
        let mut value = base.clone();
        value.definition_id = [9; 16];
        mutations.push(value);
        let mut value = base.clone();
        value.descriptor_fingerprint = [9; 16];
        mutations.push(value);
        let mut value = base.clone();
        value.definition_version = 9;
        mutations.push(value);
        let mut value = base.clone();
        value.state_codec = 9;
        mutations.push(value);
        let mut value = base.clone();
        value.state_codec_version = 9;
        mutations.push(value);
        let mut value = base.clone();
        value.cursor.position = 6;
        mutations.push(value);
        let mut value = base.clone();
        value.cursor.database_id = [9; 16];
        mutations.push(value);
        let mut value = base.clone();
        value.cursor.branch_id = [9; 16];
        mutations.push(value);
        let mut value = base.clone();
        value.cursor.descriptor_fingerprint = [9; 16];
        mutations.push(value);
        for mutation in mutations {
            assert!(!validates(&mutation, &expected));
        }
    }

    #[test]
    fn checksum_truncation_and_oversize_are_rejected_before_state_exposure() {
        let dir = tempfile::tempdir().unwrap();
        let state = b"state";
        let info = publish(dir.path(), manifest(5, 1, state), state).unwrap();
        let path = snapshot_dir(dir.path()).join(info.id);
        let mut bytes = std::fs::read(&path).unwrap();
        bytes.pop();
        std::fs::write(&path, &bytes).unwrap();
        assert!(decode_file(&path).is_err());
        assert!(publish(
            dir.path(),
            manifest(6, 2, &[]),
            &vec![0; MAX_SNAPSHOT_STATE_BYTES + 1]
        )
        .is_err());
    }
}
