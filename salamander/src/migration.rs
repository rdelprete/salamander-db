//! Read-only v0.1 to v2 offline migration.

use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::format::{
    derive_import_id, derive_stream_id, BatchId, BranchId, CodecId, EventId, EventType, Metadata,
    RecordEnvelopeV2, StreamRevision,
};
use crate::log::{Log, MIGRATION_IN_PROGRESS};
use crate::{BranchInfo, BranchName, BranchStatus, OwnedStoredRecord, Result, SalamanderError};

const COMPLETE_FILE: &str = "migration.complete.json";
const LEGACY_HEADER_LEN: usize = 16;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationReport {
    pub source_records: u64,
    pub previously_imported: u64,
    pub newly_imported: u64,
    pub destination_head: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchMigrationReport {
    pub source_records: u64,
    pub migrated_records: u64,
    pub removed_marker_records: u64,
    pub branches_created: u64,
    pub destination_head: u64,
}

#[derive(Debug)]
struct LegacyFork {
    child_stream: String,
    parent_stream: String,
    source_fork_position: u64,
    marker_position: u64,
    created_at_unix_nanos: i64,
    id: BranchId,
}

/// Rewrite a pre-WP-03 v2 database into a new database whose fork lineage is
/// represented exclusively by engine-owned branch metadata.
pub fn migrate_legacy_branches(
    source: impl AsRef<Path>,
    destination: impl AsRef<Path>,
) -> Result<BranchMigrationReport> {
    let source = source.as_ref();
    let destination = destination.as_ref();
    if source == destination {
        return Err(SalamanderError::Migration(
            "branch migration requires a different destination directory".into(),
        ));
    }
    if destination.exists() {
        return Err(SalamanderError::Migration(
            "branch migration destination must not already exist".into(),
        ));
    }

    let source_log = Log::open(source)?;
    if source_log.system_records().next().is_some() {
        return Err(SalamanderError::Migration(
            "source already contains engine system metadata".into(),
        ));
    }
    let source_database_id = source_log.database_id();
    let records = source_log.records_from(0).collect::<Result<Vec<_>>>()?;
    let forks = discover_legacy_forks(&records, source_database_id)?;
    validate_legacy_forks(&forks, &records)?;
    let marker_positions: HashSet<_> = forks.iter().map(|fork| fork.marker_position).collect();

    std::fs::create_dir_all(destination)?;
    write_json_atomic(
        &destination.join(MIGRATION_IN_PROGRESS),
        &serde_json::json!({ "version": 1, "kind": "legacy-branches", "source": source.display().to_string() }),
    )?;
    let mut destination_log = Log::open_for_migration(destination)?;
    let destination_database_id = destination_log.database_id();

    let mut branch_by_stream = HashMap::new();
    let mut canonical_stream_by_legacy_stream = HashMap::new();
    for fork in &forks {
        let parent = branch_by_stream
            .get(&fork.parent_stream)
            .copied()
            .unwrap_or(BranchId::ZERO);
        let fork_position = destination_position(fork.source_fork_position, &marker_positions);
        let canonical_stream = canonical_stream_by_legacy_stream
            .get(&fork.parent_stream)
            .cloned()
            .unwrap_or_else(|| fork.parent_stream.clone());
        let mut metadata = Metadata::new();
        metadata.insert(
            "salamander.legacy_stream".into(),
            fork.child_stream.as_bytes().to_vec(),
        );
        metadata.insert(
            "session_stream".into(),
            canonical_stream.as_bytes().to_vec(),
        );
        let info = BranchInfo {
            id: fork.id,
            name: BranchName::new(fork.child_stream.clone())?,
            parent: Some(parent),
            fork_position: Some(fork_position),
            created_at_unix_nanos: fork.created_at_unix_nanos,
            metadata,
            status: BranchStatus::Active,
        };
        append_branch_creation(&mut destination_log, destination_database_id, &info)?;
        branch_by_stream.insert(fork.child_stream.clone(), fork.id);
        canonical_stream_by_legacy_stream.insert(fork.child_stream.clone(), canonical_stream);
    }

    let mut revisions: HashMap<(BranchId, String), u64> = HashMap::new();
    for record in &records {
        if marker_positions.contains(&record.position) {
            continue;
        }
        let legacy_stream = record_stream(record)?;
        let branch = branch_by_stream
            .get(&legacy_stream)
            .copied()
            .unwrap_or(BranchId::ZERO);
        let stream = canonical_stream_by_legacy_stream
            .get(&legacy_stream)
            .cloned()
            .unwrap_or(legacy_stream);
        let revision = revisions.entry((branch, stream.clone())).or_insert(0);
        let mut envelope = record.envelope.clone();
        envelope.database_id = destination_database_id;
        envelope.branch_id = branch;
        envelope.stream_id = derive_stream_id(destination_database_id, branch, &stream);
        envelope.stream_revision = StreamRevision(*revision);
        envelope
            .metadata
            .insert("salamander.stream_name".into(), stream.as_bytes().to_vec());
        envelope.batch_id = BatchId::from_bytes(envelope.event_id.into_bytes());
        envelope.batch_index = 0;
        destination_log.append_batch(&[(envelope, record.payload.clone())])?;
        *revision += 1;
    }
    destination_log.commit()?;
    let destination_head = destination_log.head();
    drop(destination_log);
    drop(source_log);

    let report = BranchMigrationReport {
        source_records: records.len() as u64,
        migrated_records: destination_head,
        removed_marker_records: marker_positions.len() as u64,
        branches_created: forks.len() as u64,
        destination_head,
    };
    write_json_atomic(
        &destination.join("branch-migration.complete.json"),
        &serde_json::json!({
            "version": 1,
            "source_records": report.source_records,
            "migrated_records": report.migrated_records,
            "removed_marker_records": report.removed_marker_records,
            "branches_created": report.branches_created,
        }),
    )?;
    std::fs::remove_file(destination.join(MIGRATION_IN_PROGRESS))?;
    sync_dir(destination)?;
    Ok(report)
}

fn discover_legacy_forks(
    records: &[OwnedStoredRecord],
    source_database_id: crate::DatabaseId,
) -> Result<Vec<LegacyFork>> {
    let mut forks = Vec::new();
    for record in records {
        let stream = record_stream(record)?;
        let marker =
            legacy_agent_marker(&record.payload).or_else(|| legacy_json_marker(&record.payload));
        if let Some((parent_stream, source_fork_position)) = marker {
            forks.push(LegacyFork {
                child_stream: stream,
                parent_stream,
                source_fork_position,
                marker_position: record.position,
                created_at_unix_nanos: record.envelope.timestamp_unix_nanos,
                id: BranchId::from_bytes(derive_import_id(
                    source_database_id.into_bytes(),
                    record.position,
                )),
            });
        }
    }
    forks.sort_by_key(|fork| fork.marker_position);
    Ok(forks)
}

fn legacy_agent_marker(payload: &[u8]) -> Option<(String, u64)> {
    let body: crate::agent::EventBody = bincode::deserialize(payload).ok()?;
    let crate::agent::EventBody::SessionStarted { config_hash, .. } = body else {
        return None;
    };
    let prefix = ["forked", "_from="].concat();
    let rest = config_hash.strip_prefix(&prefix)?;
    let (parent, position) = rest.rsplit_once('@')?;
    Some((parent.to_string(), position.parse().ok()?))
}

fn legacy_json_marker(payload: &[u8]) -> Option<(String, u64)> {
    let body = bincode::deserialize::<crate::Json>(payload)
        .map(|json| json.0)
        .or_else(|_| serde_json::from_slice::<serde_json::Value>(payload))
        .ok()?;
    let key = ["__salamander", "_fork__"].concat();
    let marker = body.get(&key)?;
    Some((
        marker.get("parent")?.as_str()?.to_string(),
        marker.get("at")?.as_u64()?,
    ))
}

fn validate_legacy_forks(forks: &[LegacyFork], records: &[OwnedStoredRecord]) -> Result<()> {
    let mut children = HashSet::new();
    let mut known_branches = HashSet::new();
    let all_children: HashSet<_> = forks
        .iter()
        .map(|fork| fork.child_stream.as_str())
        .collect();
    for fork in forks {
        if !children.insert(fork.child_stream.clone()) {
            return Err(SalamanderError::Migration(format!(
                "multiple legacy fork markers target stream {}",
                fork.child_stream
            )));
        }
        if fork.source_fork_position > records.len() as u64
            || fork.source_fork_position > fork.marker_position
        {
            return Err(SalamanderError::Migration(format!(
                "invalid fork position {} for marker at {}",
                fork.source_fork_position, fork.marker_position
            )));
        }
        if all_children.contains(fork.parent_stream.as_str())
            && !known_branches.contains(&fork.parent_stream)
        {
            return Err(SalamanderError::Migration(
                "legacy branch ancestry is not topological".into(),
            ));
        }
        if fork.source_fork_position > 0 && fork.source_fork_position < records.len() as u64 {
            let left = &records[fork.source_fork_position as usize - 1];
            let right = &records[fork.source_fork_position as usize];
            if left.envelope.batch_id == right.envelope.batch_id {
                return Err(SalamanderError::Migration(format!(
                    "legacy fork position {} splits a batch",
                    fork.source_fork_position
                )));
            }
        }
        known_branches.insert(fork.child_stream.clone());
    }
    Ok(())
}

fn destination_position(source_position: u64, markers: &HashSet<u64>) -> u64 {
    source_position
        - markers
            .iter()
            .filter(|position| **position < source_position)
            .count() as u64
}

fn record_stream(record: &OwnedStoredRecord) -> Result<String> {
    let bytes = record
        .envelope
        .metadata
        .get("salamander.stream_name")
        .or_else(|| record.envelope.metadata.get("salamander.v1_namespace"))
        .ok_or_else(|| {
            SalamanderError::Migration(format!("record {} has no stream name", record.position))
        })?;
    std::str::from_utf8(bytes).map(str::to_owned).map_err(|_| {
        SalamanderError::Migration(format!(
            "record {} stream name is not UTF-8",
            record.position
        ))
    })
}

fn append_branch_creation(
    log: &mut Log,
    database_id: crate::DatabaseId,
    info: &BranchInfo,
) -> Result<()> {
    let payload = serde_json::to_vec(info)
        .map_err(|error| SalamanderError::Serialization(error.to_string()))?;
    let event_bytes = derive_import_id(info.id.into_bytes(), 0);
    let envelope = RecordEnvelopeV2 {
        event_id: EventId::from_bytes(event_bytes),
        database_id,
        branch_id: info.id,
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
    log.append_system(&envelope, &payload)
}

#[derive(Debug, Serialize, Deserialize)]
struct MigrationMarker {
    version: u32,
    source_identity_hex: String,
    source_path: String,
}

#[derive(Debug, Deserialize)]
struct LegacyManifest {
    active_segment_base: u64,
    #[serde(default = "legacy_storage_version")]
    storage_format_version: u32,
}

fn legacy_storage_version() -> u32 {
    1
}

struct LegacyEvent {
    offset: u64,
    timestamp_ms: u64,
    namespace: String,
    body: Vec<u8>,
}

pub fn migrate_v1(
    source: impl AsRef<Path>,
    destination: impl AsRef<Path>,
) -> Result<MigrationReport> {
    let source = source.as_ref();
    let destination = destination.as_ref();
    if source == destination {
        return Err(SalamanderError::Migration(
            "source and destination must be different directories".into(),
        ));
    }
    if source.join("LOCK").exists() {
        return Err(SalamanderError::Migration(
            "v0.1 source has a LOCK file; migration requires an offline source".into(),
        ));
    }

    let manifest_bytes = std::fs::read(source.join("manifest.json"))?;
    let legacy_manifest: LegacyManifest = serde_json::from_slice(&manifest_bytes)
        .map_err(|error| SalamanderError::Migration(format!("invalid v0.1 manifest: {error}")))?;
    if legacy_manifest.storage_format_version != 1 {
        return Err(SalamanderError::Migration(format!(
            "source storage format is {}, expected v0.1 generation 1",
            legacy_manifest.storage_format_version
        )));
    }

    let segment_paths = discover_legacy_segments(source)?;
    let source_identity = fingerprint_source(&manifest_bytes, &segment_paths)?;
    let events = read_legacy_events(&segment_paths, legacy_manifest.active_segment_base)?;

    if let Some(report) = completed_report(destination, source_identity, &events)? {
        return Ok(report);
    }

    prepare_destination(source, destination, source_identity)?;
    let mut log = Log::open_for_migration(destination)?;
    let previously_imported = log.head();
    if previously_imported > events.len() as u64 {
        return Err(SalamanderError::Migration(
            "destination contains more records than the source".into(),
        ));
    }
    verify_imported_prefix(&log, &events, source_identity)?;

    let database_id = log.database_id();
    for event in events.iter().skip(previously_imported as usize) {
        let id = derive_import_id(source_identity, event.offset);
        let mut metadata = Metadata::new();
        metadata.insert(
            "salamander.stream_name".into(),
            event.namespace.as_bytes().to_vec(),
        );
        metadata.insert(
            "salamander.v1_namespace".into(),
            event.namespace.as_bytes().to_vec(),
        );
        metadata.insert(
            "salamander.v1_offset".into(),
            event.offset.to_le_bytes().to_vec(),
        );
        let envelope = RecordEnvelopeV2 {
            event_id: EventId::from_bytes(id),
            database_id,
            branch_id: BranchId::ZERO,
            stream_id: derive_stream_id(database_id, BranchId::ZERO, &event.namespace),
            stream_revision: StreamRevision(event.offset),
            timestamp_unix_nanos: (event.timestamp_ms as i64).saturating_mul(1_000_000),
            event_type: EventType::new("salamander.v1-import")?,
            schema_version: 1,
            codec: CodecId::RUST_BINCODE_V1,
            batch_id: BatchId::from_bytes(id),
            batch_index: 0,
            metadata,
        };
        let position = log.append_enveloped(&envelope, &event.body)?;
        if position != event.offset {
            return Err(SalamanderError::Migration(format!(
                "destination position {position} differs from v0.1 offset {}",
                event.offset
            )));
        }
    }
    log.commit()?;
    verify_imported_prefix(&log, &events, source_identity)?;
    let destination_head = log.head();
    drop(log);

    let complete = MigrationMarker {
        version: 1,
        source_identity_hex: hex(&source_identity),
        source_path: source.display().to_string(),
    };
    write_json_atomic(&destination.join(COMPLETE_FILE), &complete)?;
    std::fs::remove_file(destination.join(MIGRATION_IN_PROGRESS))?;
    sync_dir(destination)?;

    Ok(MigrationReport {
        source_records: events.len() as u64,
        previously_imported,
        newly_imported: events.len() as u64 - previously_imported,
        destination_head,
    })
}

fn completed_report(
    destination: &Path,
    identity: [u8; 16],
    events: &[LegacyEvent],
) -> Result<Option<MigrationReport>> {
    let complete_path = destination.join(COMPLETE_FILE);
    if !complete_path.exists() {
        return Ok(None);
    }
    let marker: MigrationMarker =
        serde_json::from_slice(&std::fs::read(complete_path)?).map_err(|error| {
            SalamanderError::Migration(format!("invalid completion marker: {error}"))
        })?;
    if marker.source_identity_hex != hex(&identity) {
        return Err(SalamanderError::Migration(
            "completed destination belongs to a different v0.1 source".into(),
        ));
    }
    let log = Log::open(destination)?;
    verify_imported_prefix(&log, events, identity)?;
    let head = log.head();
    Ok(Some(MigrationReport {
        source_records: events.len() as u64,
        previously_imported: head,
        newly_imported: 0,
        destination_head: head,
    }))
}

fn prepare_destination(source: &Path, destination: &Path, identity: [u8; 16]) -> Result<()> {
    std::fs::create_dir_all(destination)?;
    let marker_path = destination.join(MIGRATION_IN_PROGRESS);
    let expected_hex = hex(&identity);
    if marker_path.exists() {
        let marker: MigrationMarker = serde_json::from_slice(&std::fs::read(&marker_path)?)
            .map_err(|error| {
                SalamanderError::Migration(format!("invalid migration marker: {error}"))
            })?;
        if marker.source_identity_hex != expected_hex {
            return Err(SalamanderError::Migration(
                "destination belongs to a different v0.1 source".into(),
            ));
        }
        return Ok(());
    }
    if destination.join("manifest.json").exists() || destination.join(COMPLETE_FILE).exists() {
        return Err(SalamanderError::Migration(
            "destination is already a database or completed migration".into(),
        ));
    }
    let marker = MigrationMarker {
        version: 1,
        source_identity_hex: expected_hex,
        source_path: source.display().to_string(),
    };
    write_json_atomic(&marker_path, &marker)
}

fn verify_imported_prefix(log: &Log, source: &[LegacyEvent], identity: [u8; 16]) -> Result<()> {
    let mut seen = 0usize;
    for item in log.records_from(0) {
        let record = item?;
        let expected = source.get(seen).ok_or_else(|| {
            SalamanderError::Migration("destination prefix exceeds source".into())
        })?;
        if record.position != expected.offset
            || record.envelope.event_id.into_bytes() != derive_import_id(identity, expected.offset)
            || record.payload != expected.body
            || record
                .envelope
                .metadata
                .get("salamander.v1_offset")
                .map(Vec::as_slice)
                != Some(expected.offset.to_le_bytes().as_slice())
        {
            return Err(SalamanderError::Migration(format!(
                "destination prefix differs at offset {}",
                expected.offset
            )));
        }
        seen += 1;
    }
    if seen != log.head() as usize {
        return Err(SalamanderError::Migration(
            "destination head does not match verified prefix".into(),
        ));
    }
    Ok(())
}

fn discover_legacy_segments(source: &Path) -> Result<Vec<(u64, PathBuf)>> {
    let mut paths = Vec::new();
    for entry in std::fs::read_dir(source.join("log"))? {
        let path = entry?.path();
        if path.extension().and_then(|value| value.to_str()) != Some("seg") {
            continue;
        }
        let base = path
            .file_stem()
            .and_then(|value| value.to_str())
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or_else(|| {
                SalamanderError::Migration(format!("invalid segment name: {}", path.display()))
            })?;
        paths.push((base, path));
    }
    paths.sort_by_key(|(base, _)| *base);
    if paths.is_empty() {
        return Err(SalamanderError::Migration(
            "v0.1 source has no segments".into(),
        ));
    }
    Ok(paths)
}

fn read_legacy_events(paths: &[(u64, PathBuf)], active_base: u64) -> Result<Vec<LegacyEvent>> {
    let mut events = Vec::new();
    let mut expected_offset = 0u64;
    for (base, path) in paths {
        if *base != expected_offset {
            return Err(SalamanderError::Migration(format!(
                "v0.1 segment {} begins at {base}, expected {expected_offset}",
                path.display()
            )));
        }
        let bytes = std::fs::read(path)?;
        let mut cursor = 0usize;
        while cursor < bytes.len() {
            match decode_legacy_record(&bytes[cursor..])? {
                Some((offset, payload, consumed)) => {
                    if offset != expected_offset {
                        return Err(SalamanderError::Migration(format!(
                            "v0.1 offset {offset} is not expected {expected_offset}"
                        )));
                    }
                    events.push(split_legacy_event(offset, payload)?);
                    expected_offset += 1;
                    cursor += consumed;
                }
                None if *base == active_base => break,
                None => {
                    return Err(SalamanderError::Migration(format!(
                        "closed v0.1 segment {} has a torn tail",
                        path.display()
                    )))
                }
            }
        }
    }
    Ok(events)
}

fn decode_legacy_record(buf: &[u8]) -> Result<Option<(u64, &[u8], usize)>> {
    if buf.len() < LEGACY_HEADER_LEN {
        return Ok(None);
    }
    let payload_len = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
    let stored_crc = u32::from_le_bytes(buf[4..8].try_into().unwrap());
    let offset_bytes = &buf[8..16];
    let offset = u64::from_le_bytes(offset_bytes.try_into().unwrap());
    let total = LEGACY_HEADER_LEN
        .checked_add(payload_len)
        .ok_or_else(|| SalamanderError::Migration("v0.1 record length overflow".into()))?;
    if buf.len() < total {
        return Ok(None);
    }
    let payload = &buf[LEGACY_HEADER_LEN..total];
    let computed = crc32c::crc32c_append(crc32c::crc32c(offset_bytes), payload);
    if computed != stored_crc {
        return Err(SalamanderError::Corrupt {
            offset,
            reason: "v0.1 CRC mismatch during migration".into(),
        });
    }
    Ok(Some((offset, payload, total)))
}

fn split_legacy_event(offset: u64, payload: &[u8]) -> Result<LegacyEvent> {
    if payload.len() < 24 {
        return Err(SalamanderError::Migration(format!(
            "v0.1 event at {offset} is shorter than its fixed envelope"
        )));
    }
    let timestamp_ms = u64::from_le_bytes(payload[8..16].try_into().unwrap());
    let namespace_len = u64::from_le_bytes(payload[16..24].try_into().unwrap());
    let namespace_len = usize::try_from(namespace_len)
        .map_err(|_| SalamanderError::Migration("v0.1 namespace length overflow".into()))?;
    let body_start = 24usize
        .checked_add(namespace_len)
        .ok_or_else(|| SalamanderError::Migration("v0.1 namespace length overflow".into()))?;
    if body_start > payload.len() {
        return Err(SalamanderError::Migration(format!(
            "v0.1 namespace at {offset} is truncated"
        )));
    }
    let namespace = std::str::from_utf8(&payload[24..body_start])
        .map_err(|_| {
            SalamanderError::Migration(format!("v0.1 namespace at {offset} is not UTF-8"))
        })?
        .to_string();
    Ok(LegacyEvent {
        offset,
        timestamp_ms,
        namespace,
        body: payload[body_start..].to_vec(),
    })
}

fn fingerprint_source(manifest: &[u8], paths: &[(u64, PathBuf)]) -> Result<[u8; 16]> {
    let mut state = [0xcbf2_9ce4_8422_2325u64, 0x8422_2325_cbf2_9ce4u64];
    hash_bytes(&mut state, manifest);
    for (_, path) in paths {
        let mut file = File::open(path)?;
        let mut buffer = [0u8; 64 * 1024];
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            hash_bytes(&mut state, &buffer[..read]);
        }
    }
    let mut identity = [0; 16];
    identity[..8].copy_from_slice(&state[0].to_le_bytes());
    identity[8..].copy_from_slice(&state[1].to_le_bytes());
    Ok(identity)
}

fn hash_bytes(state: &mut [u64; 2], bytes: &[u8]) {
    for byte in bytes {
        state[0] ^= u64::from(*byte);
        state[0] = state[0].wrapping_mul(0x0000_0100_0000_01b3);
        state[1] ^= u64::from(*byte).rotate_left(1);
        state[1] = state[1].wrapping_mul(0x9e37_79b1_85eb_ca87);
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn write_json_atomic(path: &Path, value: &impl Serialize) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| SalamanderError::Migration("marker has no parent".into()))?;
    let tmp = path.with_extension("tmp");
    let bytes = serde_json::to_vec_pretty(value)
        .map_err(|error| SalamanderError::Migration(error.to_string()))?;
    {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
    }
    std::fs::rename(tmp, path)?;
    sync_dir(parent)
}

#[cfg(unix)]
fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_dir(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_import_ids_change_with_offset() {
        let source = [7; 16];
        assert_eq!(derive_import_id(source, 3), derive_import_id(source, 3));
        assert_ne!(derive_import_id(source, 3), derive_import_id(source, 4));
    }

    #[test]
    fn legacy_event_split_preserves_body_bytes() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&99u64.to_le_bytes());
        payload.extend_from_slice(&1234u64.to_le_bytes());
        payload.extend_from_slice(&2u64.to_le_bytes());
        payload.extend_from_slice(b"ns");
        payload.extend_from_slice(&[9, 8, 7]);
        let event = split_legacy_event(5, &payload).unwrap();
        assert_eq!(event.offset, 5);
        assert_eq!(event.timestamp_ms, 1234);
        assert_eq!(event.namespace, "ns");
        assert_eq!(event.body, vec![9, 8, 7]);
    }
}
