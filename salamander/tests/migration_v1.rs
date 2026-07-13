use std::fs;

use salamander::agent::{EventBody, KvProjection};
use salamander::format::{decode, FormatLimits};
use salamander::{migrate_v1, AgentDb, Event, Projection, SalamanderError};

fn append_v1_frame(out: &mut Vec<u8>, offset: u64, payload: &[u8]) {
    let offset_bytes = offset.to_le_bytes();
    let crc = crc32c::crc32c_append(crc32c::crc32c(&offset_bytes), payload);
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(&offset_bytes);
    out.extend_from_slice(payload);
}

fn v1_fixture(root: &std::path::Path) -> (Vec<u8>, Vec<u8>) {
    fs::create_dir_all(root.join("log")).unwrap();
    let manifest = serde_json::to_vec_pretty(&serde_json::json!({
        "format_version": 2,
        "payload_format_version": 1,
        "active_segment_base": 0,
        "next_offset": 2
    }))
    .unwrap();
    fs::write(root.join("manifest.json"), &manifest).unwrap();

    let events = [
        Event {
            offset: 0,
            timestamp_ms: 100,
            namespace: "legacy".to_string(),
            body: EventBody::Put {
                key: "answer".into(),
                value: b"41".to_vec(),
            },
        },
        Event {
            offset: 1,
            timestamp_ms: 200,
            namespace: "legacy".to_string(),
            body: EventBody::Put {
                key: "answer".into(),
                value: b"42".to_vec(),
            },
        },
    ];
    let mut segment = Vec::new();
    for event in events {
        append_v1_frame(
            &mut segment,
            event.offset,
            &bincode::serialize(&event).unwrap(),
        );
    }
    fs::write(root.join("log/00000000000000000000.seg"), &segment).unwrap();
    (manifest, segment)
}

#[test]
fn migration_preserves_source_and_replays_typed_bodies() {
    let source = tempfile::tempdir().unwrap();
    let destination = tempfile::tempdir().unwrap();
    let destination_path = destination.path().join("converted");
    let (manifest_before, segment_before) = v1_fixture(source.path());

    let report = migrate_v1(source.path(), &destination_path).unwrap();
    assert_eq!(report.source_records, 2);
    assert_eq!(report.previously_imported, 0);
    assert_eq!(report.newly_imported, 2);
    assert_eq!(report.destination_head, 2);

    assert_eq!(
        fs::read(source.path().join("manifest.json")).unwrap(),
        manifest_before
    );
    assert_eq!(
        fs::read(source.path().join("log/00000000000000000000.seg")).unwrap(),
        segment_before
    );

    let db = AgentDb::open(&destination_path).unwrap();
    let projection: KvProjection = db.projection().unwrap();
    assert_eq!(projection.state().get("answer"), Some(&b"42".to_vec()));
    let mut timestamps = Vec::new();
    db.replay("legacy", 0..db.head(), |event| {
        timestamps.push(event.timestamp_ms)
    })
    .unwrap();
    assert_eq!(timestamps, vec![100, 200]);
}

#[test]
fn repeating_completed_migration_is_idempotent() {
    let source = tempfile::tempdir().unwrap();
    let destination = tempfile::tempdir().unwrap();
    let destination_path = destination.path().join("converted");
    v1_fixture(source.path());

    migrate_v1(source.path(), &destination_path).unwrap();
    let second = migrate_v1(source.path(), &destination_path).unwrap();
    assert_eq!(second.source_records, 2);
    assert_eq!(second.previously_imported, 2);
    assert_eq!(second.newly_imported, 0);
    assert_eq!(second.destination_head, 2);
}

#[test]
fn interrupted_destination_resumes_from_verified_prefix() {
    let source = tempfile::tempdir().unwrap();
    let destination = tempfile::tempdir().unwrap();
    let destination_path = destination.path().join("converted");
    v1_fixture(source.path());
    migrate_v1(source.path(), &destination_path).unwrap();

    let segment_path = destination_path.join("log/00000000000000000000.seg");
    let segment = fs::read(&segment_path).unwrap();
    let (_, first_len) = decode(&segment, FormatLimits::default()).unwrap().unwrap();
    fs::write(&segment_path, &segment[..first_len]).unwrap();

    let complete_path = destination_path.join("migration.complete.json");
    fs::copy(
        &complete_path,
        destination_path.join("migration.in-progress.json"),
    )
    .unwrap();
    fs::remove_file(complete_path).unwrap();

    let resumed = migrate_v1(source.path(), &destination_path).unwrap();
    assert_eq!(resumed.previously_imported, 1);
    assert_eq!(resumed.newly_imported, 1);
    assert_eq!(resumed.destination_head, 2);
    assert!(!destination_path.join("migration.in-progress.json").exists());

    let db = AgentDb::open(destination_path).unwrap();
    let projection: KvProjection = db.projection().unwrap();
    assert_eq!(projection.state().get("answer"), Some(&b"42".to_vec()));
}

#[test]
fn ordinary_open_rejects_incomplete_migration_destination() {
    let destination = tempfile::tempdir().unwrap();
    fs::write(destination.path().join("migration.in-progress.json"), b"{}").unwrap();
    assert!(matches!(
        AgentDb::open(destination.path()),
        Err(SalamanderError::MigrationIncomplete(_))
    ));
}
