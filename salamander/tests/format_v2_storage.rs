use std::fs;

use salamander::format::{decode, FormatLimits, FrameKind};
use salamander::{AgentDb, EventId, EventType, SalamanderError};

#[test]
fn fresh_database_writes_v2_frames_and_manifest_identity() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = AgentDb::open(dir.path()).unwrap();
    db.append(
        "stream-a",
        salamander::agent::EventBody::Decision {
            summary: "v2".into(),
            rationale: "storage boundary".into(),
        },
    )
    .unwrap();
    db.commit().unwrap();
    drop(db);

    let segment = fs::read(dir.path().join("log/00000000000000000000.seg")).unwrap();
    assert_eq!(&segment[..4], b"SDB2");
    let mut kinds = Vec::new();
    let mut cursor = 0;
    while cursor < segment.len() {
        let (record, consumed) = decode(&segment[cursor..], FormatLimits::default())
            .unwrap()
            .unwrap();
        kinds.push(record.kind);
        cursor += consumed;
    }
    assert_eq!(
        kinds,
        vec![
            FrameKind::BatchBegin,
            FrameKind::Event,
            FrameKind::BatchCommit
        ]
    );

    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(dir.path().join("manifest.json")).unwrap()).unwrap();
    assert_eq!(manifest["storage_format_version"], 2);
    let database_id = manifest["database_id"].as_array().unwrap();
    assert_eq!(database_id.len(), 16);
    assert!(database_id.iter().any(|byte| byte.as_u64() != Some(0)));

    let reopened = AgentDb::open(dir.path()).unwrap();
    let mut seen = Vec::new();
    reopened
        .replay("stream-a", 0..reopened.head(), |event| {
            seen.push((event.offset, event.namespace.clone()));
        })
        .unwrap();
    assert_eq!(seen, vec![(0, "stream-a".to_string())]);
}

#[test]
fn writer_rejects_manifest_without_v2_storage_generation() {
    let dir = tempfile::tempdir().unwrap();
    drop(AgentDb::open(dir.path()).unwrap());

    let path = dir.path().join("manifest.json");
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    manifest
        .as_object_mut()
        .unwrap()
        .remove("storage_format_version");
    fs::write(&path, serde_json::to_vec_pretty(&manifest).unwrap()).unwrap();

    assert!(matches!(
        AgentDb::open(dir.path()),
        Err(SalamanderError::UnsupportedStorageFormat {
            found: 1,
            supported: 2
        })
    ));
}

#[test]
fn public_v2_identifiers_are_distinct_types() {
    let event = EventId::from_bytes([7; 16]);
    assert_eq!(event.into_bytes(), [7; 16]);
    assert_eq!(
        EventType::new("example.created").unwrap().as_str(),
        "example.created"
    );
}
