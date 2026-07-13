use salamander::agent::EventBody;
use salamander::{migrate_legacy_branches, AgentDb, BranchId, BranchStatus, Json, JsonDb};

fn agent_marker(parent: &str, at: u64) -> EventBody {
    EventBody::SessionStarted {
        agent_id: "agent".into(),
        config_hash: format!("{}{}@{at}", ["forked", "_from="].concat(), parent),
    }
}

fn json_marker(parent: &str, at: u64) -> Json {
    let key = ["__salamander", "_fork__"].concat();
    Json(serde_json::json!({ (key): { "parent": parent, "at": at } }))
}

#[test]
fn agent_lineage_becomes_canonical_branch_metadata() {
    let source = tempfile::tempdir().unwrap();
    let output_root = tempfile::tempdir().unwrap();
    let destination = output_root.path().join("converted");
    {
        let mut db = AgentDb::open(source.path()).unwrap();
        db.append(
            "session",
            EventBody::Decision {
                summary: "shared".into(),
                rationale: String::new(),
            },
        )
        .unwrap();
        db.append("session-fork-1", agent_marker("session", 1))
            .unwrap();
        db.append(
            "session-fork-1",
            EventBody::Decision {
                summary: "tail".into(),
                rationale: String::new(),
            },
        )
        .unwrap();
        db.commit().unwrap();
    }

    let report = migrate_legacy_branches(source.path(), &destination).unwrap();
    assert_eq!(report.source_records, 3);
    assert_eq!(report.removed_marker_records, 1);
    assert_eq!(report.branches_created, 1);
    assert_eq!(report.destination_head, 2);

    let db = AgentDb::open(&destination).unwrap();
    let branch = db.branch_named("session-fork-1").unwrap();
    assert_eq!(branch.parent, Some(BranchId::ZERO));
    assert_eq!(branch.fork_position, Some(1));
    assert_eq!(branch.status, BranchStatus::Active);
    let mut summaries = Vec::new();
    db.replay_branch(branch.id, "session", 0..db.head(), |event| {
        if let EventBody::Decision { summary, .. } = &event.body {
            summaries.push(summary.clone());
        }
    })
    .unwrap();
    assert_eq!(summaries, vec!["shared", "tail"]);
}

#[test]
fn json_lineage_maps_to_the_same_branch_shape_and_drops_marker_payload() {
    let source = tempfile::tempdir().unwrap();
    let output_root = tempfile::tempdir().unwrap();
    let destination = output_root.path().join("converted");
    {
        let mut db = JsonDb::open(source.path()).unwrap();
        db.append("session", Json(serde_json::json!({"message": "shared"})))
            .unwrap();
        db.append("session-fork-1", json_marker("session", 1))
            .unwrap();
        db.append(
            "session-fork-1",
            Json(serde_json::json!({"message": "tail"})),
        )
        .unwrap();
        db.commit().unwrap();
    }

    let report = migrate_legacy_branches(source.path(), &destination).unwrap();
    assert_eq!(report.removed_marker_records, 1);
    let db = JsonDb::open(&destination).unwrap();
    let branch = db.branch_named("session-fork-1").unwrap();
    assert_eq!(branch.parent, Some(BranchId::ZERO));
    assert_eq!(branch.fork_position, Some(1));
    let mut child = Vec::new();
    db.replay_branch(branch.id, "session", 0..db.head(), |event| {
        child.push(event.body.0.clone())
    })
    .unwrap();
    assert_eq!(
        child,
        vec![
            serde_json::json!({"message": "shared"}),
            serde_json::json!({"message": "tail"})
        ]
    );
}

#[test]
fn migration_rejects_in_place_and_existing_destinations() {
    let source = tempfile::tempdir().unwrap();
    {
        let mut db = JsonDb::open(source.path()).unwrap();
        db.append("s", Json(serde_json::json!({"value": 1})))
            .unwrap();
        db.commit().unwrap();
    }
    assert!(migrate_legacy_branches(source.path(), source.path()).is_err());
    let existing = tempfile::tempdir().unwrap();
    assert!(migrate_legacy_branches(source.path(), existing.path()).is_err());
}

#[test]
fn chained_legacy_namespaces_flatten_to_one_canonical_stream() {
    let source = tempfile::tempdir().unwrap();
    let output_root = tempfile::tempdir().unwrap();
    let destination = output_root.path().join("converted");
    {
        let mut db = JsonDb::open(source.path()).unwrap();
        db.append("root", Json(serde_json::json!({"n": 0})))
            .unwrap();
        db.append("root-fork-1", json_marker("root", 1)).unwrap();
        db.append("root-fork-1", Json(serde_json::json!({"n": 1})))
            .unwrap();
        db.append("root-fork-1-fork-3", json_marker("root-fork-1", 3))
            .unwrap();
        db.append("root-fork-1-fork-3", Json(serde_json::json!({"n": 2})))
            .unwrap();
        db.commit().unwrap();
    }

    migrate_legacy_branches(source.path(), &destination).unwrap();
    let db = JsonDb::open(destination).unwrap();
    let leaf = db.branch_named("root-fork-1-fork-3").unwrap();
    assert_eq!(db.branch_ancestry(leaf.id).unwrap().len(), 3);
    let mut values = Vec::new();
    db.replay_branch(leaf.id, "root", 0..db.head(), |event| {
        values.push(event.body.0["n"].as_u64().unwrap())
    })
    .unwrap();
    assert_eq!(values, vec![0, 1, 2]);
}
