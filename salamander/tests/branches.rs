use salamander::agent::EventBody;
use salamander::{
    AgentDb, AppendRequest, BranchId, BranchName, BranchStatus, Durability, EventType,
    ExpectedRevision, Metadata, NewEvent, SalamanderError, StreamName,
};
use std::fs;

fn put(key: &str) -> NewEvent<EventBody> {
    NewEvent::new(
        EventType::new("test.put").unwrap(),
        EventBody::Put {
            key: key.into(),
            value: key.as_bytes().to_vec(),
        },
    )
}

#[test]
fn archived_branch_is_readable_but_rejects_writes_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let child = {
        let mut db = AgentDb::open(dir.path()).unwrap();
        let child = db
            .fork_branch(
                BranchId::ZERO,
                0,
                BranchName::new("finished").unwrap(),
                Metadata::new(),
            )
            .unwrap();
        db.append_on_branch(
            child.id,
            "s",
            EventBody::Put {
                key: "preserved".into(),
                value: vec![1],
            },
        )
        .unwrap();
        let head = db.head();
        assert_eq!(
            db.archive_branch(child.id).unwrap().status,
            BranchStatus::Archived
        );
        assert_eq!(
            db.archive_branch(child.id).unwrap().status,
            BranchStatus::Archived
        );
        assert_eq!(db.head(), head);
        child
    };

    let mut db = AgentDb::open(dir.path()).unwrap();
    assert_eq!(db.branch(child.id).unwrap().status, BranchStatus::Archived);
    let mut keys = Vec::new();
    db.replay_branch(child.id, "s", 0..db.head(), |event| {
        if let EventBody::Put { key, .. } = &event.body {
            keys.push(key.clone());
        }
    })
    .unwrap();
    assert_eq!(keys, vec!["preserved"]);
    assert!(matches!(
        db.append_on_branch(
            child.id,
            "s",
            EventBody::Delete {
                key: "preserved".into()
            }
        ),
        Err(SalamanderError::BranchArchived(_))
    ));
    assert!(matches!(
        db.archive_branch(BranchId::ZERO),
        Err(SalamanderError::InvalidArgument(_))
    ));
}

#[test]
fn branch_metadata_is_durable_and_does_not_consume_user_position() {
    let dir = tempfile::tempdir().unwrap();
    let child = {
        let mut db = AgentDb::open(dir.path()).unwrap();
        assert_eq!(db.head(), 0);
        let child = db
            .fork_branch(
                BranchId::ZERO,
                0,
                BranchName::new("experiment").unwrap(),
                Metadata::new(),
            )
            .unwrap();
        assert_eq!(db.head(), 0);
        assert_eq!(db.branch_children(BranchId::ZERO), vec![child.clone()]);
        child
    };

    let db = AgentDb::open(dir.path()).unwrap();
    assert_eq!(db.branch(child.id), Some(&child));
    assert_eq!(db.branch_ancestry(child.id).unwrap().len(), 2);
    assert_eq!(db.head(), 0);
}

#[test]
fn fork_requires_a_committed_batch_boundary_and_unique_name() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = AgentDb::open(dir.path()).unwrap();
    db.append_batch(AppendRequest {
        branch: BranchId::ZERO,
        stream: StreamName::new("s").unwrap(),
        expected: ExpectedRevision::NoStream,
        idempotency_key: None,
        events: vec![put("a"), put("b")],
        durability: Durability::Sync,
    })
    .unwrap();

    assert!(matches!(
        db.fork_branch(
            BranchId::ZERO,
            1,
            BranchName::new("middle").unwrap(),
            Metadata::new()
        ),
        Err(SalamanderError::NotBatchBoundary(1))
    ));
    let child = db
        .fork_branch(
            BranchId::ZERO,
            2,
            BranchName::new("after-batch").unwrap(),
            Metadata::new(),
        )
        .unwrap();
    assert!(matches!(
        db.fork_branch(
            BranchId::ZERO,
            2,
            BranchName::new("after-batch").unwrap(),
            Metadata::new()
        ),
        Err(SalamanderError::BranchExists(_))
    ));
    assert_eq!(db.branch_ancestry(child.id).unwrap()[0].id, BranchId::ZERO);
    assert!(matches!(
        db.fork_branch(
            BranchId::from_bytes([9; 16]),
            0,
            BranchName::new("missing-parent").unwrap(),
            Metadata::new()
        ),
        Err(SalamanderError::BranchNotFound(_))
    ));
    assert!(matches!(
        db.fork_branch(
            BranchId::ZERO,
            db.head() + 1,
            BranchName::new("future").unwrap(),
            Metadata::new()
        ),
        Err(SalamanderError::OffsetBeyondHead(_))
    ));
}

#[test]
fn lineage_depth_limit_is_enforced() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = AgentDb::open(dir.path()).unwrap();
    let mut parent = BranchId::ZERO;
    for depth in 1..salamander::MAX_LINEAGE_DEPTH {
        parent = db
            .fork_branch(
                parent,
                0,
                BranchName::new(format!("depth-{depth}")).unwrap(),
                Metadata::new(),
            )
            .unwrap()
            .id;
    }
    assert!(matches!(
        db.fork_branch(
            parent,
            0,
            BranchName::new("too-deep").unwrap(),
            Metadata::new()
        ),
        Err(SalamanderError::InvalidBranchAncestry(_))
    ));
}

#[test]
fn every_torn_branch_system_frame_recovers_as_absent_or_complete() {
    let fixture = tempfile::tempdir().unwrap();
    let segment = fixture.path().join("log/00000000000000000000.seg");
    {
        let mut db = AgentDb::open(fixture.path()).unwrap();
        db.fork_branch(
            BranchId::ZERO,
            0,
            BranchName::new("atomic-branch").unwrap(),
            Metadata::new(),
        )
        .unwrap();
    }
    let bytes = fs::read(&segment).unwrap();
    for cut in 0..=bytes.len() {
        let root = tempfile::tempdir().unwrap();
        let copy = root.path().join("db");
        fs::create_dir_all(copy.join("log")).unwrap();
        fs::copy(
            fixture.path().join("manifest.json"),
            copy.join("manifest.json"),
        )
        .unwrap();
        fs::write(copy.join("log/00000000000000000000.seg"), &bytes[..cut]).unwrap();
        let db = AgentDb::open(&copy).unwrap();
        if let Some(branch) = db.branch_named("atomic-branch") {
            assert_eq!(branch.parent, Some(BranchId::ZERO));
            assert_eq!(branch.fork_position, Some(0));
            assert_eq!(db.branch_ancestry(branch.id).unwrap().len(), 2);
        }
        assert_eq!(db.head(), 0);
    }
}

#[test]
fn chained_branch_ancestry_is_flattened_root_first() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = AgentDb::open(dir.path()).unwrap();
    let one = db
        .fork_branch(
            BranchId::ZERO,
            0,
            BranchName::new("one").unwrap(),
            Metadata::new(),
        )
        .unwrap();
    let two = db
        .fork_branch(one.id, 0, BranchName::new("two").unwrap(), Metadata::new())
        .unwrap();
    let ids: Vec<_> = db
        .branch_ancestry(two.id)
        .unwrap()
        .into_iter()
        .map(|branch| branch.id)
        .collect();
    assert_eq!(ids, vec![BranchId::ZERO, one.id, two.id]);
}

#[test]
fn branch_replay_inherits_prefix_and_isolates_divergent_tails() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = AgentDb::open(dir.path()).unwrap();
    db.append_batch(AppendRequest {
        branch: BranchId::ZERO,
        stream: StreamName::new("s").unwrap(),
        expected: ExpectedRevision::NoStream,
        idempotency_key: None,
        events: vec![put("shared")],
        durability: Durability::Sync,
    })
    .unwrap();
    let child = db
        .fork_branch(
            BranchId::ZERO,
            1,
            BranchName::new("child").unwrap(),
            Metadata::new(),
        )
        .unwrap();
    let sibling = db
        .fork_branch(
            BranchId::ZERO,
            1,
            BranchName::new("sibling").unwrap(),
            Metadata::new(),
        )
        .unwrap();

    db.append_batch(AppendRequest {
        branch: BranchId::ZERO,
        stream: StreamName::new("s").unwrap(),
        expected: ExpectedRevision::Exact(salamander::StreamRevision(0)),
        idempotency_key: None,
        events: vec![put("parent-after")],
        durability: Durability::Buffered,
    })
    .unwrap();
    for (branch, key) in [(child.id, "child-tail"), (sibling.id, "sibling-tail")] {
        db.append_batch(AppendRequest {
            branch,
            stream: StreamName::new("s").unwrap(),
            expected: ExpectedRevision::NoStream,
            idempotency_key: None,
            events: vec![put(key)],
            durability: Durability::Buffered,
        })
        .unwrap();
    }

    let keys = |db: &AgentDb, branch| {
        let mut keys = Vec::new();
        db.replay_branch(branch, "s", 0..db.head(), |event| {
            if let EventBody::Put { key, .. } = &event.body {
                keys.push(key.clone());
            }
        })
        .unwrap();
        keys
    };
    assert_eq!(keys(&db, BranchId::ZERO), vec!["shared", "parent-after"]);
    assert_eq!(keys(&db, child.id), vec!["shared", "child-tail"]);
    assert_eq!(keys(&db, sibling.id), vec!["shared", "sibling-tail"]);
    assert_eq!(
        db.branch_common_ancestor(child.id, sibling.id).unwrap().id,
        BranchId::ZERO
    );
}
