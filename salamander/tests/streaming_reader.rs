//! WP-04 — public streaming-reader behavior: `Salamander::read` with
//! `ReplayPlan` selection, branch-aware visibility, paging via
//! continuation, and partition-class selectors. Crate-internal reader
//! mechanics (segment seek, sidecar skip proofs, buffer bounds, digest
//! verification) are covered by unit tests in `log/`.

use std::ops::Bound;

use salamander::agent::EventBody;
use salamander::{
    AgentDb, AppendRequest, BranchId, BranchName, Durability, EventType, ExpectedRevision,
    Metadata, NewEvent, RecordReader, ReplayEnd, ReplayPlan, SalamanderError, StreamName,
    StreamSelector,
};

fn put(key: &str) -> NewEvent<EventBody> {
    NewEvent::new(
        EventType::new("test.put").unwrap(),
        EventBody::Put {
            key: key.into(),
            value: b"v".to_vec(),
        },
    )
}

fn append(db: &mut AgentDb, stream: &str, keys: &[&str]) -> salamander::AppendReceipt {
    db.append_batch(AppendRequest {
        branch: BranchId::ZERO,
        stream: StreamName::new(stream).unwrap(),
        expected: ExpectedRevision::Any,
        idempotency_key: None,
        events: keys.iter().map(|k| put(k)).collect(),
        durability: Durability::Sync,
    })
    .unwrap()
}

fn positions(db: &AgentDb, plan: ReplayPlan) -> salamander::Result<Vec<u64>> {
    let mut reader = db.read(plan)?;
    let mut out = Vec::new();
    while let Some(record) = reader.next()? {
        out.push(record.position);
    }
    Ok(out)
}

#[test]
fn stream_selector_uses_receipt_stream_ids() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = AgentDb::open(dir.path()).unwrap();
    let orders = append(&mut db, "orders", &["a", "b"]);
    append(&mut db, "audit", &["x"]);
    append(&mut db, "orders", &["c"]);

    let got = positions(
        &db,
        ReplayPlan {
            streams: StreamSelector::Streams(vec![orders.stream_id]),
            ..ReplayPlan::default()
        },
    )
    .unwrap();
    assert_eq!(got, vec![0, 1, 3]);
}

#[test]
fn position_window_and_until_beyond_head_are_enforced() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = AgentDb::open(dir.path()).unwrap();
    append(&mut db, "s", &["a", "b", "c", "d"]);

    let got = positions(
        &db,
        ReplayPlan {
            from: Bound::Included(1),
            until: ReplayEnd::At(3),
            ..ReplayPlan::default()
        },
    )
    .unwrap();
    assert_eq!(got, vec![1, 2]);

    let got = positions(
        &db,
        ReplayPlan {
            from: Bound::Excluded(1),
            ..ReplayPlan::default()
        },
    )
    .unwrap();
    assert_eq!(got, vec![2, 3]);

    let error = positions(
        &db,
        ReplayPlan {
            until: ReplayEnd::At(99),
            ..ReplayPlan::default()
        },
    )
    .unwrap_err();
    assert!(matches!(error, SalamanderError::OffsetBeyondHead(99)));

    // A from beyond head is an empty read, not an error.
    let got = positions(
        &db,
        ReplayPlan {
            from: Bound::Included(99),
            ..ReplayPlan::default()
        },
    )
    .unwrap();
    assert!(got.is_empty());
}

#[test]
fn continuation_paging_is_gapless_and_duplicate_free() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = AgentDb::open(dir.path()).unwrap();
    for i in 0..17 {
        append(&mut db, "s", &[&format!("k{i}")]);
    }

    let mut collected = Vec::new();
    let mut from = Bound::Included(0);
    loop {
        let mut reader = db
            .read(ReplayPlan {
                from,
                max_events: Some(5),
                ..ReplayPlan::default()
            })
            .unwrap();
        let before = collected.len();
        while let Some(record) = reader.next().unwrap() {
            collected.push(record.position);
        }
        if collected.len() == before {
            break;
        }
        from = Bound::Included(reader.continuation());
    }
    assert_eq!(collected, (0..17).collect::<Vec<u64>>());
}

#[test]
fn branch_plan_sees_parent_history_only_through_the_fork_point() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = AgentDb::open(dir.path()).unwrap();
    append(&mut db, "s", &["p0", "p1"]);
    let fork = db
        .fork_branch(
            BranchId::ZERO,
            2,
            BranchName::new("alt").unwrap(),
            Metadata::new(),
        )
        .unwrap();
    append(&mut db, "s", &["parent-after-fork"]);
    db.append_on_branch(
        fork.id,
        "s",
        EventBody::Put {
            key: "child".into(),
            value: b"v".to_vec(),
        },
    )
    .unwrap();
    db.commit().unwrap();

    let child = positions(
        &db,
        ReplayPlan {
            branch: fork.id,
            ..ReplayPlan::default()
        },
    )
    .unwrap();
    // Parent events before the fork (0, 1) plus the child's own (3);
    // the parent's post-fork event (2) is invisible to the child.
    assert_eq!(child, vec![0, 1, 3]);

    let parent = positions(&db, ReplayPlan::default()).unwrap();
    assert_eq!(parent, vec![0, 1, 2]);
}

#[test]
fn partition_classes_cover_every_stream_exactly_once() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = AgentDb::open(dir.path()).unwrap();
    for i in 0..8 {
        append(&mut db, &format!("stream-{i}"), &["k"]);
    }
    let all = positions(&db, ReplayPlan::default()).unwrap();

    const COUNT: u32 = 4;
    let mut union = Vec::new();
    for index in 0..COUNT {
        let part = positions(
            &db,
            ReplayPlan {
                streams: StreamSelector::PartitionClass {
                    count: COUNT,
                    index,
                },
                ..ReplayPlan::default()
            },
        )
        .unwrap();
        union.extend(part);
    }
    union.sort_unstable();
    assert_eq!(union, all, "partition classes must tile the log");

    let error = db
        .read(ReplayPlan {
            streams: StreamSelector::PartitionClass { count: 0, index: 0 },
            ..ReplayPlan::default()
        })
        .unwrap_err();
    assert!(matches!(error, SalamanderError::InvalidArgument(_)));
}

#[test]
fn unselected_payloads_are_yielded_untouched_for_selected_streams_only() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = AgentDb::open(dir.path()).unwrap();
    let target = append(&mut db, "wanted", &["a"]);
    append(&mut db, "noise", &["b"]);

    let mut reader = db
        .read(ReplayPlan {
            streams: StreamSelector::Streams(vec![target.stream_id]),
            ..ReplayPlan::default()
        })
        .unwrap();
    let record = reader.next_owned().unwrap().unwrap();
    assert_eq!(record.envelope.stream_id, target.stream_id);
    // Payload bytes come back verbatim and opaque (INV-9); the noise
    // stream's record is filtered from the envelope alone.
    assert!(!record.payload.is_empty());
    assert_eq!(record.position, 0);
    assert!(reader.next_owned().unwrap().is_none());
}
