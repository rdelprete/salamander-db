use std::collections::BTreeMap;
use std::fs;
use std::ops::Bound;

use salamander::{
    agent::{EventBody, KvProjection},
    AgentDb, BranchId, BranchName, DiffRequest, Engine, EngineAppendBatch, EngineOptions,
    EventData, FeedRequest, Metadata, PayloadCodec, RecordReader, ReplayEnd, ReplayPlan,
    ReplayRequest, RetentionBlocker, SalamanderError,
};
use tempfile::tempdir;

fn set_floor(path: &std::path::Path, floor: u64) {
    let manifest_path = path.join("manifest.json");
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
    manifest["retention_floor"] = floor.into();
    fs::write(manifest_path, serde_json::to_vec_pretty(&manifest).unwrap()).unwrap();
}

#[test]
fn old_manifests_default_to_zero_and_planning_is_non_destructive() {
    let dir = tempdir().unwrap();
    let mut db = AgentDb::open(dir.path()).unwrap();
    db.append(
        "s",
        EventBody::SessionStarted {
            agent_id: "a".into(),
            config_hash: "v1".into(),
        },
    )
    .unwrap();
    db.commit().unwrap();
    drop(db);

    let manifest_path = dir.path().join("manifest.json");
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
    manifest.as_object_mut().unwrap().remove("retention_floor");
    fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(&manifest).unwrap(),
    )
    .unwrap();

    let db = AgentDb::open(dir.path()).unwrap();
    assert_eq!(db.retention_floor(), 0);
    let plan = db.plan_retention(db.durable_head()).unwrap();
    assert_eq!(plan.requested_floor, 1);
    assert_eq!(plan.current_floor, 0);
    assert!(plan
        .blockers
        .contains(&RetentionBlocker::EngineAnchorUnavailable));
    assert_eq!(db.retention_floor(), 0);
    drop(db);

    let persisted: serde_json::Value =
        serde_json::from_slice(&fs::read(manifest_path).unwrap()).unwrap();
    assert!(persisted.get("retention_floor").is_none());
}

#[test]
fn policy_selectors_resolve_through_the_explicit_floor_planner() {
    let dir = tempdir().unwrap();
    let mut db = AgentDb::open(dir.path()).unwrap();
    for n in 0..3 {
        db.append(
            "s",
            EventBody::ToolResult {
                call_id: n.to_string(),
                ok: true,
                content: "ok".into(),
            },
        )
        .unwrap();
    }
    db.commit().unwrap();

    let latest = db
        .preview_retention_policy(salamander::RetentionPolicy::KeepLatestEvents(2))
        .unwrap();
    assert_eq!(latest.selected_floor, 1);
    assert_eq!(latest.plan.requested_floor, 1);
    assert!(latest.target_satisfied);

    let all_by_age = db
        .preview_retention_policy(salamander::RetentionPolicy::KeepNewerThan(i64::MIN))
        .unwrap();
    assert_eq!(all_by_age.selected_floor, 0);
    let none_by_age = db
        .preview_retention_policy(salamander::RetentionPolicy::KeepNewerThan(i64::MAX))
        .unwrap();
    assert_eq!(none_by_age.selected_floor, 3);

    let impossible = db
        .preview_retention_policy(salamander::RetentionPolicy::TargetLogBytes(0))
        .unwrap();
    assert!(!impossible.target_satisfied);
    assert_eq!(impossible.plan.current_floor, 0);
}

#[test]
fn reads_and_forks_below_a_persisted_floor_fail_without_clamping() {
    let dir = tempdir().unwrap();
    let mut db = AgentDb::open(dir.path()).unwrap();
    for n in 0..4 {
        db.append(
            "s",
            EventBody::Put {
                key: n.to_string(),
                value: vec![n],
            },
        )
        .unwrap();
    }
    db.commit().unwrap();
    drop(db);
    set_floor(dir.path(), 2);

    let mut db = AgentDb::open(dir.path()).unwrap();
    assert_eq!(db.retention_floor(), 2);
    let error = match db.read(ReplayPlan::default()) {
        Ok(_) => panic!("read below floor unexpectedly succeeded"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        SalamanderError::PositionUnavailable {
            requested: 0,
            floor: 2,
            head: 4,
            bootstrap_available: false,
        }
    ));

    let mut reader = db
        .read(ReplayPlan {
            from: Bound::Included(2),
            until: ReplayEnd::Head,
            ..ReplayPlan::default()
        })
        .unwrap();
    let mut positions = Vec::new();
    while let Some(record) = reader.next().unwrap() {
        positions.push(record.position);
    }
    assert_eq!(positions, [2, 3]);
    assert!(matches!(
        db.replay("s", 0..4, |_| {}),
        Err(SalamanderError::PositionUnavailable {
            requested: 0,
            floor: 2,
            ..
        })
    ));
    assert!(matches!(
        db.view_at::<KvProjection>(3),
        Err(SalamanderError::PositionUnavailable {
            requested: 0,
            floor: 2,
            bootstrap_available: false,
            ..
        })
    ));

    let error = db
        .fork_branch(
            BranchId::ZERO,
            1,
            BranchName::new("too-old").unwrap(),
            Metadata::new(),
        )
        .unwrap_err();
    assert!(matches!(
        error,
        SalamanderError::PositionUnavailable {
            requested: 1,
            floor: 2,
            ..
        }
    ));
}

#[test]
fn floor_beyond_head_is_manifest_corruption_and_plans_never_move_backward() {
    let dir = tempdir().unwrap();
    let mut db = AgentDb::open(dir.path()).unwrap();
    db.append(
        "s",
        EventBody::SessionEnded {
            reason: "done".into(),
        },
    )
    .unwrap();
    db.commit().unwrap();
    drop(db);
    set_floor(dir.path(), 2);

    assert!(matches!(
        AgentDb::open(dir.path()),
        Err(SalamanderError::Manifest(message)) if message.contains("beyond head")
    ));

    set_floor(dir.path(), 1);
    let db = AgentDb::open(dir.path()).unwrap();
    assert!(matches!(
        db.plan_retention(0),
        Err(SalamanderError::InvalidArgument(message))
            if message.contains("cannot move backward")
    ));
}

#[test]
fn diff_uses_the_retained_shared_window() {
    let dir = tempdir().unwrap();
    let mut db = AgentDb::open(dir.path()).unwrap();
    for n in 0..2 {
        db.append(
            "s",
            EventBody::Put {
                key: n.to_string(),
                value: vec![n],
            },
        )
        .unwrap();
    }
    db.commit().unwrap();
    let child = db
        .fork_branch(
            BranchId::ZERO,
            2,
            BranchName::new("child").unwrap(),
            Metadata::new(),
        )
        .unwrap();
    db.append_on_branch(
        child.id,
        "s",
        EventBody::Put {
            key: "child".into(),
            value: vec![9],
        },
    )
    .unwrap();
    db.commit().unwrap();
    drop(db);
    set_floor(dir.path(), 1);

    let db = AgentDb::open(dir.path()).unwrap();
    let diff = db.diff(DiffRequest::new(BranchId::ZERO, child.id)).unwrap();
    assert_eq!(diff.divergence, 2);
    assert_eq!(diff.shared.from, Bound::Included(1));
}

#[test]
fn facade_readers_and_feeds_return_the_stable_unavailable_code() {
    let dir = tempdir().unwrap();
    let engine = Engine::open(EngineOptions::new(dir.path())).unwrap();
    engine
        .append(EngineAppendBatch {
            branch_id: [0; 16],
            stream: "s".into(),
            expected: salamander::ExpectedRevisionDto::Any,
            idempotency_key: None,
            events: vec![EventData {
                event_id: None,
                event_type: "test".into(),
                schema_version: 1,
                metadata: Metadata::new(),
                codec: PayloadCodec::Bytes,
                payload: vec![1],
            }],
            durability: salamander::DurabilityDto::Sync,
        })
        .unwrap();
    engine.close().unwrap();
    set_floor(dir.path(), 1);

    let engine = Engine::open(EngineOptions::new(dir.path())).unwrap();
    assert_eq!(engine.retention_floor().unwrap(), 1);
    assert_eq!(
        engine
            .open_reader(ReplayRequest::default())
            .unwrap_err()
            .code,
        "position_unavailable"
    );
    assert_eq!(
        engine.open_feed(FeedRequest::default()).unwrap_err().code,
        "position_unavailable"
    );
    let plan = engine.plan_retention(1).unwrap();
    assert_eq!(plan.current_floor, 1);
    assert!(plan
        .blockers
        .contains(&RetentionBlocker::EngineAnchorUnavailable));
}

#[test]
fn core_anchor_is_verified_reused_and_satisfies_the_planner() {
    let dir = tempdir().unwrap();
    let mut db = AgentDb::open(dir.path()).unwrap();
    db.append(
        "s",
        EventBody::Put {
            key: "before".into(),
            value: vec![1],
        },
    )
    .unwrap();
    db.commit().unwrap();

    let requested_floor = db.durable_head();
    let before = db.plan_retention(requested_floor).unwrap();
    assert!(before
        .blockers
        .contains(&RetentionBlocker::EngineAnchorUnavailable));

    let anchor = db.create_retention_anchor(requested_floor).unwrap();
    assert_eq!(anchor.floor, before.effective_floor);
    assert_eq!(anchor.head, db.durable_head());
    assert!(anchor.bytes > 16);
    assert_eq!(db.create_retention_anchor(requested_floor).unwrap(), anchor);

    let after = db.plan_retention(requested_floor).unwrap();
    assert!(!after
        .blockers
        .contains(&RetentionBlocker::EngineAnchorUnavailable));
    drop(db);

    let mut reopened = AgentDb::open(dir.path()).unwrap();
    let after_reopen = reopened.plan_retention(requested_floor).unwrap();
    assert!(!after_reopen
        .blockers
        .contains(&RetentionBlocker::EngineAnchorUnavailable));
    reopened
        .append(
            "s",
            EventBody::Put {
                key: "after".into(),
                value: vec![2],
            },
        )
        .unwrap();
}

#[test]
fn corrupt_non_authoritative_anchor_falls_back_to_complete_log_truth() {
    let dir = tempdir().unwrap();
    let mut db = AgentDb::open(dir.path()).unwrap();
    db.append(
        "s",
        EventBody::Put {
            key: "safe".into(),
            value: vec![7],
        },
    )
    .unwrap();
    db.create_retention_anchor(db.head()).unwrap();
    drop(db);

    let anchor_path = dir.path().join("retention").join("core.anchor");
    let mut bytes = fs::read(&anchor_path).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xff;
    fs::write(anchor_path, bytes).unwrap();

    let reopened = AgentDb::open(dir.path()).unwrap();
    let mut reader = reopened
        .read(ReplayPlan {
            from: Bound::Included(0),
            until: ReplayEnd::Head,
            ..ReplayPlan::default()
        })
        .unwrap();
    let mut count = 0;
    while reader.next().unwrap().is_some() {
        count += 1;
    }
    assert_eq!(count, 1);
    assert!(reopened
        .plan_retention(reopened.head())
        .unwrap()
        .blockers
        .contains(&RetentionBlocker::EngineAnchorUnavailable));
}

#[test]
fn branch_and_consumer_bootstraps_are_promoted_and_restored() {
    let dir = tempdir().unwrap();
    let child_id = {
        let engine = Engine::open(EngineOptions::new(dir.path())).unwrap();
        let child = engine
            .fork([0; 16], 0, "child".into(), BTreeMap::new())
            .unwrap();
        assert_eq!(
            engine
                .register_branch_bootstrap(child.id, 0, b"branch-state".to_vec())
                .unwrap(),
            0
        );
        assert_eq!(
            engine
                .register_consumer_bootstrap("consumer-a".into(), 0, b"consumer-state".to_vec())
                .unwrap(),
            0
        );
        let anchor = engine.create_retention_anchor(0).unwrap();
        assert_eq!(anchor.branch_bootstraps, 1);
        assert_eq!(anchor.consumer_bootstraps, 1);
        assert_eq!(
            anchor.bootstrap_bytes,
            (b"branch-state".len() + b"consumer-state".len()) as u64
        );
        engine.close().unwrap();
        child.id
    };

    let engine = Engine::open(EngineOptions::new(dir.path())).unwrap();
    assert_eq!(
        engine.branch_bootstrap(child_id).unwrap().unwrap(),
        b"branch-state"
    );
    assert_eq!(
        engine
            .consumer_bootstrap("consumer-a".into())
            .unwrap()
            .unwrap(),
        b"consumer-state"
    );
    assert_eq!(
        engine
            .register_branch_bootstrap(child_id, 0, b"branch-state".to_vec())
            .unwrap(),
        0
    );
    let anchor = engine.create_retention_anchor(0).unwrap();
    assert_eq!(anchor.branch_bootstraps, 1);
    assert_eq!(anchor.consumer_bootstraps, 1);
}

#[test]
fn apply_rejects_blocked_unknown_and_stale_plans() {
    let dir = tempdir().unwrap();
    let engine = Engine::open(EngineOptions::new(dir.path())).unwrap();
    engine
        .append(EngineAppendBatch {
            branch_id: [0; 16],
            stream: "s".into(),
            expected: salamander::ExpectedRevisionDto::Any,
            idempotency_key: None,
            events: vec![EventData {
                event_id: None,
                event_type: "one".into(),
                schema_version: 1,
                metadata: Metadata::new(),
                codec: PayloadCodec::Bytes,
                payload: vec![1],
            }],
            durability: salamander::DurabilityDto::Sync,
        })
        .unwrap();
    let blocked = engine.plan_retention(0).unwrap();
    assert_eq!(
        engine.apply_retention(blocked.plan_id).unwrap_err().code,
        "retention_blocked"
    );
    assert_eq!(
        engine.apply_retention([0xff; 16]).unwrap_err().code,
        "retention_plan_unknown"
    );

    engine.create_retention_anchor(0).unwrap();
    let stale = engine.plan_retention(0).unwrap();
    engine
        .append(EngineAppendBatch {
            branch_id: [0; 16],
            stream: "s".into(),
            expected: salamander::ExpectedRevisionDto::Any,
            idempotency_key: None,
            events: vec![EventData {
                event_id: None,
                event_type: "two".into(),
                schema_version: 1,
                metadata: Metadata::new(),
                codec: PayloadCodec::Bytes,
                payload: vec![2],
            }],
            durability: salamander::DurabilityDto::Sync,
        })
        .unwrap();
    assert_eq!(
        engine.apply_retention(stale.plan_id).unwrap_err().code,
        "retention_plan_stale"
    );
}

#[test]
fn promoted_bootstraps_clear_real_branch_and_consumer_blockers() {
    let dir = tempdir().unwrap();
    let engine = Engine::open(EngineOptions::new(dir.path())).unwrap();
    engine
        .append(EngineAppendBatch {
            branch_id: [0; 16],
            stream: "s".into(),
            expected: salamander::ExpectedRevisionDto::Any,
            idempotency_key: None,
            events: vec![EventData {
                event_id: None,
                event_type: "small".into(),
                schema_version: 1,
                metadata: Metadata::new(),
                codec: PayloadCodec::Bytes,
                payload: vec![1],
            }],
            durability: salamander::DurabilityDto::Sync,
        })
        .unwrap();
    let feed = engine
        .open_feed(FeedRequest {
            consumer_id: Some("lagging".into()),
            ..FeedRequest::default()
        })
        .unwrap();
    engine.next_feed_page(feed, None).unwrap();
    assert_eq!(engine.acknowledge_feed(feed).unwrap(), 1);
    engine.close_feed(feed).unwrap();
    let child = engine
        .fork([0; 16], 1, "old-child".into(), BTreeMap::new())
        .unwrap();

    for n in 0..11 {
        engine
            .append(EngineAppendBatch {
                branch_id: [0; 16],
                stream: "s".into(),
                expected: salamander::ExpectedRevisionDto::Any,
                idempotency_key: None,
                events: vec![EventData {
                    event_id: None,
                    event_type: format!("large-{n}"),
                    schema_version: 1,
                    metadata: Metadata::new(),
                    codec: PayloadCodec::Bytes,
                    payload: vec![n as u8; 7 * 1024 * 1024],
                }],
                durability: salamander::DurabilityDto::Buffered,
            })
            .unwrap();
    }
    engine.commit().unwrap();
    let keep_from = engine.durable_head().unwrap();
    let before = engine.plan_retention(keep_from).unwrap();
    assert!(before.effective_floor > 1);
    assert!(before.blockers.iter().any(|blocker| matches!(
        blocker,
        RetentionBlocker::BranchRequiresBootstrap { branch, .. }
            if branch.as_str() == "old-child"
    )));
    let status = engine.retention_status(Some(keep_from)).unwrap();
    assert_eq!(status.floor, 0);
    assert_eq!(status.effective_floor, before.effective_floor);
    assert!(!status.anchor_ready);
    assert_eq!(status.open_readers, 0);
    assert_eq!(status.open_feeds, 0);
    assert_eq!(status.consumers.len(), 1);
    assert!(status.consumers[0].behind_effective_floor);
    assert!(!status.consumers[0].bootstrap_available);
    assert_eq!(status.reclaimable_bytes, before.reclaimable_bytes);
    assert!(before.blockers.iter().any(|blocker| matches!(
        blocker,
        RetentionBlocker::ConsumerRequiresBootstrap { consumer_id, position: 1 }
            if consumer_id == "lagging"
    )));

    assert_eq!(
        engine
            .register_branch_bootstrap(child.id, keep_from, b"branch".to_vec())
            .unwrap(),
        before.effective_floor
    );
    assert_eq!(
        engine
            .register_consumer_bootstrap_for_feed(
                "lagging".into(),
                keep_from,
                salamander::FeedFilter {
                    branches: vec![[0; 16]],
                    ..salamander::FeedFilter::default()
                },
                b"consumer".to_vec(),
            )
            .unwrap(),
        before.effective_floor
    );
    engine.create_retention_anchor(keep_from).unwrap();
    let ready_status = engine.retention_status(Some(keep_from)).unwrap();
    assert!(ready_status.anchor_ready);
    assert!(ready_status.blockers.is_empty());
    assert!(ready_status.consumers[0].bootstrap_available);
    let after = engine.plan_retention(keep_from).unwrap();
    assert!(!after.blockers.iter().any(|blocker| matches!(
        blocker,
        RetentionBlocker::BranchRequiresBootstrap { .. }
            | RetentionBlocker::ConsumerRequiresBootstrap { .. }
    )));
    assert!(after.blockers.is_empty());
    let oldest_segment = fs::read_dir(dir.path().join("log"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .min()
        .unwrap();
    let oldest_name = oldest_segment.file_name().unwrap().to_owned();
    let leaked_copy = dir.path().join("old-generation-backup.seg");
    fs::copy(&oldest_segment, &leaked_copy).unwrap();
    let applied = engine.apply_retention(after.plan_id).unwrap();
    assert_eq!(applied.floor, before.effective_floor);
    assert_eq!(applied.generation, 1);
    assert!(applied.reclaimed_bytes > 0);
    engine.close().unwrap();

    let engine = Engine::open(EngineOptions::new(dir.path())).unwrap();
    assert_eq!(engine.retention_floor().unwrap(), before.effective_floor);
    let retained_status = engine.retention_status(Some(keep_from)).unwrap();
    assert_eq!(retained_status.generation, 1);
    assert_eq!(retained_status.floor, before.effective_floor);
    assert!(retained_status.cleanup.pending_segments.is_empty());
    assert_eq!(retained_status.cleanup.pending_bytes, 0);
    assert_eq!(
        engine.branch_bootstrap(child.id).unwrap().unwrap(),
        b"branch"
    );
    assert_eq!(
        engine
            .consumer_bootstrap("lagging".into())
            .unwrap()
            .unwrap(),
        b"consumer"
    );
    let wrong_scope = engine
        .open_feed(FeedRequest {
            from: None,
            consumer_id: Some("lagging".into()),
            ..FeedRequest::default()
        })
        .unwrap_err();
    assert!(wrong_scope.feed_bootstrap.is_none());
    let unavailable = engine
        .open_feed(FeedRequest {
            from: None,
            consumer_id: Some("lagging".into()),
            filter: salamander::FeedFilter {
                branches: vec![[0; 16]],
                ..salamander::FeedFilter::default()
            },
            ..FeedRequest::default()
        })
        .unwrap_err();
    assert_eq!(unavailable.code, "position_unavailable");
    let descriptor = *unavailable.feed_bootstrap.unwrap();
    assert_eq!(descriptor.floor, before.effective_floor);
    assert_eq!(descriptor.resume_from, before.effective_floor);
    assert_eq!(descriptor.byte_length, b"consumer".len() as u64);
    assert_eq!(descriptor.codec, "opaque");
    assert_eq!(descriptor.codec_version, 1);
    assert_eq!(
        engine
            .fetch_feed_bootstrap(descriptor.clone(), b"consumer".len())
            .unwrap(),
        b"consumer"
    );
    assert_eq!(
        engine
            .fetch_feed_bootstrap(descriptor.clone(), b"consumer".len() - 1)
            .unwrap_err()
            .code,
        "resource_limit"
    );
    let resumed = engine
        .resume_feed(descriptor.clone(), 128, 1024 * 1024)
        .unwrap();
    let page = engine.next_feed_page(resumed, None).unwrap();
    let positions = page
        .batches
        .iter()
        .flat_map(|batch| batch.events.iter().map(|event| event.position))
        .collect::<Vec<_>>();
    assert_eq!(
        positions,
        (before.effective_floor..keep_from).collect::<Vec<_>>()
    );
    engine.close_feed(resumed).unwrap();
    let mut mismatched = descriptor;
    mismatched.generation += 1;
    assert_eq!(
        engine
            .resume_feed(mismatched, 128, 1024 * 1024)
            .unwrap_err()
            .code,
        "feed_bootstrap_mismatch"
    );
    assert_eq!(
        engine
            .open_reader(ReplayRequest::default())
            .unwrap_err()
            .code,
        "position_unavailable"
    );
    engine.close().unwrap();

    fs::copy(leaked_copy, dir.path().join("log").join(oldest_name)).unwrap();
    let anchor_path = dir.path().join("retention").join("core.anchor");
    let mut anchor = fs::read(&anchor_path).unwrap();
    let last = anchor.len() - 1;
    anchor[last] ^= 0x80;
    fs::write(anchor_path, anchor).unwrap();
    assert!(Engine::open(EngineOptions::new(dir.path())).is_err());
}
