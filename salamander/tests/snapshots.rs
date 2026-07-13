use std::collections::BTreeMap;
use std::fs;

use salamander::{
    DurabilityDto, Engine, EngineAppendBatch, EngineOptions, EventData, ExpectedRevisionDto,
    QueryDefinition, QueryOperation,
};

fn event(index: u64) -> EngineAppendBatch {
    EngineAppendBatch {
        branch_id: [0; 16],
        stream: "events".into(),
        expected: ExpectedRevisionDto::Any,
        idempotency_key: None,
        events: vec![EventData::json(
            serde_json::to_vec(&serde_json::json!({"id": format!("k{index}")})).unwrap(),
        )],
        durability: DurabilityDto::Buffered,
    }
}

fn definition(key: &str) -> QueryDefinition {
    QueryDefinition {
        key_field: key.into(),
        indexes: BTreeMap::new(),
        filter: None,
    }
}

fn register(engine: &Engine) -> salamander::QueryHandle {
    engine
        .register_query("rows".into(), definition("id"))
        .unwrap()
}

#[test]
fn snapshot_plus_suffix_equals_full_replay_at_every_prefix() {
    for prefix in 0..8u64 {
        let dir = tempfile::tempdir().unwrap();
        {
            let engine = Engine::open(EngineOptions::new(dir.path())).unwrap();
            let handle = register(&engine);
            for index in 0..prefix {
                engine.append(event(index)).unwrap();
            }
            engine.create_snapshot(handle).unwrap();
            engine.close().unwrap();
        }
        let engine = Engine::open(EngineOptions::new(dir.path())).unwrap();
        let handle = engine.query_named("rows".into()).unwrap();
        for index in prefix..8 {
            engine.append(event(index)).unwrap();
        }
        assert_eq!(engine.query(handle, QueryOperation::Len).unwrap().len, 8);
    }
}

#[test]
fn corrupt_newest_falls_back_to_prior_generation_and_suffix_replay() {
    let dir = tempfile::tempdir().unwrap();
    let newest = {
        let engine = Engine::open(EngineOptions::new(dir.path())).unwrap();
        let handle = register(&engine);
        for index in 0..5 {
            engine.append(event(index)).unwrap();
        }
        engine.create_snapshot(handle).unwrap();
        for index in 5..10 {
            engine.append(event(index)).unwrap();
        }
        let newest = engine.create_snapshot(handle).unwrap();
        assert_eq!(engine.list_snapshots(handle).unwrap().len(), 2);
        engine.close().unwrap();
        newest
    };
    let path = dir.path().join("derived/snapshots").join(newest.id);
    let mut bytes = fs::read(&path).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xff;
    fs::write(path, bytes).unwrap();

    let engine = Engine::open(EngineOptions::new(dir.path())).unwrap();
    let handle = engine.query_named("rows".into()).unwrap();
    assert_eq!(engine.query(handle, QueryOperation::Len).unwrap().len, 10);
}

#[test]
fn deleting_all_derived_state_changes_no_answers() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = Engine::open(EngineOptions::new(dir.path())).unwrap();
        let handle = register(&engine);
        for index in 0..12 {
            engine.append(event(index)).unwrap();
        }
        engine.create_snapshot(handle).unwrap();
        engine.delete_all_derived_state().unwrap();
        assert_eq!(engine.query(handle, QueryOperation::Len).unwrap().len, 12);
        engine.close().unwrap();
    }
    let engine = Engine::open(EngineOptions::new(dir.path())).unwrap();
    let handle = engine.query_named("rows".into()).unwrap();
    assert_eq!(engine.query(handle, QueryOperation::Len).unwrap().len, 12);
}

#[test]
fn snapshot_management_and_definition_upgrade_are_safe() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(EngineOptions::new(dir.path())).unwrap();
    engine
        .append(EngineAppendBatch {
            branch_id: [0; 16],
            stream: "events".into(),
            expected: ExpectedRevisionDto::Any,
            idempotency_key: None,
            events: vec![EventData::json(
                serde_json::to_vec(&serde_json::json!({"id":"old", "slug":"new"})).unwrap(),
            )],
            durability: DurabilityDto::Buffered,
        })
        .unwrap();
    let handle = register(&engine);
    let snapshot = engine.create_snapshot(handle).unwrap();
    assert_eq!(
        engine.verify_snapshot(snapshot.id.clone()).unwrap(),
        snapshot
    );
    assert!(engine.delete_snapshot(snapshot.id.clone()).unwrap());
    assert!(!engine.delete_snapshot(snapshot.id).unwrap());

    engine.create_snapshot(handle).unwrap();
    let upgraded = engine
        .register_query("rows".into(), definition("slug"))
        .unwrap();
    assert!(engine
        .query(upgraded, QueryOperation::Get("old".into()))
        .unwrap()
        .rows
        .is_empty());
    assert_eq!(
        engine
            .query(upgraded, QueryOperation::Get("new".into()))
            .unwrap()
            .rows
            .len(),
        1
    );
    engine.rebuild_projection(upgraded).unwrap();
    assert!(engine.list_snapshots(upgraded).unwrap().is_empty());
}

#[test]
fn snapshot_from_another_database_is_ignored() {
    let source = tempfile::tempdir().unwrap();
    let target = tempfile::tempdir().unwrap();
    let snapshot = {
        let engine = Engine::open(EngineOptions::new(source.path())).unwrap();
        let handle = register(&engine);
        engine.append(event(0)).unwrap();
        let snapshot = engine.create_snapshot(handle).unwrap();
        engine.close().unwrap();
        snapshot
    };
    {
        let engine = Engine::open(EngineOptions::new(target.path())).unwrap();
        register(&engine);
        engine.append(event(0)).unwrap();
        engine.append(event(1)).unwrap();
        engine.close().unwrap();
    }
    let target_snapshots = target.path().join("derived/snapshots");
    fs::create_dir_all(&target_snapshots).unwrap();
    fs::copy(
        source.path().join("derived/snapshots").join(&snapshot.id),
        target_snapshots.join(&snapshot.id),
    )
    .unwrap();
    let engine = Engine::open(EngineOptions::new(target.path())).unwrap();
    let handle = engine.query_named("rows".into()).unwrap();
    assert_eq!(engine.query(handle, QueryOperation::Len).unwrap().len, 2);
}

#[test]
fn replay_budget_schedules_snapshots_and_corrupt_catalog_is_ignored() {
    let dir = tempfile::tempdir().unwrap();
    let mut options = EngineOptions::new(dir.path());
    options.snapshot_every_events = Some(3);
    {
        let engine = Engine::open(options).unwrap();
        let handle = register(&engine);
        for index in 0..3 {
            engine.append(event(index)).unwrap();
        }
        // WP-09 keeps cold projections off the append path; automatic
        // snapshots begin only after a query has healed the projection.
        assert!(engine.list_snapshots(handle).unwrap().is_empty());
        engine.query(handle, QueryOperation::Len).unwrap();
        engine.append(event(3)).unwrap();
        engine.append(event(4)).unwrap();
        engine.append(event(5)).unwrap();
        assert_eq!(engine.list_snapshots(handle).unwrap().len(), 1);
        engine.close().unwrap();
    }
    fs::write(
        dir.path().join("derived/snapshots/catalog.json"),
        b"not json",
    )
    .unwrap();
    let engine = Engine::open(EngineOptions::new(dir.path())).unwrap();
    let handle = engine.query_named("rows".into()).unwrap();
    assert_eq!(engine.query(handle, QueryOperation::Len).unwrap().len, 6);
}
