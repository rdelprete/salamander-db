use std::collections::BTreeMap;

use proptest::prelude::*;
use salamander::{
    partition_of, DurabilityDto, Engine, EngineAppendBatch, EngineOptions, EventData,
    ExpectedRevisionDto, PartitionStatus, PayloadCodec, QueryConsistency, QueryDefinition,
    QueryOperation, StreamId,
};

fn event(stream: &str, key: &str) -> EngineAppendBatch {
    EngineAppendBatch {
        branch_id: [0; 16],
        stream: stream.into(),
        expected: ExpectedRevisionDto::Any,
        idempotency_key: None,
        durability: DurabilityDto::Buffered,
        events: vec![EventData {
            event_id: None,
            event_type: "row".into(),
            schema_version: 1,
            metadata: BTreeMap::new(),
            codec: PayloadCodec::Json,
            payload: serde_json::to_vec(&serde_json::json!({"id": key})).unwrap(),
        }],
    }
}

fn definition() -> QueryDefinition {
    QueryDefinition {
        key_field: "id".into(),
        indexes: BTreeMap::new(),
        filter: None,
    }
}

#[test]
fn open_and_registration_replay_no_events_until_a_partition_is_read() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(EngineOptions::new(dir.path())).unwrap();
    let mut expected = [0usize; 4];
    for index in 0..40 {
        let receipt = engine
            .append(event(&format!("s{index}"), &format!("k{index}")))
            .unwrap();
        expected[partition_of(StreamId::from_bytes(receipt.stream_id), 4) as usize] += 1;
    }
    let handle = engine
        .register_partitioned_query("rows".into(), definition(), 4)
        .unwrap();
    assert!(engine
        .partition_status(handle)
        .unwrap()
        .iter()
        .all(|status| matches!(status, PartitionStatus::Cold { .. })));
    let partition = expected.iter().position(|count| *count > 0).unwrap() as u32;
    let result = engine
        .query_partitions(
            handle,
            vec![partition],
            QueryOperation::Len,
            QueryConsistency::RequireHead,
        )
        .unwrap();
    assert_eq!(result.len, expected[partition as usize] as u64);
    let statuses = engine.partition_status(handle).unwrap();
    assert!(matches!(
        statuses[partition as usize],
        PartitionStatus::Ready { .. }
    ));
    assert_eq!(
        statuses
            .iter()
            .filter(|status| matches!(status, PartitionStatus::Cold { .. }))
            .count(),
        3
    );
    assert_eq!(engine.query(handle, QueryOperation::Len).unwrap().len, 40);
}

#[test]
fn partition_heal_order_is_commutative_and_equals_heal_all() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(EngineOptions::new(dir.path())).unwrap();
    for index in 0..80 {
        engine
            .append(event(&format!("s{index}"), &format!("k{index}")))
            .unwrap();
    }
    let left = engine
        .register_partitioned_query("left".into(), definition(), 4)
        .unwrap();
    let right = engine
        .register_partitioned_query("right".into(), definition(), 4)
        .unwrap();
    for partition in 0..4 {
        engine
            .query_partitions(
                left,
                vec![partition],
                QueryOperation::Len,
                QueryConsistency::RequireHead,
            )
            .unwrap();
    }
    for partition in (0..4).rev() {
        engine
            .query_partitions(
                right,
                vec![partition],
                QueryOperation::Len,
                QueryConsistency::RequireHead,
            )
            .unwrap();
    }
    assert_eq!(
        engine.query(left, QueryOperation::Len).unwrap(),
        engine.query(right, QueryOperation::Len).unwrap()
    );
    assert_eq!(engine.query(left, QueryOperation::Len).unwrap().len, 80);
}

#[test]
fn mixed_cursor_partition_snapshots_heal_independently_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(EngineOptions::new(dir.path())).unwrap();
    let mut by_partition: [Option<String>; 2] = [None, None];
    let mut index = 0;
    while by_partition.iter().any(Option::is_none) {
        let stream = format!("s{index}");
        let receipt = engine.append(event(&stream, &format!("k{index}"))).unwrap();
        by_partition[partition_of(StreamId::from_bytes(receipt.stream_id), 2) as usize]
            .get_or_insert(stream);
        index += 1;
    }
    let handle = engine
        .register_partitioned_query("rows".into(), definition(), 2)
        .unwrap();
    let first = engine.create_partition_snapshot(handle, 0).unwrap();
    engine
        .append(event(by_partition[1].as_ref().unwrap(), "later"))
        .unwrap();
    let second = engine.create_partition_snapshot(handle, 1).unwrap();
    assert!(first.manifest.cursor.position < second.manifest.cursor.position);
    let expected = engine.query(handle, QueryOperation::Len).unwrap().len;
    engine.close().unwrap();

    let engine = Engine::open(EngineOptions::new(dir.path())).unwrap();
    let handle = engine.query_named("rows".into()).unwrap();
    assert!(engine
        .partition_status(handle)
        .unwrap()
        .iter()
        .all(|status| matches!(status, PartitionStatus::Cold { .. })));
    assert_eq!(
        engine.query(handle, QueryOperation::Len).unwrap().len,
        expected
    );
}

#[test]
fn partition_scheme_change_invalidates_all_derived_partitions() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(EngineOptions::new(dir.path())).unwrap();
    engine.append(event("s", "k")).unwrap();
    let old = engine
        .register_partitioned_query("rows".into(), definition(), 2)
        .unwrap();
    engine.query(old, QueryOperation::Len).unwrap();
    engine.create_snapshot(old).unwrap();
    let changed = engine
        .register_partitioned_query("rows".into(), definition(), 4)
        .unwrap();
    assert_eq!(old, changed);
    assert!(engine.list_snapshots(changed).unwrap().is_empty());
    assert!(engine
        .partition_status(changed)
        .unwrap()
        .iter()
        .all(|status| matches!(status, PartitionStatus::Cold { .. })));
    assert_eq!(engine.query(changed, QueryOperation::Len).unwrap().len, 1);
}

#[test]
fn corrupt_core_catalog_and_partition_snapshot_fall_back_to_log_truth() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(EngineOptions::new(dir.path())).unwrap();
    for index in 0..12 {
        engine
            .append(event(&format!("s{index}"), &format!("k{index}")))
            .unwrap();
    }
    engine.commit().unwrap();
    let handle = engine
        .register_partitioned_query("rows".into(), definition(), 2)
        .unwrap();
    let snapshot = engine.create_partition_snapshot(handle, 0).unwrap();
    engine.close().unwrap();
    std::fs::write(dir.path().join("core-catalog.bin"), b"hostile cache").unwrap();
    std::fs::write(
        dir.path().join("derived/snapshots").join(snapshot.id),
        b"hostile snapshot",
    )
    .unwrap();
    let engine = Engine::open(EngineOptions::new(dir.path())).unwrap();
    let handle = engine.query_named("rows".into()).unwrap();
    assert_eq!(engine.query(handle, QueryOperation::Len).unwrap().len, 12);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    #[test]
    fn lazy_healing_matches_full_replay_for_random_histories_and_access_orders(
        streams in prop::collection::vec(0u8..12, 1..80),
        access in prop::collection::vec(0u8..4, 0..16),
    ) {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(EngineOptions::new(dir.path())).unwrap();
        let mut counts = [0u64; 4];
        for (index, stream) in streams.iter().enumerate() {
            let receipt = engine.append(event(&format!("s{stream}"), &format!("k{index}"))).unwrap();
            counts[partition_of(StreamId::from_bytes(receipt.stream_id), 4) as usize] += 1;
        }
        let lazy = engine.register_partitioned_query("lazy".into(), definition(), 4).unwrap();
        let eager = engine.register_partitioned_query("eager".into(), definition(), 4).unwrap();
        let expected = engine.query(eager, QueryOperation::Len).unwrap();
        let mut touched = [false; 4];
        for partition in access {
            touched[partition as usize] = true;
            let result = engine.query_partitions(lazy, vec![partition as u32], QueryOperation::Len, QueryConsistency::RequireHead).unwrap();
            let partial = counts.iter().enumerate().filter(|(index, _)| touched[*index]).map(|(_, count)| count).sum::<u64>();
            prop_assert_eq!(result.len, partial);
        }
        prop_assert_eq!(engine.query(lazy, QueryOperation::Len).unwrap(), expected);
    }
}
