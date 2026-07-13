use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use salamander::{
    DurabilityDto, Engine, EngineAppendBatch, EngineOptions, EventData, ExpectedRevisionDto,
    PayloadCodec, ProjectionDescriptor, ProjectionFailure, ProjectionRuntime, ProjectionScope,
    ProjectionStatus, QueryConsistency, QueryDefinition, QueryOperation, QueryResult, RecordDto,
    StaleReason,
};

fn json_batch(value: serde_json::Value) -> EngineAppendBatch {
    EngineAppendBatch {
        branch_id: [0; 16],
        stream: "events".into(),
        expected: ExpectedRevisionDto::Any,
        idempotency_key: None,
        events: vec![EventData::json(serde_json::to_vec(&value).unwrap())],
        durability: DurabilityDto::Buffered,
    }
}

fn bytes_batch(value: &[u8]) -> EngineAppendBatch {
    EngineAppendBatch {
        branch_id: [0; 16],
        stream: "raw".into(),
        expected: ExpectedRevisionDto::Any,
        idempotency_key: None,
        events: vec![EventData {
            event_id: None,
            event_type: "runtime.input".into(),
            schema_version: 1,
            metadata: BTreeMap::new(),
            codec: PayloadCodec::Bytes,
            payload: value.to_vec(),
        }],
        durability: DurabilityDto::Buffered,
    }
}

fn query_definition(key: &str) -> QueryDefinition {
    QueryDefinition {
        key_field: key.into(),
        indexes: BTreeMap::new(),
        filter: None,
    }
}

fn descriptor(name: &str, version: u32) -> ProjectionDescriptor {
    ProjectionDescriptor {
        name: name.into(),
        definition_id: [version as u8; 16],
        definition_version: version,
        input_types: Vec::new(),
        state_codec: 1,
        state_codec_version: 1,
        scope: ProjectionScope::default(),
        partition_scheme: Default::default(),
    }
}

#[test]
fn portable_descriptor_rebuilds_incrementally_and_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = Engine::open(EngineOptions::new(dir.path())).unwrap();
        let handle = engine
            .register_query("by-id".into(), query_definition("id"))
            .unwrap();
        for index in 0..20 {
            engine
                .append(json_batch(serde_json::json!({"id": format!("k{index}")})))
                .unwrap();
            let result = engine.query(handle, QueryOperation::Len).unwrap();
            assert_eq!(result.len, index + 1);
            assert!(matches!(
                engine.projection_status(handle).unwrap(),
                ProjectionStatus::Ready { cursor } if cursor.position == index + 1
            ));
        }
        engine.close().unwrap();
    }

    let engine = Engine::open(EngineOptions::new(dir.path())).unwrap();
    let handle = engine.query_named("by-id".into()).unwrap();
    assert_eq!(engine.query(handle, QueryOperation::Len).unwrap().len, 20);
    assert!(matches!(
        engine.projection_status(handle).unwrap(),
        ProjectionStatus::Ready { cursor } if cursor.position == 20
    ));
}

#[test]
fn descriptor_change_discards_old_state_and_rebuilds() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(EngineOptions::new(dir.path())).unwrap();
    engine
        .append(json_batch(serde_json::json!({"id":"old", "slug":"new"})))
        .unwrap();
    let first = engine
        .register_query("lookup".into(), query_definition("id"))
        .unwrap();
    let identical = engine
        .register_query("lookup".into(), query_definition("id"))
        .unwrap();
    assert_eq!(first, identical);
    assert_eq!(
        engine
            .query(first, QueryOperation::Get("old".into()))
            .unwrap()
            .rows
            .len(),
        1
    );
    let second = engine
        .register_query("lookup".into(), query_definition("slug"))
        .unwrap();
    assert_eq!(first, second);
    assert!(engine
        .query(second, QueryOperation::Get("old".into()))
        .unwrap()
        .rows
        .is_empty());
    assert_eq!(
        engine
            .query(second, QueryOperation::Get("new".into()))
            .unwrap()
            .rows
            .len(),
        1
    );
}

#[test]
fn consistency_modes_and_durable_drop_are_enforced() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = Engine::open(EngineOptions::new(dir.path())).unwrap();
        engine
            .append(json_batch(serde_json::json!({"id":"a"})))
            .unwrap();
        let handle = engine
            .register_query("temporary".into(), query_definition("id"))
            .unwrap();
        assert_eq!(
            engine
                .query_consistent(handle, QueryOperation::Len, QueryConsistency::WaitFor(1))
                .unwrap()
                .len,
            1
        );
        assert!(engine
            .query_consistent(handle, QueryOperation::Len, QueryConsistency::WaitFor(2))
            .is_err());
        assert!(engine.remove_query("temporary".into()).unwrap());
        engine.close().unwrap();
    }
    let engine = Engine::open(EngineOptions::new(dir.path())).unwrap();
    assert!(engine.query_named("temporary".into()).is_err());
}

struct TestRuntime {
    count: Arc<Mutex<u64>>,
    behavior: Behavior,
}

enum Behavior {
    Healthy,
    Error,
    Panic,
}

impl ProjectionRuntime for TestRuntime {
    fn reset(&mut self) -> Result<(), ProjectionFailure> {
        *self.count.lock().unwrap() = 0;
        Ok(())
    }

    fn apply(&mut self, _record: &RecordDto) -> Result<(), ProjectionFailure> {
        match self.behavior {
            Behavior::Healthy => {
                *self.count.lock().unwrap() += 1;
                Ok(())
            }
            Behavior::Error => Err(ProjectionFailure {
                code: "apply".into(),
                message: "deliberate failure".into(),
            }),
            Behavior::Panic => panic!("deliberate projection panic"),
        }
    }

    fn query(&self, _operation: QueryOperation) -> Result<QueryResult, ProjectionFailure> {
        Ok(QueryResult {
            rows: Vec::new(),
            len: *self.count.lock().unwrap(),
        })
    }
}

#[test]
fn apply_errors_and_panics_isolate_projections_from_truth_and_each_other() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(EngineOptions::new(dir.path())).unwrap();
    let healthy_count = Arc::new(Mutex::new(0));
    let healthy = engine
        .register_runtime(
            descriptor("healthy", 1),
            Box::new(TestRuntime {
                count: healthy_count.clone(),
                behavior: Behavior::Healthy,
            }),
        )
        .unwrap();
    let failed = engine
        .register_runtime(
            descriptor("failed", 1),
            Box::new(TestRuntime {
                count: Arc::new(Mutex::new(0)),
                behavior: Behavior::Error,
            }),
        )
        .unwrap();
    let panicked = engine
        .register_runtime(
            descriptor("panicked", 1),
            Box::new(TestRuntime {
                count: Arc::new(Mutex::new(0)),
                behavior: Behavior::Panic,
            }),
        )
        .unwrap();

    let receipt = engine.append(bytes_batch(b"truth survives")).unwrap();
    assert_eq!(receipt.first_position, 0);
    assert_eq!(engine.head().unwrap(), 1);
    engine.query(healthy, QueryOperation::Len).unwrap();
    assert_eq!(*healthy_count.lock().unwrap(), 1);
    assert!(matches!(
        engine.projection_status(healthy).unwrap(),
        ProjectionStatus::Ready { cursor } if cursor.position == 1
    ));
    for handle in [failed, panicked] {
        assert!(engine.query(handle, QueryOperation::Len).is_err());
        assert!(matches!(
            engine.projection_status(handle).unwrap(),
            ProjectionStatus::Failed { cursor, .. } if cursor.position == 0
        ));
        assert_eq!(
            engine
                .query_consistent(handle, QueryOperation::Len, QueryConsistency::AllowStale)
                .unwrap()
                .len,
            0
        );
    }
}

#[test]
fn native_descriptor_reopens_stale_until_code_is_registered_again() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = Engine::open(EngineOptions::new(dir.path())).unwrap();
        engine
            .register_runtime(
                descriptor("native", 7),
                Box::new(TestRuntime {
                    count: Arc::new(Mutex::new(0)),
                    behavior: Behavior::Healthy,
                }),
            )
            .unwrap();
        engine.close().unwrap();
    }
    let engine = Engine::open(EngineOptions::new(dir.path())).unwrap();
    let stale = engine.query_named("native".into()).unwrap();
    assert!(matches!(
        engine.projection_status(stale).unwrap(),
        ProjectionStatus::Stale {
            reason: StaleReason::DescriptorChanged,
            ..
        }
    ));
    let resumed = engine
        .register_runtime(
            descriptor("native", 7),
            Box::new(TestRuntime {
                count: Arc::new(Mutex::new(0)),
                behavior: Behavior::Healthy,
            }),
        )
        .unwrap();
    assert_eq!(stale, resumed);
    engine.query(resumed, QueryOperation::Len).unwrap();
    assert!(matches!(
        engine.projection_status(resumed).unwrap(),
        ProjectionStatus::Ready { .. }
    ));
}

#[test]
fn typed_native_adapter_and_portable_runtime_have_identical_state_and_cursors() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(EngineOptions::new(dir.path())).unwrap();
    let portable = engine
        .register_query("portable".into(), query_definition("id"))
        .unwrap();
    let count = Arc::new(Mutex::new(0));
    let native = engine
        .register_runtime(
            descriptor("native-count", 1),
            Box::new(TestRuntime {
                count,
                behavior: Behavior::Healthy,
            }),
        )
        .unwrap();
    for index in 0..12 {
        engine
            .append(json_batch(serde_json::json!({"id": format!("k{index}")})))
            .unwrap();
        assert_eq!(
            engine.query(portable, QueryOperation::Len).unwrap().len,
            engine.query(native, QueryOperation::Len).unwrap().len
        );
        let portable_cursor = match engine.projection_status(portable).unwrap() {
            ProjectionStatus::Ready { cursor } => cursor.position,
            other => panic!("unexpected status: {other:?}"),
        };
        let native_cursor = match engine.projection_status(native).unwrap() {
            ProjectionStatus::Ready { cursor } => cursor.position,
            other => panic!("unexpected status: {other:?}"),
        };
        assert_eq!(portable_cursor, native_cursor);
    }
}
