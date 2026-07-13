use std::collections::BTreeMap;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use salamander::{
    DurabilityDto, Engine, EngineAppendBatch, EngineOptions, EventData, ExpectedRevisionDto,
    QueryDefinition, QueryOperation,
};

fn populate(path: &std::path::Path, count: u64, snapshot: bool) {
    let engine = Engine::open(EngineOptions::new(path)).unwrap();
    let handle = engine
        .register_query(
            "rows".into(),
            QueryDefinition {
                key_field: "id".into(),
                indexes: BTreeMap::new(),
                filter: None,
            },
        )
        .unwrap();
    for index in 0..count {
        engine
            .append(EngineAppendBatch {
                branch_id: [0; 16],
                stream: "events".into(),
                expected: ExpectedRevisionDto::Any,
                idempotency_key: None,
                events: vec![EventData::json(
                    serde_json::to_vec(&serde_json::json!({"id": index.to_string()})).unwrap(),
                )],
                durability: DurabilityDto::Buffered,
            })
            .unwrap();
    }
    engine.commit().unwrap();
    if snapshot {
        engine.create_snapshot(handle).unwrap();
    }
    engine.close().unwrap();
}

fn open_and_count(path: &std::path::Path) -> u64 {
    let engine = Engine::open(EngineOptions::new(path)).unwrap();
    let handle = engine.query_named("rows".into()).unwrap();
    let count = engine.query(handle, QueryOperation::Len).unwrap().len;
    engine.close().unwrap();
    count
}

fn bench_snapshot_restore(c: &mut Criterion) {
    let count = std::env::var("SALAMANDER_SNAPSHOT_BENCH_EVENTS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(100_000);
    let cold = tempfile::tempdir().unwrap();
    let warm = tempfile::tempdir().unwrap();
    populate(cold.path(), count, false);
    populate(warm.path(), count, true);

    c.bench_function("projection_open/full_replay", |bench| {
        bench.iter(|| black_box(open_and_count(cold.path())))
    });
    c.bench_function("projection_open/snapshot_restore", |bench| {
        bench.iter(|| black_box(open_and_count(warm.path())))
    });
}

criterion_group!(benches, bench_snapshot_restore);
criterion_main!(benches);
