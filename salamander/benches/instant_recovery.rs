use std::collections::BTreeMap;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use salamander::{
    DurabilityDto, Engine, EngineAppendBatch, EngineOptions, EventData, ExpectedRevisionDto,
    PayloadCodec, QueryConsistency, QueryDefinition, QueryOperation,
};

fn fixture(events: usize) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(EngineOptions::new(dir.path())).unwrap();
    for index in 0..events {
        engine
            .append(EngineAppendBatch {
                branch_id: [0; 16],
                stream: format!("s{}", index % 128),
                expected: ExpectedRevisionDto::Any,
                idempotency_key: None,
                durability: DurabilityDto::Buffered,
                events: vec![EventData {
                    event_id: None,
                    event_type: "row".into(),
                    schema_version: 1,
                    metadata: BTreeMap::new(),
                    codec: PayloadCodec::Json,
                    payload: serde_json::to_vec(&serde_json::json!({"id": format!("k{index}")}))
                        .unwrap(),
                }],
            })
            .unwrap();
    }
    engine.commit().unwrap();
    engine
        .register_partitioned_query(
            "rows".into(),
            QueryDefinition {
                key_field: "id".into(),
                indexes: BTreeMap::new(),
                filter: None,
            },
            64,
        )
        .unwrap();
    engine.close().unwrap();
    dir
}

fn bench(c: &mut Criterion) {
    let fixture = fixture(100_000);
    c.bench_function("instant_recovery/open_catalog_only", |b| {
        b.iter(|| {
            let engine = Engine::open(EngineOptions::new(fixture.path())).unwrap();
            black_box(engine.head().unwrap());
            engine.close().unwrap();
        })
    });
    c.bench_function("instant_recovery/heal_one_partition", |b| {
        b.iter(|| {
            let engine = Engine::open(EngineOptions::new(fixture.path())).unwrap();
            let handle = engine.query_named("rows".into()).unwrap();
            black_box(
                engine
                    .query_partitions(
                        handle,
                        vec![0],
                        QueryOperation::Len,
                        QueryConsistency::RequireHead,
                    )
                    .unwrap(),
            );
            engine.close().unwrap();
        })
    });
    c.bench_function("instant_recovery/heal_all_partitions", |b| {
        b.iter(|| {
            let engine = Engine::open(EngineOptions::new(fixture.path())).unwrap();
            let handle = engine.query_named("rows".into()).unwrap();
            black_box(engine.query(handle, QueryOperation::Len).unwrap());
            engine.close().unwrap();
        })
    });
}

criterion_group!(benches, bench);
criterion_main!(benches);
