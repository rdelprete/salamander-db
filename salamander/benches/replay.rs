//! WP-04 — streaming-reader throughput and selectivity.
//!
//! Three measurements per size:
//!
//! - `full_scan` — stream every record through the bounded reader
//!   (replaces the old materializing scan; linear, but O(1) memory).
//! - `single_stream` — a `StreamSelector::Streams` plan for one of
//!   `STREAM_FAN` streams. With sidecar postings this touches only the
//!   selected records' segments; the gap to `full_scan / STREAM_FAN`
//!   measures the selective-read win WP-09 healing relies on.
//! - `tail_seek` — read the final 100 records. Sub-linear thanks to
//!   segment binary search plus in-segment seek points.
//!
//! Sizes come from `SALAMANDER_BENCH_SIZES` (comma-separated counts);
//! default `100000,1000000`. The spec's canonical runs:
//!
//! ```text
//! SALAMANDER_BENCH_SIZES=1000000 cargo bench --bench replay
//! SALAMANDER_BENCH_SIZES=10000000 cargo bench --bench replay   # 10M physical
//! ```
//!
//! (The 100M *planning* case is a unit test — `intersecting_range` over
//! synthetic segment metadata — since it measures index arithmetic, not
//! I/O.)

use std::ops::Bound;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use salamander::agent::EventBody;
use salamander::{
    AgentDb, AppendRequest, BranchId, Durability, EventType, ExpectedRevision, NewEvent,
    RecordReader, ReplayPlan, StreamId, StreamName, StreamSelector,
};
use tempfile::TempDir;

const STREAM_FAN: u64 = 16;

fn sizes() -> Vec<u64> {
    match std::env::var("SALAMANDER_BENCH_SIZES") {
        Ok(s) => s
            .split(',')
            .filter_map(|p| p.trim().parse::<u64>().ok())
            .filter(|&n| n > 0)
            .collect(),
        Err(_) => vec![100_000, 1_000_000],
    }
}

/// `n` events spread round-robin over `STREAM_FAN` streams, committed in
/// batches of 64 to keep fixture generation fast. Returns the fixture dir
/// and one stream's id for selective plans.
fn populate(n: u64) -> (TempDir, StreamId) {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut chosen = None;
    {
        let mut db = AgentDb::open(dir.path()).expect("open");
        let mut appended = 0u64;
        while appended < n {
            let stream = appended % STREAM_FAN;
            let batch = (n - appended).min(64);
            let receipt = db
                .append_batch(AppendRequest {
                    branch: BranchId::ZERO,
                    stream: StreamName::new(format!("stream-{stream}")).expect("name"),
                    expected: ExpectedRevision::Any,
                    idempotency_key: None,
                    events: (0..batch)
                        .map(|i| {
                            NewEvent::new(
                                EventType::new("bench.put").expect("type"),
                                EventBody::Put {
                                    key: "k".into(),
                                    value: (appended + i).to_le_bytes().to_vec(),
                                },
                            )
                        })
                        .collect(),
                    durability: Durability::Buffered,
                })
                .expect("append");
            if stream == 0 {
                chosen = Some(receipt.stream_id);
            }
            appended += batch;
        }
        db.commit().expect("commit");
    }
    (dir, chosen.expect("at least one batch"))
}

fn drain(db: &AgentDb, plan: ReplayPlan) -> u64 {
    let mut reader = db.read(plan).expect("plan");
    let mut count = 0u64;
    while reader.next().expect("read").is_some() {
        count += 1;
    }
    count
}

fn bench_replay(c: &mut Criterion) {
    let mut group = c.benchmark_group("replay");
    group.sample_size(10);

    for n in sizes() {
        let (dir, stream) = populate(n);
        let db = AgentDb::open(dir.path()).expect("reopen");
        group.throughput(Throughput::Elements(n));

        group.bench_with_input(BenchmarkId::new("full_scan", n), &n, |b, _| {
            b.iter(|| black_box(drain(&db, ReplayPlan::default())))
        });

        group.bench_with_input(BenchmarkId::new("single_stream", n), &n, |b, _| {
            b.iter(|| {
                black_box(drain(
                    &db,
                    ReplayPlan {
                        streams: StreamSelector::Streams(vec![stream]),
                        ..ReplayPlan::default()
                    },
                ))
            })
        });

        group.bench_with_input(BenchmarkId::new("tail_seek", n), &n, |b, _| {
            b.iter(|| {
                black_box(drain(
                    &db,
                    ReplayPlan {
                        from: Bound::Included(n.saturating_sub(100)),
                        ..ReplayPlan::default()
                    },
                ))
            })
        });
    }
    group.finish();
}

criterion_group!(benches, bench_replay);
criterion_main!(benches);
