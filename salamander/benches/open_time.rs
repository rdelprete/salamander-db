//! Cold-open and eager-replay scaling benchmark.
//!
//! Criterion bench: cold-start cost vs. log size — the "baseline villain"
//! that motivates Phase 2 (DESIGN.md §9, §8). Two measurements per size:
//!
//! - `open_only` — `AgentDb::open`, which recovers the *log* (scans the
//!   active segment for a torn tail) but replays no projections. Roughly
//!   flat in log size: only the last segment is touched.
//! - `open_and_replay` — `open` plus a full-log `KvProjection` rebuild.
//!   Linear in log size — this is the cost Phase 1 pays to reach usable
//!   derived state, and what instant recovery (Phase 2) exists to erase.
//!
//! Sizes come from the `SALAMANDER_BENCH_SIZES` env var (comma-separated
//! event counts); the default is a quick `100000,1000000`. The canonical
//! large-scale run is:
//!
//! ```text
//! SALAMANDER_BENCH_SIZES=1000000,10000000,50000000 cargo bench --bench open_time
//! ```
//!
//! Fixtures are generated once per size, outside the measured loop.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use salamander::agent::{EventBody, KvProjection};
use salamander::{AgentDb, Projection};
use tempfile::TempDir;

const NAMESPACE: &str = "bench";

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

/// Build a fixture directory holding `n` committed events. One constant key
/// keeps the projection's map size flat, so `open_and_replay` measures the
/// pure per-event decode+apply cost rather than BTree growth.
fn populate(n: u64) -> TempDir {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let mut db = AgentDb::open(dir.path()).expect("open");
        for i in 0..n {
            db.append(
                NAMESPACE,
                EventBody::Put {
                    key: "bench".to_string(),
                    value: i.to_le_bytes().to_vec(),
                },
            )
            .expect("append");
        }
        db.commit().expect("commit");
    } // drop releases the single-writer lock so the bench can reopen freely
    dir
}

fn bench_open_time(c: &mut Criterion) {
    // Generate every fixture up front; each stays alive (as a TempDir) for
    // the whole run and is cleaned up when this Vec drops.
    let fixtures: Vec<(u64, TempDir)> = sizes().into_iter().map(|n| (n, populate(n))).collect();

    let mut group = c.benchmark_group("open_time");
    // Opening a large log is expensive; 10 is criterion's minimum sample
    // count and keeps a 50M-event run from taking hours.
    group.sample_size(10);

    for (n, dir) in &fixtures {
        group.throughput(Throughput::Elements(*n));

        group.bench_with_input(BenchmarkId::new("open_only", n), dir, |b, dir| {
            b.iter(|| {
                let db = AgentDb::open(dir.path()).expect("open");
                black_box(db.head())
            });
        });

        group.bench_with_input(BenchmarkId::new("open_and_replay", n), dir, |b, dir| {
            b.iter(|| {
                let db = AgentDb::open(dir.path()).expect("open");
                let kv = db.projection::<KvProjection>().expect("projection");
                black_box(kv.state().len())
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_open_time);
criterion_main!(benches);
