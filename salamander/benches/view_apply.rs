//! WP-3 / query-layer design OQ-Q3 — the per-view apply-cost bench.
//!
//! "Document and trust": no caps, no warnings on fan-out cost, but the docs
//! should cite a *measured* number rather than a vibe. This measures the
//! marginal cost of maintaining one `IndexedView` per appended event, with
//! and without a secondary index, isolated from log I/O by driving
//! `View::apply` directly on pre-built events.

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use salamander::agent::EventBody;
use salamander::{Change, Event, IndexedView, View};

const N: u64 = 100_000;

fn events() -> Vec<Event<EventBody>> {
    (0..N)
        .map(|i| Event {
            offset: i,
            timestamp_ms: 0,
            namespace: "ns".to_string(),
            // 1024 distinct keys → the fold exercises both inserts and
            // updates (overwrites), the realistic maintenance mix.
            body: EventBody::Put {
                key: format!("k{}", i % 1024),
                value: i.to_le_bytes().to_vec(),
            },
        })
        .collect()
}

fn primary_only() -> IndexedView<String, Vec<u8>, EventBody> {
    IndexedView::builder().project(project).build()
}

fn with_secondary_index() -> IndexedView<String, Vec<u8>, EventBody> {
    IndexedView::builder()
        .project(project)
        .index("by_len", |v: &Vec<u8>| {
            vec![(v.len() as u64).to_le_bytes().to_vec()]
        })
        .build()
}

fn project(e: &Event<EventBody>) -> Option<Change<String, Vec<u8>>> {
    match &e.body {
        EventBody::Put { key, value } => Some(Change::put(key.clone(), value.clone())),
        EventBody::Delete { key } => Some(Change::delete(key.clone())),
        _ => None,
    }
}

fn bench_view_apply(c: &mut Criterion) {
    let evs = events();

    let mut group = c.benchmark_group("view_apply");
    group.throughput(Throughput::Elements(N));

    group.bench_function("primary_only", |b| {
        b.iter(|| {
            let mut view = primary_only();
            for e in &evs {
                view.apply(black_box(e));
            }
            black_box(view.cursor())
        });
    });

    group.bench_function("with_secondary_index", |b| {
        b.iter(|| {
            let mut view = with_secondary_index();
            for e in &evs {
                view.apply(black_box(e));
            }
            black_box(view.cursor())
        });
    });

    group.finish();
}

criterion_group!(benches, bench_view_apply);
criterion_main!(benches);
