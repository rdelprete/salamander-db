use std::collections::BTreeMap;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use salamander::{
    CommittedBatch, DurabilityDto, Engine, EngineAppendBatch, EngineOptions, ErrorCategory,
    EventData, ExpectedRevisionDto, FeedFilter, FeedRequest, PayloadCodec,
};

fn batch(
    stream: &str,
    event_type: &str,
    payloads: &[&[u8]],
    durability: DurabilityDto,
) -> EngineAppendBatch {
    EngineAppendBatch {
        branch_id: [0; 16],
        stream: stream.into(),
        expected: ExpectedRevisionDto::Any,
        idempotency_key: None,
        events: payloads
            .iter()
            .map(|payload| EventData {
                event_id: None,
                event_type: event_type.into(),
                schema_version: 1,
                metadata: BTreeMap::new(),
                codec: PayloadCodec::Bytes,
                payload: payload.to_vec(),
            })
            .collect(),
        durability,
    }
}

fn request(from: Option<u64>) -> FeedRequest {
    FeedRequest {
        from,
        ..FeedRequest::default()
    }
}

fn collect(engine: &Engine, from: u64) -> Vec<CommittedBatch> {
    let feed = engine.open_feed(request(Some(from))).unwrap();
    let mut result = Vec::new();
    loop {
        let page = engine.next_feed_page(feed, None).unwrap();
        result.extend(page.batches);
        if page.continuation == page.durable_head {
            break;
        }
    }
    engine.close_feed(feed).unwrap();
    result
}

#[test]
fn feed_exposes_only_complete_durable_batches_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let expected_ids;
    {
        let engine = Engine::open(EngineOptions::new(dir.path())).unwrap();
        let first = engine
            .append(batch(
                "orders",
                "order.created",
                &[b"a", b"b"],
                DurabilityDto::Buffered,
            ))
            .unwrap();
        assert_eq!(engine.head().unwrap(), 2);
        assert_eq!(engine.durable_head().unwrap(), 0);
        assert!(collect(&engine, 0).is_empty());
        engine.commit().unwrap();
        let second = engine
            .append(batch("orders", "order.paid", &[b"c"], DurabilityDto::Sync))
            .unwrap();
        expected_ids = vec![first.batch_id, second.batch_id];
        let feed = collect(&engine, 0);
        assert_eq!(
            feed.iter().map(|batch| batch.batch_id).collect::<Vec<_>>(),
            expected_ids
        );
        assert_eq!(feed[0].events.len(), 2);
        assert!(feed.iter().all(|batch| batch
            .events
            .iter()
            .all(|event| event.batch_id == batch.batch_id)));
        engine.close().unwrap();
    }
    let reopened = Engine::open(EngineOptions::new(dir.path())).unwrap();
    assert_eq!(
        collect(&reopened, 0)
            .iter()
            .map(|batch| batch.batch_id)
            .collect::<Vec<_>>(),
        expected_ids
    );
}

#[test]
fn pages_resume_without_gaps_or_duplicates_and_preserve_filter_boundaries() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(EngineOptions::new(dir.path())).unwrap();
    for (stream, kind, value) in [
        ("a", "wanted", b"1".as_slice()),
        ("b", "ignored", b"2"),
        ("a", "wanted", b"3"),
    ] {
        engine
            .append(batch(stream, kind, &[value], DurabilityDto::Sync))
            .unwrap();
    }
    let feed = engine
        .open_feed(FeedRequest {
            from: Some(0),
            filter: FeedFilter {
                event_types: vec!["wanted".into()],
                ..FeedFilter::default()
            },
            page_batches: 1,
            page_bytes: 1024,
            consumer_id: None,
        })
        .unwrap();
    let first = engine.next_feed_page(feed, None).unwrap();
    assert_eq!(first.batches.len(), 1);
    let resume = first.continuation;
    let second = engine.next_feed_page(feed, None).unwrap();
    assert_eq!(second.batches.len(), 1);
    assert_eq!(second.batches[0].events[0].payload, b"3");
    let resumed = engine
        .open_feed(FeedRequest {
            from: Some(resume),
            page_batches: 1,
            ..request(Some(resume))
        })
        .unwrap();
    let replayed = engine.next_feed_page(resumed, None).unwrap();
    assert_eq!(replayed.batches, second.batches);
}

#[test]
fn durable_advance_wakes_waiter_but_buffering_does_not() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(EngineOptions::new(dir.path())).unwrap();
    let feed = engine.open_feed(request(Some(0))).unwrap();
    let waiter = engine.clone();
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || tx.send(waiter.next_feed_page(feed, Some(5_000))).unwrap());
    thread::sleep(Duration::from_millis(50));
    engine
        .append(batch("orders", "created", &[b"x"], DurabilityDto::Buffered))
        .unwrap();
    assert!(rx.recv_timeout(Duration::from_millis(100)).is_err());
    engine.commit().unwrap();
    let page = rx.recv_timeout(Duration::from_secs(2)).unwrap().unwrap();
    assert_eq!(page.batches.len(), 1);
    assert!(!page.timed_out);
}

#[test]
fn cancellation_and_engine_close_unblock_waiters_promptly() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(EngineOptions::new(dir.path())).unwrap();
    let feed = engine.open_feed(request(Some(0))).unwrap();
    let waiter = engine.clone();
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || tx.send(waiter.next_feed_page(feed, Some(30_000))).unwrap());
    thread::sleep(Duration::from_millis(50));
    let started = Instant::now();
    engine.cancel_feed(feed).unwrap();
    let error = rx
        .recv_timeout(Duration::from_secs(2))
        .unwrap()
        .unwrap_err();
    assert_eq!(error.category, ErrorCategory::Cancelled);
    assert!(started.elapsed() < Duration::from_secs(2));

    let feed = engine.open_feed(request(Some(0))).unwrap();
    let waiter = engine.clone();
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || tx.send(waiter.next_feed_page(feed, Some(30_000))).unwrap());
    thread::sleep(Duration::from_millis(50));
    engine.close().unwrap();
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap_err()
            .category,
        ErrorCategory::Cancelled
    );
}

#[test]
fn consumer_checkpoint_survives_reopen_and_can_be_cleared() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(EngineOptions::new(dir.path())).unwrap();
    engine
        .append(batch("orders", "created", &[b"x"], DurabilityDto::Sync))
        .unwrap();
    let feed = engine
        .open_feed(FeedRequest {
            consumer_id: Some("projection-a".into()),
            ..request(Some(0))
        })
        .unwrap();
    let page = engine.next_feed_page(feed, None).unwrap();
    assert_eq!(engine.acknowledge_feed(feed).unwrap(), page.continuation);
    engine.close().unwrap();

    let engine = Engine::open(EngineOptions::new(dir.path())).unwrap();
    let resumed = engine
        .open_feed(FeedRequest {
            consumer_id: Some("projection-a".into()),
            ..request(None)
        })
        .unwrap();
    assert!(engine
        .next_feed_page(resumed, None)
        .unwrap()
        .batches
        .is_empty());
    engine
        .clear_consumer_checkpoint("projection-a".into())
        .unwrap();
    let reset = engine
        .open_feed(FeedRequest {
            consumer_id: Some("projection-a".into()),
            ..request(None)
        })
        .unwrap();
    assert_eq!(engine.next_feed_page(reset, None).unwrap().batches.len(), 1);
}

#[test]
fn follower_ingestion_preserves_identity_and_is_idempotent() {
    let source_dir = tempfile::tempdir().unwrap();
    let target_dir = tempfile::tempdir().unwrap();
    let source = Engine::open(EngineOptions::new(source_dir.path())).unwrap();
    source
        .append(batch(
            "orders",
            "created",
            &[b"one", b"two"],
            DurabilityDto::Sync,
        ))
        .unwrap();
    let original = collect(&source, 0).remove(0);
    let target = Engine::open(EngineOptions::new(target_dir.path())).unwrap();
    let first = target.ingest_batch(original.clone()).unwrap();
    let retry = target.ingest_batch(original.clone()).unwrap();
    assert_eq!(first.batch_id, original.batch_id);
    assert_eq!(retry, first);
    assert_eq!(target.head().unwrap(), 2);
    let replicated = collect(&target, 0).remove(0);
    assert_eq!(replicated.batch_id, original.batch_id);
    assert_eq!(
        replicated
            .events
            .iter()
            .map(|event| event.event_id)
            .collect::<Vec<_>>(),
        original
            .events
            .iter()
            .map(|event| event.event_id)
            .collect::<Vec<_>>()
    );

    let mut conflicting = original;
    conflicting.events[0].payload = b"changed".to_vec();
    assert_eq!(
        target.ingest_batch(conflicting).unwrap_err().category,
        ErrorCategory::Conflict
    );
}

#[test]
fn future_position_is_explicitly_unavailable() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(EngineOptions::new(dir.path())).unwrap();
    let error = engine.open_feed(request(Some(1))).unwrap_err();
    assert_eq!(error.code, "position_unavailable");
}
