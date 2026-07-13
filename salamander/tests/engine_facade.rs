use std::collections::BTreeMap;
use std::sync::{Arc, Barrier};
use std::thread;

use salamander::{
    DurabilityDto, Engine, EngineAppendBatch, EngineOptions, ErrorCategory, EventData,
    ExpectedRevisionDto, PayloadCodec, QueryDefinition, QueryOperation, ReplayRequest,
    MAX_FACADE_PAYLOAD_BYTES,
};

fn batch(stream: &str, payload: Vec<u8>, codec: PayloadCodec) -> EngineAppendBatch {
    EngineAppendBatch {
        branch_id: [0; 16],
        stream: stream.into(),
        expected: ExpectedRevisionDto::Any,
        idempotency_key: None,
        events: vec![EventData {
            event_id: None,
            event_type: "test.event".into(),
            schema_version: 1,
            metadata: BTreeMap::new(),
            codec,
            payload,
        }],
        durability: DurabilityDto::Buffered,
    }
}

#[test]
fn multiple_host_threads_share_one_total_append_order() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(EngineOptions::new(dir.path())).unwrap();
    let barrier = Arc::new(Barrier::new(9));
    let mut threads = Vec::new();
    for worker in 0..8u8 {
        let engine = engine.clone();
        let barrier = barrier.clone();
        threads.push(thread::spawn(move || {
            barrier.wait();
            (0..50)
                .map(|n| {
                    engine
                        .append(batch("shared", vec![worker, n], PayloadCodec::Bytes))
                        .unwrap()
                        .first_position
                })
                .collect::<Vec<_>>()
        }));
    }
    barrier.wait();
    let mut positions = threads
        .into_iter()
        .flat_map(|thread| thread.join().unwrap())
        .collect::<Vec<_>>();
    positions.sort_unstable();
    assert_eq!(positions, (0..400).collect::<Vec<_>>());
    assert_eq!(engine.head().unwrap(), 400);
}

#[test]
fn engine_handle_is_safely_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Engine>();
}

#[test]
fn concurrent_no_stream_check_has_exactly_one_winner() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(EngineOptions::new(dir.path())).unwrap();
    let barrier = Arc::new(Barrier::new(5));
    let threads: Vec<_> = (0..4)
        .map(|n| {
            let engine = engine.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                let mut request = batch("once", vec![n], PayloadCodec::Bytes);
                request.expected = ExpectedRevisionDto::NoStream;
                barrier.wait();
                engine.append(request)
            })
        })
        .collect();
    barrier.wait();
    let results: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter_map(|result| result.as_ref().err())
            .filter(|error| error.category == ErrorCategory::Conflict)
            .count(),
        3
    );
}

#[test]
fn paged_replay_round_trips_empty_and_non_utf8_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(EngineOptions::new(dir.path())).unwrap();
    engine
        .append(batch("bytes", Vec::new(), PayloadCodec::Bytes))
        .unwrap();
    engine
        .append(batch("bytes", vec![0, 0xff, 0x80, 1], PayloadCodec::Bytes))
        .unwrap();
    let handle = engine
        .open_reader(ReplayRequest {
            stream: Some("bytes".into()),
            page_events: 1,
            ..ReplayRequest::default()
        })
        .unwrap();
    let first = engine.next_page(handle).unwrap();
    let second = engine.next_page(handle).unwrap();
    assert_eq!(first.records[0].payload, Vec::<u8>::new());
    assert_eq!(second.records[0].payload, vec![0, 0xff, 0x80, 1]);
    assert_eq!(first.continuation, 1);
    assert_eq!(second.continuation, 2);
}

#[test]
fn reader_head_is_pinned_when_handle_opens() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(EngineOptions::new(dir.path())).unwrap();
    engine
        .append(batch("s", vec![1], PayloadCodec::Bytes))
        .unwrap();
    let reader = engine.open_reader(ReplayRequest::default()).unwrap();
    engine
        .append(batch("s", vec![2], PayloadCodec::Bytes))
        .unwrap();
    let page = engine.next_page(reader).unwrap();
    assert_eq!(page.records.len(), 1);
    assert!(page.done);
}

#[test]
fn invalid_json_is_a_codec_error_and_appends_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(EngineOptions::new(dir.path())).unwrap();
    let error = engine
        .append(batch("json", b"{".to_vec(), PayloadCodec::Json))
        .unwrap_err();
    assert_eq!(error.category, ErrorCategory::Codec);
    assert_eq!(engine.head().unwrap(), 0);
}

#[test]
fn cancellation_and_closed_handles_are_typed() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(EngineOptions::new(dir.path())).unwrap();
    let reader = engine.open_reader(ReplayRequest::default()).unwrap();
    engine.cancel_reader(reader).unwrap();
    assert_eq!(
        engine.next_page(reader).unwrap_err().category,
        ErrorCategory::Cancelled
    );
    engine.close_reader(reader).unwrap();
    assert_eq!(
        engine.next_page(reader).unwrap_err().category,
        ErrorCategory::NotFound
    );
    engine.close().unwrap();
    assert_eq!(
        engine.head().unwrap_err().category,
        ErrorCategory::Cancelled
    );
}

#[test]
fn payload_and_page_limits_fail_before_allocation_or_write() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(EngineOptions::new(dir.path())).unwrap();
    let error = engine
        .append(batch(
            "too-large",
            vec![0; MAX_FACADE_PAYLOAD_BYTES + 1],
            PayloadCodec::Bytes,
        ))
        .unwrap_err();
    assert_eq!(error.category, ErrorCategory::ResourceLimit);
    assert_eq!(engine.head().unwrap(), 0);
    assert_eq!(
        engine
            .open_reader(ReplayRequest {
                page_events: 0,
                ..ReplayRequest::default()
            })
            .unwrap_err()
            .category,
        ErrorCategory::ResourceLimit
    );
}

#[test]
fn branch_pages_and_declarative_queries_are_engine_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(EngineOptions::new(dir.path())).unwrap();
    engine
        .append(batch(
            "chat",
            br#"{"id":"a","kind":"user"}"#.to_vec(),
            PayloadCodec::Json,
        ))
        .unwrap();
    let branch = engine
        .fork([0; 16], 1, "alternative".into(), BTreeMap::new())
        .unwrap();
    let mut child = batch(
        "chat",
        br#"{"id":"b","kind":"tool"}"#.to_vec(),
        PayloadCodec::Json,
    );
    child.branch_id = branch.id;
    engine.append(child).unwrap();
    let reader = engine
        .open_reader(ReplayRequest {
            branch_id: branch.id,
            stream: Some("chat".into()),
            ..ReplayRequest::default()
        })
        .unwrap();
    assert_eq!(engine.next_page(reader).unwrap().records.len(), 2);

    let query = engine
        .register_query(
            "events".into(),
            QueryDefinition {
                key_field: "id".into(),
                indexes: BTreeMap::from([("by_kind".into(), "kind".into())]),
                filter: None,
            },
        )
        .unwrap();
    let result = engine
        .query(query, QueryOperation::Get("a".into()))
        .unwrap();
    assert_eq!(result.rows, vec![br#"{"id":"a","kind":"user"}"#.to_vec()]);
    assert_eq!(engine.query(query, QueryOperation::Len).unwrap().len, 1);
}
