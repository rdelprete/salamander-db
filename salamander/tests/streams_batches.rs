use salamander::agent::EventBody;
use salamander::{
    AgentDb, AppendRequest, BranchId, Durability, EventId, EventType, ExpectedRevision,
    IdempotencyKey, NewEvent, ReceiptDurability, SalamanderError, StreamName, StreamRevision,
};

fn event(key: &str, value: &[u8]) -> NewEvent<EventBody> {
    NewEvent::new(
        EventType::new("test.put").unwrap(),
        EventBody::Put {
            key: key.into(),
            value: value.to_vec(),
        },
    )
}

fn request(expected: ExpectedRevision, idempotency: Option<&[u8]>) -> AppendRequest<EventBody> {
    AppendRequest {
        branch: BranchId::ZERO,
        stream: StreamName::new("orders").unwrap(),
        expected,
        idempotency_key: idempotency.map(|key| IdempotencyKey::new(key.to_vec()).unwrap()),
        events: vec![event("a", b"1"), event("b", b"2")],
        durability: Durability::Sync,
    }
}

#[test]
fn batch_assigns_contiguous_positions_and_stream_revisions() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = AgentDb::open(dir.path()).unwrap();
    let first = db
        .append_batch(request(ExpectedRevision::NoStream, None))
        .unwrap();
    assert_eq!(first.first_position, 0);
    assert_eq!(first.last_position, 1);
    assert_eq!(first.previous_revision, None);
    assert_eq!(first.current_revision, StreamRevision(1));
    assert_eq!(first.durability, ReceiptDurability::Synced);

    let second = db
        .append_batch(AppendRequest {
            branch: BranchId::ZERO,
            stream: StreamName::new("orders").unwrap(),
            expected: ExpectedRevision::Exact(StreamRevision(1)),
            idempotency_key: None,
            events: vec![event("c", b"3")],
            durability: Durability::Buffered,
        })
        .unwrap();
    assert_eq!(second.first_position, 2);
    assert_eq!(second.current_revision, StreamRevision(2));
    assert_eq!(db.head(), 3);
}

#[test]
fn revision_conflict_writes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = AgentDb::open(dir.path()).unwrap();
    db.append_batch(request(ExpectedRevision::NoStream, None))
        .unwrap();
    let head = db.head();
    let error = db
        .append_batch(request(ExpectedRevision::Exact(StreamRevision(99)), None))
        .unwrap_err();
    assert!(matches!(error, SalamanderError::RevisionConflict { .. }));
    assert_eq!(db.head(), head);
}

#[test]
fn idempotent_retry_survives_reopen_and_conflicting_reuse_fails() {
    let dir = tempfile::tempdir().unwrap();
    let original = {
        let mut db = AgentDb::open(dir.path()).unwrap();
        db.append_batch(request(ExpectedRevision::NoStream, Some(b"command-1")))
            .unwrap()
    };

    let mut db = AgentDb::open(dir.path()).unwrap();
    let retry = db
        .append_batch(request(ExpectedRevision::NoStream, Some(b"command-1")))
        .unwrap();
    assert_eq!(retry.batch_id, original.batch_id);
    assert_eq!(retry.first_position, original.first_position);
    assert_eq!(retry.last_position, original.last_position);
    assert_eq!(db.head(), 2);

    let mut conflicting = request(ExpectedRevision::Any, Some(b"command-1"));
    conflicting.events[0] = event("a", b"different");
    assert!(matches!(
        db.append_batch(conflicting),
        Err(SalamanderError::IdempotencyConflict)
    ));
    assert_eq!(db.head(), 2);
}

#[test]
fn supplied_event_id_retries_original_batch_and_rejects_changed_content() {
    let dir = tempfile::tempdir().unwrap();
    let id = EventId::from_bytes([42; 16]);
    let make = |value: &[u8]| {
        let mut new_event = event("stable", value);
        new_event.event_id = Some(id);
        AppendRequest {
            branch: BranchId::ZERO,
            stream: StreamName::new("event-ids").unwrap(),
            expected: ExpectedRevision::Any,
            idempotency_key: None,
            events: vec![new_event],
            durability: Durability::Sync,
        }
    };

    let original = {
        let mut db = AgentDb::open(dir.path()).unwrap();
        db.append_batch(make(b"same")).unwrap()
    };
    let mut db = AgentDb::open(dir.path()).unwrap();
    let retry = db.append_batch(make(b"same")).unwrap();
    assert_eq!(retry.batch_id, original.batch_id);
    assert_eq!(db.head(), 1);
    assert!(matches!(
        db.append_batch(make(b"changed")),
        Err(SalamanderError::EventIdConflict)
    ));
    assert_eq!(db.head(), 1);
}
