//! WP-5 groundwork — the dynamic-JSON payload surface (`JsonDb` / `Json`)
//! that the Python bindings will bind to. Proves the *whole* engine stack
//! works over dynamic JSON with no engine changes: append / commit / reopen
//! / replay, a registered `IndexedView` with a secondary index, group
//! commit, and time-travel — exactly what the bindings need to expose.
//!
//! The `regression` test below pins the reason `Json` exists: a bare
//! `serde_json::Value` payload does *not* round-trip under bincode.

use salamander::{Change, Event, IndexedView, Json, JsonDb, Projection};
use serde_json::{json, Value};

fn j(value: Value) -> Json {
    Json(value)
}

#[test]
fn json_payloads_survive_reopen_and_replay() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = JsonDb::open(dir.path()).unwrap();
        db.append(
            "s",
            j(json!({ "kind": "user_msg", "text": "find the bug" })),
        )
        .unwrap();
        db.append(
            "s",
            j(json!({ "kind": "tool_call", "tool": "grep", "args": { "q": "500" } })),
        )
        .unwrap();
        db.append(
            "s",
            j(json!({ "kind": "decision", "summary": "root cause" })),
        )
        .unwrap();
        db.commit().unwrap();
        assert_eq!(db.head(), 3);
    }

    // Reopen cold and read the dynamic payloads back, nested fields and all.
    let db = JsonDb::open(dir.path()).unwrap();
    let mut kinds = Vec::new();
    let mut nested = None;
    db.replay("s", 0..db.head(), |e: &Event<Json>| {
        kinds.push(e.body.get("kind").unwrap().as_str().unwrap().to_string());
        if e.body.get("kind").unwrap() == "tool_call" {
            nested = e.body["args"]["q"].as_str().map(str::to_string);
        }
    })
    .unwrap();

    assert_eq!(kinds, ["user_msg", "tool_call", "decision"]);
    assert_eq!(nested.as_deref(), Some("500"));
}

#[test]
fn indexed_view_over_json_payloads() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = JsonDb::open(dir.path()).unwrap();

    // Key each row by its "id" field; index by the "kind" field. Both are
    // extracted dynamically from the JSON — the shape the Python side uses.
    let view = IndexedView::builder()
        .project(|e: &Event<Json>| {
            let id = e.body.get("id")?.as_str()?.to_string();
            Some(Change::put(id, e.body.clone()))
        })
        .index("by_kind", |v: &Json| {
            v.get("kind")
                .and_then(Value::as_str)
                .map(|s| vec![s.as_bytes().to_vec()])
                .unwrap_or_default()
        })
        .build();
    db.register("events", Box::new(view)).unwrap();

    db.append("s", j(json!({ "id": "a", "kind": "user" })))
        .unwrap();
    db.append("s", j(json!({ "id": "b", "kind": "tool" })))
        .unwrap();
    db.append("s", j(json!({ "id": "c", "kind": "user" })))
        .unwrap();
    // A payload with no "id" is ignored by the view's projector.
    db.append("s", j(json!({ "note": "no id here" }))).unwrap();

    let view = db
        .view::<IndexedView<String, Json, Json>>("events")
        .unwrap();

    assert_eq!(view.get("a").unwrap().get("kind").unwrap(), "user");
    assert_eq!(view.len(), 3);
    assert_eq!(view.by("by_kind", b"user").len(), 2);
    assert_eq!(view.by("by_kind", b"tool").len(), 1);
    assert!(view.by("by_kind", b"missing").is_empty());
}

#[test]
fn json_db_honors_a_commit_policy() {
    use salamander::CommitPolicy;

    let dir = tempfile::tempdir().unwrap();
    let mut db = JsonDb::open_with_policy(dir.path(), CommitPolicy::every_count(2)).unwrap();

    db.append("s", j(json!({ "n": 1 }))).unwrap();
    assert_eq!(db.uncommitted_count(), 1);
    db.append("s", j(json!({ "n": 2 }))).unwrap();
    assert_eq!(db.uncommitted_count(), 0, "count=2 policy auto-commits");
}

#[test]
fn historical_json_view_via_replay_to() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = JsonDb::open(dir.path()).unwrap();

    db.append("s", j(json!({ "id": "a" }))).unwrap();
    let n = db.head();
    db.append("s", j(json!({ "id": "b" }))).unwrap();

    let builder = || {
        IndexedView::<String, Json, Json>::builder()
            .project(|e: &Event<Json>| {
                let id = e.body.get("id")?.as_str()?.to_string();
                Some(Change::put(id, e.body.clone()))
            })
            .build()
    };

    let past = db.replay_to(builder(), n).unwrap();
    assert_eq!(past.state().len(), 1);
    assert!(past.get("a").is_some() && past.get("b").is_none());

    let now = db.replay_to(builder(), db.head()).unwrap();
    assert_eq!(now.state().len(), 2);
}

/// Pins *why* [`Json`] exists: the bare `serde_json::Value` payload the spec
/// first assumed does not round-trip, because bincode can't drive `Value`'s
/// `deserialize_any`. If a future self-describing payload codec fixes this,
/// this test will start failing and should be revisited (WP-5 design §2).
#[test]
fn bare_serde_json_value_payload_fails_to_replay() {
    use salamander::Salamander;

    let dir = tempfile::tempdir().unwrap();
    let mut db: Salamander<Value> = Salamander::open(dir.path()).unwrap();
    db.append("s", json!({ "x": 1 })).unwrap();
    db.commit().unwrap();

    // Replay tries to bincode-decode the Value payload and fails.
    let err = db
        .replay("s", 0..db.head(), |_e: &Event<Value>| {})
        .unwrap_err();
    assert!(
        matches!(err, salamander::SalamanderError::Corrupt { .. }),
        "expected a Corrupt decode error, got {err:?}"
    );
}
