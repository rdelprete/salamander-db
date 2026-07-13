//! WP-3 exit tests (query-layer design §8) — the query layer through the
//! public API.
//!
//! - `incremental == batch` (INV-2): a live registered view, maintained one
//!   event at a time, must equal a fresh view replayed in one shot — at
//!   *every* prefix, not just at the end. This is the property live
//!   maintenance introduces and could break.
//! - secondary-index correctness: `by(index, key)` returns exactly the
//!   values whose *current* row maps to `key`, with no stale entries after
//!   updates and deletes.
//! - the documented usage (`get`/`range`/`prefix`/`by`) end to end.

use std::collections::BTreeMap;

use proptest::prelude::*;
use salamander::agent::EventBody;
use salamander::{AgentDb, Change, Event, IndexedView, Projection};

/// A view over the agent KV vocabulary: primary key = the KV key, value =
/// the bytes, plus a secondary index on value length.
fn len_view() -> IndexedView<String, Vec<u8>, EventBody> {
    IndexedView::builder()
        .project(|e: &Event<EventBody>| match &e.body {
            EventBody::Put { key, value } => Some(Change::put(key.clone(), value.clone())),
            EventBody::Delete { key } => Some(Change::delete(key.clone())),
            _ => None,
        })
        .index("by_len", |v: &Vec<u8>| {
            vec![(v.len() as u64).to_le_bytes().to_vec()]
        })
        .build()
}

/// Random put/delete streams over a small key space so updates and deletes
/// actually collide.
fn op_seq() -> impl Strategy<Value = Vec<EventBody>> {
    let key = (0u8..5).prop_map(|n| format!("k{n}"));
    let put = (key.clone(), proptest::collection::vec(any::<u8>(), 0..4))
        .prop_map(|(key, value)| EventBody::Put { key, value });
    let del = key.prop_map(|key| EventBody::Delete { key });
    proptest::collection::vec(prop_oneof![put, del], 0..40)
}

/// The independent oracle: fold the ops into a plain map, ignoring the view.
fn oracle(ops: &[EventBody]) -> BTreeMap<String, Vec<u8>> {
    let mut map = BTreeMap::new();
    for op in ops {
        match op {
            EventBody::Put { key, value } => {
                map.insert(key.clone(), value.clone());
            }
            EventBody::Delete { key } => {
                map.remove(key);
            }
            _ => {}
        }
    }
    map
}

proptest! {
    #[test]
    fn live_view_equals_batch_replay_at_every_prefix(ops in op_seq()) {
        let dir = tempfile::tempdir().unwrap();
        let mut db = AgentDb::open(dir.path()).unwrap();
        db.register("kv", Box::new(len_view())).unwrap();

        for op in &ops {
            db.append("ns", op.clone()).unwrap();
            let head = db.head();

            // A throwaway view replayed to the current head in one shot.
            let batch = db.replay_to(len_view(), head).unwrap();
            let live = db
                .view::<IndexedView<String, Vec<u8>, EventBody>>("kv")
                .unwrap();

            // Incremental maintenance must match the clean fold exactly.
            prop_assert_eq!(live.state(), batch.state());
        }

        // And both must match the independent oracle.
        let live = db
            .view::<IndexedView<String, Vec<u8>, EventBody>>("kv")
            .unwrap();
        prop_assert_eq!(live.state(), &oracle(&ops));
    }

    #[test]
    fn secondary_index_has_no_stale_entries(ops in op_seq()) {
        let dir = tempfile::tempdir().unwrap();
        let mut db = AgentDb::open(dir.path()).unwrap();
        db.register("kv", Box::new(len_view())).unwrap();
        for op in &ops {
            db.append("ns", op.clone()).unwrap();
        }

        let final_state = oracle(&ops);
        let live = db
            .view::<IndexedView<String, Vec<u8>, EventBody>>("kv")
            .unwrap();

        // For every length present in the final state, `by` must return
        // exactly the values whose current row has that length — a stale
        // entry from an overwritten/deleted key would show up as an extra.
        let mut expected_by_len: BTreeMap<u64, usize> = BTreeMap::new();
        for value in final_state.values() {
            *expected_by_len.entry(value.len() as u64).or_default() += 1;
        }
        for (len, count) in expected_by_len {
            let hits = live.by("by_len", &len.to_le_bytes());
            prop_assert_eq!(hits.len(), count);
            prop_assert!(hits.iter().all(|v| v.len() as u64 == len));
        }
    }
}

// ── the documented usage, end to end ─────────────────────────────────────

#[derive(Clone, Debug, PartialEq)]
struct ToolRow {
    tool: String,
}

fn tools_view() -> IndexedView<String, ToolRow, EventBody> {
    IndexedView::builder()
        .project(|e: &Event<EventBody>| match &e.body {
            EventBody::ToolCall { call_id, tool, .. } => {
                Some(Change::put(call_id.clone(), ToolRow { tool: tool.clone() }))
            }
            _ => None,
        })
        .index("by_tool", |row: &ToolRow| {
            vec![row.tool.clone().into_bytes()]
        })
        .build()
}

fn tool_call(call_id: &str, tool: &str) -> EventBody {
    EventBody::ToolCall {
        call_id: call_id.to_string(),
        tool: tool.to_string(),
        args_json: "{}".to_string(),
    }
}

#[test]
fn get_range_prefix_and_by_through_public_api() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = AgentDb::open(dir.path()).unwrap();
    db.register("tools", Box::new(tools_view())).unwrap();

    db.append("s", tool_call("t1", "grep_logs")).unwrap();
    db.append("s", tool_call("t2", "git_blame")).unwrap();
    db.append("s", tool_call("t3", "grep_logs")).unwrap();
    // A non-ToolCall event the view ignores.
    db.append(
        "s",
        EventBody::Decision {
            summary: "x".into(),
            rationale: "y".into(),
        },
    )
    .unwrap();

    let tools = db
        .view::<IndexedView<String, ToolRow, EventBody>>("tools")
        .unwrap();

    // point
    assert_eq!(
        tools.get("t1"),
        Some(&ToolRow {
            tool: "grep_logs".into()
        })
    );
    assert_eq!(tools.get("nope"), None);

    // ordered scan
    let scan: Vec<_> = tools
        .range("t1".to_string().."t3".to_string())
        .map(|(k, _)| k.clone())
        .collect();
    assert_eq!(scan, vec!["t1".to_string(), "t2".to_string()]);

    // prefix
    assert_eq!(tools.prefix("t").count(), 3);

    // secondary index: every call to grep_logs
    let grep = tools.by("by_tool", b"grep_logs");
    assert_eq!(grep.len(), 2);
    assert!(grep.iter().all(|r| r.tool == "grep_logs"));
    assert_eq!(tools.by("by_tool", b"git_blame").len(), 1);
}

#[test]
fn deregister_returns_the_view_and_removes_it() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = AgentDb::open(dir.path()).unwrap();
    db.register("tools", Box::new(tools_view())).unwrap();
    db.append("s", tool_call("t1", "grep_logs")).unwrap();

    assert!(db
        .view::<IndexedView<String, ToolRow, EventBody>>("tools")
        .is_some());

    let removed = db.deregister("tools");
    assert!(removed.is_some());
    assert!(db
        .view::<IndexedView<String, ToolRow, EventBody>>("tools")
        .is_none());
    assert!(db.deregister("tools").is_none());
}

#[test]
fn historical_view_via_replay_to_answers_as_of_n() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = AgentDb::open(dir.path()).unwrap();

    db.append("s", tool_call("t1", "grep_logs")).unwrap();
    let n = db.head(); // snapshot point: only t1 exists
    db.append("s", tool_call("t2", "git_blame")).unwrap();

    // "as of n" sees only the first call…
    let past = db.replay_to(tools_view(), n).unwrap();
    assert_eq!(past.state().len(), 1);
    assert!(past.get("t1").is_some());
    assert!(past.get("t2").is_none());

    // …while a full replay sees both.
    let now = db.replay_to(tools_view(), db.head()).unwrap();
    assert_eq!(now.state().len(), 2);
}
