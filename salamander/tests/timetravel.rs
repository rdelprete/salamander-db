//! Time-travel and fork behavior through the public API.
//! view_at(n) equals a fold stopped at n; fork diverges without disturbing
//! the parent namespace (DESIGN.md §5).

use salamander::agent::{EventBody, KvProjection, Role, SessionProjection, TranscriptEntry};
use salamander::{AgentDb, Projection};

#[test]
fn view_at_n_equals_fold_stopped_at_n() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = AgentDb::open(dir.path()).unwrap();

    let mut expected = std::collections::BTreeMap::new();
    let mut expected_at_n = None;
    for i in 0..5u32 {
        db.append(
            "ns",
            EventBody::Put {
                key: format!("k{i}"),
                value: vec![i as u8],
            },
        )
        .unwrap();
        expected.insert(format!("k{i}"), vec![i as u8]);
        if i == 2 {
            expected_at_n = Some(expected.clone());
        }
    }
    db.commit().unwrap();

    let n = 3; // events 0, 1, 2 applied -- matches the snapshot taken at i == 2
    let view: KvProjection = db.view_at(n).unwrap();
    assert_eq!(*view.state(), expected_at_n.unwrap());
    assert_eq!(view.cursor(), n);
}

#[test]
fn fork_diverges_without_mutating_parent_namespace() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = AgentDb::open(dir.path()).unwrap();

    db.append(
        "parent",
        EventBody::SessionStarted {
            agent_id: "agent-1".into(),
            config_hash: "cfg".into(),
        },
    )
    .unwrap();
    db.append(
        "parent",
        EventBody::ModelTurn {
            role: Role::User,
            content: "hi".into(),
            model: "m".into(),
        },
    )
    .unwrap();
    let fork_point = db.head();

    db.append(
        "parent",
        EventBody::ModelTurn {
            role: Role::Assistant,
            content: "parent continues".into(),
            model: "m".into(),
        },
    )
    .unwrap();
    db.commit().unwrap();

    let branch = db.fork("parent", fork_point).unwrap();
    db.append_on_branch(
        branch.id,
        "parent",
        EventBody::ModelTurn {
            role: Role::Assistant,
            content: "forked branch".into(),
            model: "m".into(),
        },
    )
    .unwrap();
    db.commit().unwrap();

    let parent_view = db.session_view("parent").unwrap();
    assert_eq!(
        turn_contents(&parent_view),
        vec!["hi".to_string(), "parent continues".to_string()]
    );

    let fork_view = db.session_view_on_branch(branch.id, "parent").unwrap();
    assert_eq!(
        turn_contents(&fork_view),
        vec!["hi".to_string(), "forked branch".to_string()]
    );
}

#[test]
fn fork_replays_pending_tool_calls_from_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = AgentDb::open(dir.path()).unwrap();

    db.append(
        "parent",
        EventBody::SessionStarted {
            agent_id: "agent-1".into(),
            config_hash: "cfg".into(),
        },
    )
    .unwrap();
    db.append(
        "parent",
        EventBody::ToolCall {
            call_id: "call-1".into(),
            tool: "search".into(),
            args_json: "{}".into(),
        },
    )
    .unwrap();
    // Forked before the matching ToolResult ever arrives.
    let fork_point = db.head();
    db.commit().unwrap();

    let branch = db.fork("parent", fork_point).unwrap();

    let fork_view = db.session_view_on_branch(branch.id, "parent").unwrap();
    assert!(fork_view.state().pending.contains_key("call-1"));

    // The parent's own pending state is unaffected by forking off of it.
    let parent_view = db.session_view("parent").unwrap();
    assert!(parent_view.state().pending.contains_key("call-1"));
}

#[test]
fn forking_same_point_twice_is_rejected() {
    use salamander::SalamanderError;

    let dir = tempfile::tempdir().unwrap();
    let mut db = AgentDb::open(dir.path()).unwrap();

    db.append(
        "parent",
        EventBody::SessionStarted {
            agent_id: "agent-1".into(),
            config_hash: "cfg".into(),
        },
    )
    .unwrap();
    db.append(
        "parent",
        EventBody::ModelTurn {
            role: Role::User,
            content: "hi".into(),
            model: "m".into(),
        },
    )
    .unwrap();
    db.commit().unwrap();
    let fork_point = db.head();

    // First fork of this parent@offset succeeds.
    db.fork("parent", fork_point).unwrap();
    let head_after_first = db.head();

    // Second fork of the *same* parent at the *same* offset collides on the
    // deterministic child namespace and must be refused (review C-3) --
    // never silently interleave two branches into one stream.
    let err = db.fork("parent", fork_point).unwrap_err();
    assert!(matches!(err, SalamanderError::BranchExists(_)));

    // The rejected fork appended nothing.
    assert_eq!(db.head(), head_after_first);
}

fn turn_contents(proj: &SessionProjection) -> Vec<String> {
    proj.state()
        .transcript
        .iter()
        .filter_map(|entry| match entry {
            TranscriptEntry::ModelTurn { content, .. } => Some(content.clone()),
            _ => None,
        })
        .collect()
}
