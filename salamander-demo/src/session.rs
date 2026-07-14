//! Flagship branching-session demo.
//!
//! Scripts a fake agent debugging session, prints the transcript, then
//! forks at the root-cause decision and shows the parent and the fork
//! diverging from a shared prefix. Exercises `append` / `session_view` /
//! `fork` and the offset-as-fork-point idea end to end. This is the
//! terminal recording for the README GIF.

use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use salamander::agent::{EventBody, Role, SessionState, TranscriptEntry};
use salamander::{AgentDb, Event, Projection};

const PARENT: &str = "debug-session";
const MODEL: &str = "sonnet-x";

pub fn run() {
    let dir = scratch_dir();
    let mut db = AgentDb::open(&dir).expect("open db");

    // ── 1. Record the parent session up to the root-cause decision ──────
    append(
        &mut db,
        PARENT,
        EventBody::SessionStarted {
            agent_id: "assistant-alpha".into(),
            config_hash: "cfg-v1".into(),
        },
    );
    append(
        &mut db,
        PARENT,
        turn(
            Role::User,
            "The checkout page throws a 500 on submit. Can you find the cause?",
        ),
    );
    append(
        &mut db,
        PARENT,
        turn(
            Role::Assistant,
            "Let me check the server logs and the recent deploys.",
        ),
    );
    append(
        &mut db,
        PARENT,
        EventBody::ToolCall {
            call_id: "t1".into(),
            tool: "grep_logs".into(),
            args_json: r#"{"pattern":"500","service":"checkout"}"#.into(),
        },
    );
    append(
        &mut db,
        PARENT,
        EventBody::ToolResult {
            call_id: "t1".into(),
            ok: true,
            content: "NullPointerException in CartValidator.validate() at line 88".into(),
        },
    );
    append(
        &mut db,
        PARENT,
        EventBody::ToolCall {
            call_id: "t2".into(),
            tool: "git_blame".into(),
            args_json: r#"{"file":"CartValidator.java","line":88}"#.into(),
        },
    );
    append(
        &mut db,
        PARENT,
        EventBody::ToolResult {
            call_id: "t2".into(),
            ok: true,
            content: "line 88 last changed in deploy #4213 (2h ago): 'skip null coupon check'"
                .into(),
        },
    );
    append(
        &mut db,
        PARENT,
        EventBody::Decision {
            summary: "Root cause: deploy #4213 removed a null-check on coupon".into(),
            rationale: "The NPE stack and git blame both point at the coupon null-check removal."
                .into(),
        },
    );

    // The fork point: the next offset the log will assign. Everything at an
    // offset < here is shared history; the branch begins at exactly this
    // number (DESIGN.md §2 — the offset *is* the fork coordinate).
    let fork_point = db.head();

    // ── 2. Parent continues: it decides to roll the deploy back ─────────
    append(
        &mut db,
        PARENT,
        turn(
            Role::Assistant,
            "I'll roll back deploy #4213 to restore the null check.",
        ),
    );
    append(
        &mut db,
        PARENT,
        EventBody::ToolCall {
            call_id: "t3".into(),
            tool: "rollback_deploy".into(),
            args_json: r#"{"deploy":"4213"}"#.into(),
        },
    );
    append(
        &mut db,
        PARENT,
        EventBody::ToolResult {
            call_id: "t3".into(),
            ok: true,
            content: "rolled back to #4212. Checkout 500s stopped.".into(),
        },
    );
    append(
        &mut db,
        PARENT,
        EventBody::SessionEnded {
            reason: "resolved via rollback".into(),
        },
    );
    db.commit().expect("commit parent");

    // ── 3. Print the full parent session, marking the fork point ────────
    println!("SalamanderDB — session demo\n");
    println!("▶ Recorded a debugging session under namespace \"{PARENT}\":\n");
    print_raw_stream(&db, PARENT, fork_point);

    // ── 4. Fork at the decision and take a *different* path ─────────────
    println!("\n▶ Forking at offset {fork_point} (just after the root-cause decision)…");
    let fork = db.fork(PARENT, fork_point).expect("fork");
    println!(
        "  new branch: \"{}\"  — it replays offsets 0..{fork_point}, then diverges.\n",
        fork.name.as_str()
    );

    append_branch(
        &mut db,
        fork.id,
        turn(
            Role::Assistant,
            "Rather than roll back, I'll forward-fix the null check so we keep the coupon feature.",
        ),
    );
    append_branch(
        &mut db,
        fork.id,
        EventBody::ToolCall {
            call_id: "f1".into(),
            tool: "open_pr".into(),
            args_json: r#"{"branch":"hotfix-coupon-null"}"#.into(),
        },
    );
    append_branch(
        &mut db,
        fork.id,
        EventBody::ToolResult {
            call_id: "f1".into(),
            ok: true,
            content: "PR #991 opened with the null guard restored.".into(),
        },
    );
    append_branch(
        &mut db,
        fork.id,
        EventBody::Decision {
            summary: "Forward-fix instead of rollback".into(),
            rationale: "Keeps the coupon feature from #4213 shipped while fixing the NPE.".into(),
        },
    );
    db.commit().expect("commit fork");

    // ── 5. Show both branches side by side ──────────────────────────────
    let parent_view = db.session_view(PARENT).expect("parent view");
    let fork_view = db
        .session_view_on_branch(fork.id, PARENT)
        .expect("fork view");
    let parent_lines = transcript_labels(parent_view.state());
    let fork_lines = transcript_labels(fork_view.state());

    println!(
        "▶ Two branches, same first {} transcript entries, then divergent:\n",
        shared_prefix_len(&parent_lines, &fork_lines)
    );
    print_side_by_side(PARENT, &parent_lines, fork.name.as_str(), &fork_lines);

    println!("\n  Both branches share the history before offset {fork_point}.");
    println!("  The parent is untouched by the fork — its transcript is exactly as recorded.");
    println!(
        "  ({} events total across both namespaces; the log stayed linear.)",
        db.head()
    );

    let _ = std::fs::remove_dir_all(&dir);
}

// ── helpers ─────────────────────────────────────────────────────────────

fn append(db: &mut AgentDb, ns: &str, body: EventBody) {
    db.append(ns, body).expect("append");
}

fn append_branch(db: &mut AgentDb, branch: salamander::BranchId, body: EventBody) {
    db.append_on_branch(branch, PARENT, body)
        .expect("append branch");
}

fn turn(role: Role, content: &str) -> EventBody {
    EventBody::ModelTurn {
        role,
        content: content.into(),
        model: MODEL.into(),
    }
}

/// Print the parent's raw event stream by offset, inserting a divider at
/// the fork point so the shared-vs-branch boundary is exact.
fn print_raw_stream(db: &AgentDb, ns: &str, fork_point: u64) {
    let mut rows: Vec<(u64, String)> = Vec::new();
    db.replay(ns, 0..db.head(), |e: &Event<EventBody>| {
        rows.push((e.offset, label_event(&e.body)))
    })
    .expect("replay");

    for (offset, label) in rows {
        if offset == fork_point {
            println!(
                "      · · · · · · · · · ·  fork point (offset {fork_point})  · · · · · · · · · ·"
            );
        }
        println!("  [{offset:>2}] {}", truncate(&label, 78));
    }
}

fn print_side_by_side(parent_ns: &str, parent: &[String], fork_ns: &str, fork: &[String]) {
    const W: usize = 44;
    let diverge = shared_prefix_len(parent, fork);
    let rows = parent.len().max(fork.len());

    println!(
        "  {:<W$}   {:<W$}",
        format!("PARENT  {parent_ns}"),
        format!("FORK  {fork_ns}")
    );
    println!("  {:-<W$}   {:-<W$}", "", "");
    for i in 0..rows {
        let p = parent.get(i).map(String::as_str).unwrap_or("");
        let f = fork.get(i).map(String::as_str).unwrap_or("");
        let marker = if i == diverge { "  ◀ diverge" } else { "" };
        println!(
            "  {:<W$}   {:<W$}{}",
            truncate(p, W),
            truncate(f, W),
            marker
        );
    }
}

fn shared_prefix_len(a: &[String], b: &[String]) -> usize {
    a.iter().zip(b).take_while(|(x, y)| x == y).count()
}

fn transcript_labels(state: &SessionState) -> Vec<String> {
    state.transcript.iter().map(label_transcript).collect()
}

fn label_transcript(e: &TranscriptEntry) -> String {
    match e {
        TranscriptEntry::ModelTurn { role, content, .. } => {
            format!("{}: {content}", role_name(*role))
        }
        TranscriptEntry::ToolCall { tool, .. } => format!("→ call {tool}(…)"),
        TranscriptEntry::ToolResult { ok, content, .. } => format!("← {} {content}", ok_mark(*ok)),
        TranscriptEntry::Decision { summary, .. } => format!("★ decision: {summary}"),
    }
}

fn label_event(body: &EventBody) -> String {
    match body {
        EventBody::SessionStarted { agent_id, .. } => {
            format!("session started (agent \"{agent_id}\")")
        }
        EventBody::ModelTurn { role, content, .. } => format!("{}: {content}", role_name(*role)),
        EventBody::ToolCall { tool, .. } => format!("→ call {tool}(…)"),
        EventBody::ToolResult { ok, content, .. } => format!("← {} {content}", ok_mark(*ok)),
        EventBody::Decision { summary, .. } => format!("★ decision: {summary}"),
        EventBody::SessionEnded { reason } => format!("session ended: {reason}"),
        EventBody::Put { .. } | EventBody::Delete { .. } => "(kv event)".into(),
    }
}

fn role_name(role: Role) -> &'static str {
    match role {
        Role::System => "system",
        Role::User => "user",
        Role::Assistant => "assistant",
        Role::Tool => "tool",
    }
}

fn ok_mark(ok: bool) -> &'static str {
    if ok {
        "ok"
    } else {
        "ERR"
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let kept: String = s.chars().take(max.saturating_sub(1)).collect();
        format!("{kept}…")
    }
}

fn scratch_dir() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let mut dir = std::env::temp_dir();
    dir.push(format!(
        "salamander-session-demo-{}-{}",
        std::process::id(),
        nanos
    ));
    dir
}
