//! Diff two timelines. Run with:
//!
//!     cargo run --example 08_diff -p salamander-db
//!
//! `diff` answers "where exactly do these two timelines agree, and what
//! does each say after that?" — from the branch catalog alone. A fork's
//! position is durable ancestry and inherited replay is positional, so the
//! answer is a position plus three replay plans; no record is read or
//! compared to compute it (docs/specs/first-class-diff.md).

use std::ops::Range;

use salamander::agent::{EventBody, Role};
use salamander::{AgentDb, BranchId, DiffRequest};

fn main() -> salamander::Result<()> {
    let dir = fresh_dir("diff");
    let mut db = AgentDb::open(&dir)?;

    // The debugging fable: agree on the root cause, then disagree on the fix.
    db.append("chat", turn(Role::User, "the checkout page 500s on submit"))?;
    db.append(
        "chat",
        turn(
            Role::Assistant,
            "root cause: deploy #4213 dropped a null check",
        ),
    )?;
    let decision_point = db.head();
    db.append("chat", turn(Role::Assistant, "rolling back deploy #4213"))?;
    db.commit()?;

    let fork = db.fork("chat", decision_point)?;
    db.append_on_branch(
        fork.id,
        "chat",
        turn(Role::Assistant, "forward-fixing with PR #991 instead"),
    )?;
    db.commit()?;

    // One call: the common ancestor, the exact divergence position, and a
    // replay plan for the shared prefix and each divergent suffix. The
    // plans feed `db.read` for bounded-memory streaming; this example uses
    // the positions with the range-based replay helper for brevity.
    let diff = db.diff(DiffRequest::new(BranchId::ZERO, fork.id))?;
    println!(
        "timelines \"{}\" and \"{}\" share history up to offset {}",
        diff.left.branch.name.as_str(),
        diff.right.branch.name.as_str(),
        diff.divergence
    );

    println!("\nshared prefix:");
    print_turns(&db, diff.common_ancestor.id, 0..diff.divergence)?;
    println!("\n{} since the split:", diff.left.branch.name.as_str());
    print_turns(&db, diff.left.branch.id, diff.divergence..diff.left.until)?;
    println!("\n{} since the split:", diff.right.branch.name.as_str());
    print_turns(&db, diff.right.branch.id, diff.divergence..diff.right.until)?;

    Ok(())
}

fn print_turns(db: &AgentDb, branch: BranchId, range: Range<u64>) -> salamander::Result<()> {
    db.replay_branch(branch, "chat", range, |event| {
        if let EventBody::ModelTurn { role, content, .. } = &event.body {
            println!("  {role:?}: {content}");
        }
    })
}

fn turn(role: Role, content: &str) -> EventBody {
    EventBody::ModelTurn {
        role,
        content: content.into(),
        model: "demo".into(),
    }
}

fn fresh_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("salamander-example-{name}"));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}
