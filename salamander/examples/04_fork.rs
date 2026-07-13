//! Fork a session. Run with:
//!
//!     cargo run --example 04_fork
//!
//! `fork(ns, n)` branches a session at offset `n` and lets you run a
//! different future from the same shared history. The log stays linear — the
//! fork is one new `SessionStarted` event whose lineage points back at the
//! parent; `session_view` replays the shared prefix, then the branch.

use salamander::agent::{EventBody, Role, TranscriptEntry};
use salamander::{AgentDb, Projection};

fn main() -> salamander::Result<()> {
    let dir = fresh_dir("fork");
    let mut db = AgentDb::open(&dir)?;

    db.append(
        "chat",
        EventBody::SessionStarted {
            agent_id: "assistant".into(),
            config_hash: "cfg-v1".into(),
        },
    )?;
    db.append("chat", turn(Role::User, "plan a trip to Kyoto"))?;
    db.append("chat", turn(Role::Assistant, "How many days do you have?"))?;

    // Branch here — right after the question, before the user answered.
    let branch_point = db.head();

    db.append("chat", turn(Role::User, "5 days, temples focus"))?;
    db.commit()?;

    // The fork replays offsets 0..branch_point, then diverges on its own.
    let other = db.fork("chat", branch_point)?;
    db.append_on_branch(
        other.id,
        "chat",
        turn(Role::User, "actually 3 days, food focus"),
    )?;
    db.commit()?;

    println!("parent  \"chat\":");
    print_turns(&db, None, "chat")?;
    println!("\nfork    \"{}\":", other.name.as_str());
    print_turns(&db, Some(other.id), "chat")?;
    println!(
        "\n{} events total across both namespaces — the log stayed linear.",
        db.head()
    );

    Ok(())
}

/// `session_view` stitches a forked session onto its parent's replayed
/// prefix, so the branch shows the shared history followed by its own turns.
fn print_turns(
    db: &AgentDb,
    branch: Option<salamander::BranchId>,
    ns: &str,
) -> salamander::Result<()> {
    let view = match branch {
        Some(branch) => db.session_view_on_branch(branch, ns)?,
        None => db.session_view(ns)?,
    };
    for entry in &view.state().transcript {
        if let TranscriptEntry::ModelTurn { role, content, .. } = entry {
            println!("  {role:?}: {content}");
        }
    }
    Ok(())
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
