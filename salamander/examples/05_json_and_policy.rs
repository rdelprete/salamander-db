//! Dynamic JSON + group commit. Run with:
//!
//!     cargo run --example 05_json_and_policy
//!
//! `JsonDb` carries arbitrary JSON payloads (the surface the Python bindings
//! bind to), and a `CommitPolicy` lets the DB fsync on a threshold instead
//! of you calling `commit()` by hand.

use salamander::{CommitPolicy, Event, Json, JsonDb};
use serde_json::json;

fn main() -> salamander::Result<()> {
    let dir = fresh_dir("json");

    // Auto-commit every 2 events; no manual commit() needed.
    let mut db = JsonDb::open_with_policy(&dir, CommitPolicy::every_count(2))?;

    db.append("s", Json(json!({ "kind": "user_msg", "text": "hi" })))?;
    println!("after 1 append: {} uncommitted", db.uncommitted_count());

    db.append(
        "s",
        Json(json!({ "kind": "tool_call", "tool": "search", "args": { "q": "kyoto" } })),
    )?;
    println!(
        "after 2 appends: {} uncommitted (the count=2 policy just fsynced)",
        db.uncommitted_count()
    );

    // Read the dynamic payloads back — nested fields and all.
    println!("\nreplaying the stream:");
    db.replay("s", 0..db.head(), |e: &Event<Json>| {
        let kind = e.body.get("kind").and_then(|k| k.as_str()).unwrap_or("?");
        println!("  offset {}: kind = {kind}", e.offset);
        if let Some(q) = e.body["args"]["q"].as_str() {
            println!("             args.q = {q}");
        }
    })?;

    Ok(())
}

fn fresh_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("salamander-example-{name}"));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}
