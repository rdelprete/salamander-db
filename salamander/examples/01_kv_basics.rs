//! The core loop. Run with:
//!
//!     cargo run --example 01_kv_basics
//!
//! Append events, `commit()` (fsync), then rebuild derived state by
//! replaying the log. Nothing is stored but the log; every view is a fold of
//! it — which is why state survives a reopen and why you can rewind to any
//! past offset for free.

use salamander::agent::{EventBody, KvProjection};
use salamander::{AgentDb, Projection};

fn main() -> salamander::Result<()> {
    let dir = fresh_dir("kv_basics");
    println!("data dir: {}\n", dir.display());

    // ── write a few key/value events, then make them durable ────────────
    {
        let mut db = AgentDb::open(&dir)?;
        db.append("notes", put("title", b"Salamander"))?; // offset 0
        db.append("notes", put("lang", b"Rust"))?; //         offset 1
        db.append("notes", put("lang", b"Rust 2021"))?; //    offset 2 (overwrite)
        db.append(
            "notes",
            EventBody::Delete {
                key: "title".into(),
            },
        )?; // offset 3
        db.commit()?; // fsync — everything up to head is now durable
        println!("appended 4 events; head (next offset) = {}", db.head());
    } // `db` drops here → the single-writer LOCK is released, so we can reopen

    // ── reopen from cold and rebuild state purely by replaying the log ──
    let db = AgentDb::open(&dir)?;
    let kv: KvProjection = db.projection()?; // a fresh fold, replayed to head
    println!("\nlive state after reopen + replay:");
    for (k, v) in kv.state() {
        println!("  {k} = {}", String::from_utf8_lossy(v));
    }
    // "title" is gone (deleted at offset 3); "lang" shows the latest value.

    // ── time-travel: state as of offset 2 (events 0 and 1 only) ─────────
    let past: KvProjection = db.view_at(2)?;
    println!("\nstate as of offset 2 (before the overwrite and delete):");
    for (k, v) in past.state() {
        println!("  {k} = {}", String::from_utf8_lossy(v));
    }

    Ok(())
}

fn put(key: &str, value: &[u8]) -> EventBody {
    EventBody::Put {
        key: key.into(),
        value: value.to_vec(),
    }
}

/// A throwaway data dir under the OS temp dir, wiped at the start of each run
/// and left afterward so you can inspect `log/` and `manifest.json`.
fn fresh_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("salamander-example-{name}"));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}
