//! IMPLEMENTATION.md Step 3 / DESIGN.md §9 — the M1 write-storm demo.
//!
//! Appends N events, closes the DB, reopens it (full recovery), then scans
//! every record back and verifies the stream round-tripped intact. This is
//! the M1 artifact: proof the log survives a large volume across a
//! close/reopen cycle, and the first honest recovery-time numbers.
//!
//! Usage: `salamander-demo storm [count]` (default 1,000,000).

use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use salamander::agent::{EventBody, KvProjection};
use salamander::{AgentDb, Event, Projection};

const NAMESPACE: &str = "storm";
const DEFAULT_COUNT: u64 = 1_000_000;
/// fsync cadence: commit every this many appends, so the storm exercises a
/// realistic mixed durability pattern rather than one fsync at the very end.
const COMMIT_EVERY: u64 = 10_000;

pub fn run(mut args: impl Iterator<Item = String>) {
    let count = args
        .next()
        .and_then(|a| a.parse::<u64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_COUNT);

    let dir = scratch_dir();
    let _ = std::fs::remove_dir_all(&dir); // fresh start if a prior run left it behind

    println!("SalamanderDB — write storm (M1)\n");
    println!("▶ Appending {count} events under {}…", dir.display());

    // ── 1. Write the storm ──────────────────────────────────────────────
    let t_write = Instant::now();
    {
        let mut db = AgentDb::open(&dir).expect("open");
        for i in 0..count {
            db.append(NAMESPACE, event_for(i)).expect("append");
            if (i + 1) % COMMIT_EVERY == 0 {
                db.commit().expect("commit");
            }
        }
        db.commit().expect("final commit");
        assert_eq!(db.head(), count);
    } // db drops here → single-writer lock released, ready to reopen
    let write_elapsed = t_write.elapsed();
    let on_disk = log_bytes(&dir);

    // ── 2. Reopen: full recovery of the log from a cold directory ───────
    let t_open = Instant::now();
    let db = AgentDb::open(&dir).expect("reopen");
    let open_elapsed = t_open.elapsed();
    assert_eq!(
        db.head(),
        count,
        "head after reopen must equal what was written"
    );

    // ── 3. Rebuild derived state: a full-log projection replay ──────────
    // This is the O(log-size) cost that `open` itself defers — the M5
    // "baseline villain" and the motivation for Phase 2's snapshots.
    let t_replay = Instant::now();
    let kv = db.projection::<KvProjection>().expect("projection");
    let replay_elapsed = t_replay.elapsed();

    // ── 4. Verify the raw stream: every record, in order, intact ────────
    let t_scan = Instant::now();
    let mut seen: u64 = 0;
    let mut digest: u64 = FNV_OFFSET;
    db.replay(NAMESPACE, 0..db.head(), |e: &Event<EventBody>| {
        verify_event(e, seen);
        digest = fnv1a(digest, &e.offset.to_le_bytes());
        seen += 1;
    })
    .expect("replay");
    let scan_elapsed = t_scan.elapsed();

    assert_eq!(
        seen, count,
        "scan must see every appended record exactly once"
    );
    let expected_digest = (0..count).fold(FNV_OFFSET, |d, i| fnv1a(d, &i.to_le_bytes()));
    assert_eq!(
        digest, expected_digest,
        "stream digest mismatch — records lost, dupd, or reordered"
    );

    // ── 5. Report ───────────────────────────────────────────────────────
    println!(
        "\n  wrote    {count} events ({}) in {}",
        human_bytes(on_disk),
        human_dur(write_elapsed)
    );
    println!(
        "  reopened (log recovery)          in {}",
        human_dur(open_elapsed)
    );
    println!(
        "  replayed (rebuilt KvProjection)  in {}",
        human_dur(replay_elapsed)
    );
    println!(
        "  scanned + content-verified       in {}",
        human_dur(scan_elapsed)
    );
    println!("  live keys after replay: {}", kv.state().len());
    println!("  stream digest OK (0x{digest:016x})");
    println!(
        "\n  Note: log recovery is cheap and roughly flat — `open` only scans the\n  \
         active segment. The replay is the linear cost (the M5 baseline villain),\n  \
         which Phase 2's projection snapshots exist to erase."
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Deterministic event for offset `i`: the value encodes `i`, so a reader
/// can verify content by offset alone. One namespace, one key — this is a
/// log-throughput demo, not a KV demo, so the key stays constant and the
/// identity lives in the value.
fn event_for(i: u64) -> EventBody {
    EventBody::Put {
        key: "storm".to_string(),
        value: i.to_le_bytes().to_vec(),
    }
}

fn verify_event(e: &Event<EventBody>, expected_offset: u64) {
    assert_eq!(
        e.offset, expected_offset,
        "offset gap: expected {expected_offset}, got {}",
        e.offset
    );
    match &e.body {
        EventBody::Put { value, .. } => {
            assert_eq!(
                value.as_slice(),
                e.offset.to_le_bytes(),
                "content corruption at offset {}",
                e.offset
            );
        }
        other => panic!("unexpected body at offset {}: {other:?}", e.offset),
    }
}

// ── FNV-1a, a tiny non-cryptographic stream digest ───────────────────────
const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

fn fnv1a(mut hash: u64, bytes: &[u8]) -> u64 {
    for &b in bytes {
        hash ^= b as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

// ── formatting helpers ───────────────────────────────────────────────────

fn log_bytes(dir: &Path) -> u64 {
    std::fs::read_dir(dir.join("log"))
        .map(|rd| {
            rd.flatten()
                .filter_map(|e| e.metadata().ok())
                .map(|m| m.len())
                .sum()
        })
        .unwrap_or(0)
}

fn human_bytes(bytes: u64) -> String {
    const MIB: f64 = 1024.0 * 1024.0;
    const KIB: f64 = 1024.0;
    let b = bytes as f64;
    if b >= MIB {
        format!("{:.1} MiB", b / MIB)
    } else if b >= KIB {
        format!("{:.1} KiB", b / KIB)
    } else {
        format!("{bytes} B")
    }
}

fn human_dur(d: std::time::Duration) -> String {
    let ms = d.as_secs_f64() * 1000.0;
    if ms >= 1000.0 {
        format!("{:.2} s", ms / 1000.0)
    } else {
        format!("{ms:.1} ms")
    }
}

fn scratch_dir() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let mut dir = std::env::temp_dir();
    dir.push(format!("salamander-storm-{}-{}", std::process::id(), nanos));
    dir
}
