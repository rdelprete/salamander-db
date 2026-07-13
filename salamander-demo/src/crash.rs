//! IMPLEMENTATION.md Step 6 — M4 the conscience harness.
//!
//! `crashtest child <dir>`: opens the DB, runs a randomized write storm
//! (random event sizes, random commit cadence), printing each durable
//! offset to stdout right after commit() returns. Runs forever.
//!
//! `crashtest parent <dir>`: spawns the child, sleeps a random 50-500ms,
//! kill -9s it, records the last durable offset the child *reported*,
//! reopens the dir, rebuilds KvProjection, and asserts:
//!   (a) head() >= the last reported durable offset,
//!   (b) INV-1 holds vs. an independent fold.
//! One shot per process — `scripts/crash_loop.sh` provides the outer loop,
//! a fresh directory per iteration. The stdout-durable-offset protocol
//! gives the parent ground truth about what the child was *promised*
//! (DESIGN.md §6, C1 vs C2): a commit that never got flushed to stdout
//! before the kill simply isn't part of that ground truth, even if it did
//! land on disk — the protocol only needs a *lower bound*, not a perfect
//! account of every commit.

use std::io::{BufRead, Write};
use std::process::{Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use salamander::agent::{EventBody, KvProjection};
use salamander::{AgentDb, Change, CommitPolicy, Event, IndexedView, Projection};

const NAMESPACE: &str = "storm";

/// The IndexedView the harness registers: primary key = the KV key, value =
/// the bytes, plus a secondary index on value length. Registering it drags
/// the live query layer under the same `kill -9` conscience test as the log
/// (query-layer design §8) — the child maintains it via fan-out during the
/// storm; the parent rebuilds it from the durable log on reopen and checks
/// it against an independent fold (INV-2 after a crash).
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

pub fn run(mut args: impl Iterator<Item = String>) {
    match args.next().as_deref() {
        Some("child") => child(args),
        Some("parent") => parent(args),
        _ => {
            eprintln!("usage: salamander-demo crashtest <child|parent> <dir>");
            std::process::exit(2);
        }
    }
}

fn child(mut args: impl Iterator<Item = String>) {
    let dir = args.next().unwrap_or_else(|| usage_and_exit("child"));
    let mut db = AgentDb::open(&dir).expect("child: open");
    // Register a live view so the fan-out path runs (and can be crashed)
    // during the storm.
    db.register("kv", Box::new(len_view()))
        .expect("child: register view");
    // Run with a byte-threshold group-commit policy active (WP-4 exit
    // criterion): auto-commits fsync between the explicit commits below, so
    // the crash harness exercises the auto-commit durability path under
    // kill -9. The explicit-commit-and-print protocol still provides the
    // parent's durable-offset ground truth; auto-commits only make *more*
    // durable, never less, so the `head >= last_reported` lower bound holds.
    db.set_commit_policy(CommitPolicy::every_bytes(4096));
    let mut rng = Rng::seeded();
    let stdout = std::io::stdout();

    loop {
        let key = format!("k{}", rng.next_u64() % 64);
        let body = if rng.next_u64().is_multiple_of(8) {
            EventBody::Delete { key }
        } else {
            let size = 1 + (rng.next_u64() % 256) as usize;
            let value: Vec<u8> = (0..size).map(|_| (rng.next_u64() & 0xFF) as u8).collect();
            EventBody::Put { key, value }
        };

        db.append(NAMESPACE, body).expect("child: append");

        // Random commit cadence (DESIGN.md §3.3): roughly one in four
        // appends gets fsynced immediately, the rest ride along with a
        // later commit.
        if rng.next_u64().is_multiple_of(4) {
            let offset = db.commit().expect("child: commit");
            // Stdout is block-buffered once redirected to a pipe (not
            // line-buffered like a terminal) -- without an explicit flush
            // after every line, an offset could sit in this process's own
            // buffer indefinitely and never reach the parent, even though
            // the corresponding commit() genuinely fsynced.
            let mut out = stdout.lock();
            writeln!(out, "{offset}").expect("child: write durable offset");
            out.flush().expect("child: flush durable offset");
        }
    }
}

fn parent(mut args: impl Iterator<Item = String>) {
    let dir = args.next().unwrap_or_else(|| usage_and_exit("parent"));
    std::fs::create_dir_all(&dir).expect("parent: create dir");

    let exe = std::env::current_exe().expect("parent: current_exe");
    let mut rng = Rng::seeded();

    let mut child = Command::new(&exe)
        .args(["crashtest", "child", &dir])
        .stdout(Stdio::piped())
        .spawn()
        .expect("parent: spawn child");

    let delay_ms = 50 + (rng.next_u64() % 450);
    std::thread::sleep(Duration::from_millis(delay_ms));

    // kill() is SIGKILL on Unix and TerminateProcess on Windows -- both
    // are an unconditional, no-cleanup halt, matching what DESIGN.md §6's
    // crash cases assume ("crash mid-append," not "graceful shutdown").
    child.kill().expect("parent: kill child");
    child.wait().expect("parent: wait for child");

    // Whatever the child managed to flush before dying is still sitting
    // in the pipe's kernel buffer and safe to read now.
    let stdout = child.stdout.take().expect("parent: child stdout");
    let mut last_reported: Option<u64> = None;
    for line in std::io::BufReader::new(stdout).lines() {
        let Ok(line) = line else { break };
        if let Ok(offset) = line.trim().parse::<u64>() {
            last_reported = Some(offset);
        }
    }

    // The child was killed with no chance to run its lock guard's Drop, so
    // it left a stale LOCK behind (review M-2). This is exactly the
    // "crashed process, remove the LOCK by hand" case — here the parent is
    // that hand, standing in for an operator, before reopening.
    let _ = std::fs::remove_file(std::path::Path::new(&dir).join("LOCK"));

    let mut db = AgentDb::open(&dir).expect("parent: reopen after kill");
    // Rebuild the live view from the durable log (catch_up replays it to
    // head) so we can check INV-2 survived the crash alongside INV-1.
    db.register("kv", Box::new(len_view()))
        .expect("parent: register view");

    if let Some(last) = last_reported {
        assert!(
            db.head() >= last,
            "head {} is behind the last durable offset {} the child reported",
            db.head(),
            last
        );
    }

    // The conscience check: INV-1 (DESIGN.md §6) -- the projection must
    // equal an independently computed fold of the same log.
    let kv: KvProjection = db.projection().expect("parent: rebuild projection");
    let mut independent = std::collections::BTreeMap::new();
    db.replay(NAMESPACE, 0..db.head(), |event| match &event.body {
        EventBody::Put { key, value } => {
            independent.insert(key.clone(), value.clone());
        }
        EventBody::Delete { key } => {
            independent.remove(key);
        }
        _ => {}
    })
    .expect("parent: independent replay");

    assert_eq!(
        *kv.state(),
        independent,
        "INV-1 violated: KvProjection diverged from an independent fold of the same log"
    );

    // INV-2 (query-layer design §6): the registered view's primary store
    // must equal the same fold, and its secondary index must be consistent
    // with it -- no stale entries surviving the crash.
    let view = db
        .view::<IndexedView<String, Vec<u8>, EventBody>>("kv")
        .expect("parent: view present");
    assert_eq!(
        view.state(),
        &independent,
        "INV-2 violated: IndexedView primary diverged from the fold"
    );
    let mut expected_by_len: std::collections::BTreeMap<u64, usize> =
        std::collections::BTreeMap::new();
    for value in independent.values() {
        *expected_by_len.entry(value.len() as u64).or_default() += 1;
    }
    for (len, count) in expected_by_len {
        let hits = view.by("by_len", &len.to_le_bytes());
        assert_eq!(
            hits.len(),
            count,
            "INV-2 violated: by_len index inconsistent at length {len}"
        );
    }

    println!(
        "OK head={} records={} view_keys={} last_reported={:?}",
        db.head(),
        kv.state().len(),
        view.state().len(),
        last_reported
    );
}

fn usage_and_exit(mode: &str) -> ! {
    eprintln!("usage: salamander-demo crashtest {mode} <dir>");
    std::process::exit(2)
}

/// splitmix64 -- not the demo's dependency allowlist concern (DESIGN.md
/// §1.2 governs the `salamander` crate; this is `salamander-demo`), but a
/// hand-rolled PRNG this small avoids reaching for a new one at all.
struct Rng(u64);

impl Rng {
    fn seeded() -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let pid = std::process::id() as u64;
        Rng(nanos ^ pid.wrapping_mul(0x9E3779B97F4A7C15))
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }
}
