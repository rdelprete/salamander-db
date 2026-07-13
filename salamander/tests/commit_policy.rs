//! WP-4 exit tests (docs/phase-1.5.md §2, WP-4) — group commit.
//!
//! The `commit()` fsync is observable in-process only indirectly (the page
//! cache survives an ordinary process exit — see the note in `log/mod.rs`),
//! so these assert on the *trigger*: an auto-commit resets the uncommitted
//! counters, so `uncommitted_count()` dropping to 0 witnesses that the
//! policy fired an fsync at exactly the right append.

use salamander::agent::EventBody;
use salamander::{AgentDb, CommitPolicy};

fn put(i: u64) -> EventBody {
    EventBody::Put {
        key: format!("k{i}"),
        value: i.to_le_bytes().to_vec(),
    }
}

#[test]
fn manual_policy_never_auto_commits() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = AgentDb::open(dir.path()).unwrap(); // default = Manual

    for i in 0..5 {
        db.append("ns", put(i)).unwrap();
    }
    // Nothing committed automatically: all five are still pending.
    assert_eq!(db.uncommitted_count(), 5);
    assert!(db.uncommitted_bytes() > 0);

    // An explicit commit clears the tally.
    db.commit().unwrap();
    assert_eq!(db.uncommitted_count(), 0);
    assert_eq!(db.uncommitted_bytes(), 0);
}

#[test]
fn count_threshold_commits_exactly_on_crossing() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = AgentDb::open_with_policy(dir.path(), CommitPolicy::every_count(3)).unwrap();

    db.append("ns", put(0)).unwrap();
    db.append("ns", put(1)).unwrap();
    assert_eq!(db.uncommitted_count(), 2, "not yet at the threshold");

    db.append("ns", put(2)).unwrap();
    assert_eq!(
        db.uncommitted_count(),
        0,
        "third append crosses count=3 → auto-commit"
    );

    // The cycle repeats from the reset counters.
    db.append("ns", put(3)).unwrap();
    assert_eq!(db.uncommitted_count(), 1);
}

#[test]
fn byte_threshold_commits_once_bytes_accumulate() {
    let dir = tempfile::tempdir().unwrap();
    // Each Put serializes to well under 100 bytes; a handful crosses it.
    let mut db = AgentDb::open_with_policy(dir.path(), CommitPolicy::every_bytes(100)).unwrap();

    let mut committed_at = None;
    for i in 0..50 {
        db.append("ns", put(i)).unwrap();
        if db.uncommitted_count() == 0 {
            committed_at = Some(i);
            break;
        }
        assert!(
            db.uncommitted_bytes() < 100,
            "must not exceed the threshold un-committed"
        );
    }
    assert!(
        committed_at.is_some(),
        "byte threshold should have fired within 50 appends"
    );
}

#[test]
fn time_threshold_zero_commits_every_append() {
    let dir = tempfile::tempdir().unwrap();
    // 0 ms means "at least 0 ms since last commit" — always true right after
    // an append, so every append auto-commits.
    let mut db = AgentDb::open_with_policy(dir.path(), CommitPolicy::every_millis(0)).unwrap();

    db.append("ns", put(0)).unwrap();
    assert_eq!(db.uncommitted_count(), 0);
    db.append("ns", put(1)).unwrap();
    assert_eq!(db.uncommitted_count(), 0);
}

#[test]
fn combined_policy_fires_on_whichever_threshold_hits_first() {
    let dir = tempfile::tempdir().unwrap();
    // Large count budget, small byte budget: bytes wins.
    let policy = CommitPolicy::every_count(10_000).and_bytes(60);
    let mut db = AgentDb::open_with_policy(dir.path(), policy).unwrap();

    let mut fired = false;
    for i in 0..20 {
        db.append("ns", put(i)).unwrap();
        if db.uncommitted_count() == 0 {
            fired = true;
            break;
        }
    }
    assert!(
        fired,
        "byte threshold should trip well before the count threshold"
    );
    assert_eq!(db.commit_policy(), policy);
}

#[test]
fn set_commit_policy_takes_effect_on_next_append() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = AgentDb::open(dir.path()).unwrap(); // Manual

    db.append("ns", put(0)).unwrap();
    db.append("ns", put(1)).unwrap();
    assert_eq!(db.uncommitted_count(), 2); // Manual: nothing auto-committed

    // Switch to commit-every-append; the pending two carry over, and the
    // next append (count 3 >= 1) trips the threshold and flushes all of it.
    db.set_commit_policy(CommitPolicy::every_count(1));
    db.append("ns", put(2)).unwrap();
    assert_eq!(db.uncommitted_count(), 0);
}
