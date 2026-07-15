//! First-class diff (docs/specs/first-class-diff.md): the DIFF contract
//! on the typed API. Every worked case from the spec's table appears as a
//! unit-style test; the property test then pits the engine's
//! catalog-arithmetic diff against a brute-force oracle — the full
//! double-replay-and-zip that application code (chat.py's `/diff`) used to
//! implement — over randomly grown branch trees.

use proptest::prelude::*;
use salamander::agent::EventBody;
use salamander::{
    AgentDb, AppendRequest, BranchId, BranchName, DiffRequest, Durability, EventType,
    ExpectedRevision, Metadata, NewEvent, RecordReader, ReplayEnd, ReplayPlan, SalamanderError,
    StreamId, StreamName, StreamSelector, TimelineDiff,
};

/// Appends one single-event committed batch to `stream` on `branch`,
/// returning the record's global position and the stream's engine id.
/// Single-event batches make every position a fork-legal batch boundary.
fn put(db: &mut AgentDb, branch: BranchId, stream: &str, key: &str) -> (u64, StreamId) {
    let receipt = db
        .append_batch(AppendRequest {
            branch,
            stream: StreamName::new(stream).unwrap(),
            expected: ExpectedRevision::Any,
            idempotency_key: None,
            events: vec![NewEvent::new(
                EventType::new("test.put").unwrap(),
                EventBody::Put {
                    key: key.into(),
                    value: key.as_bytes().to_vec(),
                },
            )],
            durability: Durability::Buffered,
        })
        .unwrap();
    (receipt.last_position, receipt.stream_id)
}

fn fork(db: &mut AgentDb, parent: BranchId, at: u64, name: &str) -> BranchId {
    db.commit().unwrap();
    db.fork_branch(parent, at, BranchName::new(name).unwrap(), Metadata::new())
        .unwrap()
        .id
}

/// Materializes a replay plan as `(position, event_id)` pairs — physical
/// record identity, which is what the DIFF contract speaks about.
fn resolve(db: &AgentDb, plan: ReplayPlan) -> Vec<(u64, [u8; 16])> {
    let mut reader = db.read(plan).unwrap();
    let mut records = Vec::new();
    while let Some(record) = reader.next().unwrap() {
        records.push((record.position, record.envelope.event_id.into_bytes()));
    }
    records
}

/// The brute-force timeline: a whole branch-scoped replay up to `until`.
fn timeline(db: &AgentDb, branch: BranchId, until: u64) -> Vec<(u64, [u8; 16])> {
    resolve(
        db,
        ReplayPlan {
            branch,
            until: ReplayEnd::At(until),
            ..ReplayPlan::default()
        },
    )
}

/// Asserts DIFF-1/2 for one resolved diff: the shared plan is the zip-LCP
/// of the two brute-forced timelines (exactness and maximality), and
/// shared ⧺ suffix reconstructs each side elementwise.
fn assert_diff_contract(db: &AgentDb, diff: &TimelineDiff) {
    let left = timeline(db, diff.left.branch.id, diff.left.until);
    let right = timeline(db, diff.right.branch.id, diff.right.until);
    let shared = resolve(db, diff.shared.clone());
    let lcp = left.iter().zip(&right).take_while(|(a, b)| a == b).count();
    assert_eq!(
        &shared[..],
        &left[..lcp],
        "shared plan must be the exact zip-LCP"
    );
    let reconstruct = |suffix| {
        let mut all = shared.clone();
        all.extend(resolve(db, suffix));
        all
    };
    assert_eq!(reconstruct(diff.left.suffix.clone()), left, "DIFF-2 left");
    assert_eq!(
        reconstruct(diff.right.suffix.clone()),
        right,
        "DIFF-2 right"
    );
}

#[test]
fn ancestor_vs_descendant_diverges_at_the_fork() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = AgentDb::open(dir.path()).unwrap();
    for key in ["a", "b"] {
        put(&mut db, BranchId::ZERO, "s", key);
    }
    let child = fork(&mut db, BranchId::ZERO, 2, "child");
    put(&mut db, BranchId::ZERO, "s", "main-after");
    put(&mut db, child, "s", "child-after");
    db.commit().unwrap();

    let diff = db.diff(DiffRequest::new(BranchId::ZERO, child)).unwrap();
    assert_eq!(diff.common_ancestor.id, BranchId::ZERO);
    assert_eq!(diff.divergence, 2);
    assert_eq!(resolve(&db, diff.shared.clone()).len(), 2);
    assert_eq!(resolve(&db, diff.left.suffix.clone()).len(), 1);
    assert_eq!(resolve(&db, diff.right.suffix.clone()).len(), 1);
    assert_diff_contract(&db, &diff);
}

#[test]
fn siblings_diverge_at_the_earlier_fork_and_inherit_accordingly() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = AgentDb::open(dir.path()).unwrap();
    put(&mut db, BranchId::ZERO, "s", "shared");
    let early = fork(&mut db, BranchId::ZERO, 1, "early");
    put(&mut db, BranchId::ZERO, "s", "main-mid");
    let late = fork(&mut db, BranchId::ZERO, 2, "late");
    put(&mut db, early, "s", "early-tail");
    put(&mut db, late, "s", "late-tail");
    db.commit().unwrap();

    let diff = db.diff(DiffRequest::new(early, late)).unwrap();
    assert_eq!(diff.common_ancestor.id, BranchId::ZERO);
    assert_eq!(diff.divergence, 1);
    // The late sibling's timeline still contains main's record at
    // position 1 — inherited history past the other side's fork point is
    // part of its divergent suffix.
    assert_eq!(resolve(&db, diff.right.suffix.clone()).len(), 2);
    assert_diff_contract(&db, &diff);
}

#[test]
fn same_branch_with_two_untils_is_the_rewind_diff() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = AgentDb::open(dir.path()).unwrap();
    for key in ["a", "b", "c"] {
        put(&mut db, BranchId::ZERO, "s", key);
    }
    db.commit().unwrap();

    let diff = db
        .diff(DiffRequest {
            left_until: ReplayEnd::At(1),
            right_until: ReplayEnd::At(3),
            ..DiffRequest::new(BranchId::ZERO, BranchId::ZERO)
        })
        .unwrap();
    assert_eq!(diff.divergence, 1);
    assert!(resolve(&db, diff.left.suffix.clone()).is_empty());
    assert_eq!(resolve(&db, diff.right.suffix.clone()).len(), 2);
    assert_diff_contract(&db, &diff);
}

#[test]
fn diff_with_self_is_empty_and_diff_is_symmetric() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = AgentDb::open(dir.path()).unwrap();
    put(&mut db, BranchId::ZERO, "s", "a");
    let child = fork(&mut db, BranchId::ZERO, 1, "child");
    put(&mut db, child, "s", "b");
    db.commit().unwrap();

    // DIFF-4: self-diff at equal untils has empty suffixes.
    let same = db.diff(DiffRequest::new(child, child)).unwrap();
    assert_eq!(same.divergence, db.head());
    assert!(resolve(&db, same.left.suffix.clone()).is_empty());
    assert!(resolve(&db, same.right.suffix.clone()).is_empty());

    // DIFF-3: swapping sides mirrors the result.
    let forward = db.diff(DiffRequest::new(BranchId::ZERO, child)).unwrap();
    let backward = db.diff(DiffRequest::new(child, BranchId::ZERO)).unwrap();
    assert_eq!(forward.common_ancestor, backward.common_ancestor);
    assert_eq!(forward.divergence, backward.divergence);
    assert_eq!(forward.left, backward.right);
    assert_eq!(forward.right, backward.left);
}

#[test]
fn unknown_branches_and_beyond_head_untils_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = AgentDb::open(dir.path()).unwrap();
    put(&mut db, BranchId::ZERO, "s", "a");
    db.commit().unwrap();

    assert!(matches!(
        db.diff(DiffRequest::new(
            BranchId::ZERO,
            BranchId::from_bytes([9; 16])
        )),
        Err(SalamanderError::BranchNotFound(_))
    ));
    assert!(matches!(
        db.diff(DiffRequest {
            left_until: ReplayEnd::At(db.head() + 1),
            ..DiffRequest::new(BranchId::ZERO, BranchId::ZERO)
        }),
        Err(SalamanderError::OffsetBeyondHead(_))
    ));
}

#[test]
fn archived_branches_diff_normally() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = AgentDb::open(dir.path()).unwrap();
    put(&mut db, BranchId::ZERO, "s", "a");
    let child = fork(&mut db, BranchId::ZERO, 1, "finished");
    put(&mut db, child, "s", "b");
    db.commit().unwrap();
    db.archive_branch(child).unwrap();

    let diff = db.diff(DiffRequest::new(BranchId::ZERO, child)).unwrap();
    assert_eq!(diff.divergence, 1);
    assert_diff_contract(&db, &diff);
}

#[test]
fn stream_scoping_filters_suffixes_without_moving_the_divergence() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = AgentDb::open(dir.path()).unwrap();
    let (_, stream_a) = put(&mut db, BranchId::ZERO, "a", "a0");
    let child = fork(&mut db, BranchId::ZERO, 1, "child");
    // The fork diverges only in stream "b" — a diff scoped to stream "a"
    // sees an empty suffix, but the histories still diverge at the fork.
    put(&mut db, child, "b", "b0");
    put(&mut db, BranchId::ZERO, "a", "a1");
    db.commit().unwrap();

    let diff = db
        .diff(DiffRequest {
            streams: StreamSelector::Streams(vec![stream_a]),
            ..DiffRequest::new(BranchId::ZERO, child)
        })
        .unwrap();
    assert_eq!(diff.divergence, 1);
    assert_eq!(resolve(&db, diff.left.suffix.clone()).len(), 1);
    assert!(resolve(&db, diff.right.suffix.clone()).is_empty());
}

#[test]
fn forks_of_forks_share_the_deepest_common_node() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = AgentDb::open(dir.path()).unwrap();
    put(&mut db, BranchId::ZERO, "s", "root");
    let child = fork(&mut db, BranchId::ZERO, 1, "child");
    put(&mut db, child, "s", "child-0");
    let grand_a = fork(&mut db, child, 2, "grand-a");
    let grand_b = fork(&mut db, child, 2, "grand-b");
    put(&mut db, grand_a, "s", "a-tail");
    put(&mut db, grand_b, "s", "b-tail");
    db.commit().unwrap();

    let diff = db.diff(DiffRequest::new(grand_a, grand_b)).unwrap();
    assert_eq!(diff.common_ancestor.id, child);
    assert_eq!(diff.divergence, 2);
    assert_diff_contract(&db, &diff);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    /// Grows a random three-branch tree (main, a fork of main, and a fork
    /// of either), interleaves appends, then checks the DIFF contract for
    /// a random timeline pair against the brute-force oracle.
    #[test]
    fn diff_matches_the_double_replay_oracle(
        main_events in 1usize..5,
        fork_a_frac in 0.0f64..=1.0,
        a_events in 0usize..4,
        b_from_a in any::<bool>(),
        fork_b_frac in 0.0f64..=1.0,
        b_events in 0usize..4,
        main_tail in 0usize..3,
        pair_seed in any::<u8>(),
        left_frac in 0.0f64..=1.0,
        right_frac in 0.0f64..=1.0,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let mut db = AgentDb::open(dir.path()).unwrap();
        for i in 0..main_events {
            put(&mut db, BranchId::ZERO, "s", &format!("m{i}"));
        }
        let at = |frac: f64, head: u64| (frac * head as f64) as u64;
        let fork_a_at = at(fork_a_frac, db.head());
        let a = fork(&mut db, BranchId::ZERO, fork_a_at, "a");
        for i in 0..a_events {
            put(&mut db, a, "s", &format!("a{i}"));
        }
        let b_parent = if b_from_a { a } else { BranchId::ZERO };
        let fork_b_at = at(fork_b_frac, db.head());
        let b = fork(&mut db, b_parent, fork_b_at, "b");
        for i in 0..b_events {
            put(&mut db, b, "s", &format!("b{i}"));
        }
        for i in 0..main_tail {
            put(&mut db, BranchId::ZERO, "s", &format!("t{i}"));
        }
        db.commit().unwrap();

        let branches = [BranchId::ZERO, a, b];
        let left = branches[pair_seed as usize % 3];
        let right = branches[(pair_seed as usize / 3) % 3];
        let diff = db.diff(DiffRequest {
            left_until: ReplayEnd::At(at(left_frac, db.head())),
            right_until: ReplayEnd::At(at(right_frac, db.head())),
            ..DiffRequest::new(left, right)
        }).unwrap();

        // DIFF-1/2 against the oracle (exactness, maximality,
        // reconstruction).
        assert_diff_contract(&db, &diff);
        // DIFF-3: the mirrored request reports the same split.
        let mirrored = db.diff(DiffRequest {
            left_until: ReplayEnd::At(diff.right.until),
            right_until: ReplayEnd::At(diff.left.until),
            ..DiffRequest::new(right, left)
        }).unwrap();
        prop_assert_eq!(mirrored.divergence, diff.divergence);
        prop_assert_eq!(&mirrored.common_ancestor, &diff.common_ancestor);
    }
}
