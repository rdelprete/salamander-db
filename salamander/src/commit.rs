//! docs/phase-1.5.md §2 (WP-4) / OQ-1 — the group-commit policy.
//!
//! `commit()` fsyncs and is always available. A `CommitPolicy` lets the DB
//! call it *for* the caller once a threshold is crossed, so a write storm
//! can amortize fsyncs without the caller hand-rolling a commit cadence.
//! Additive: the default is `Manual` (Phase-1 behavior — no auto-commit).

use std::time::Duration;

/// When [`crate::Salamander::append`] should trigger an automatic
/// `commit()` on the caller's behalf.
///
/// Thresholds are **combinable** — set any subset with the `and_*` builders;
/// a commit fires when *any* active threshold is crossed. Byte and count
/// thresholds are **exact** (checked after every append, so a crossing
/// commits immediately). The time threshold is **best-effort**: with no
/// background thread in Phase 1.5 (that's a Phase 3 concern), it can only be
/// noticed on the next append after the interval has elapsed — an idle DB
/// does not self-commit.
///
/// ```
/// use salamander::CommitPolicy;
/// // fsync every 64 KiB *or* every 200 ms, whichever comes first:
/// let policy = CommitPolicy::every_bytes(64 * 1024).and_millis(200);
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CommitPolicy {
    every_bytes: Option<u64>,
    every_count: Option<u64>,
    every_millis: Option<u64>,
}

impl CommitPolicy {
    /// No auto-commit — the caller drives durability by calling `commit()`
    /// itself. This is the default (`CommitPolicy::default()`).
    pub const fn manual() -> Self {
        Self {
            every_bytes: None,
            every_count: None,
            every_millis: None,
        }
    }

    /// Commit once this many uncommitted **payload bytes** have accumulated.
    pub const fn every_bytes(bytes: u64) -> Self {
        Self {
            every_bytes: Some(bytes),
            every_count: None,
            every_millis: None,
        }
    }

    /// Commit once this many events have been appended since the last commit.
    pub const fn every_count(count: u64) -> Self {
        Self {
            every_bytes: None,
            every_count: Some(count),
            every_millis: None,
        }
    }

    /// Commit on the first append at least this many milliseconds after the
    /// last commit (best-effort — see the type docs).
    pub const fn every_millis(millis: u64) -> Self {
        Self {
            every_bytes: None,
            every_count: None,
            every_millis: Some(millis),
        }
    }

    /// Add a byte threshold to an existing policy (combinable).
    pub const fn and_bytes(mut self, bytes: u64) -> Self {
        self.every_bytes = Some(bytes);
        self
    }

    /// Add a count threshold to an existing policy (combinable).
    pub const fn and_count(mut self, count: u64) -> Self {
        self.every_count = Some(count);
        self
    }

    /// Add a time threshold to an existing policy (combinable).
    pub const fn and_millis(mut self, millis: u64) -> Self {
        self.every_millis = Some(millis);
        self
    }

    /// Whether an append that left `bytes`/`count` uncommitted, `elapsed`
    /// since the last commit, should trigger a commit now. Only ever called
    /// with `count >= 1` (right after an append), so a bare time threshold
    /// never fsyncs an empty log.
    pub(crate) fn should_commit(&self, bytes: u64, count: u64, elapsed: Duration) -> bool {
        crossed(self.every_bytes, bytes)
            || crossed(self.every_count, count)
            || crossed(self.every_millis, elapsed.as_millis() as u64)
    }
}

fn crossed(threshold: Option<u64>, value: u64) -> bool {
    threshold.is_some_and(|t| value >= t)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECOND: Duration = Duration::from_secs(1);
    const ZERO: Duration = Duration::ZERO;

    #[test]
    fn manual_never_commits() {
        let p = CommitPolicy::manual();
        assert!(!p.should_commit(u64::MAX, u64::MAX, SECOND));
        assert_eq!(p, CommitPolicy::default());
    }

    #[test]
    fn byte_threshold_is_inclusive() {
        let p = CommitPolicy::every_bytes(100);
        assert!(!p.should_commit(99, 1, ZERO));
        assert!(p.should_commit(100, 1, ZERO));
        assert!(p.should_commit(101, 1, ZERO));
    }

    #[test]
    fn count_threshold_is_inclusive() {
        let p = CommitPolicy::every_count(3);
        assert!(!p.should_commit(0, 2, ZERO));
        assert!(p.should_commit(0, 3, ZERO));
    }

    #[test]
    fn time_threshold_fires_only_past_the_interval() {
        let p = CommitPolicy::every_millis(200);
        assert!(!p.should_commit(0, 1, Duration::from_millis(199)));
        assert!(p.should_commit(0, 1, Duration::from_millis(200)));
    }

    #[test]
    fn combined_fires_on_whichever_crosses_first() {
        // Big count budget, small byte budget: bytes should trigger.
        let p = CommitPolicy::every_count(1_000).and_bytes(50);
        assert!(p.should_commit(50, 1, ZERO));
        // Neither crossed yet.
        assert!(!p.should_commit(49, 2, ZERO));
    }
}
