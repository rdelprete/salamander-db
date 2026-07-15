//! Engine-owned branch metadata and ancestry catalog.

use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};

use crate::format::{BranchId, Metadata, OwnedStoredRecord};
use crate::{Result, SalamanderError};

/// Name of the default branch every database has.
pub const DEFAULT_BRANCH_NAME: &str = "main";
/// Maximum number of branch nodes accepted in one ancestry chain.
pub const MAX_LINEAGE_DEPTH: usize = 64;

/// Validated, stable human-readable branch label.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BranchName(String);

impl BranchName {
    /// Maximum length of a branch name, in bytes.
    pub const MAX_BYTES: usize = 255;

    /// Validates and constructs a branch name, rejecting empty, oversized,
    /// or NUL-containing input.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.is_empty() || value.as_bytes().contains(&0) {
            return Err(SalamanderError::InvalidArgument(
                "branch name must be nonempty and contain no NUL".into(),
            ));
        }
        if value.len() > Self::MAX_BYTES {
            return Err(SalamanderError::ResourceLimit {
                resource: "branch name bytes",
                actual: value.len() as u64,
                maximum: Self::MAX_BYTES as u64,
            });
        }
        Ok(Self(value))
    }

    /// The name as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Whether a branch accepts new writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BranchStatus {
    /// Accepts reads and writes.
    Active,
    /// Retains readable history but rejects new writes.
    Archived,
}

/// Durable identity and immutable ancestry for one branch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BranchInfo {
    /// Permanent machine identity.
    pub id: BranchId,
    /// Permanent human-readable label.
    pub name: BranchName,
    /// Immediate ancestor, or `None` for the default branch.
    pub parent: Option<BranchId>,
    /// Exclusive upper position inherited from `parent`.
    pub fork_position: Option<u64>,
    /// Creation wall-clock time for diagnostics, not ordering.
    pub created_at_unix_nanos: i64,
    /// Caller-supplied opaque branch metadata.
    pub metadata: BTreeMap<String, Vec<u8>>,
    /// Current non-destructive lifecycle state.
    pub status: BranchStatus,
}

#[derive(Clone)]
pub(crate) struct BranchCatalog {
    by_id: HashMap<BranchId, BranchInfo>,
    by_name: HashMap<String, BranchId>,
}

impl BranchCatalog {
    pub(crate) fn rebuild(
        system_records: impl Iterator<Item = Result<OwnedStoredRecord>>,
    ) -> Result<Self> {
        let default = BranchInfo {
            id: BranchId::ZERO,
            name: BranchName::new(DEFAULT_BRANCH_NAME)?,
            parent: None,
            fork_position: None,
            created_at_unix_nanos: 0,
            metadata: Metadata::new(),
            status: BranchStatus::Active,
        };
        let mut catalog = Self {
            by_id: HashMap::from([(default.id, default.clone())]),
            by_name: HashMap::from([(default.name.as_str().to_string(), default.id)]),
        };
        for item in system_records {
            let record = item?;
            let event_type = record.envelope.event_type.as_str();
            if event_type != "salamander.branch.created"
                && event_type != "salamander.branch.archived"
            {
                continue;
            }
            let info: BranchInfo = serde_json::from_slice(&record.payload).map_err(|error| {
                SalamanderError::Corrupt {
                    offset: record.position,
                    reason: format!("branch metadata decode: {error}"),
                }
            })?;
            if event_type == "salamander.branch.created" {
                catalog.insert(info)?;
            } else {
                catalog.archive(info)?;
            }
        }
        Ok(catalog)
    }

    pub(crate) fn insert(&mut self, info: BranchInfo) -> Result<()> {
        if self.by_id.contains_key(&info.id) {
            return Err(SalamanderError::BranchExists(
                info.name.as_str().to_string(),
            ));
        }
        if self.by_name.contains_key(info.name.as_str()) {
            return Err(SalamanderError::BranchExists(
                info.name.as_str().to_string(),
            ));
        }
        if let Some(parent) = info.parent {
            if !self.by_id.contains_key(&parent) {
                return Err(SalamanderError::BranchNotFound(format!("{parent:?}")));
            }
        }
        self.validate_depth(&info)?;
        self.by_name.insert(info.name.as_str().to_string(), info.id);
        self.by_id.insert(info.id, info);
        Ok(())
    }

    pub(crate) fn archive(&mut self, archived: BranchInfo) -> Result<()> {
        let current = self
            .by_id
            .get_mut(&archived.id)
            .ok_or_else(|| SalamanderError::BranchNotFound(format!("{:?}", archived.id)))?;
        if current.id == BranchId::ZERO {
            return Err(SalamanderError::InvalidArgument(
                "the default branch cannot be archived".into(),
            ));
        }
        let mut expected = current.clone();
        expected.status = BranchStatus::Archived;
        if archived != expected {
            return Err(SalamanderError::InvalidBranchAncestry(
                "archive metadata may only change branch status".into(),
            ));
        }
        *current = archived;
        Ok(())
    }

    fn validate_depth(&self, info: &BranchInfo) -> Result<()> {
        let mut parent = info.parent;
        for _ in 0..MAX_LINEAGE_DEPTH {
            let Some(id) = parent else {
                return Ok(());
            };
            if id == info.id {
                return Err(SalamanderError::InvalidBranchAncestry(
                    "cycle detected".into(),
                ));
            }
            parent = self.by_id.get(&id).and_then(|branch| branch.parent);
        }
        Err(SalamanderError::InvalidBranchAncestry(format!(
            "lineage exceeds maximum depth {MAX_LINEAGE_DEPTH}"
        )))
    }

    pub(crate) fn get(&self, id: BranchId) -> Option<&BranchInfo> {
        self.by_id.get(&id)
    }

    pub(crate) fn named(&self, name: &str) -> Option<&BranchInfo> {
        self.by_name.get(name).and_then(|id| self.by_id.get(id))
    }

    pub(crate) fn ancestry(&self, id: BranchId) -> Result<Vec<BranchInfo>> {
        let mut result = Vec::new();
        let mut current = Some(id);
        while let Some(branch_id) = current {
            let branch = self
                .by_id
                .get(&branch_id)
                .ok_or_else(|| SalamanderError::BranchNotFound(format!("{branch_id:?}")))?;
            result.push(branch.clone());
            current = branch.parent;
            if result.len() > MAX_LINEAGE_DEPTH {
                return Err(SalamanderError::InvalidBranchAncestry(
                    "lineage depth exceeded".into(),
                ));
            }
        }
        result.reverse();
        Ok(result)
    }

    pub(crate) fn children(&self, id: BranchId) -> Vec<BranchInfo> {
        let mut children: Vec<_> = self
            .by_id
            .values()
            .filter(|branch| branch.parent == Some(id))
            .cloned()
            .collect();
        children.sort_by(|left, right| left.name.cmp(&right.name));
        children
    }

    pub(crate) fn replay_scopes(&self, id: BranchId, upto: u64) -> Result<Vec<(BranchId, u64)>> {
        let ancestry = self.ancestry(id)?;
        // A fork inherits history strictly up to its position — including
        // what its parent had itself inherited — so each level's upper
        // bound is the *running minimum* of every downstream fork position
        // on the path, not just the immediate child's. The two differ only
        // when a fork sits below its parent's own fork point (legal, if
        // odd); capping by the immediate child alone leaked grandparent
        // records into that window.
        let mut upper = upto;
        let mut scopes: Vec<(BranchId, u64)> = ancestry
            .iter()
            .enumerate()
            .rev()
            .map(|(index, branch)| {
                if let Some(child) = ancestry.get(index + 1) {
                    upper = upper.min(child.fork_position.unwrap_or(upto));
                }
                (branch.id, upper)
            })
            .collect();
        scopes.reverse();
        Ok(scopes)
    }

    /// The common ancestor and exclusive divergence position of two
    /// timelines, per the DIFF contract
    /// (`docs/specs/first-class-diff.md`). Pure catalog arithmetic over
    /// engine-owned ancestry positions — payload bytes are never consulted
    /// (DIFF-6), and no log I/O happens here (untils arrive pre-resolved).
    pub(crate) fn divergence(
        &self,
        left: BranchId,
        left_until: u64,
        right: BranchId,
        right_until: u64,
    ) -> Result<(BranchInfo, u64)> {
        let left_path = self.ancestry(left)?;
        let right_path = self.ancestry(right)?;
        Ok(divergence_of(
            &left_path,
            left_until,
            &right_path,
            right_until,
        ))
    }

    pub(crate) fn common_ancestor(&self, left: BranchId, right: BranchId) -> Result<BranchInfo> {
        let left = self.ancestry(left)?;
        let right = self.ancestry(right)?;
        left.into_iter()
            .zip(right)
            .take_while(|(a, b)| a.id == b.id)
            .map(|(branch, _)| branch)
            .last()
            .ok_or_else(|| {
                SalamanderError::InvalidBranchAncestry("branches have no common root".into())
            })
    }
}

/// Divergence over two resolved root-first ancestry paths: the deepest
/// shared branch node, and the smallest of — each side's exclusive until
/// and *every* non-shared node's fork position on either path. Every
/// non-shared node caps the divergence (its local records sit at
/// positions at or above its fork and are visible to one side only), and
/// fork positions are not monotone along a path — a fork below its
/// parent's own fork point is legal — so the minimum runs over the whole
/// divergent tail, mirroring the cascaded caps in `replay_scopes`. Every
/// ancestry begins at the default branch, so the shared prefix is never
/// empty.
fn divergence_of(
    left_path: &[BranchInfo],
    left_until: u64,
    right_path: &[BranchInfo],
    right_until: u64,
) -> (BranchInfo, u64) {
    let shared = left_path
        .iter()
        .zip(right_path)
        .take_while(|(a, b)| a.id == b.id)
        .count();
    debug_assert!(shared >= 1, "ancestries always share the default branch");
    let ancestor = left_path[shared - 1].clone();
    let side_min = |path: &[BranchInfo], until: u64| {
        path[shared..]
            .iter()
            .filter_map(|branch| branch.fork_position)
            .fold(until, u64::min)
    };
    let divergence = side_min(left_path, left_until).min(side_min(right_path, right_until));
    (ancestor, divergence)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: u8, parent: Option<u8>, fork_position: Option<u64>) -> BranchInfo {
        BranchInfo {
            id: BranchId::from_bytes([id; 16]),
            name: BranchName::new(format!("branch-{id}")).unwrap(),
            parent: parent.map(|p| BranchId::from_bytes([p; 16])),
            fork_position,
            created_at_unix_nanos: 0,
            metadata: Metadata::new(),
            status: BranchStatus::Active,
        }
    }

    fn root() -> BranchInfo {
        BranchInfo {
            id: BranchId::ZERO,
            name: BranchName::new(DEFAULT_BRANCH_NAME).unwrap(),
            parent: None,
            fork_position: None,
            created_at_unix_nanos: 0,
            metadata: Metadata::new(),
            status: BranchStatus::Active,
        }
    }

    #[test]
    fn ancestor_vs_descendant_diverges_at_the_fork() {
        let main = vec![root()];
        let fork = vec![root(), node(1, Some(0), Some(8))];
        let (ancestor, d) = divergence_of(&main, 12, &fork, 17);
        assert_eq!(ancestor.id, BranchId::ZERO);
        assert_eq!(d, 8);
    }

    #[test]
    fn siblings_diverge_at_the_earlier_fork() {
        let left = vec![root(), node(1, Some(0), Some(5))];
        let right = vec![root(), node(2, Some(0), Some(9))];
        let (ancestor, d) = divergence_of(&left, 20, &right, 20);
        assert_eq!(ancestor.id, BranchId::ZERO);
        assert_eq!(d, 5);
    }

    #[test]
    fn same_branch_diverges_at_the_smaller_until() {
        let path = vec![root(), node(1, Some(0), Some(3))];
        let (ancestor, d) = divergence_of(&path, 4, &path, 9);
        assert_eq!(ancestor.id, path[1].id);
        assert_eq!(d, 4);
        let (_, d) = divergence_of(&path, 9, &path, 9);
        assert_eq!(d, 9);
    }

    #[test]
    fn an_until_below_the_fork_caps_the_divergence() {
        let main = vec![root()];
        let fork = vec![root(), node(1, Some(0), Some(8))];
        let (_, d) = divergence_of(&main, 3, &fork, 17);
        assert_eq!(d, 3);
    }

    #[test]
    fn grandchildren_share_the_deepest_common_node() {
        let child = node(1, Some(0), Some(4));
        let left = vec![root(), child.clone(), node(2, Some(1), Some(7))];
        let right = vec![root(), child.clone(), node(3, Some(1), Some(10))];
        let (ancestor, d) = divergence_of(&left, 20, &right, 20);
        assert_eq!(ancestor.id, child.id);
        assert_eq!(d, 7);
    }

    #[test]
    fn fork_at_zero_diverges_at_zero() {
        let main = vec![root()];
        let fork = vec![root(), node(1, Some(0), Some(0))];
        let (_, d) = divergence_of(&main, 6, &fork, 6);
        assert_eq!(d, 0);
    }

    #[test]
    fn a_fork_below_its_parents_fork_caps_the_divergence() {
        // b = fork(a, 0) where a = fork(main, 1): b inherits *nothing*
        // (its position caps the whole inherited prefix), so diffing b
        // against a — or against anything — diverges at 0, not at a's
        // fork. Found by the diff property test's double-replay oracle.
        let a = node(1, Some(0), Some(1));
        let a_path = vec![root(), a.clone()];
        let b_path = vec![root(), a, node(2, Some(1), Some(0))];
        let (ancestor, d) = divergence_of(&b_path, 2, &a_path, 2);
        assert_eq!(ancestor.id, b_path[1].id);
        assert_eq!(d, 0);
    }

    #[test]
    fn replay_scopes_cascade_downstream_fork_caps() {
        let mut catalog = BranchCatalog {
            by_id: HashMap::from([(BranchId::ZERO, root())]),
            by_name: HashMap::from([(DEFAULT_BRANCH_NAME.to_string(), BranchId::ZERO)]),
        };
        catalog.insert(node(1, Some(0), Some(3))).unwrap();
        catalog.insert(node(2, Some(1), Some(1))).unwrap();
        // Grandchild forked at 1, below its parent's fork at 3: every
        // inherited level is capped at 1 — the parent's cap must not leak
        // root records from [1, 3) into the grandchild's timeline.
        assert_eq!(
            catalog
                .replay_scopes(BranchId::from_bytes([2; 16]), 10)
                .unwrap(),
            vec![
                (BranchId::ZERO, 1),
                (BranchId::from_bytes([1; 16]), 1),
                (BranchId::from_bytes([2; 16]), 10),
            ]
        );
    }
}
