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
        let mut scopes = Vec::with_capacity(ancestry.len());
        for (index, branch) in ancestry.iter().enumerate() {
            let upper = ancestry
                .get(index + 1)
                .and_then(|child| child.fork_position)
                .unwrap_or(upto)
                .min(upto);
            scopes.push((branch.id, upper));
        }
        Ok(scopes)
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
