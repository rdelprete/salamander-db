//! The agent-memory vocabulary — SalamanderDB's labeled beachhead (P1).
//!
//! Everything here is a *provided module over the generic engine*: a
//! concrete payload type ([`EventBody`]), projections that fold it
//! ([`SessionProjection`], [`KvProjection`]), and the session/fork
//! operations that only make sense for an agent session. The core engine
//! ([`crate::Salamander`], [`crate::Event`], [`crate::Projection`]) never
//! references any of it — "SQLite for event-sourced state, built first for
//! agent memory", not built *around* it.
//!
//! `session_view` and `fork` live here as an `impl Salamander<EventBody>`
//! block: they read the `SessionStarted` / `ToolCall` vocabulary, so they
//! are typed as operations on an agent database specifically, not on the
//! generic engine.

pub mod kv;
pub mod session;

use serde::{Deserialize, Serialize};

use crate::projection::Projection;
use crate::{BranchId, BranchInfo, BranchName, Metadata, Result, Salamander};

pub use kv::KvProjection;
pub use session::{
    PendingToolCall, SessionProjection, SessionState, SessionStatus, TranscriptEntry,
};

/// A `Salamander` specialized to the agent vocabulary. Agent users open
/// this instead of spelling out `Salamander<EventBody>` — "zero friction"
/// (Phase 1.5 spec, WP-1 step 3).
pub type AgentDb = Salamander<EventBody>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    System,
    User,
    Assistant,
    Tool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EventBody {
    // Generic KV — keeps the engine honest as a general-purpose store.
    Put {
        key: String,
        value: Vec<u8>,
    },
    Delete {
        key: String,
    },

    // Agent session vocabulary.
    SessionStarted {
        agent_id: String,
        config_hash: String,
    },
    ModelTurn {
        role: Role,
        content: String,
        model: String,
    },
    ToolCall {
        call_id: String,
        tool: String,
        args_json: String,
    },
    ToolResult {
        call_id: String,
        ok: bool,
        content: String,
    },
    Decision {
        summary: String,
        rationale: String,
    },
    SessionEnded {
        reason: String,
    },
}

/// Agent-vocabulary operations. These are inherent methods on the engine
/// *specialized* to `EventBody`, so they are only in scope for an
/// `AgentDb` — a `Salamander<MyOwnPayload>` never sees `fork` or
/// `session_view`, which is exactly the P1 boundary made real in the type
/// system.
impl Salamander<EventBody> {
    /// A `SessionProjection` for `namespace` on the default branch.
    pub fn session_view(&self, namespace: &str) -> Result<SessionProjection> {
        self.session_view_on_branch(BranchId::ZERO, namespace)
    }

    pub fn session_view_on_branch(
        &self,
        branch: BranchId,
        namespace: &str,
    ) -> Result<SessionProjection> {
        let mut projection = SessionProjection::new(namespace);
        self.replay_branch(branch, namespace, 0..self.head(), |event| {
            projection.apply(event);
        })?;
        Ok(projection)
    }

    /// Create an engine-owned branch at `n` while retaining `namespace` as
    /// the session stream name on both histories.
    pub fn fork(&mut self, namespace: &str, n: u64) -> Result<BranchInfo> {
        let mut metadata = Metadata::new();
        metadata.insert("session_stream".into(), namespace.as_bytes().to_vec());
        self.fork_branch(
            BranchId::ZERO,
            n,
            BranchName::new(format!("{namespace}-fork-{n}"))?,
            metadata,
        )
    }
}
