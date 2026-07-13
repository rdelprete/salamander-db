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

/// The speaker of a [`EventBody::ModelTurn`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    /// A system prompt or instruction.
    System,
    /// A message from the end user.
    User,
    /// A message from the model.
    Assistant,
    /// Output attributed to a tool.
    Tool,
}

/// The built-in agent-memory payload: a key/value vocabulary plus the
/// typed events of an agent session (turns, tool calls, decisions). This is
/// one provided payload over the generic engine — you can define your own
/// instead.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EventBody {
    /// Set `key` to `value` in the key/value projection.
    Put {
        /// The key to set.
        key: String,
        /// The value to store.
        value: Vec<u8>,
    },
    /// Remove `key` from the key/value projection.
    Delete {
        /// The key to remove.
        key: String,
    },

    /// Marks the start of an agent session.
    SessionStarted {
        /// Identifier of the agent.
        agent_id: String,
        /// Hash of the agent's configuration at session start.
        config_hash: String,
    },
    /// One conversational turn.
    ModelTurn {
        /// Who produced the turn.
        role: Role,
        /// The turn's text content.
        content: String,
        /// The model that produced it.
        model: String,
    },
    /// An invocation of a tool.
    ToolCall {
        /// Correlates this call with its [`EventBody::ToolResult`].
        call_id: String,
        /// Name of the tool invoked.
        tool: String,
        /// JSON-encoded arguments.
        args_json: String,
    },
    /// The result of a [`EventBody::ToolCall`].
    ToolResult {
        /// The `call_id` of the originating call.
        call_id: String,
        /// Whether the call succeeded.
        ok: bool,
        /// The result content.
        content: String,
    },
    /// A recorded decision — the natural fork point in a session.
    Decision {
        /// One-line summary of the decision.
        summary: String,
        /// Why it was made.
        rationale: String,
    },
    /// Marks the end of a session.
    SessionEnded {
        /// Why the session ended.
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

    /// A [`SessionProjection`] for `namespace` on a specific branch.
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
