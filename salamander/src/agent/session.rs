//! DESIGN.md §5 — SessionProjection: per-namespace agent session view. The
//! demo workhorse (M3 flagship). Namespace filtering happens in `apply` —
//! the projection ignores events from other namespaces.
//!
//! A projection over `EventBody`: its `Projection::Body` associated type
//! pins it to an `AgentDb`, so it can only be built from a log carrying the
//! agent vocabulary.

use std::collections::HashMap;

use super::{EventBody, Role};
use crate::event::Event;
use crate::projection::{NamespaceScoped, Projection};

#[derive(Debug, Clone)]
pub enum TranscriptEntry {
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionStatus {
    NotStarted,
    Active,
    Ended { reason: String },
}

#[derive(Debug, Clone)]
pub struct PendingToolCall {
    pub tool: String,
    pub args_json: String,
}

#[derive(Debug)]
pub struct SessionState {
    /// Set from this session's own `SessionStarted` event, if it's been
    /// applied yet. `None` until then (or for a namespace that never
    /// started a session at all).
    pub agent_id: Option<String>,
    pub transcript: Vec<TranscriptEntry>,
    pub pending: HashMap<String, PendingToolCall>,
    pub status: SessionStatus,
}

impl Default for SessionState {
    fn default() -> Self {
        Self {
            agent_id: None,
            transcript: Vec::new(),
            pending: HashMap::new(),
            status: SessionStatus::NotStarted,
        }
    }
}

pub struct SessionProjection {
    namespace: String,
    state: SessionState,
    cursor: u64,
}

impl SessionProjection {
    pub fn new(namespace: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            state: SessionState::default(),
            cursor: 0,
        }
    }
}

impl Projection for SessionProjection {
    type Body = EventBody;
    type State = SessionState;

    fn apply(&mut self, event: &Event<EventBody>) {
        if event.namespace != self.namespace {
            self.cursor = event.offset + 1;
            return;
        }

        match &event.body {
            EventBody::SessionStarted { agent_id, .. } => {
                self.state.status = SessionStatus::Active;
                self.state.agent_id = Some(agent_id.clone());
            }
            EventBody::ModelTurn {
                role,
                content,
                model,
            } => {
                self.state.transcript.push(TranscriptEntry::ModelTurn {
                    role: *role,
                    content: content.clone(),
                    model: model.clone(),
                });
            }
            EventBody::ToolCall {
                call_id,
                tool,
                args_json,
            } => {
                self.state.pending.insert(
                    call_id.clone(),
                    PendingToolCall {
                        tool: tool.clone(),
                        args_json: args_json.clone(),
                    },
                );
                self.state.transcript.push(TranscriptEntry::ToolCall {
                    call_id: call_id.clone(),
                    tool: tool.clone(),
                    args_json: args_json.clone(),
                });
            }
            EventBody::ToolResult {
                call_id,
                ok,
                content,
            } => {
                self.state.pending.remove(call_id);
                self.state.transcript.push(TranscriptEntry::ToolResult {
                    call_id: call_id.clone(),
                    ok: *ok,
                    content: content.clone(),
                });
            }
            EventBody::Decision { summary, rationale } => {
                self.state.transcript.push(TranscriptEntry::Decision {
                    summary: summary.clone(),
                    rationale: rationale.clone(),
                });
            }
            EventBody::SessionEnded { reason } => {
                self.state.status = SessionStatus::Ended {
                    reason: reason.clone(),
                };
            }
            EventBody::Put { .. } | EventBody::Delete { .. } => {}
        }

        self.cursor = event.offset + 1;
    }

    fn cursor(&self) -> u64 {
        self.cursor
    }

    fn state(&self) -> &Self::State {
        &self.state
    }
}

impl NamespaceScoped for SessionProjection {
    fn new_for(namespace: &str) -> Self {
        SessionProjection::new(namespace)
    }
}
