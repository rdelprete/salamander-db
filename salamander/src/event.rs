//! DESIGN.md §4 — the event envelope, generic over its payload.
//!
//! Design rule (P1): the engine frames, orders, and persists bodies but
//! never interprets them. Only projections do. The payload type `B` is a
//! free parameter — the agent vocabulary in [`crate::agent`] is just one
//! choice of `B`, not something the core depends on. Adding a new payload
//! variant must never require touching `log/`.

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// One durable, offset-ordered fact carrying a payload of type `B`.
///
/// The engine treats `body` as opaque bytes-to-be; it round-trips it
/// through serde without ever matching on it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event<B> {
    /// Assigned by the log at append.
    pub offset: u64,
    /// Wall clock, informational only — offset is the ordering.
    pub timestamp_ms: u64,
    /// e.g. session id.
    pub namespace: String,
    /// The application payload; opaque to the engine.
    pub body: B,
}

/// The bound set every payload type must satisfy, aliased under one name so
/// the engine's generic signatures stay legible (`B: Body` rather than the
/// four-trait mouthful everywhere).
///
/// It is *blanket-implemented*: any type that is serde-serializable,
/// owned-deserializable, cloneable, and `'static` is a `Body`
/// automatically, so implementers never write `impl Body` by hand.
/// `DeserializeOwned` (rather than `Deserialize<'de>`) is what lets the log
/// hand back a payload that owns its data, decoupled from the lifetime of
/// the record bytes it was decoded from.
pub trait Body: Serialize + DeserializeOwned + Clone + 'static {}

impl<T: Serialize + DeserializeOwned + Clone + 'static> Body for T {}
