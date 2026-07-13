//! WP-5 seam — the dynamic-JSON payload surface the Python bindings bind to.
//!
//! A Python event is a `dict` ⇒ a JSON object, so the natural payload is
//! `serde_json::Value`. But there is a sharp edge worth understanding:
//! **`serde_json::Value` cannot be deserialized by the engine's `bincode`
//! payload codec.** `Value` decodes via serde's `deserialize_any` (it works
//! out its shape from the input), and bincode is *not* a self-describing
//! format — it has no shape markers in the bytes, so it returns an error
//! rather than guess. `Salamander<serde_json::Value>` would therefore
//! compile but fail at replay.
//!
//! [`Json`] sidesteps this with no engine change: it wraps a `Value` but
//! serializes *through JSON text* — as a length-prefixed string, which
//! bincode round-trips fine. So [`JsonDb`] = `Salamander<Json>` gives Python
//! dynamic JSON payloads over the whole existing stack (log, projections,
//! time-travel, query layer, group commit). The payload generalization from
//! WP-1 is what makes this a ~40-line newtype rather than a rewrite.
//!
//! (A future *self-describing* payload-format version could store `Value`
//! natively and drop the text round-trip — WP-2's payload-format versioning
//! left that door open. `Json` is the ships-today answer; see
//! `docs/phase-1.5.md` §4.1.)
//!
//! Note `fork` / `session_view` are agent-vocabulary operations on
//! `Salamander<agent::EventBody>` and are *not* available on a `JsonDb`;
//! dynamic-JSON users get the generic engine surface and layer any agent
//! semantics on top in Python.
//!
//! ```
//! use salamander::{Json, JsonDb};
//! use serde_json::json;
//!
//! # fn main() -> salamander::Result<()> {
//! let dir = tempfile::tempdir().unwrap();
//!
//! let mut db: JsonDb = JsonDb::open(dir.path())?;
//! db.append("session-1", Json(json!({ "kind": "user_msg", "text": "hi" })))?;
//! db.append("session-1", Json(json!({ "kind": "tool_call", "tool": "search" })))?;
//! db.commit()?;
//! drop(db); // release the single-writer lock before reopening
//!
//! // Reopen from disk and read the JSON payloads straight back out.
//! let db: JsonDb = JsonDb::open(dir.path())?;
//! let mut kinds = Vec::new();
//! db.replay("session-1", 0..db.head(), |e| {
//!     kinds.push(e.body.get("kind").unwrap().as_str().unwrap().to_string());
//! })?;
//! assert_eq!(kinds, ["user_msg", "tool_call"]);
//! # Ok(())
//! # }
//! ```

use std::ops::Deref;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

use crate::Salamander;

/// A dynamic-JSON payload: a `serde_json::Value` that serializes through
/// JSON text so it round-trips under the engine's bincode codec (see the
/// [module docs](self) for why the bare `Value` does not). `Deref`s to the
/// inner `Value`, so `payload.get("field")`, `.as_str()`, etc. work directly.
#[derive(Debug, Clone, PartialEq)]
pub struct Json(pub Value);

impl Json {
    /// Consume the wrapper, yielding the inner `serde_json::Value`.
    pub fn into_inner(self) -> Value {
        self.0
    }
}

impl From<Value> for Json {
    fn from(value: Value) -> Self {
        Json(value)
    }
}

impl Deref for Json {
    type Target = Value;
    fn deref(&self) -> &Value {
        &self.0
    }
}

impl Serialize for Json {
    /// Serialize as a JSON *string* — bincode encodes that as a
    /// length-prefixed byte run, which its deserializer can read back
    /// without any self-describing shape markers.
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let text = serde_json::to_string(&self.0).map_err(serde::ser::Error::custom)?;
        serializer.serialize_str(&text)
    }
}

impl<'de> Deserialize<'de> for Json {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // `String::deserialize` asks the format for a string, not
        // `deserialize_any` — so bincode can satisfy it. Then re-parse the
        // JSON text into a `Value`.
        let text = String::deserialize(deserializer)?;
        serde_json::from_str(&text)
            .map(Json)
            .map_err(serde::de::Error::custom)
    }
}

/// A `Salamander` whose payload is dynamic JSON (via [`Json`]) — the
/// FFI-facing surface for the Phase-1.5 Python bindings (WP-5). A Python
/// `dict` maps to a `Json`, so events cross the boundary without schema
/// codegen. See the [module docs](self).
pub type JsonDb = Salamander<Json>;
