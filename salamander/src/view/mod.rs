//! docs/phase-1.5.md §3 — the query layer.
//!
//! The core idea (§3): **an index is just another deterministic fold over
//! the log.** Instead of constructing a projection on demand and handing
//! back a snapshot (Phase 1's model), the DB *owns* registered **views**
//! and fans every appended event out to them synchronously, so a query
//! never sees a stale view (INV-2). Because a view is a fold, it inherits
//! every guarantee the log already has: INV-1 covers it, the M4 crash
//! harness covers it, and `log/` still interprets nothing.
//!
//! This module defines the object-safe [`View`] trait the registry drives,
//! and [`IndexedView`] — the batteries-included queryable view with
//! secondary indexes (`get`/`range`/`prefix`/`by`). The registry itself
//! (`register`/`deregister`/`view`/fan-out) lives on [`crate::Salamander`].

use std::any::Any;

use crate::event::{Body, Event};
use crate::log::Log;
use crate::projection::decode_stored_event;
use crate::Result;

pub mod indexed;

pub use indexed::{Change, IndexedView};

/// The universal secondary-index key type (query-layer design OQ-Q1). One
/// byte-vector key type per view keeps the generics ergonomic and is the
/// natural serialized form once Phase 2 snapshots indexes to disk.
pub type IndexKey = Vec<u8>;

/// A live view the DB drives **type-erased**, as `dyn View<B>`.
///
/// Deliberately **object-safe** — no associated types, no `Self`-returning
/// methods — which is exactly why it is *not* `: Projection`. `Projection`
/// has `type State` and `state(&self) -> &Self::State`, and an associated
/// type makes `dyn Projection` impossible. A `View` carries only what the
/// registry needs to *drive* it (`apply`/`cursor`) and *downcast* it
/// (`as_any`); the query methods live on the concrete type, reached after
/// downcast via [`crate::Salamander::view`].
///
/// `IndexedView` implements **both** `View` (so the registry can store it
/// as `dyn View`) and `Projection` (so `replay_into`/`view_at` still build
/// it on demand for time-travel). A concrete type implementing two traits
/// is fine; only the *stored* form is erased.
pub trait View<B>: Any {
    /// Fold one event into the view. Same determinism contract as
    /// `Projection::apply` (query-layer design §6, INV-2).
    fn apply(&mut self, event: &Event<B>);

    /// All events with offset < cursor have been applied.
    fn cursor(&self) -> u64;

    /// Upcast to `dyn Any` so the registry can downcast to the concrete
    /// view type on query. (`Box<dyn View<B>>` is not automatically
    /// `dyn Any`; this is the standard bridge.)
    fn as_any(&self) -> &dyn Any;
}

/// Fold `log[view.cursor(), upto)` into a type-erased view — the
/// object-safe sibling of `replay_into` (which needs the non-object-safe
/// `Projection`). Both walk the same loop; they differ only in what they
/// can be called on. Used by `register` to catch a freshly-registered view
/// up to head before it starts receiving live fan-out.
pub(crate) fn catch_up<B: Body>(view: &mut dyn View<B>, log: &Log, upto: u64) -> Result<()> {
    for item in log.records_from(view.cursor()) {
        let record = item?;
        if record.position >= upto {
            break;
        }
        let event = decode_stored_event::<B>(&record)?;
        view.apply(&event);
    }
    Ok(())
}
