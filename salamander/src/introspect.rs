//! Read-only introspection: raw `replay(range, f)`. Thin on purpose — this
//! is the seam `salamander-scope` attaches to later (OQ-5), kept separate
//! from `db.rs` so that future stays clean (IMPLEMENTATION.md §1, layout
//! principles).
//!
//! Phase 1: `Log` isn't part of the crate's public API yet (OQ-5 defers
//! that decision to Phase 1.5), so this module's own function is
//! `pub(crate)` for now — the fully-public entry point is
//! `Salamander::replay`, which just calls through to it.

use std::ops::Range;

use crate::event::{Body, Event};
use crate::log::Log;
use crate::projection::decode_stored_event;
use crate::Result;

pub(crate) fn replay<B: Body>(
    log: &Log,
    namespace: &str,
    range: Range<u64>,
    mut f: impl FnMut(&Event<B>),
) -> Result<()> {
    for item in log.records_from(range.start) {
        let record = item?;
        if record.position >= range.end {
            break;
        }
        let event = decode_stored_event::<B>(&record)?;
        if event.namespace == namespace {
            f(&event);
        }
    }
    Ok(())
}
