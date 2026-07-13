//! `KvProjection`: a `BTreeMap<String, Vec<u8>>` folded over `Put`/`Delete`
//! events. The engine-correctness workhorse; used by the crash harness.
//!
//! Kept in the `agent` module for now (Phase 1.5 spec, WP-1 step 4): its
//! `Put`/`Delete` live in `EventBody`, so it folds `EventBody` like the
//! session view does. The query layer's `IndexedView` (WP-3) supersedes it
//! as the general-purpose, payload-agnostic store.

use std::collections::BTreeMap;

use super::EventBody;
use crate::event::Event;
use crate::projection::Projection;

/// A last-write-wins key/value store folded from [`EventBody::Put`] and
/// [`EventBody::Delete`] events. The simplest built-in projection; the
/// query layer's [`crate::IndexedView`] is the general-purpose store.
#[derive(Debug, Default)]
pub struct KvProjection {
    state: BTreeMap<String, Vec<u8>>,
    cursor: u64,
}

impl Projection for KvProjection {
    type Body = EventBody;
    type State = BTreeMap<String, Vec<u8>>;

    fn apply(&mut self, event: &Event<EventBody>) {
        match &event.body {
            EventBody::Put { key, value } => {
                self.state.insert(key.clone(), value.clone());
            }
            EventBody::Delete { key } => {
                self.state.remove(key);
            }
            _ => {}
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
