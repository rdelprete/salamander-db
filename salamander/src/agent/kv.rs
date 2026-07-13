//! DESIGN.md §5 — KvProjection: BTreeMap<String, Vec<u8>> over Put/Delete.
//! The engine-correctness workhorse; used by the crash harness (M4).
//!
//! Kept in the `agent` module for now (Phase 1.5 spec, WP-1 step 4): its
//! `Put`/`Delete` live in `EventBody`, so it folds `EventBody` like the
//! session view does. The query layer's `IndexedView` (WP-3) supersedes it
//! as the general-purpose, payload-agnostic store.

use std::collections::BTreeMap;

use super::EventBody;
use crate::event::Event;
use crate::projection::Projection;

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
