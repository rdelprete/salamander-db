//! WP-1 exit criterion (Phase 1.5 spec §3) — the engine carries a
//! *non-agent* payload type end to end: append / commit / reopen /
//! projection, with no agent vocabulary anywhere in sight. This is the test
//! that proves `Salamander<B>` is genuinely payload-generic and not just
//! `Salamander<EventBody>` wearing a type parameter.

use salamander::{Event, Projection, Salamander};
use serde::{Deserialize, Serialize};

/// A payload that has nothing to do with agents or KV.
#[derive(Clone, Serialize, Deserialize)]
enum Temp {
    Reading(i32),
    Reset,
}

/// Running total of readings; `Reset` zeroes it. A fold that is neither KV
/// nor a session view — exercises the `Projection` trait for an arbitrary
/// `Body`.
#[derive(Default)]
struct Thermostat {
    total: i64,
    cursor: u64,
}

impl Projection for Thermostat {
    type Body = Temp;
    type State = i64;

    fn apply(&mut self, event: &Event<Temp>) {
        match &event.body {
            Temp::Reading(c) => self.total += *c as i64,
            Temp::Reset => self.total = 0,
        }
        self.cursor = event.offset + 1;
    }

    fn cursor(&self) -> u64 {
        self.cursor
    }

    fn state(&self) -> &i64 {
        &self.total
    }
}

#[test]
fn custom_payload_survives_reopen_and_projects() {
    let dir = tempfile::tempdir().unwrap();

    // Write a run of readings with a reset in the middle, then drop the DB.
    {
        let mut db: Salamander<Temp> = Salamander::open(dir.path()).unwrap();
        db.append("sensor", Temp::Reading(20)).unwrap();
        db.append("sensor", Temp::Reading(5)).unwrap();
        db.append("sensor", Temp::Reset).unwrap();
        db.append("sensor", Temp::Reading(7)).unwrap();
        db.commit().unwrap();
        assert_eq!(db.head(), 4);
    }

    // Reopen from disk and rebuild the projection from the persisted log.
    let db: Salamander<Temp> = Salamander::open(dir.path()).unwrap();
    let t: Thermostat = db.projection().unwrap();
    assert_eq!(*t.state(), 7); // 20 + 5, reset to 0, then + 7
    assert_eq!(t.cursor(), 4);

    // Time-travel works over the custom payload too: as of offset 2, before
    // the reset, the total is 20 + 5 = 25.
    let mid: Thermostat = db.view_at(2).unwrap();
    assert_eq!(*mid.state(), 25);
}
