//! M1 exit test — IMPLEMENTATION.md Step 3/5.
//! Append/close/reopen round-trip through the public `Salamander` API.
//! Segment-roll-specific coverage (forcing a roll needs a tiny
//! `segment_max_bytes`, not exposed on the public API) lives in
//! `src/log/mod.rs`'s own test module instead.

use salamander::agent::{EventBody, KvProjection};
use salamander::{AgentDb, Projection};

#[test]
fn append_close_reopen_roundtrip_through_public_api() {
    let dir = tempfile::tempdir().unwrap();
    let n = 200u32;
    {
        let mut db = AgentDb::open(dir.path()).unwrap();
        for i in 0..n {
            db.append(
                "ns",
                EventBody::Put {
                    key: format!("k{i:04}"),
                    value: format!("v{i:04}").into_bytes(),
                },
            )
            .unwrap();
        }
        db.commit().unwrap();
        assert_eq!(db.head(), n as u64);
    }

    let db = AgentDb::open(dir.path()).unwrap();
    assert_eq!(db.head(), n as u64);
    let kv: KvProjection = db.projection().unwrap();
    for i in 0..n {
        assert_eq!(
            kv.state().get(&format!("k{i:04}")),
            Some(&format!("v{i:04}").into_bytes())
        );
    }
}
