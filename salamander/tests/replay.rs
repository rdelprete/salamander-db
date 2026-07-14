//! Replay behavior through the public API.
//! Put/get survives a process restart, through the public `Salamander` API.

use salamander::agent::{EventBody, KvProjection};
use salamander::{AgentDb, Projection};

#[test]
fn put_get_survives_restart_through_public_api() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = AgentDb::open(dir.path()).unwrap();
        db.append(
            "ns",
            EventBody::Put {
                key: "a".into(),
                value: b"1".to_vec(),
            },
        )
        .unwrap();
        db.append(
            "ns",
            EventBody::Put {
                key: "b".into(),
                value: b"2".to_vec(),
            },
        )
        .unwrap();
        db.commit().unwrap();
    }

    let db = AgentDb::open(dir.path()).unwrap();
    let kv: KvProjection = db.projection().unwrap();
    assert_eq!(kv.state().get("a"), Some(&b"1".to_vec()));
    assert_eq!(kv.state().get("b"), Some(&b"2".to_vec()));
}
