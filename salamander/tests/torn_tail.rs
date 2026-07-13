//! M1 exit test — IMPLEMENTATION.md Step 2/3/5.
//! Torn-tail recovery (DESIGN.md §6, case C1) through the public
//! `Salamander` API. Segment-roll-crossing recovery specifically needs a
//! tiny `segment_max_bytes` (not exposed on the public API) to exercise
//! without writing 64 MiB of data — that coverage lives in
//! `src/log/mod.rs`'s own test module instead.

use salamander::agent::{EventBody, KvProjection};
use salamander::{AgentDb, Projection};

#[test]
fn torn_tail_recovers_through_public_api() {
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
        db.commit().unwrap();
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

    // Hand-corrupt the tail of the active segment: trim the last byte off
    // the file, simulating a crash mid-write (DESIGN.md §6, case C1).
    let seg_path = dir.path().join("log").join("00000000000000000000.seg");
    let len = std::fs::metadata(&seg_path).unwrap().len();
    let f = std::fs::OpenOptions::new()
        .write(true)
        .open(&seg_path)
        .unwrap();
    f.set_len(len - 1).unwrap();
    drop(f);

    let mut db = AgentDb::open(dir.path()).unwrap();
    // The torn last record ("b") is gone; the first ("a") survived.
    assert_eq!(db.head(), 1);
    let kv: KvProjection = db.projection().unwrap();
    assert_eq!(kv.state().get("a"), Some(&b"1".to_vec()));
    assert_eq!(kv.state().get("b"), None);

    // Still writable after recovery, continuing at the correct offset.
    let offset = db
        .append(
            "ns",
            EventBody::Put {
                key: "c".into(),
                value: b"3".to_vec(),
            },
        )
        .unwrap();
    assert_eq!(offset, 1);
}
