//! M4 — IMPLEMENTATION.md Step 6.
//!
//! Record-level framing properties (round-trip, corruption-never-silent)
//! live in `src/log/record.rs`'s own test module (Step 1); segment-level
//! scan properties (exact recovered-prefix-length under arbitrary
//! truncation) live in `src/log/segment.rs`'s own test module (this step)
//! — neither `Segment` nor `Log` are part of the crate's public API, so
//! they can't be exercised from here.
//!
//! What *is* testable from here, now that `Salamander` is public: random
//! single-byte corruption of the active segment file must never panic on
//! reopen, and must either recover cleanly with a coherent state or
//! return a clear `Err` — never a wrong-but-successful-looking recovery.
//! This complements the crash harness (`salamander-demo crashtest`),
//! which tests the same territory via real process kills but is slow and
//! timing-dependent; this is fast, deterministic, and CI-friendly.

use proptest::prelude::*;
use salamander::agent::{EventBody, KvProjection};
use salamander::{AgentDb, Projection};

proptest! {
    #[test]
    fn corrupting_the_active_segment_never_panics_on_reopen(
        num_records in 1usize..15,
        corrupt_byte in any::<u8>(),
        corrupt_at_fraction in 0.0f64..=1.0,
    ) {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut db = AgentDb::open(dir.path()).unwrap();
            for i in 0..num_records {
                db.append(
                    "ns",
                    EventBody::Put {
                        key: format!("k{i}"),
                        value: vec![i as u8; 8],
                    },
                )
                .unwrap();
            }
            db.commit().unwrap();
        }

        let seg_path = dir.path().join("log").join("00000000000000000000.seg");
        let mut bytes = std::fs::read(&seg_path).unwrap();
        if !bytes.is_empty() {
            let idx = ((corrupt_at_fraction * bytes.len() as f64) as usize).min(bytes.len() - 1);
            bytes[idx] = corrupt_byte;
            std::fs::write(&seg_path, &bytes).unwrap();
        }

        // The property: this must never panic. If recovery succeeds, the
        // result must be internally consistent -- never more records than
        // were ever written.
        if let Ok(db) = AgentDb::open(dir.path()) {
            prop_assert!(db.head() <= num_records as u64);
            if let Ok(kv) = db.projection::<KvProjection>() {
                prop_assert!(kv.state().len() <= num_records);
            }
        }
    }
}
