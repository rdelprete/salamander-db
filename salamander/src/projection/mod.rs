//! DESIGN.md §5 — the projection contract. This trait is the product;
//! everything else serves it.

use crate::event::{Body, Event};
use crate::format::{CodecId, OwnedStoredRecord};
use crate::log::Log;
use crate::{Result, SalamanderError};

pub trait Projection {
    /// The payload type this projection folds. Ties the projection to a
    /// `Salamander<B>` with the same `B`: you can only build a projection
    /// from a log whose events carry the payload it knows how to apply.
    /// Expressed as an associated type (not a trait parameter) so the
    /// generic replay helpers below read as `P: Projection` with no extra
    /// `B` to thread through.
    type Body: Body;

    type State;

    /// Apply one event. MUST be deterministic and infallible on valid
    /// input: same events in same order => same state, every time, on
    /// every machine (DESIGN.md §6, INV-1).
    fn apply(&mut self, event: &Event<Self::Body>);

    /// Current cursor: all events with offset < cursor have been applied.
    fn cursor(&self) -> u64;

    /// Read access to derived state.
    fn state(&self) -> &Self::State;
}

/// Projections that need to know their namespace before any events are
/// applied (DESIGN.md §2) — e.g. `SessionProjection` filters to one
/// namespace, so it can't be built via plain `Default` the way
/// `KvProjection` can.
pub trait NamespaceScoped: Projection {
    fn new_for(namespace: &str) -> Self;
}

/// Deserializes one record's payload into an `Event<B>`, then stamps the
/// log-assigned `offset` onto it. The log's own offset is authoritative
/// (DESIGN.md §2: "Global, never reused") — the writer can't actually know
/// the real offset until *after* `Log::append` returns, so whatever offset
/// it serialized into the payload is at best a same-process prediction,
/// not a durable fact. Shared by `replay_into` and `introspect::replay` so
/// this invariant only has to be gotten right in one place.
pub(crate) fn decode_event<B: Body>(offset: u64, bytes: &[u8]) -> Result<Event<B>> {
    let mut event: Event<B> =
        bincode::deserialize(bytes).map_err(|e| SalamanderError::Corrupt {
            offset,
            reason: e.to_string(),
        })?;
    event.offset = offset;
    Ok(event)
}

pub(crate) fn decode_stored_event<B: Body>(record: &OwnedStoredRecord) -> Result<Event<B>> {
    if record.envelope.event_type.as_str() == "salamander.raw-v2" {
        return decode_event(record.position, &record.payload);
    }
    if record.envelope.codec != CodecId::RUST_BINCODE_V1 {
        return Err(SalamanderError::Codec(format!(
            "typed Rust replay does not support codec {}",
            record.envelope.codec.0
        )));
    }
    let body: B =
        bincode::deserialize(&record.payload).map_err(|error| SalamanderError::Corrupt {
            offset: record.position,
            reason: format!("payload decode: {error}"),
        })?;
    let namespace = record
        .envelope
        .metadata
        .get("salamander.stream_name")
        .ok_or_else(|| SalamanderError::Corrupt {
            offset: record.position,
            reason: "v2 event is missing salamander.stream_name metadata".into(),
        })
        .and_then(|bytes| {
            std::str::from_utf8(bytes)
                .map(str::to_owned)
                .map_err(|_| SalamanderError::Corrupt {
                    offset: record.position,
                    reason: "v2 stream name is not UTF-8".into(),
                })
        })?;
    Ok(Event {
        offset: record.position,
        timestamp_ms: u64::try_from(record.envelope.timestamp_unix_nanos.max(0))
            .unwrap_or(u64::MAX)
            / 1_000_000,
        namespace,
        body,
    })
}

/// Folds `log[p.cursor(), upto)` into `p` (DESIGN.md §6, INV-1). Rebuild,
/// time-travel, and fork (DESIGN.md §5) are all this function with a
/// different `upto`. The payload type is inferred from the projection's
/// `Body` associated type — the log stays bytes-only.
pub fn replay_into<P: Projection>(p: &mut P, log: &Log, upto: u64) -> Result<()> {
    for item in log.records_from(p.cursor()) {
        let record = item?;
        if record.position >= upto {
            break;
        }
        let event = decode_stored_event::<P::Body>(&record)?;
        p.apply(&event);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::{EventBody, KvProjection};
    use tempfile::tempdir;

    fn put(namespace: &str, key: &str, value: &[u8]) -> Event<EventBody> {
        Event {
            offset: 0, // placeholder -- replay_into stamps the real one on read
            timestamp_ms: 0,
            namespace: namespace.to_string(),
            body: EventBody::Put {
                key: key.to_string(),
                value: value.to_vec(),
            },
        }
    }

    fn delete(namespace: &str, key: &str) -> Event<EventBody> {
        Event {
            offset: 0,
            timestamp_ms: 0,
            namespace: namespace.to_string(),
            body: EventBody::Delete {
                key: key.to_string(),
            },
        }
    }

    fn append_bincode(log: &mut Log, event: &Event<EventBody>) -> u64 {
        let bytes = bincode::serialize(event).unwrap();
        log.append(&bytes).unwrap()
    }

    #[test]
    fn replay_into_folds_put_and_delete_events() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path()).unwrap();
        append_bincode(&mut log, &put("ns", "a", b"1"));
        append_bincode(&mut log, &put("ns", "b", b"2"));
        append_bincode(&mut log, &delete("ns", "a"));
        log.commit().unwrap();

        let mut proj = KvProjection::default();
        replay_into(&mut proj, &log, log.head()).unwrap();

        assert_eq!(proj.state().get("a"), None);
        assert_eq!(proj.state().get("b"), Some(&b"2".to_vec()));
        assert_eq!(proj.cursor(), log.head());
    }

    #[test]
    fn two_replays_of_same_log_are_identical() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path()).unwrap();
        for i in 0..5u8 {
            append_bincode(&mut log, &put("ns", &format!("k{i}"), &[i]));
        }
        log.commit().unwrap();

        let mut proj_a = KvProjection::default();
        let mut proj_b = KvProjection::default();
        replay_into(&mut proj_a, &log, log.head()).unwrap();
        replay_into(&mut proj_b, &log, log.head()).unwrap();

        assert_eq!(proj_a.state(), proj_b.state());
        assert_eq!(proj_a.cursor(), proj_b.cursor());
    }

    #[test]
    fn rebuild_after_reopen_is_identical() {
        let dir = tempdir().unwrap();
        {
            let mut log = Log::open(dir.path()).unwrap();
            for i in 0..5u8 {
                append_bincode(&mut log, &put("ns", &format!("k{i}"), &[i]));
            }
            log.commit().unwrap();
        }

        let log = Log::open(dir.path()).unwrap();
        let mut proj = KvProjection::default();
        replay_into(&mut proj, &log, log.head()).unwrap();

        for i in 0..5u8 {
            assert_eq!(proj.state().get(&format!("k{i}")), Some(&vec![i]));
        }
    }

    #[test]
    fn replay_into_stops_before_upto() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path()).unwrap();
        for i in 0..5u8 {
            append_bincode(&mut log, &put("ns", &format!("k{i}"), &[i]));
        }
        log.commit().unwrap();

        let mut proj = KvProjection::default();
        replay_into(&mut proj, &log, 3).unwrap();

        assert_eq!(proj.cursor(), 3);
        assert_eq!(proj.state().len(), 3);
        assert!(proj.state().contains_key("k2"));
        assert!(!proj.state().contains_key("k3"));
    }

    #[test]
    fn replay_into_resumes_from_existing_cursor() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path()).unwrap();
        for i in 0..5u8 {
            append_bincode(&mut log, &put("ns", &format!("k{i}"), &[i]));
        }
        log.commit().unwrap();

        let mut incremental = KvProjection::default();
        replay_into(&mut incremental, &log, 2).unwrap();
        replay_into(&mut incremental, &log, log.head()).unwrap();

        let mut one_shot = KvProjection::default();
        replay_into(&mut one_shot, &log, log.head()).unwrap();

        assert_eq!(incremental.state(), one_shot.state());
        assert_eq!(incremental.cursor(), one_shot.cursor());
    }

    #[test]
    fn replay_into_stamps_log_offset_even_if_event_embeds_a_different_one() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path()).unwrap();
        for i in 0..3u8 {
            // Deliberately wrong embedded offset -- replay_into must not
            // trust it (see the comment on replay_into itself).
            let event = Event {
                offset: 999,
                timestamp_ms: 0,
                namespace: "ns".to_string(),
                body: EventBody::Put {
                    key: format!("k{i}"),
                    value: vec![i],
                },
            };
            append_bincode(&mut log, &event);
        }
        log.commit().unwrap();

        let mut proj = KvProjection::default();
        replay_into(&mut proj, &log, log.head()).unwrap();

        // If replay_into had trusted event.offset (999) instead of the
        // log's real offset, the cursor would be stuck near 1000 and a
        // second incremental replay would silently see nothing new.
        assert_eq!(proj.cursor(), 3);
        assert_eq!(proj.state().len(), 3);
    }

    #[test]
    fn replay_into_surfaces_corrupt_payload_as_error() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path()).unwrap();
        append_bincode(&mut log, &put("ns", "a", b"1"));
        log.append(b"not a bincode-encoded Event").unwrap();
        log.commit().unwrap();

        let mut proj = KvProjection::default();
        let err = replay_into(&mut proj, &log, log.head()).unwrap_err();

        assert!(matches!(err, SalamanderError::Corrupt { offset: 1, .. }));
        // The good record before the bad one was still applied, and the
        // cursor stopped exactly at the failing record, not past it.
        assert_eq!(proj.state().get("a"), Some(&b"1".to_vec()));
        assert_eq!(proj.cursor(), 1);
    }
}
