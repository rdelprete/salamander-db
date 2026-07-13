//! Format-v2 record framing adapter used by the segmented log.
//!
//! Production callers provide an engine envelope. The payload-only functions
//! remain a narrow crate-internal adapter for low-level byte-log tests and are
//! marked with a reserved event type so typed replay can recognize them.

use crate::format::{
    self, BatchId, BranchId, CodecId, DatabaseId, EventId, EventType, FormatLimits, FrameKind,
    Metadata, OwnedStoredRecord, RecordEnvelopeV2, StoredRecord, StreamId, StreamRevision,
};
use crate::Result;

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq)]
pub struct Record<'a> {
    pub offset: u64,
    pub payload: &'a [u8],
}

#[allow(dead_code)]
pub fn encode(offset: u64, payload: &[u8], out: &mut Vec<u8>) {
    let envelope = raw_test_envelope();
    encode_enveloped(offset, &envelope, payload, out).expect("raw test envelope is valid");
}

pub fn encode_enveloped(
    offset: u64,
    envelope: &RecordEnvelopeV2,
    payload: &[u8],
    out: &mut Vec<u8>,
) -> Result<()> {
    encode_kind(FrameKind::Event, offset, envelope, payload, out)
}

pub fn encode_kind(
    kind: FrameKind,
    offset: u64,
    envelope: &RecordEnvelopeV2,
    payload: &[u8],
    out: &mut Vec<u8>,
) -> Result<()> {
    let bytes = format::encode(
        &StoredRecord {
            kind,
            flags: 0,
            position: offset,
            envelope: envelope.clone(),
            payload,
        },
        FormatLimits::default(),
    )?;
    out.extend_from_slice(&bytes);
    Ok(())
}

#[allow(dead_code)]
pub fn decode(buf: &[u8]) -> Result<Option<(Record<'_>, usize)>> {
    Ok(
        format::decode(buf, FormatLimits::default())?.map(|(stored, consumed)| {
            (
                Record {
                    offset: stored.position,
                    payload: stored.payload,
                },
                consumed,
            )
        }),
    )
}

pub fn decode_owned(buf: &[u8]) -> Result<Option<(OwnedStoredRecord, usize)>> {
    Ok(format::decode(buf, FormatLimits::default())?
        .map(|(stored, consumed)| (OwnedStoredRecord::from(stored), consumed)))
}

#[allow(dead_code)]
pub(crate) fn raw_test_envelope() -> RecordEnvelopeV2 {
    RecordEnvelopeV2 {
        event_id: EventId::ZERO,
        database_id: DatabaseId::ZERO,
        branch_id: BranchId::ZERO,
        stream_id: StreamId::ZERO,
        stream_revision: StreamRevision(0),
        timestamp_unix_nanos: 0,
        event_type: EventType::new("salamander.raw-v2").expect("static event type"),
        schema_version: 1,
        codec: CodecId::RUST_BINCODE_V1,
        batch_id: BatchId::ZERO,
        batch_index: 0,
        metadata: Metadata::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SalamanderError;
    use proptest::prelude::*;

    fn encode_record(offset: u64, payload: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();
        encode(offset, payload, &mut buf);
        buf
    }

    #[test]
    fn round_trip_single_record() {
        let buf = encode_record(42, b"hello");
        let (record, consumed) = decode(&buf).unwrap().unwrap();
        assert_eq!(record.offset, 42);
        assert_eq!(record.payload, &b"hello"[..]);
        assert_eq!(consumed, buf.len());
    }

    #[test]
    fn round_trip_empty_payload() {
        let buf = encode_record(7, &[]);
        let (record, consumed) = decode(&buf).unwrap().unwrap();
        assert_eq!(record.offset, 7);
        assert!(record.payload.is_empty());
        assert_eq!(consumed, buf.len());
    }

    #[test]
    fn truncated_record_is_incomplete() {
        let buf = encode_record(1, b"payload");
        for len in 0..buf.len() {
            assert!(decode(&buf[..len]).unwrap().is_none());
        }
    }

    #[test]
    fn corrupted_payload_yields_crc_error() {
        let mut buf = encode_record(9, b"corrupt-me");
        *buf.last_mut().unwrap() ^= 0xff;
        assert!(matches!(
            decode(&buf),
            Err(SalamanderError::Corrupt { offset: 9, .. })
        ));
    }

    #[test]
    fn multi_record_buffer_walk() {
        let records: [(u64, &[u8]); 3] = [(0, b"first"), (1, b"second-record"), (2, b"3")];
        let mut buf = Vec::new();
        for (offset, payload) in records {
            encode(offset, payload, &mut buf);
        }
        let mut pos = 0;
        for (offset, payload) in records {
            let (record, consumed) = decode(&buf[pos..]).unwrap().unwrap();
            assert_eq!(record.offset, offset);
            assert_eq!(record.payload, payload);
            pos += consumed;
        }
        assert_eq!(pos, buf.len());
    }

    proptest! {
        #[test]
        fn round_trip(offset: u64, payload in prop::collection::vec(any::<u8>(), 0..256)) {
            let bytes = encode_record(offset, &payload);
            let (record, consumed) = decode(&bytes).unwrap().unwrap();
            prop_assert_eq!(record.offset, offset);
            prop_assert_eq!(record.payload, payload.as_slice());
            prop_assert_eq!(consumed, bytes.len());
        }
    }
}
