//! Stable post-v0.1 framing and engine envelope.
//!
//! This module owns byte-level engine metadata while treating application
//! payloads as opaque bytes. It intentionally has no dependency on the log,
//! projections, agent vocabulary, or database API.
//!
//! `#[doc(hidden)]` at the crate root: the framing internals here are not a
//! stable API. The engine identity and codec types users actually need are
//! re-exported at the crate root and documented there.
#![allow(missing_docs)]

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::{Result, SalamanderError};

pub const FRAME_MAGIC: [u8; 4] = *b"SDB2";
pub const FRAME_VERSION: u16 = 2;
pub(crate) const FRAME_HEADER_LEN: usize = 32;
const ENVELOPE_VERSION: u16 = 1;
const KNOWN_FLAGS: u8 = 0;
const RESERVED_METADATA_PREFIX: &str = "salamander.";

macro_rules! id_type {
    ($name:ident) => {
        #[derive(
            Clone,
            Copy,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Hash,
            Default,
            serde::Serialize,
            serde::Deserialize,
        )]
        pub struct $name([u8; 16]);

        impl $name {
            pub const ZERO: Self = Self([0; 16]);

            pub const fn from_bytes(bytes: [u8; 16]) -> Self {
                Self(bytes)
            }

            pub const fn as_bytes(&self) -> &[u8; 16] {
                &self.0
            }

            pub const fn into_bytes(self) -> [u8; 16] {
                self.0
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                for byte in self.0 {
                    write!(f, "{byte:02x}")?;
                }
                Ok(())
            }
        }
    };
}

id_type!(DatabaseId);
id_type!(EventId);
id_type!(BranchId);
id_type!(StreamId);
id_type!(BatchId);

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Default,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct StreamRevision(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CodecId(pub u32);

impl CodecId {
    pub const JSON_UTF8: Self = Self(1);
    pub const RUST_BINCODE_V1: Self = Self(0x8000_0001);
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventType(String);

impl EventType {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.is_empty() {
            return Err(SalamanderError::InvalidFormat(
                "event type must not be empty".into(),
            ));
        }
        if value.as_bytes().contains(&0) {
            return Err(SalamanderError::InvalidFormat(
                "event type must not contain NUL".into(),
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

pub type Metadata = BTreeMap<String, Vec<u8>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameKind {
    Event = 1,
    System = 2,
    BatchBegin = 3,
    BatchCommit = 4,
}

impl TryFrom<u8> for FrameKind {
    type Error = SalamanderError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::Event),
            2 => Ok(Self::System),
            3 => Ok(Self::BatchBegin),
            4 => Ok(Self::BatchCommit),
            other => Err(SalamanderError::InvalidFormat(format!(
                "unknown frame kind {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordEnvelopeV2 {
    pub event_id: EventId,
    pub database_id: DatabaseId,
    pub branch_id: BranchId,
    pub stream_id: StreamId,
    pub stream_revision: StreamRevision,
    pub timestamp_unix_nanos: i64,
    pub event_type: EventType,
    pub schema_version: u32,
    pub codec: CodecId,
    pub batch_id: BatchId,
    pub batch_index: u32,
    pub metadata: Metadata,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredRecord<'a> {
    pub kind: FrameKind,
    pub flags: u8,
    pub position: u64,
    pub envelope: RecordEnvelopeV2,
    pub payload: &'a [u8],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedStoredRecord {
    pub kind: FrameKind,
    pub flags: u8,
    pub position: u64,
    pub envelope: RecordEnvelopeV2,
    pub payload: Vec<u8>,
}

impl OwnedStoredRecord {
    pub fn as_borrowed(&self) -> StoredRecord<'_> {
        StoredRecord {
            kind: self.kind,
            flags: self.flags,
            position: self.position,
            envelope: self.envelope.clone(),
            payload: &self.payload,
        }
    }
}

impl From<StoredRecord<'_>> for OwnedStoredRecord {
    fn from(value: StoredRecord<'_>) -> Self {
        Self {
            kind: value.kind,
            flags: value.flags,
            position: value.position,
            envelope: value.envelope,
            payload: value.payload.to_vec(),
        }
    }
}

static ID_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Generate a process-unique 128-bit identifier without requiring an ambient
/// random-number service. The timestamp half and process/counter half make
/// collisions across normal embedded-engine operation vanishingly unlikely.
pub fn generate_id_bytes() -> [u8; 16] {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let counter = ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    let process = u64::from(std::process::id());
    let mut bytes = [0; 16];
    bytes[..8].copy_from_slice(&(nanos as u64).to_le_bytes());
    bytes[8..].copy_from_slice(&(counter ^ process.rotate_left(32)).to_le_bytes());
    bytes
}

/// Deterministically map a stream name into a compact v2 placeholder ID.
/// WP-02 replaces this with the first-class stream catalog while preserving
/// the stable ID already written in v2 envelopes.
pub fn derive_stream_id(database: DatabaseId, branch: BranchId, name: &str) -> StreamId {
    let mut left = 0xcbf2_9ce4_8422_2325u64;
    let mut right = 0x8422_2325_cbf2_9ce4u64;
    for byte in database
        .as_bytes()
        .iter()
        .chain(branch.as_bytes())
        .chain(name.as_bytes())
    {
        left ^= u64::from(*byte);
        left = left.wrapping_mul(0x0000_0100_0000_01b3);
        right ^= u64::from(*byte).rotate_left(1);
        right = right.wrapping_mul(0x9e37_79b1_85eb_ca87);
    }
    let mut bytes = [0; 16];
    bytes[..8].copy_from_slice(&left.to_le_bytes());
    bytes[8..].copy_from_slice(&right.to_le_bytes());
    StreamId::from_bytes(bytes)
}

pub(crate) fn derive_import_id(source_identity: [u8; 16], offset: u64) -> [u8; 16] {
    let mut bytes = source_identity;
    for (index, byte) in offset.to_le_bytes().iter().enumerate() {
        bytes[index] ^= *byte;
        bytes[index + 8] ^= byte.rotate_left(1);
    }
    for round in 0..4u8 {
        for index in 0..16 {
            let next = bytes[(index + 1) % 16];
            bytes[index] = bytes[index]
                .wrapping_add(next)
                .rotate_left(u32::from((round + index as u8) % 7 + 1));
        }
    }
    bytes
}

#[derive(Debug, Clone, Copy)]
pub struct FormatLimits {
    pub max_envelope_bytes: u32,
    pub max_payload_bytes: u32,
    pub max_metadata_entries: u16,
    pub max_metadata_key_bytes: u16,
    pub max_metadata_value_bytes: u32,
    pub max_event_type_bytes: u16,
}

impl Default for FormatLimits {
    fn default() -> Self {
        Self {
            max_envelope_bytes: 1024 * 1024,
            max_payload_bytes: 16 * 1024 * 1024,
            max_metadata_entries: 256,
            max_metadata_key_bytes: 1024,
            max_metadata_value_bytes: 64 * 1024,
            max_event_type_bytes: 1024,
        }
    }
}

pub fn encode(record: &StoredRecord<'_>, limits: FormatLimits) -> Result<Vec<u8>> {
    validate_flags(record.flags)?;
    enforce_limit(
        "payload bytes",
        record.payload.len(),
        limits.max_payload_bytes as usize,
    )?;
    let envelope = encode_envelope(&record.envelope, limits)?;
    enforce_limit(
        "envelope bytes",
        envelope.len(),
        limits.max_envelope_bytes as usize,
    )?;

    let mut out = Vec::with_capacity(FRAME_HEADER_LEN + envelope.len() + record.payload.len());
    out.extend_from_slice(&FRAME_MAGIC);
    out.extend_from_slice(&FRAME_VERSION.to_le_bytes());
    out.push(record.kind as u8);
    out.push(record.flags);
    out.extend_from_slice(&(envelope.len() as u32).to_le_bytes());
    out.extend_from_slice(&(record.payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&record.position.to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes());
    out.extend_from_slice(&envelope);
    out.extend_from_slice(record.payload);

    let crc = frame_crc(&out);
    out[24..28].copy_from_slice(&crc.to_le_bytes());
    Ok(out)
}

/// Header-only probe: how many bytes the frame starting at `buf[0]` spans,
/// or `None` when fewer than a full header is available. Validates the
/// fixed header (magic, version, kind, flags, limits) but reads no envelope
/// or payload bytes and computes no checksum — the streaming reader uses it
/// to size its buffer fill before a full `decode`.
pub(crate) fn frame_total_len(buf: &[u8], limits: FormatLimits) -> Result<Option<usize>> {
    if buf.len() < FRAME_HEADER_LEN {
        return Ok(None);
    }
    if buf[0..4] != FRAME_MAGIC {
        return Err(SalamanderError::InvalidFormat("bad v2 frame magic".into()));
    }
    let version = read_u16(&buf[4..6]);
    if version != FRAME_VERSION {
        return Err(SalamanderError::InvalidFormat(format!(
            "unsupported frame version {version}"
        )));
    }
    FrameKind::try_from(buf[6])?;
    validate_flags(buf[7])?;
    let envelope_len = read_u32(&buf[8..12]) as usize;
    let payload_len = read_u32(&buf[12..16]) as usize;
    enforce_limit(
        "envelope bytes",
        envelope_len,
        limits.max_envelope_bytes as usize,
    )?;
    enforce_limit(
        "payload bytes",
        payload_len,
        limits.max_payload_bytes as usize,
    )?;
    FRAME_HEADER_LEN
        .checked_add(envelope_len)
        .and_then(|n| n.checked_add(payload_len))
        .map(Some)
        .ok_or_else(|| SalamanderError::InvalidFormat("frame length overflow".into()))
}

pub fn decode(buf: &[u8], limits: FormatLimits) -> Result<Option<(StoredRecord<'_>, usize)>> {
    if buf.len() < FRAME_HEADER_LEN {
        return Ok(None);
    }
    if buf[0..4] != FRAME_MAGIC {
        return Err(SalamanderError::InvalidFormat("bad v2 frame magic".into()));
    }
    let version = read_u16(&buf[4..6]);
    if version != FRAME_VERSION {
        return Err(SalamanderError::InvalidFormat(format!(
            "unsupported frame version {version}"
        )));
    }
    let kind = FrameKind::try_from(buf[6])?;
    let flags = buf[7];
    validate_flags(flags)?;
    let envelope_len = read_u32(&buf[8..12]) as usize;
    let payload_len = read_u32(&buf[12..16]) as usize;
    enforce_limit(
        "envelope bytes",
        envelope_len,
        limits.max_envelope_bytes as usize,
    )?;
    enforce_limit(
        "payload bytes",
        payload_len,
        limits.max_payload_bytes as usize,
    )?;
    let position = read_u64(&buf[16..24]);
    let stored_crc = read_u32(&buf[24..28]);
    if read_u32(&buf[28..32]) != 0 {
        return Err(SalamanderError::InvalidFormat(
            "nonzero reserved frame header field".into(),
        ));
    }
    let total = FRAME_HEADER_LEN
        .checked_add(envelope_len)
        .and_then(|n| n.checked_add(payload_len))
        .ok_or_else(|| SalamanderError::InvalidFormat("frame length overflow".into()))?;
    if buf.len() < total {
        return Ok(None);
    }
    let computed_crc = frame_crc(&buf[..total]);
    if stored_crc != computed_crc {
        return Err(SalamanderError::Corrupt {
            offset: position,
            reason: format!(
                "v2 crc mismatch: stored {stored_crc:#010x}, computed {computed_crc:#010x}"
            ),
        });
    }
    let envelope_end = FRAME_HEADER_LEN + envelope_len;
    let envelope = decode_envelope(&buf[FRAME_HEADER_LEN..envelope_end], limits)?;
    let payload = &buf[envelope_end..total];
    Ok(Some((
        StoredRecord {
            kind,
            flags,
            position,
            envelope,
            payload,
        },
        total,
    )))
}

fn encode_envelope(envelope: &RecordEnvelopeV2, limits: FormatLimits) -> Result<Vec<u8>> {
    enforce_limit(
        "event type bytes",
        envelope.event_type.as_str().len(),
        limits.max_event_type_bytes as usize,
    )?;
    enforce_limit(
        "metadata entries",
        envelope.metadata.len(),
        limits.max_metadata_entries as usize,
    )?;

    let mut out = Vec::new();
    out.extend_from_slice(&ENVELOPE_VERSION.to_le_bytes());
    out.extend_from_slice(envelope.event_id.as_bytes());
    out.extend_from_slice(envelope.database_id.as_bytes());
    out.extend_from_slice(envelope.branch_id.as_bytes());
    out.extend_from_slice(envelope.stream_id.as_bytes());
    out.extend_from_slice(&envelope.stream_revision.0.to_le_bytes());
    out.extend_from_slice(&envelope.timestamp_unix_nanos.to_le_bytes());
    write_short_bytes(&mut out, envelope.event_type.as_str().as_bytes())?;
    out.extend_from_slice(&envelope.schema_version.to_le_bytes());
    out.extend_from_slice(&envelope.codec.0.to_le_bytes());
    out.extend_from_slice(envelope.batch_id.as_bytes());
    out.extend_from_slice(&envelope.batch_index.to_le_bytes());
    out.extend_from_slice(&(envelope.metadata.len() as u16).to_le_bytes());
    for (key, value) in &envelope.metadata {
        validate_metadata_key(key)?;
        enforce_limit(
            "metadata key bytes",
            key.len(),
            limits.max_metadata_key_bytes as usize,
        )?;
        enforce_limit(
            "metadata value bytes",
            value.len(),
            limits.max_metadata_value_bytes as usize,
        )?;
        write_short_bytes(&mut out, key.as_bytes())?;
        out.extend_from_slice(&(value.len() as u32).to_le_bytes());
        out.extend_from_slice(value);
    }
    Ok(out)
}

fn decode_envelope(buf: &[u8], limits: FormatLimits) -> Result<RecordEnvelopeV2> {
    let mut cursor = Cursor::new(buf);
    let version = cursor.u16()?;
    if version != ENVELOPE_VERSION {
        return Err(SalamanderError::InvalidFormat(format!(
            "unsupported envelope version {version}"
        )));
    }
    let event_id = EventId::from_bytes(cursor.array()?);
    let database_id = DatabaseId::from_bytes(cursor.array()?);
    let branch_id = BranchId::from_bytes(cursor.array()?);
    let stream_id = StreamId::from_bytes(cursor.array()?);
    let stream_revision = StreamRevision(cursor.u64()?);
    let timestamp_unix_nanos = cursor.i64()?;
    let event_type_bytes = cursor.short_bytes(limits.max_event_type_bytes as usize)?;
    let event_type = EventType::new(
        std::str::from_utf8(event_type_bytes)
            .map_err(|_| SalamanderError::InvalidFormat("event type is not UTF-8".into()))?,
    )?;
    let schema_version = cursor.u32()?;
    let codec = CodecId(cursor.u32()?);
    let batch_id = BatchId::from_bytes(cursor.array()?);
    let batch_index = cursor.u32()?;
    let metadata_count = cursor.u16()? as usize;
    enforce_limit(
        "metadata entries",
        metadata_count,
        limits.max_metadata_entries as usize,
    )?;
    let mut metadata = Metadata::new();
    for _ in 0..metadata_count {
        let key_bytes = cursor.short_bytes(limits.max_metadata_key_bytes as usize)?;
        let key = std::str::from_utf8(key_bytes)
            .map_err(|_| SalamanderError::InvalidFormat("metadata key is not UTF-8".into()))?
            .to_string();
        validate_metadata_key(&key)?;
        let value_len = cursor.u32()? as usize;
        enforce_limit(
            "metadata value bytes",
            value_len,
            limits.max_metadata_value_bytes as usize,
        )?;
        let value = cursor.take(value_len)?.to_vec();
        if metadata.insert(key, value).is_some() {
            return Err(SalamanderError::InvalidFormat(
                "duplicate metadata key".into(),
            ));
        }
    }
    if !cursor.is_empty() {
        return Err(SalamanderError::InvalidFormat(
            "trailing bytes in v2 envelope".into(),
        ));
    }
    Ok(RecordEnvelopeV2 {
        event_id,
        database_id,
        branch_id,
        stream_id,
        stream_revision,
        timestamp_unix_nanos,
        event_type,
        schema_version,
        codec,
        batch_id,
        batch_index,
        metadata,
    })
}

fn frame_crc(bytes: &[u8]) -> u32 {
    let mut crc = crc32c::crc32c(&bytes[..24]);
    crc = crc32c::crc32c_append(crc, &bytes[28..]);
    crc
}

fn validate_flags(flags: u8) -> Result<()> {
    if flags & !KNOWN_FLAGS != 0 {
        return Err(SalamanderError::InvalidFormat(format!(
            "unknown mandatory frame flags {flags:#04x}"
        )));
    }
    Ok(())
}

fn validate_metadata_key(key: &str) -> Result<()> {
    if key.is_empty() || key.as_bytes().contains(&0) {
        return Err(SalamanderError::InvalidFormat(
            "metadata key must be nonempty UTF-8 without NUL".into(),
        ));
    }
    if key.starts_with(RESERVED_METADATA_PREFIX) && !is_known_reserved_key(key) {
        return Err(SalamanderError::InvalidFormat(format!(
            "unknown reserved metadata key {key}"
        )));
    }
    Ok(())
}

fn is_known_reserved_key(key: &str) -> bool {
    matches!(
        key,
        "salamander.stream_name"
            | "salamander.v1_offset"
            | "salamander.v1_namespace"
            | "salamander.batch_count"
            | "salamander.batch_digest"
            | "salamander.idempotency_key"
            | "salamander.request_digest"
    )
}

fn enforce_limit(resource: &'static str, actual: usize, maximum: usize) -> Result<()> {
    if actual > maximum {
        return Err(SalamanderError::ResourceLimit {
            resource,
            actual: actual as u64,
            maximum: maximum as u64,
        });
    }
    Ok(())
}

fn write_short_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    let len = u16::try_from(bytes.len()).map_err(|_| SalamanderError::ResourceLimit {
        resource: "short byte string",
        actual: bytes.len() as u64,
        maximum: u16::MAX as u64,
    })?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

fn read_u16(bytes: &[u8]) -> u16 {
    u16::from_le_bytes(bytes.try_into().expect("two-byte slice"))
}

fn read_u32(bytes: &[u8]) -> u32 {
    u32::from_le_bytes(bytes.try_into().expect("four-byte slice"))
}

fn read_u64(bytes: &[u8]) -> u64 {
    u64::from_le_bytes(bytes.try_into().expect("eight-byte slice"))
}

struct Cursor<'a> {
    remaining: &'a [u8],
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { remaining: bytes }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        if self.remaining.len() < len {
            return Err(SalamanderError::InvalidFormat(
                "truncated v2 envelope".into(),
            ));
        }
        let (head, tail) = self.remaining.split_at(len);
        self.remaining = tail;
        Ok(head)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        Ok(self
            .take(N)?
            .try_into()
            .expect("slice has requested length"))
    }

    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.array()?))
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_le_bytes(self.array()?))
    }

    fn short_bytes(&mut self, maximum: usize) -> Result<&'a [u8]> {
        let len = self.u16()? as usize;
        enforce_limit("short byte string", len, maximum)?;
        self.take(len)
    }

    fn is_empty(&self) -> bool {
        self.remaining.is_empty()
    }
}

pub trait TypedCodec<T> {
    fn id(&self) -> CodecId;
    fn encode(&self, value: &T) -> Result<Vec<u8>>;
    fn decode(&self, bytes: &[u8]) -> Result<T>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct JsonCodec;

impl<T> TypedCodec<T> for JsonCodec
where
    T: Serialize + DeserializeOwned,
{
    fn id(&self) -> CodecId {
        CodecId::JSON_UTF8
    }

    fn encode(&self, value: &T) -> Result<Vec<u8>> {
        serde_json::to_vec(value).map_err(|error| SalamanderError::Codec(error.to_string()))
    }

    fn decode(&self, bytes: &[u8]) -> Result<T> {
        serde_json::from_slice(bytes).map_err(|error| SalamanderError::Codec(error.to_string()))
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct BincodeCodec;

impl<T> TypedCodec<T> for BincodeCodec
where
    T: Serialize + DeserializeOwned,
{
    fn id(&self) -> CodecId {
        CodecId::RUST_BINCODE_V1
    }

    fn encode(&self, value: &T) -> Result<Vec<u8>> {
        bincode::serialize(value).map_err(|error| SalamanderError::Codec(error.to_string()))
    }

    fn decode(&self, bytes: &[u8]) -> Result<T> {
        bincode::deserialize(bytes).map_err(|error| SalamanderError::Codec(error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn id(byte: u8) -> [u8; 16] {
        [byte; 16]
    }

    fn record<'a>(payload: &'a [u8]) -> StoredRecord<'a> {
        let mut metadata = Metadata::new();
        metadata.insert("actor".into(), b"agent-7".to_vec());
        StoredRecord {
            kind: FrameKind::Event,
            flags: 0,
            position: 42,
            envelope: RecordEnvelopeV2 {
                event_id: EventId::from_bytes(id(1)),
                database_id: DatabaseId::from_bytes(id(2)),
                branch_id: BranchId::from_bytes(id(3)),
                stream_id: StreamId::from_bytes(id(4)),
                stream_revision: StreamRevision(9),
                timestamp_unix_nanos: -123,
                event_type: EventType::new("chat.message").unwrap(),
                schema_version: 7,
                codec: CodecId::JSON_UTF8,
                batch_id: BatchId::from_bytes(id(5)),
                batch_index: 2,
                metadata,
            },
            payload,
        }
    }

    fn decode_hex(text: &str) -> Vec<u8> {
        let compact: String = text.chars().filter(|c| !c.is_whitespace()).collect();
        assert_eq!(compact.len() % 2, 0, "hex fixture has an odd digit count");
        compact
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                let pair = std::str::from_utf8(pair).unwrap();
                u8::from_str_radix(pair, 16).unwrap()
            })
            .collect()
    }

    #[test]
    fn round_trip_preserves_envelope_and_opaque_payload() {
        let original = record(&[0, 159, 255, 1]);
        let bytes = encode(&original, FormatLimits::default()).unwrap();
        let (decoded, consumed) = decode(&bytes, FormatLimits::default()).unwrap().unwrap();
        assert_eq!(decoded, original);
        assert_eq!(consumed, bytes.len());
    }

    #[test]
    fn checked_in_golden_event_frame_is_stable() {
        let expected = encode(&record(&[0, 1, 159, 255]), FormatLimits::default()).unwrap();
        let fixture = decode_hex(include_str!(
            "../../tests/fixtures/format-v2/event-minimal.hex"
        ));
        assert_eq!(fixture, expected, "v2 byte layout changed");
        let (decoded, consumed) = decode(&fixture, FormatLimits::default()).unwrap().unwrap();
        assert_eq!(consumed, fixture.len());
        assert_eq!(decoded, record(&[0, 1, 159, 255]));
    }

    #[test]
    fn short_frame_is_incomplete_not_corrupt() {
        let bytes = encode(&record(b"body"), FormatLimits::default()).unwrap();
        for length in 0..bytes.len() {
            let result = decode(&bytes[..length], FormatLimits::default());
            assert!(matches!(result, Ok(None)));
        }
    }

    #[test]
    fn rejects_unknown_flags_before_payload_access() {
        let mut bytes = encode(&record(b"body"), FormatLimits::default()).unwrap();
        bytes[7] = 0x80;
        assert!(matches!(
            decode(&bytes, FormatLimits::default()),
            Err(SalamanderError::InvalidFormat(_))
        ));
    }

    #[test]
    fn rejects_bad_magic_version_and_reserved_header() {
        let original = encode(&record(b"body"), FormatLimits::default()).unwrap();

        let mut bad_magic = original.clone();
        bad_magic[0] ^= 1;
        assert!(matches!(
            decode(&bad_magic, FormatLimits::default()),
            Err(SalamanderError::InvalidFormat(_))
        ));

        let mut bad_version = original.clone();
        bad_version[4..6].copy_from_slice(&99u16.to_le_bytes());
        assert!(matches!(
            decode(&bad_version, FormatLimits::default()),
            Err(SalamanderError::InvalidFormat(_))
        ));

        let mut bad_reserved = original;
        bad_reserved[28] = 1;
        assert!(matches!(
            decode(&bad_reserved, FormatLimits::default()),
            Err(SalamanderError::InvalidFormat(_))
        ));
    }

    #[test]
    fn rejects_checksum_mutation() {
        let mut bytes = encode(&record(b"body"), FormatLimits::default()).unwrap();
        *bytes.last_mut().unwrap() ^= 1;
        assert!(matches!(
            decode(&bytes, FormatLimits::default()),
            Err(SalamanderError::Corrupt { offset: 42, .. })
        ));
    }

    #[test]
    fn limits_are_checked_before_waiting_for_declared_body() {
        let mut bytes = encode(&record(b"body"), FormatLimits::default()).unwrap();
        bytes[12..16].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(
            decode(&bytes, FormatLimits::default()),
            Err(SalamanderError::ResourceLimit {
                resource: "payload bytes",
                ..
            })
        ));
    }

    #[test]
    fn json_codec_is_portable_utf8_json() {
        let value = serde_json::json!({"hello": [1, true, null]});
        let bytes = JsonCodec.encode(&value).unwrap();
        assert_eq!(
            std::str::from_utf8(&bytes).unwrap(),
            r#"{"hello":[1,true,null]}"#
        );
        let decoded: serde_json::Value = JsonCodec.decode(&bytes).unwrap();
        assert_eq!(decoded, value);
    }

    proptest! {
        #[test]
        fn arbitrary_payload_round_trips(payload in prop::collection::vec(any::<u8>(), 0..4096)) {
            let original = record(&payload);
            let bytes = encode(&original, FormatLimits::default()).unwrap();
            let (decoded, consumed) = decode(&bytes, FormatLimits::default()).unwrap().unwrap();
            prop_assert_eq!(decoded.payload, payload.as_slice());
            prop_assert_eq!(decoded.envelope, original.envelope);
            prop_assert_eq!(consumed, bytes.len());
        }
    }
}
