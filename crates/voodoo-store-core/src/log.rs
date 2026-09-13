use crc32fast::Hasher;
use thiserror::Error;

const MAGIC: [u8; 4] = *b"VDS1";
const HEADER_LEN: usize = 4 + 1 + 8 + 8 + 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RecordKind {
    Put = 1,
    Delete = 2,
    Commit = 3,
}

impl TryFrom<u8> for RecordKind {
    type Error = StoreError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Put),
            2 => Ok(Self::Delete),
            3 => Ok(Self::Commit),
            other => Err(StoreError::UnknownRecordKind(other)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogRecord {
    pub kind: RecordKind,
    pub tx_id: u64,
    pub sequence: u64,
    pub payload: Vec<u8>,
}

impl LogRecord {
    pub fn new(kind: RecordKind, tx_id: u64, sequence: u64, payload: Vec<u8>) -> Self {
        Self {
            kind,
            tx_id,
            sequence,
            payload,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>, StoreError> {
        let payload_len = u32::try_from(self.payload.len()).map_err(|_| StoreError::PayloadTooLarge)?;

        let mut out = Vec::with_capacity(HEADER_LEN + self.payload.len() + 4);
        out.extend_from_slice(&MAGIC);
        out.push(self.kind as u8);
        out.extend_from_slice(&self.tx_id.to_le_bytes());
        out.extend_from_slice(&self.sequence.to_le_bytes());
        out.extend_from_slice(&payload_len.to_le_bytes());
        out.extend_from_slice(&self.payload);

        let checksum = checksum(&out);
        out.extend_from_slice(&checksum.to_le_bytes());
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, StoreError> {
        if bytes.len() < HEADER_LEN + 4 {
            return Err(StoreError::TruncatedRecord);
        }

        if bytes[0..4] != MAGIC {
            return Err(StoreError::InvalidMagic);
        }

        let kind = RecordKind::try_from(bytes[4])?;
        let tx_id = u64::from_le_bytes(bytes[5..13].try_into().expect("fixed header slice"));
        let sequence = u64::from_le_bytes(bytes[13..21].try_into().expect("fixed header slice"));
        let payload_len = u32::from_le_bytes(bytes[21..25].try_into().expect("fixed header slice")) as usize;
        let expected_len = HEADER_LEN
            .checked_add(payload_len)
            .and_then(|len| len.checked_add(4))
            .ok_or(StoreError::PayloadTooLarge)?;

        if bytes.len() != expected_len {
            return Err(StoreError::TruncatedRecord);
        }

        let checksum_offset = HEADER_LEN + payload_len;
        let expected_checksum = u32::from_le_bytes(
            bytes[checksum_offset..checksum_offset + 4]
                .try_into()
                .expect("checksum slice"),
        );
        let actual_checksum = checksum(&bytes[..checksum_offset]);

        if expected_checksum != actual_checksum {
            return Err(StoreError::ChecksumMismatch {
                expected: expected_checksum,
                actual: actual_checksum,
            });
        }

        Ok(Self {
            kind,
            tx_id,
            sequence,
            payload: bytes[HEADER_LEN..checksum_offset].to_vec(),
        })
    }
}

fn checksum(bytes: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(bytes);
    hasher.finalize()
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum StoreError {
    #[error("record has invalid magic bytes")]
    InvalidMagic,
    #[error("record is truncated or has an invalid length")]
    TruncatedRecord,
    #[error("unknown record kind {0}")]
    UnknownRecordKind(u8),
    #[error("payload is too large")]
    PayloadTooLarge,
    #[error("record checksum mismatch: expected {expected:#010x}, got {actual:#010x}")]
    ChecksumMismatch { expected: u32, actual: u32 },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_round_trip_is_lossless() {
        let original = LogRecord::new(RecordKind::Put, 42, 7, b"hello voodoo".to_vec());
        let encoded = original.encode().unwrap();
        let decoded = LogRecord::decode(&encoded).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn corruption_is_detected() {
        let record = LogRecord::new(RecordKind::Put, 1, 1, b"durable".to_vec());
        let mut encoded = record.encode().unwrap();
        encoded[HEADER_LEN] ^= 0xff;

        assert!(matches!(
            LogRecord::decode(&encoded),
            Err(StoreError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn truncated_records_are_rejected() {
        let record = LogRecord::new(RecordKind::Commit, 9, 10, Vec::new());
        let mut encoded = record.encode().unwrap();
        encoded.pop();

        assert_eq!(LogRecord::decode(&encoded), Err(StoreError::TruncatedRecord));
    }
}
