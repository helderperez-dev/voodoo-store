//! Versioned, checksummed header for every Voodoo Store file.
//!
//! The header is deliberately language-neutral and fixed-width. It identifies
//! the file before log recovery starts and gives future engines a place to
//! negotiate format compatibility and durable feature flags.

use crc32fast::Hasher;
use thiserror::Error;

pub const STORE_MAGIC: [u8; 8] = *b"VSTORE01";
pub const FORMAT_MAJOR: u16 = 1;
pub const FORMAT_MINOR: u16 = 0;
pub const STORE_HEADER_LEN: usize = 64;

const CHECKSUM_OFFSET: usize = STORE_HEADER_LEN - 4;

/// Stable identity and compatibility metadata stored at byte zero of a store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreHeader {
    pub format_major: u16,
    pub format_minor: u16,
    pub store_id: [u8; 16],
    pub created_at_ms: i64,
    pub feature_flags: u64,
}

impl StoreHeader {
    pub const fn new(store_id: [u8; 16], created_at_ms: i64) -> Self {
        Self {
            format_major: FORMAT_MAJOR,
            format_minor: FORMAT_MINOR,
            store_id,
            created_at_ms,
            feature_flags: 0,
        }
    }

    pub fn encode(&self) -> [u8; STORE_HEADER_LEN] {
        let mut out = [0u8; STORE_HEADER_LEN];
        out[0..8].copy_from_slice(&STORE_MAGIC);
        out[8..10].copy_from_slice(&self.format_major.to_le_bytes());
        out[10..12].copy_from_slice(&self.format_minor.to_le_bytes());
        out[12..28].copy_from_slice(&self.store_id);
        out[28..36].copy_from_slice(&self.created_at_ms.to_le_bytes());
        out[36..44].copy_from_slice(&self.feature_flags.to_le_bytes());

        let checksum = checksum(&out[..CHECKSUM_OFFSET]);
        out[CHECKSUM_OFFSET..].copy_from_slice(&checksum.to_le_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, HeaderError> {
        if bytes.len() < STORE_HEADER_LEN {
            return Err(HeaderError::Truncated);
        }
        if bytes[0..8] != STORE_MAGIC {
            return Err(HeaderError::InvalidMagic);
        }

        let expected = u32::from_le_bytes(
            bytes[CHECKSUM_OFFSET..STORE_HEADER_LEN]
                .try_into()
                .expect("fixed checksum slice"),
        );
        let actual = checksum(&bytes[..CHECKSUM_OFFSET]);
        if expected != actual {
            return Err(HeaderError::ChecksumMismatch { expected, actual });
        }

        let format_major = u16::from_le_bytes(bytes[8..10].try_into().expect("fixed major slice"));
        let format_minor = u16::from_le_bytes(bytes[10..12].try_into().expect("fixed minor slice"));
        if format_major != FORMAT_MAJOR {
            return Err(HeaderError::UnsupportedMajor {
                found: format_major,
                supported: FORMAT_MAJOR,
            });
        }

        Ok(Self {
            format_major,
            format_minor,
            store_id: bytes[12..28].try_into().expect("fixed store id slice"),
            created_at_ms: i64::from_le_bytes(
                bytes[28..36].try_into().expect("fixed created-at slice"),
            ),
            feature_flags: u64::from_le_bytes(
                bytes[36..44].try_into().expect("fixed feature-flags slice"),
            ),
        })
    }
}

fn checksum(bytes: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(bytes);
    hasher.finalize()
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum HeaderError {
    #[error("store header is truncated")]
    Truncated,
    #[error("store header has invalid magic bytes")]
    InvalidMagic,
    #[error("unsupported store format major version {found}; this engine supports {supported}")]
    UnsupportedMajor { found: u16, supported: u16 },
    #[error("store header checksum mismatch: expected {expected:#010x}, got {actual:#010x}")]
    ChecksumMismatch { expected: u32, actual: u32 },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_round_trip_is_lossless() {
        let header = StoreHeader::new([7; 16], 1_726_000_000_000);
        let encoded = header.encode();
        assert_eq!(StoreHeader::decode(&encoded).unwrap(), header);
    }

    #[test]
    fn header_corruption_is_detected() {
        let header = StoreHeader::new([3; 16], 42);
        let mut encoded = header.encode();
        encoded[30] ^= 0xff;
        assert!(matches!(
            StoreHeader::decode(&encoded),
            Err(HeaderError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn incompatible_major_version_is_rejected() {
        let header = StoreHeader::new([1; 16], 42);
        let mut encoded = header.encode();
        encoded[8..10].copy_from_slice(&(FORMAT_MAJOR + 1).to_le_bytes());
        let checksum = checksum(&encoded[..CHECKSUM_OFFSET]);
        encoded[CHECKSUM_OFFSET..].copy_from_slice(&checksum.to_le_bytes());
        assert!(matches!(
            StoreHeader::decode(&encoded),
            Err(HeaderError::UnsupportedMajor { .. })
        ));
    }
}
