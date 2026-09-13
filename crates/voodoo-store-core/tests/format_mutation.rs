use voodoo_store_core::{LogRecord, RecordKind, StoreHeader};

#[test]
fn every_single_byte_record_mutation_is_rejected() {
    let record = LogRecord::new(
        RecordKind::Put,
        42,
        99,
        b"deterministic-integrity-payload".to_vec(),
    );
    let encoded = record.encode().unwrap();

    for offset in 0..encoded.len() {
        let mut corrupted = encoded.clone();
        corrupted[offset] ^= 0x01;
        assert!(
            LogRecord::decode(&corrupted).is_err(),
            "record mutation at offset {offset} was accepted"
        );
    }
}

#[test]
fn every_single_byte_header_mutation_is_rejected() {
    let header = StoreHeader::new([0x5a; 16], 1_800_000_000_000);
    let encoded = header.encode();

    for offset in 0..encoded.len() {
        let mut corrupted = encoded;
        corrupted[offset] ^= 0x01;
        assert!(
            StoreHeader::decode(&corrupted).is_err(),
            "header mutation at offset {offset} was accepted"
        );
    }
}

#[test]
fn decoder_rejects_truncation_at_every_record_boundary() {
    let encoded = LogRecord::new(RecordKind::Put, 7, 11, b"truncation-matrix".to_vec())
        .encode()
        .unwrap();

    for len in 0..encoded.len() {
        assert!(
            LogRecord::decode(&encoded[..len]).is_err(),
            "truncated record length {len} was accepted"
        );
    }
}
