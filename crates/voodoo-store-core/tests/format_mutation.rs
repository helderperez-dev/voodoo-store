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

#[test]
fn deterministic_random_mutations_never_bypass_integrity_checks() {
    let encoded = LogRecord::new(
        RecordKind::Put,
        123,
        456,
        b"randomized-corruption-campaign".to_vec(),
    )
    .encode()
    .unwrap();

    // A small deterministic fuzz-smoke corpus keeps this gate reproducible in
    // CI while exercising multi-byte mutations beyond the exhaustive one-bit
    // matrix above. The decoder must return an error, never panic or accept a
    // corrupted record.
    let mut state = 0x9e37_79b9_u32;
    for case in 0..1_000 {
        let mut corrupted = encoded.clone();
        let mutations = (next(&mut state) % 3) + 1;
        for _ in 0..mutations {
            let offset = (next(&mut state) as usize) % corrupted.len();
            let bit = 1u8 << (next(&mut state) % 8);
            corrupted[offset] ^= bit;
        }
        assert!(
            LogRecord::decode(&corrupted).is_err(),
            "random mutation case {case} was accepted"
        );
    }
}

fn next(state: &mut u32) -> u32 {
    *state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
    *state
}
