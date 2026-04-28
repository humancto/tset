//! Rust writer → Rust reader full round-trip.

use tset_core::hashing::hash_bytes;
use tset_core::tokenizers::ByteLevelTokenizer;
use tset_core::{Reader, Writer};

#[test]
fn rust_writer_then_rust_reader_full_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rt.tset");

    let mut w = Writer::create(&path, None);
    let docs: &[&[u8]] = &[
        b"alpha document text",
        b"beta document body",
        b"gamma payload here",
    ];
    let mut hashes = Vec::new();
    for d in docs {
        hashes.push(w.add_document(d).unwrap());
    }
    w.add_tokenizer_view(Box::new(ByteLevelTokenizer)).unwrap();
    w.close().unwrap();

    let r = Reader::open(&path).unwrap();
    assert_eq!(
        r.tokenizer_ids().unwrap(),
        vec!["byte-level-v1".to_string()]
    );
    let total = r.view_total_tokens("byte-level-v1").unwrap();
    let expected: u64 = docs.iter().map(|d| d.len() as u64).sum();
    assert_eq!(total, expected);

    for (h, d) in hashes.iter().zip(docs.iter()) {
        let bytes = r.get_document(h).unwrap();
        assert_eq!(bytes.as_slice(), *d);
    }

    // Explicit manifest-hash assertion: re-hash the manifest bytes from
    // disk and compare to header.manifest_hash. Reader::open already does
    // this, but state it in a test to lock the contract.
    let raw = std::fs::read(&path).unwrap();
    let off = r.header.manifest_offset as usize;
    let size = r.header.manifest_size as usize;
    assert_eq!(hash_bytes(&raw[off..off + size]), r.header.manifest_hash);
}

#[test]
fn rust_writer_rejects_add_document_after_view() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ord.tset");
    let mut w = Writer::create(&path, None);
    w.add_document(b"first").unwrap();
    w.add_tokenizer_view(Box::new(ByteLevelTokenizer)).unwrap();
    let err = w.add_document(b"too late").err().unwrap();
    assert!(matches!(err, tset_core::TsetError::BadManifest(_)));
}

#[test]
fn open_with_options_skip_reproducibility_succeeds() {
    use tset_core::reader::OpenOptions;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("skip.tset");
    let mut w = Writer::create(&path, None);
    w.add_document(b"alpha").unwrap();
    w.add_tokenizer_view(Box::new(ByteLevelTokenizer)).unwrap();
    w.close().unwrap();

    // Default: full verification (incl reproducibility check)
    let _r = Reader::open(&path).unwrap();
    // Skip-reproducibility opens cleanly too — and is faster on big shards
    let opts = OpenOptions {
        skip_reproducibility: true,
    };
    let _r = Reader::open_with_options(&path, opts).unwrap();
}
