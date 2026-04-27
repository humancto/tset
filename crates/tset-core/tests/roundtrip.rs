//! Rust writer → Rust reader full round-trip.

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
    assert_eq!(r.tokenizer_ids().unwrap(), vec!["byte-level-v1".to_string()]);
    let total = r.view_total_tokens("byte-level-v1").unwrap();
    let expected: u64 = docs.iter().map(|d| d.len() as u64).sum();
    assert_eq!(total, expected);

    for (h, d) in hashes.iter().zip(docs.iter()) {
        let bytes = r.get_document(h).unwrap();
        assert_eq!(bytes.as_slice(), *d);
    }
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
