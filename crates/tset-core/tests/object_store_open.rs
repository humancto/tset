//! End-to-end: write a shard, hand it to an `ObjectStore` impl,
//! download to a tempfile, open with the standard Reader.

use tset_core::object_store::{download_to_tempfile, InMemory};
use tset_core::tokenizers::ByteLevelTokenizer;
use tset_core::{Reader, Writer};

#[test]
fn open_shard_via_in_memory_object_store() {
    // Write a shard to a tempfile
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("src.tset");
    let mut w = Writer::create(&path, None);
    w.add_document(b"alpha document").unwrap();
    w.add_document(b"beta gamma").unwrap();
    w.add_tokenizer_view(Box::new(ByteLevelTokenizer)).unwrap();
    w.close().unwrap();

    // Pretend it's in S3 — load into an InMemory store, then download
    let bytes = std::fs::read(&path).unwrap();
    let store = InMemory::new(bytes);
    let downloaded = download_to_tempfile(&store).unwrap();

    // Standard Reader must succeed
    let r = Reader::open(downloaded.path()).unwrap();
    assert_eq!(r.tokenizer_ids().unwrap(), vec!["byte-level-v1"]);
    let total = r.view_total_tokens("byte-level-v1").unwrap();
    assert_eq!(total, b"alpha documentbeta gamma".len() as u64);
}
