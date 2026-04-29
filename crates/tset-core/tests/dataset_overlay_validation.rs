//! Regression tests for Codex P2 finding on PR #8: Rust must reject
//! malformed exclusion hashes in the dataset Merkle root computation,
//! matching Python's `bytes.fromhex` semantics. Different invalid
//! exclusion strings must NOT silently collapse to the same leaf.

use std::fs;

use tset_core::dataset::{Dataset, DatasetWriter, EXCLUSIONS_NAME};
use tset_core::tokenizers::ByteLevelTokenizer;
use tset_core::{TsetError, Writer};

fn build_minimal_dataset(root: &std::path::Path) {
    fs::create_dir_all(root.join("shards")).unwrap();
    let shard_path = root.join("shards").join("only.tset");
    let mut w = Writer::create(&shard_path, Some("only".to_string()));
    w.add_document(b"alpha").unwrap();
    w.add_tokenizer_view(Box::new(ByteLevelTokenizer)).unwrap();
    w.close().unwrap();

    let mut dw = DatasetWriter::create(root).unwrap();
    dw.register_shard("only").unwrap();
    dw.close().unwrap();
}

fn write_exclusions(root: &std::path::Path, hex_strings: &[&str]) {
    let payload = serde_json::json!({
        "snapshot_id": "manual-test",
        "excluded_doc_hashes": hex_strings,
    });
    fs::write(
        root.join(EXCLUSIONS_NAME),
        serde_json::to_string_pretty(&payload).unwrap(),
    )
    .unwrap();
}

#[test]
fn malformed_exclusion_hex_is_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    build_minimal_dataset(root);
    write_exclusions(root, &["zz".repeat(32).as_str()]);

    // Open succeeds — we don't validate at open time, matching Python.
    let ds = Dataset::open(root).unwrap();
    // dataset_merkle_root MUST surface BadManifest, not silently
    // collapse the bad string to empty bytes.
    let err = ds.dataset_merkle_root().unwrap_err();
    assert!(
        matches!(err, TsetError::BadManifest(_)),
        "expected BadManifest, got {:?}",
        err
    );
}

#[test]
fn wrong_length_exclusion_hex_is_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    build_minimal_dataset(root);
    // Valid hex but only 16 bytes long; must still be rejected.
    write_exclusions(root, &["a".repeat(32).as_str()]);

    let ds = Dataset::open(root).unwrap();
    let err = ds.dataset_merkle_root().unwrap_err();
    assert!(matches!(err, TsetError::BadManifest(_)));
}

#[test]
fn distinct_invalid_exclusions_do_not_collapse() {
    // The integrity-relevant property Codex flagged: with the old
    // unwrap_or_default, BOTH invalid strings hashed to BLAKE3(0x22)
    // (empty bytes after the tag), so two distinct invalid exclusions
    // produced the same leaf and the same root. After the fix, both
    // produce BadManifest — the test asserts the function rejects them
    // rather than producing an indistinguishable "valid" root.
    let tmp_a = tempfile::tempdir().unwrap();
    build_minimal_dataset(tmp_a.path());
    write_exclusions(tmp_a.path(), &["zz".repeat(32).as_str()]);
    let err_a = Dataset::open(tmp_a.path())
        .unwrap()
        .dataset_merkle_root()
        .unwrap_err();

    let tmp_b = tempfile::tempdir().unwrap();
    build_minimal_dataset(tmp_b.path());
    write_exclusions(tmp_b.path(), &["xx".repeat(32).as_str()]);
    let err_b = Dataset::open(tmp_b.path())
        .unwrap()
        .dataset_merkle_root()
        .unwrap_err();

    // Both fail loudly; neither produces a valid root that an attacker
    // could pass off as a legitimate commitment.
    assert!(matches!(err_a, TsetError::BadManifest(_)));
    assert!(matches!(err_b, TsetError::BadManifest(_)));
}

#[test]
fn valid_exclusion_hex_is_accepted() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    build_minimal_dataset(root);
    // 64 hex chars = 32 bytes
    write_exclusions(root, &["ab".repeat(32).as_str()]);

    let ds = Dataset::open(root).unwrap();
    ds.dataset_merkle_root().expect("valid hex should compute");
}
