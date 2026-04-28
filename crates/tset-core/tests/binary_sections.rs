//! End-to-end: Writer.enable_binary_sections() emits TSMT/TLOG/TCOL
//! sections; manifest gains pointer fields; sections decode cleanly;
//! the existing reader still verifies the shard.

use tset_core::sections::{decode_tcol_section, decode_tlog_section, decode_tsmt_section};
use tset_core::tokenizers::ByteLevelTokenizer;
use tset_core::{Reader, Writer};

#[test]
fn writer_with_binary_sections_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bin.tset");

    let mut w = Writer::create(&path, None);
    w.enable_binary_sections();
    w.add_document(b"alpha document").unwrap();
    w.add_document(b"beta gamma").unwrap();
    w.add_tokenizer_view(Box::new(ByteLevelTokenizer)).unwrap();
    w.close().unwrap();

    // Standard reader still verifies the shard
    let r = Reader::open(&path).unwrap();
    assert_eq!(r.tokenizer_ids().unwrap(), vec!["byte-level-v1".to_string()]);

    // Manifest carries the pointer fields
    let manifest = r.manifest().raw();
    let smt_section = manifest
        .get("smt_section")
        .expect("smt_section pointer in manifest");
    let tlog_section = manifest
        .get("audit_log_section")
        .expect("audit_log_section pointer in manifest");
    let tcol_section = manifest
        .get("metadata_columns_section")
        .expect("metadata_columns_section pointer in manifest");

    let raw = std::fs::read(&path).unwrap();

    // Decode each section by reading its byte range
    let smt_off = smt_section.get("offset").unwrap().as_u64().unwrap() as usize;
    let smt_size = smt_section.get("size").unwrap().as_u64().unwrap() as usize;
    let tsmt = decode_tsmt_section(&raw[smt_off..smt_off + smt_size]).unwrap();
    assert_eq!(tsmt.num_present, 2);

    let tlog_off = tlog_section.get("offset").unwrap().as_u64().unwrap() as usize;
    let tlog_size = tlog_section.get("size").unwrap().as_u64().unwrap() as usize;
    let tlog = decode_tlog_section(&raw[tlog_off..tlog_off + tlog_size]).unwrap();
    assert_eq!(
        tlog.audit_json
            .get("entries")
            .and_then(|v| v.as_array())
            .map(|a| a.len()),
        Some(4) // 2 ingestion + 1 tokenizer_added + 1 version_snapshot
    );

    let tcol_off = tcol_section.get("offset").unwrap().as_u64().unwrap() as usize;
    let tcol_size = tcol_section.get("size").unwrap().as_u64().unwrap() as usize;
    let tcol = decode_tcol_section(&raw[tcol_off..tcol_off + tcol_size]).unwrap();
    assert_eq!(tcol.row_count, 2);
}

#[test]
fn reader_smt_root_resolves_from_on_disk_section_when_present() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ondisk.tset");

    let mut w = Writer::create(&path, None);
    w.enable_binary_sections();
    w.add_document(b"alpha").unwrap();
    w.add_document(b"beta").unwrap();
    w.add_tokenizer_view(Box::new(ByteLevelTokenizer)).unwrap();
    w.close().unwrap();

    let r = Reader::open(&path).unwrap();
    let on_disk_root = r.on_disk_smt().unwrap().unwrap().smt_root;
    let smt_root = r.smt_root();
    assert_eq!(on_disk_root, smt_root);

    // TLOG / TCOL accessors return Some when the section is present
    assert!(r.on_disk_audit_log().unwrap().is_some());
    assert!(r.on_disk_columns().unwrap().is_some());
}

#[test]
fn reader_returns_none_for_on_disk_sections_when_absent() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("plain2.tset");
    let mut w = Writer::create(&path, None);
    w.add_document(b"x").unwrap();
    w.add_tokenizer_view(Box::new(ByteLevelTokenizer)).unwrap();
    w.close().unwrap();
    let r = Reader::open(&path).unwrap();
    assert!(r.on_disk_smt().unwrap().is_none());
    assert!(r.on_disk_audit_log().unwrap().is_none());
    assert!(r.on_disk_columns().unwrap().is_none());
}

#[test]
fn reader_rejects_tampered_tsmt_section_content() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tampered.tset");
    let mut w = Writer::create(&path, None);
    w.enable_binary_sections();
    w.add_document(b"alpha").unwrap();
    w.add_document(b"beta").unwrap();
    w.add_tokenizer_view(Box::new(ByteLevelTokenizer)).unwrap();
    w.close().unwrap();

    // Locate the TSMT section's payload in the file. The keys array
    // sits at section_offset + TSMT_HEADER_SIZE (80). Flip a byte there.
    let r = Reader::open(&path).unwrap();
    let smt_section = r.manifest().raw().get("smt_section").unwrap();
    let off = smt_section["offset"].as_u64().unwrap() as usize;
    let target = off + 80; // first byte of first key
    drop(r);

    let mut data = std::fs::read(&path).unwrap();
    data[target] ^= 0xff;
    std::fs::write(&path, &data).unwrap();

    let err = Reader::open(&path).err().unwrap();
    assert!(
        matches!(err, tset_core::TsetError::BadManifest(s) if s.contains("TSMT") && s.contains("content_hash")),
        "expected TSMT content_hash mismatch error",
    );
}

#[test]
fn reader_rejects_tampered_tlog_section_content() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tampered_tlog.tset");
    let mut w = Writer::create(&path, None);
    w.enable_binary_sections();
    w.add_document(b"alpha").unwrap();
    w.add_tokenizer_view(Box::new(ByteLevelTokenizer)).unwrap();
    w.close().unwrap();

    let r = Reader::open(&path).unwrap();
    let tlog = r.manifest().raw().get("audit_log_section").unwrap();
    let off = tlog["offset"].as_u64().unwrap() as usize;
    // Flip a byte deep into the JSON payload (past the 80B header)
    let target = off + 80 + 5;
    drop(r);

    let mut data = std::fs::read(&path).unwrap();
    data[target] ^= 0xff;
    std::fs::write(&path, &data).unwrap();

    let err = Reader::open(&path).err().unwrap();
    assert!(
        matches!(err, tset_core::TsetError::BadManifest(s) if s.contains("TLOG") && s.contains("content_hash")),
        "expected TLOG content_hash mismatch error",
    );
}

#[test]
fn reader_rejects_tampered_tcol_section_content() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tampered_tcol.tset");
    let mut w = Writer::create(&path, None);
    w.enable_binary_sections();
    w.add_document_with_metadata(
        b"alpha",
        Some(&serde_json::json!({"lang": "en"}).as_object().unwrap().clone()),
    )
    .unwrap();
    w.add_tokenizer_view(Box::new(ByteLevelTokenizer)).unwrap();
    w.close().unwrap();

    let r = Reader::open(&path).unwrap();
    let tcol = r.manifest().raw().get("metadata_columns_section").unwrap();
    let off = tcol["offset"].as_u64().unwrap() as usize;
    // Past the 56B header
    let target = off + 56 + 5;
    drop(r);

    let mut data = std::fs::read(&path).unwrap();
    data[target] ^= 0xff;
    std::fs::write(&path, &data).unwrap();

    let err = Reader::open(&path).err().unwrap();
    assert!(
        matches!(err, tset_core::TsetError::BadManifest(s) if s.contains("TCOL") && s.contains("content_hash")),
        "expected TCOL content_hash mismatch error",
    );
}

#[test]
fn writer_default_does_not_emit_binary_sections() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("plain.tset");

    let mut w = Writer::create(&path, None);
    // No enable_binary_sections() call
    w.add_document(b"alpha").unwrap();
    w.add_tokenizer_view(Box::new(ByteLevelTokenizer)).unwrap();
    w.close().unwrap();

    let r = Reader::open(&path).unwrap();
    let manifest = r.manifest().raw();
    assert!(manifest.get("smt_section").is_none());
    assert!(manifest.get("audit_log_section").is_none());
    assert!(manifest.get("metadata_columns_section").is_none());
}
