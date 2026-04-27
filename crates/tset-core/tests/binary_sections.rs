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
