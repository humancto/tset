//! Confirm Reader::open returns errors (not panics) for various
//! malformed-input shapes. These don't construct a real shard — they
//! exercise the early-return paths in header/footer/manifest decode.

use std::io::Write;

use tset_core::{Reader, TsetError};

fn write_tmp(bytes: &[u8]) -> tempfile::NamedTempFile {
    let mut f = tempfile::NamedTempFile::new().unwrap();
    f.write_all(bytes).unwrap();
    f.flush().unwrap();
    f
}

#[test]
fn rejects_empty_file() {
    let f = write_tmp(b"");
    let err = Reader::open(f.path()).err().unwrap();
    assert!(matches!(err, TsetError::BadManifest(_)));
}

#[test]
fn rejects_short_file() {
    let f = write_tmp(&[0u8; 100]);
    let err = Reader::open(f.path()).err().unwrap();
    assert!(matches!(err, TsetError::BadManifest(_)));
}

#[test]
fn rejects_bad_header_magic() {
    let mut bytes = vec![0u8; 4096 + 40 + 1024];
    bytes[0..4].copy_from_slice(b"NOPE");
    let f = write_tmp(&bytes);
    let err = Reader::open(f.path()).err().unwrap();
    assert!(matches!(err, TsetError::BadHeaderMagic(_)));
}

#[test]
fn rejects_unsupported_version() {
    let mut bytes = vec![0u8; 4096 + 40];
    bytes[0..4].copy_from_slice(b"TSET");
    bytes[4] = 9; // version_major = 9
    let f = write_tmp(&bytes);
    let err = Reader::open(f.path()).err().unwrap();
    assert!(matches!(err, TsetError::UnsupportedVersion { .. }));
}

#[test]
fn rejects_manifest_offset_beyond_eof() {
    let mut bytes = vec![0u8; 4096 + 40];
    bytes[0..4].copy_from_slice(b"TSET");
    bytes[4] = 0;
    bytes[5] = 2;
    // manifest_offset = u64::MAX
    bytes[16..24].copy_from_slice(&u64::MAX.to_le_bytes());
    bytes[24..32].copy_from_slice(&100u64.to_le_bytes());
    let n = bytes.len();
    bytes[n - 4..].copy_from_slice(b"TEND");
    let f = write_tmp(&bytes);
    let err = Reader::open(f.path()).err().unwrap();
    assert!(matches!(err, TsetError::BadManifest(_)));
}
