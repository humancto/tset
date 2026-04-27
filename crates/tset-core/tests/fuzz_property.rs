//! Stand-in for cargo-fuzz: exercises Reader::open against a curated
//! set of arbitrary inputs that exercise different decode branches.
//!
//! Anything that panics here would also panic under `cargo fuzz`. The
//! contract is the same: `Reader::open` returns `Err(TsetError::*)`
//! for every malformed input, never panics.

use std::io::Write;

use tset_core::Reader;

fn try_open(bytes: &[u8]) {
    let mut f = tempfile::NamedTempFile::new().unwrap();
    f.write_all(bytes).unwrap();
    f.flush().unwrap();
    // Whatever the result, no panic
    let _ = Reader::open(f.path());
}

#[test]
fn reader_open_does_not_panic_on_random_inputs() {
    // 1. Empty
    try_open(b"");
    // 2. Less than HEADER_SIZE
    try_open(&[0u8; 100]);
    // 3. HEADER_SIZE bytes of zeros (bad magic)
    try_open(&[0u8; 4096]);
    // 4. Valid magic, garbage rest
    let mut buf = vec![0u8; 8192];
    buf[0..4].copy_from_slice(b"TSET");
    try_open(&buf);
    // 5. Valid magic + valid version_minor + huge manifest_offset
    let mut buf = vec![0u8; 8192];
    buf[0..4].copy_from_slice(b"TSET");
    buf[5] = 3;
    buf[16..24].copy_from_slice(&u64::MAX.to_le_bytes());
    let n = buf.len();
    buf[n - 4..].copy_from_slice(b"TEND");
    try_open(&buf);
    // 6. Valid header pointing at a manifest size that overflows
    let mut buf = vec![0u8; 8192];
    buf[0..4].copy_from_slice(b"TSET");
    buf[5] = 3;
    buf[16..24].copy_from_slice(&4096u64.to_le_bytes());
    buf[24..32].copy_from_slice(&u64::MAX.to_le_bytes());
    let n = buf.len();
    buf[n - 4..].copy_from_slice(b"TEND");
    try_open(&buf);
    // 7. Random byte patterns — all common boundary values
    let patterns: &[&[u8]] = &[
        &[0xff; 5000],
        &[0xaa, 0x55, 0xaa, 0x55],
        b"TSET\x00\x03\x00\x00",
        b"TSETTESTGARBAGEBYTES",
    ];
    for p in patterns {
        try_open(p);
    }
    // 8. Truncated valid shard prefix
    let mut buf = vec![0u8; 4096 + 40 - 1];
    buf[0..4].copy_from_slice(b"TSET");
    try_open(&buf);
    // 9. Bad footer magic at the end
    let mut buf = vec![0u8; 8192];
    buf[0..4].copy_from_slice(b"TSET");
    let n = buf.len();
    buf[n - 4..].copy_from_slice(b"NOPE");
    try_open(&buf);
    // 10. version_major > supported
    let mut buf = vec![0u8; 8192];
    buf[0..4].copy_from_slice(b"TSET");
    buf[4] = 99;
    let n = buf.len();
    buf[n - 4..].copy_from_slice(b"TEND");
    try_open(&buf);
}
