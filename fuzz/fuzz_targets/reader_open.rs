//! Fuzz target for `tset_core::Reader::open` against arbitrary input.
//!
//! Run with:
//!     cargo install cargo-fuzz
//!     cargo +nightly fuzz run reader_open
//!
//! The contract: `Reader::open` must NEVER panic on any sequence of
//! bytes. It either succeeds (input was a valid TSET shard) or returns
//! `Err(TsetError::*)` with a descriptive variant. Any panic is a
//! security bug — it means a hostile shard can crash the reader.

#![no_main]

use libfuzzer_sys::fuzz_target;
use std::io::Write;
use tset_core::Reader;

fuzz_target!(|data: &[u8]| {
    // Write the fuzzer's bytes into a tempfile and try to open it.
    // Reader::open mmaps a file on disk; we don't have a bytes-direct
    // entry point yet (that would let us skip the I/O round-trip).
    let mut f = match tempfile::NamedTempFile::new() {
        Ok(f) => f,
        Err(_) => return,
    };
    if f.write_all(data).is_err() {
        return;
    }
    if f.flush().is_err() {
        return;
    }
    let _ = Reader::open(f.path());
    // Any return value is fine. Panic = bug.
});
