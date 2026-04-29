//! Fuzz the v0.3.2 on-disk section decoders.
//!
//! Targets `decode_tsmt_section`, `decode_tlog_section`, and
//! `decode_tcol_section` — the three binary-section parsers a verifier
//! invokes on hostile bytes pulled from a `(offset, size)` pointer in
//! the manifest. They are the first thing an attacker can swing at if
//! they control either the manifest or the byte range it points at.
//!
//! Each invocation feeds the same input to all three decoders. The
//! contract is identical for all of them: never panic, only ever
//! return `Ok(_)` or `Err(TsetError::*)`.
//!
//! Run with:
//!     cargo +nightly fuzz run sections_decode

#![no_main]

use libfuzzer_sys::fuzz_target;
use tset_core::sections::{decode_tcol_section, decode_tlog_section, decode_tsmt_section};

fuzz_target!(|data: &[u8]| {
    let _ = decode_tsmt_section(data);
    let _ = decode_tlog_section(data);
    let _ = decode_tcol_section(data);
});
