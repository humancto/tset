//! On-disk binary sections that complement the manifest's JSON
//! representation: TSMT (Sparse Merkle Tree), TLOG (audit log), TCOL
//! (metadata columns).
//!
//! v0.3 design: the in-manifest JSON forms remain authoritative for
//! older readers. Writers MAY additionally emit these binary sections
//! (opt-in via a Writer flag); readers MUST prefer the binary section
//! when present and fall back to the manifest fields otherwise.
//!
//! The format bump to v0.4 — when binary sections become MANDATORY and
//! the in-manifest forms can be dropped — is gated on a design partner
//! review. See SPEC §10 / RFC §10 #14–16.

use crate::constants::{HASH_SIZE, MAGIC_SMT};
use crate::error::{TsetError, TsetResult};
use crate::hashing::{hash_bytes, Hash};

/// On-disk SMT section (`TSMT`) layout:
///
/// ```text
/// [0:4]    magic           = b"TSMT"
/// [4:5]    smt_version     = u8 (1 for v0.1-fixed-256)
/// [5:8]    reserved        = zeros (3 bytes)
/// [8:16]   num_present     = u64 LE
/// [16:48]  smt_root        = 32 bytes (BLAKE3)
/// [48:80]  content_hash    = 32 bytes (BLAKE3 over the keys array)
/// [80:80+32*N]  present_keys = sorted [bytes(32); N]
/// ```
///
/// `content_hash` lets readers verify the keys array without recomputing
/// the SMT root. Same authentication property as chunk content_hashes.
pub const TSMT_VERSION: u8 = 1;
pub const TSMT_HEADER_SIZE: usize = 80;

pub fn encode_tsmt_section(present_keys: &[Hash], smt_root: &Hash) -> Vec<u8> {
    let mut keys_bytes = Vec::with_capacity(present_keys.len() * HASH_SIZE);
    let mut sorted: Vec<Hash> = present_keys.to_vec();
    sorted.sort();
    for k in &sorted {
        keys_bytes.extend_from_slice(k);
    }
    let content_hash = hash_bytes(&keys_bytes);

    let mut out = Vec::with_capacity(TSMT_HEADER_SIZE + keys_bytes.len());
    out.extend_from_slice(MAGIC_SMT);
    out.push(TSMT_VERSION);
    out.extend_from_slice(&[0u8; 3]); // reserved
    out.extend_from_slice(&(sorted.len() as u64).to_le_bytes());
    out.extend_from_slice(smt_root);
    out.extend_from_slice(&content_hash);
    out.extend_from_slice(&keys_bytes);
    out
}

pub struct TsmtSection {
    pub smt_version: u8,
    pub num_present: u64,
    pub smt_root: Hash,
    pub content_hash: Hash,
    pub present_keys: Vec<Hash>,
}

pub fn decode_tsmt_section(bytes: &[u8]) -> TsetResult<TsmtSection> {
    if bytes.len() < TSMT_HEADER_SIZE {
        return Err(TsetError::BadManifest("TSMT section truncated"));
    }
    let mut magic = [0u8; 4];
    magic.copy_from_slice(&bytes[0..4]);
    if &magic != MAGIC_SMT {
        return Err(TsetError::BadManifest("TSMT bad magic"));
    }
    let smt_version = bytes[4];
    if smt_version != TSMT_VERSION {
        return Err(TsetError::BadManifest("TSMT unsupported smt_version"));
    }
    let num_present = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
    let mut smt_root = [0u8; HASH_SIZE];
    smt_root.copy_from_slice(&bytes[16..48]);
    let mut content_hash = [0u8; HASH_SIZE];
    content_hash.copy_from_slice(&bytes[48..80]);

    let keys_len = (num_present as usize)
        .checked_mul(HASH_SIZE)
        .ok_or(TsetError::BadManifest("TSMT keys length overflow"))?;
    let keys_end = TSMT_HEADER_SIZE
        .checked_add(keys_len)
        .ok_or(TsetError::BadManifest("TSMT keys range overflow"))?;
    if keys_end > bytes.len() {
        return Err(TsetError::BadManifest("TSMT keys exceed section"));
    }
    let keys_bytes = &bytes[TSMT_HEADER_SIZE..keys_end];
    if hash_bytes(keys_bytes) != content_hash {
        return Err(TsetError::BadManifest("TSMT content_hash mismatch"));
    }

    let mut present_keys = Vec::with_capacity(num_present as usize);
    for chunk in keys_bytes.chunks_exact(HASH_SIZE) {
        let mut k = [0u8; HASH_SIZE];
        k.copy_from_slice(chunk);
        present_keys.push(k);
    }
    // Keys must be sorted on disk; reject otherwise so readers don't
    // accept ambiguous inputs.
    if present_keys.windows(2).any(|w| w[0] >= w[1]) && present_keys.len() > 1 {
        // Note: equality between adjacent keys also not allowed.
        return Err(TsetError::BadManifest("TSMT keys not strictly sorted"));
    }

    Ok(TsmtSection {
        smt_version,
        num_present,
        smt_root,
        content_hash,
        present_keys,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(seed: u8) -> Hash {
        let mut x = [0u8; HASH_SIZE];
        x[0] = seed;
        x
    }

    #[test]
    fn round_trip_empty() {
        let bytes = encode_tsmt_section(&[], &[0u8; 32]);
        let dec = decode_tsmt_section(&bytes).unwrap();
        assert_eq!(dec.num_present, 0);
        assert_eq!(dec.smt_root, [0u8; 32]);
        assert!(dec.present_keys.is_empty());
    }

    #[test]
    fn round_trip_with_keys() {
        let keys = vec![h(3), h(1), h(2)];
        let root = [42u8; 32];
        let bytes = encode_tsmt_section(&keys, &root);
        let dec = decode_tsmt_section(&bytes).unwrap();
        assert_eq!(dec.num_present, 3);
        assert_eq!(dec.smt_root, root);
        // Output is sorted regardless of input order
        assert_eq!(dec.present_keys, vec![h(1), h(2), h(3)]);
    }

    #[test]
    fn rejects_tampered_content() {
        let keys = vec![h(1), h(2)];
        let mut bytes = encode_tsmt_section(&keys, &[0u8; 32]);
        // Flip a byte in the keys area
        let n = bytes.len();
        bytes[n - 1] ^= 0xff;
        assert!(matches!(
            decode_tsmt_section(&bytes),
            Err(TsetError::BadManifest("TSMT content_hash mismatch"))
        ));
    }

    #[test]
    fn rejects_unsorted_keys() {
        // Hand-craft a section claiming unsorted keys (bypass the
        // encoder which always sorts) — content_hash matches the
        // unsorted bytes so we test the sorted-keys check, not the
        // hash check.
        let mut keys_bytes = Vec::new();
        keys_bytes.extend_from_slice(&h(2));
        keys_bytes.extend_from_slice(&h(1));
        let content_hash = hash_bytes(&keys_bytes);
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC_SMT);
        bytes.push(TSMT_VERSION);
        bytes.extend_from_slice(&[0u8; 3]);
        bytes.extend_from_slice(&2u64.to_le_bytes());
        bytes.extend_from_slice(&[0u8; 32]); // smt_root
        bytes.extend_from_slice(&content_hash);
        bytes.extend_from_slice(&keys_bytes);
        assert!(matches!(
            decode_tsmt_section(&bytes),
            Err(TsetError::BadManifest("TSMT keys not strictly sorted"))
        ));
    }
}

// ---------------------------------------------------------------------
// TLOG — on-disk audit log section
// ---------------------------------------------------------------------

use serde_json::Value;

use crate::constants::MAGIC_AUDIT_LOG;

/// On-disk audit log section (`TLOG`):
///
/// ```text
/// [0:4]    magic         = b"TLOG"
/// [4:5]    log_version   = u8 (1 = chained-blake3)
/// [5:8]    reserved      = zeros
/// [8:16]   payload_size  = u64 LE
/// [16:48]  log_root      = 32 bytes (BLAKE3, last entry's chained_root)
/// [48:80]  content_hash  = BLAKE3 over the JSON payload that follows
/// [80..80+payload_size]  = canonical JSON (sort_keys, sep=(",",":"))
/// ```
///
/// The payload is the same canonical JSON the in-manifest form uses,
/// just relocated to its own section so the manifest hash check stays
/// O(metadata size) instead of O(audit log size). Once on-disk is
/// mandatory (v0.4) the in-manifest form goes away.
pub const TLOG_VERSION: u8 = 1;
pub const TLOG_HEADER_SIZE: usize = 80;

pub fn encode_tlog_section(audit_json: &Value, log_root: &Hash) -> Vec<u8> {
    let canon = crate::tokenizers::canonical_json(audit_json);
    let payload = canon.into_bytes();
    let content_hash = hash_bytes(&payload);

    let mut out = Vec::with_capacity(TLOG_HEADER_SIZE + payload.len());
    out.extend_from_slice(MAGIC_AUDIT_LOG);
    out.push(TLOG_VERSION);
    out.extend_from_slice(&[0u8; 3]);
    out.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    out.extend_from_slice(log_root);
    out.extend_from_slice(&content_hash);
    out.extend_from_slice(&payload);
    out
}

pub struct TlogSection {
    pub log_version: u8,
    pub log_root: Hash,
    pub content_hash: Hash,
    pub audit_json: Value,
}

pub fn decode_tlog_section(bytes: &[u8]) -> TsetResult<TlogSection> {
    if bytes.len() < TLOG_HEADER_SIZE {
        return Err(TsetError::BadManifest("TLOG section truncated"));
    }
    let mut magic = [0u8; 4];
    magic.copy_from_slice(&bytes[0..4]);
    if &magic != MAGIC_AUDIT_LOG {
        return Err(TsetError::BadManifest("TLOG bad magic"));
    }
    let log_version = bytes[4];
    if log_version != TLOG_VERSION {
        return Err(TsetError::BadManifest("TLOG unsupported log_version"));
    }
    let payload_size = u64::from_le_bytes(bytes[8..16].try_into().unwrap()) as usize;
    let mut log_root = [0u8; HASH_SIZE];
    log_root.copy_from_slice(&bytes[16..48]);
    let mut content_hash = [0u8; HASH_SIZE];
    content_hash.copy_from_slice(&bytes[48..80]);
    let payload_end = TLOG_HEADER_SIZE
        .checked_add(payload_size)
        .ok_or(TsetError::BadManifest("TLOG payload range overflow"))?;
    if payload_end > bytes.len() {
        return Err(TsetError::BadManifest("TLOG payload exceeds section"));
    }
    let payload = &bytes[TLOG_HEADER_SIZE..payload_end];
    if hash_bytes(payload) != content_hash {
        return Err(TsetError::BadManifest("TLOG content_hash mismatch"));
    }
    let audit_json: Value = serde_json::from_slice(payload)?;
    Ok(TlogSection {
        log_version,
        log_root,
        content_hash,
        audit_json,
    })
}

#[cfg(test)]
mod tlog_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn round_trip_empty_log() {
        let audit = json!({"entries": [], "log_root": ""});
        let bytes = encode_tlog_section(&audit, &[0u8; 32]);
        let dec = decode_tlog_section(&bytes).unwrap();
        assert_eq!(dec.log_root, [0u8; 32]);
        assert_eq!(dec.audit_json, audit);
    }

    #[test]
    fn round_trip_with_entries() {
        let audit = json!({
            "entries": [
                {
                    "seq": 0,
                    "timestamp": 1700000000.0,
                    "event_type": "ingestion",
                    "payload": {"size": 100},
                    "prev_root": "",
                    "entry_hash": "ab",
                    "chained_root": "cd",
                }
            ],
            "log_root": "cd",
        });
        let mut root = [0u8; 32];
        root[0] = 0xcd;
        let bytes = encode_tlog_section(&audit, &root);
        let dec = decode_tlog_section(&bytes).unwrap();
        assert_eq!(dec.log_root, root);
        assert_eq!(dec.audit_json, audit);
    }

    #[test]
    fn rejects_tampered_payload() {
        let audit = json!({"entries": [], "log_root": ""});
        let mut bytes = encode_tlog_section(&audit, &[0u8; 32]);
        let n = bytes.len();
        bytes[n - 1] ^= 0xff;
        // Either content_hash mismatch or JSON parse error — both Err
        assert!(decode_tlog_section(&bytes).is_err());
    }
}
