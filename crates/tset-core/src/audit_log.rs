//! Append-only Merkle audit log.
//!
//! Wire format (matches Python `python/tset/audit_log.py`):
//!
//! ```text
//! entry_payload = {seq, timestamp, event_type, payload}
//! entry_bytes   = json.dumps(entry_payload, sort_keys=True, separators=(",", ":"))
//! entry_hash    = BLAKE3(entry_bytes)
//! chained_root  = BLAKE3( (prev_chained_root_bytes or 32 zeros) || entry_hash )
//! log_root      = last entry's chained_root, or "" if empty
//! ```

use serde_json::{json, Value};

use crate::constants::HASH_SIZE;
use crate::hashing::hash_bytes;
use crate::tokenizers::canonical_json;

#[derive(Debug, Clone)]
pub struct AuditEntry {
    pub seq: u64,
    pub timestamp: f64,
    pub event_type: String,
    pub payload: Value,
    pub prev_root: String,
    pub entry_hash: String,
    pub chained_root: String,
    /// Optional Ed25519 signature over the entry_hash bytes (not the
    /// hex string — raw 32 bytes). Hex-encoded in the manifest.
    pub signature: Option<String>,
}

impl AuditEntry {
    pub fn to_json(&self) -> Value {
        let mut out = json!({
            "seq": self.seq,
            "timestamp": serde_json::Number::from_f64(self.timestamp)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            "event_type": self.event_type.clone(),
            "payload": self.payload.clone(),
            "prev_root": self.prev_root.clone(),
            "entry_hash": self.entry_hash.clone(),
            "chained_root": self.chained_root.clone(),
        });
        if let Some(sig) = &self.signature {
            out["signature"] = json!(sig);
        }
        out
    }
}

#[derive(Default)]
pub struct AuditLog {
    pub entries: Vec<AuditEntry>,
    pub log_root: String,
    /// Optional signer. When set, every appended entry is signed.
    /// Mixed-signature audit logs (some entries signed, some not) are
    /// rejected at verify time to prevent downgrade attacks.
    signer: Option<crate::signing::AuditSigner>,
    /// Hex-encoded `writer_public_key` captured by `from_json` when an
    /// existing signed audit log is reopened. Preserved so a
    /// round-trip (from_json → to_json with NO new entries) emits the
    /// pubkey alongside the original signatures — otherwise verify
    /// would reject "signatures present but no pubkey to verify
    /// against" (Codex P1 on PR #16). Not used when a fresh signer
    /// is attached.
    loaded_writer_public_key: Option<String>,
}

impl AuditLog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_signer(signer: crate::signing::AuditSigner) -> Self {
        Self {
            signer: Some(signer),
            ..Self::default()
        }
    }

    pub fn signer_public_key(&self) -> Option<[u8; crate::signing::PUBLIC_KEY_LEN]> {
        self.signer.as_ref().map(|s| s.public_key_bytes())
    }

    /// True iff this log was reopened from a serialized JSON that
    /// carried a `writer_public_key` AND no fresh signer is attached.
    /// Higher layers (e.g. `DatasetWriter::open_existing`) use this to
    /// refuse extending a signed log without the signing key — adding
    /// unsigned entries would silently downgrade the integrity
    /// contract.
    pub fn was_loaded_signed_without_key(&self) -> bool {
        self.loaded_writer_public_key.is_some() && self.signer.is_none()
    }

    pub fn append(&mut self, event_type: &str, payload: Value, timestamp: f64) -> &AuditEntry {
        let seq = self.entries.len() as u64;
        let prev_root = self.log_root.clone();

        let entry_payload = json!({
            "seq": seq,
            "timestamp": serde_json::Number::from_f64(timestamp)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            "event_type": event_type,
            "payload": payload.clone(),
        });
        let canonical = canonical_json(&entry_payload);
        let entry_hash = hash_bytes(canonical.as_bytes());

        let mut buf = Vec::with_capacity(2 * HASH_SIZE);
        let prev_bytes = if prev_root.is_empty() {
            [0u8; HASH_SIZE]
        } else {
            let mut a = [0u8; HASH_SIZE];
            a.copy_from_slice(&hex::decode(&prev_root).expect("prev_root hex"));
            a
        };
        buf.extend_from_slice(&prev_bytes);
        buf.extend_from_slice(&entry_hash);
        let chained = hash_bytes(&buf);
        let chained_hex = hex::encode(chained);

        let signature = self.signer.as_ref().map(|s| {
            // Sign the raw entry_hash bytes (not the hex string)
            hex::encode(s.sign(&entry_hash))
        });

        self.log_root = chained_hex.clone();
        self.entries.push(AuditEntry {
            seq,
            timestamp,
            event_type: event_type.to_string(),
            payload,
            prev_root,
            entry_hash: hex::encode(entry_hash),
            chained_root: chained_hex,
            signature,
        });
        self.entries.last().unwrap()
    }

    pub fn to_json(&self) -> Value {
        let mut out = json!({
            "entries": self.entries.iter().map(|e| e.to_json()).collect::<Vec<_>>(),
            "log_root": self.log_root,
        });
        if let Some(pk) = self.signer_public_key() {
            out["writer_public_key"] = json!(hex::encode(pk));
        } else if let Some(loaded) = &self.loaded_writer_public_key {
            // Round-trip case: log was opened from a signed JSON. No
            // fresh signer attached, but we MUST emit the original
            // pubkey so verify_audit_log accepts the existing
            // signatures. Higher layers prevent appending new
            // (unsigned) entries via `was_loaded_signed_without_key`.
            out["writer_public_key"] = json!(loaded);
        }
        out
    }

    /// Reconstruct an unsigned `AuditLog` from a serialized log JSON
    /// (the shape produced by `to_json`). Used by writers that re-open
    /// an existing dataset to extend its audit log. Signature
    /// verification is the caller's responsibility — this constructor
    /// only restores the chain.
    pub fn from_json(v: &Value) -> Self {
        let log_root = v
            .get("log_root")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let mut entries: Vec<AuditEntry> = Vec::new();
        if let Some(arr) = v.get("entries").and_then(Value::as_array) {
            for e in arr {
                entries.push(AuditEntry {
                    seq: e.get("seq").and_then(Value::as_u64).unwrap_or(0),
                    timestamp: e.get("timestamp").and_then(Value::as_f64).unwrap_or(0.0),
                    event_type: e
                        .get("event_type")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string(),
                    payload: e.get("payload").cloned().unwrap_or(Value::Null),
                    prev_root: e
                        .get("prev_root")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string(),
                    entry_hash: e
                        .get("entry_hash")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string(),
                    chained_root: e
                        .get("chained_root")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string(),
                    signature: e
                        .get("signature")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                });
            }
        }
        let loaded_writer_public_key = v
            .get("writer_public_key")
            .and_then(Value::as_str)
            .map(str::to_string);
        Self {
            entries,
            log_root,
            signer: None,
            loaded_writer_public_key,
        }
    }
}

pub fn verify_audit_log(audit: &Value) -> bool {
    let Some(entries) = audit.get("entries").and_then(Value::as_array) else {
        return false;
    };
    let log_root_hex = audit.get("log_root").and_then(Value::as_str).unwrap_or("");
    // If the audit log carries a writer_public_key, every entry MUST be
    // signed. Drop-some-signatures is a downgrade attack.
    let pubkey_bytes: Option<Vec<u8>> = audit
        .get("writer_public_key")
        .and_then(Value::as_str)
        .map(|s| hex::decode(s).unwrap_or_default());
    let any_entry_signed = entries
        .iter()
        .any(|e| e.get("signature").and_then(Value::as_str).is_some());
    if pubkey_bytes.is_some() && !any_entry_signed {
        return false;
    }
    if pubkey_bytes.is_none() && any_entry_signed {
        // Signatures present but no pubkey to verify against — reject
        // rather than silently trust them.
        return false;
    }

    let mut prev_chained_hex = String::new();
    for (i, entry) in entries.iter().enumerate() {
        let seq = entry.get("seq").and_then(Value::as_u64).unwrap_or(u64::MAX);
        if seq != i as u64 {
            return false;
        }
        let stored_prev = entry.get("prev_root").and_then(Value::as_str).unwrap_or("");
        if stored_prev != prev_chained_hex {
            return false;
        }

        let event_type = entry
            .get("event_type")
            .and_then(Value::as_str)
            .unwrap_or("");
        let payload = entry.get("payload").cloned().unwrap_or(Value::Null);
        let timestamp = entry.get("timestamp").cloned().unwrap_or(Value::Null);

        let entry_payload = json!({
            "seq": seq,
            "timestamp": timestamp,
            "event_type": event_type,
            "payload": payload,
        });
        let canonical = canonical_json(&entry_payload);
        let entry_hash = hash_bytes(canonical.as_bytes());
        let stored_entry_hex = entry
            .get("entry_hash")
            .and_then(Value::as_str)
            .unwrap_or("");
        if hex::encode(entry_hash) != stored_entry_hex {
            return false;
        }

        // Signature check (when a pubkey is published in the audit_log)
        if let Some(pk) = &pubkey_bytes {
            let sig_hex = entry.get("signature").and_then(Value::as_str).unwrap_or("");
            if sig_hex.is_empty() {
                return false; // missing signature on a signed log
            }
            let sig_bytes = match hex::decode(sig_hex) {
                Ok(b) => b,
                Err(_) => return false,
            };
            if !crate::signing::verify_signature(pk, &entry_hash, &sig_bytes) {
                return false;
            }
        }

        let prev_bytes = if prev_chained_hex.is_empty() {
            [0u8; HASH_SIZE]
        } else {
            match hex::decode(&prev_chained_hex) {
                Ok(b) if b.len() == HASH_SIZE => {
                    let mut a = [0u8; HASH_SIZE];
                    a.copy_from_slice(&b);
                    a
                }
                _ => return false,
            }
        };
        let mut buf = Vec::with_capacity(2 * HASH_SIZE);
        buf.extend_from_slice(&prev_bytes);
        buf.extend_from_slice(&entry_hash);
        let chained = hash_bytes(&buf);
        let chained_hex = hex::encode(chained);
        let stored_chained = entry
            .get("chained_root")
            .and_then(Value::as_str)
            .unwrap_or("");
        if chained_hex != stored_chained {
            return false;
        }
        prev_chained_hex = chained_hex;
    }

    if entries.is_empty() {
        log_root_hex.is_empty()
    } else {
        log_root_hex == prev_chained_hex
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_log_verifies() {
        let log = AuditLog::new();
        assert!(verify_audit_log(&log.to_json()));
    }

    #[test]
    fn append_then_verify_roundtrip() {
        let mut log = AuditLog::new();
        log.append(
            "ingestion",
            json!({"doc_hash": "00", "size": 100}),
            1_700_000_000.5,
        );
        log.append(
            "tokenizer_added",
            json!({"tokenizer_id": "byte-level-v1"}),
            1_700_000_000.6,
        );
        let v = log.to_json();
        assert!(verify_audit_log(&v));
    }

    #[test]
    fn from_json_preserves_writer_public_key_for_signed_logs() {
        // Codex P1 on PR #16. Round-tripping a signed audit log
        // through (to_json → from_json → to_json) must keep the
        // writer_public_key alive — otherwise verify rejects
        // "signatures present but no pubkey to verify against".
        let signer = crate::signing::AuditSigner::generate();
        let mut log = AuditLog::with_signer(signer);
        log.append("ingestion", json!({}), 1.0);
        let original_json = log.to_json();
        assert!(verify_audit_log(&original_json));

        let reopened = AuditLog::from_json(&original_json);
        let round_tripped = reopened.to_json();
        assert!(
            round_tripped.get("writer_public_key").is_some(),
            "round-trip dropped writer_public_key — verify will reject"
        );
        assert!(verify_audit_log(&round_tripped));
        assert!(reopened.was_loaded_signed_without_key());
    }

    #[test]
    fn from_json_does_not_flag_unsigned_logs() {
        let mut log = AuditLog::new();
        log.append("ingestion", json!({}), 1.0);
        let reopened = AuditLog::from_json(&log.to_json());
        assert!(!reopened.was_loaded_signed_without_key());
    }
}
