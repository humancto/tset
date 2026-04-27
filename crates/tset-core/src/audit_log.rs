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
}

impl AuditEntry {
    pub fn to_json(&self) -> Value {
        json!({
            "seq": self.seq,
            "timestamp": serde_json::Number::from_f64(self.timestamp)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            "event_type": self.event_type.clone(),
            "payload": self.payload.clone(),
            "prev_root": self.prev_root.clone(),
            "entry_hash": self.entry_hash.clone(),
            "chained_root": self.chained_root.clone(),
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct AuditLog {
    pub entries: Vec<AuditEntry>,
    pub log_root: String,
}

impl AuditLog {
    pub fn new() -> Self {
        Self::default()
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

        self.log_root = chained_hex.clone();
        self.entries.push(AuditEntry {
            seq,
            timestamp,
            event_type: event_type.to_string(),
            payload,
            prev_root,
            entry_hash: hex::encode(entry_hash),
            chained_root: chained_hex,
        });
        self.entries.last().unwrap()
    }

    pub fn to_json(&self) -> Value {
        json!({
            "entries": self.entries.iter().map(|e| e.to_json()).collect::<Vec<_>>(),
            "log_root": self.log_root,
        })
    }
}

pub fn verify_audit_log(audit: &Value) -> bool {
    let Some(entries) = audit.get("entries").and_then(Value::as_array) else {
        return false;
    };
    let log_root_hex = audit.get("log_root").and_then(Value::as_str).unwrap_or("");

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

        let event_type = entry.get("event_type").and_then(Value::as_str).unwrap_or("");
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
        let stored_entry_hex = entry.get("entry_hash").and_then(Value::as_str).unwrap_or("");
        if hex::encode(entry_hash) != stored_entry_hex {
            return false;
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
}
