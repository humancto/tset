//! Append-only Merkle audit log verifier.
//!
//! Python's hashing scheme (verbatim):
//!
//! ```text
//! entry_payload = {seq, timestamp, event_type, payload}
//! entry_bytes   = json.dumps(entry_payload, sort_keys=True, separators=(",", ":"))
//! entry_hash    = BLAKE3(entry_bytes)
//! chained_root  = BLAKE3( (prev_chained_root_bytes or 32 zeros) || entry_hash )
//! log_root      = last entry's chained_root, or "" if empty
//! ```
//!
//! Reproducing the byte-identical canonical JSON is the load-bearing piece.

use serde_json::Value;

use crate::constants::HASH_SIZE;
use crate::hashing::hash_bytes;

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

        // canonical JSON of {event_type, payload, seq, timestamp}
        let event_type = entry.get("event_type").and_then(Value::as_str).unwrap_or("");
        let payload = entry.get("payload").cloned().unwrap_or(Value::Null);
        let timestamp = entry.get("timestamp").cloned().unwrap_or(Value::Null);

        let entry_payload = serde_json::json!({
            "seq": seq,
            "timestamp": timestamp,
            "event_type": event_type,
            "payload": payload,
        });
        let canonical = match canonical_json(&entry_payload) {
            Some(s) => s,
            None => return false,
        };
        let entry_hash = hash_bytes(canonical.as_bytes());
        let stored_entry_hex = entry.get("entry_hash").and_then(Value::as_str).unwrap_or("");
        if hex::encode(entry_hash) != stored_entry_hex {
            return false;
        }

        // chained_root = BLAKE3(prev_chained_bytes || entry_hash)
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

/// Canonical JSON matching Python's
/// `json.dumps(value, sort_keys=True, separators=(",", ":"))`.
fn canonical_json(v: &Value) -> Option<String> {
    let mut out = String::new();
    write_canonical(v, &mut out)?;
    Some(out)
}

fn write_canonical(v: &Value, out: &mut String) -> Option<()> {
    use std::fmt::Write;
    match v {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Number(n) => {
            // With serde_json's `arbitrary_precision` feature, Number's
            // Display preserves the exact lexical form parsed from the
            // input. This is required to byte-match Python's
            // json.dumps(float) for `time.time()` timestamps.
            let _ = write!(out, "{n}");
        }
        Value::String(s) => {
            out.push('"');
            for ch in s.chars() {
                match ch {
                    '"' => out.push_str("\\\""),
                    '\\' => out.push_str("\\\\"),
                    '\n' => out.push_str("\\n"),
                    '\r' => out.push_str("\\r"),
                    '\t' => out.push_str("\\t"),
                    '\u{08}' => out.push_str("\\b"),
                    '\u{0c}' => out.push_str("\\f"),
                    c if (c as u32) < 0x20 => {
                        let _ = write!(out, "\\u{:04x}", c as u32);
                    }
                    c => out.push(c),
                }
            }
            out.push('"');
        }
        Value::Array(arr) => {
            out.push('[');
            for (i, item) in arr.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_canonical(item, out)?;
            }
            out.push(']');
        }
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            out.push('{');
            for (i, k) in keys.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_quoted_string(k, out);
                out.push(':');
                write_canonical(map.get(*k).unwrap(), out)?;
            }
            out.push('}');
        }
    }
    Some(())
}

fn write_quoted_string(s: &str, out: &mut String) {
    use std::fmt::Write;
    out.push('"');
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

fn format_python_float(f: f64) -> String {
    // Python repr(float): shortest roundtrip; whole-valued floats print as "1.0".
    // Rust's default for f64 prints whole-valued as "1" — append ".0" to match.
    if !f.is_finite() {
        return f.to_string();
    }
    let s = format!("{f}");
    if !s.contains('.') && !s.contains('e') && !s.contains('E') {
        format!("{s}.0")
    } else {
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn empty_log_verifies() {
        let v = json!({"entries": [], "log_root": ""});
        assert!(verify_audit_log(&v));
    }
}
