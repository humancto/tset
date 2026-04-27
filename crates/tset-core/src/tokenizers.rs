//! Deterministic tokenizers + reproducibility test-vector helpers.
//!
//! Matches the Python reference implementation byte-for-byte:
//!   - ByteLevelTokenizer: `text.bytes()` → u32 ids in [0, 256)
//!   - WhitespaceTokenizer: split on `(\s+|\S+)`, hash each piece with
//!     BLAKE3, map to `(u64::from_le_bytes(digest[..8]) % (vocab-1)) + 1`
//!   - reproducibility_test_vector: sort docs by hash, tokenize the first
//!     N, hash the concatenated little-endian-u32 token bytes
//!
//! `Tokenizer::config_hash` canonicalizes JSON the same way Python does
//! (`sort_keys=True, separators=(",", ":")`).

use serde_json::Value;

use crate::error::{TsetError, TsetResult};
use crate::hashing::{hash_bytes, Hash};

pub trait Tokenizer: Send + Sync {
    fn tokenizer_id(&self) -> &str;
    fn vocab_size(&self) -> u32;
    fn encode(&self, text: &[u8]) -> Vec<u32>;
    fn config(&self) -> Value;

    fn config_hash(&self) -> Hash {
        let canon = canonical_json(&self.config());
        hash_bytes(canon.as_bytes())
    }
}

#[derive(Debug, Clone, Default)]
pub struct ByteLevelTokenizer;

impl ByteLevelTokenizer {
    pub const ID: &'static str = "byte-level-v1";
    pub const VOCAB: u32 = 256;
}

impl Tokenizer for ByteLevelTokenizer {
    fn tokenizer_id(&self) -> &str {
        Self::ID
    }
    fn vocab_size(&self) -> u32 {
        Self::VOCAB
    }
    fn encode(&self, text: &[u8]) -> Vec<u32> {
        text.iter().map(|&b| b as u32).collect()
    }
    fn config(&self) -> Value {
        serde_json::json!({
            "id": Self::ID,
            "vocab_size": Self::VOCAB,
            "kind": "byte",
        })
    }
}

#[derive(Debug, Clone)]
pub struct WhitespaceTokenizer {
    vocab_size: u32,
}

impl WhitespaceTokenizer {
    pub const ID: &'static str = "whitespace-hashed-v1";
    pub fn new(vocab_size: u32) -> TsetResult<Self> {
        if vocab_size < 2 {
            return Err(TsetError::BadManifest("whitespace vocab_size must be >= 2"));
        }
        Ok(Self { vocab_size })
    }
}

impl Default for WhitespaceTokenizer {
    fn default() -> Self {
        Self { vocab_size: 65536 }
    }
}

impl Tokenizer for WhitespaceTokenizer {
    fn tokenizer_id(&self) -> &str {
        Self::ID
    }
    fn vocab_size(&self) -> u32 {
        self.vocab_size
    }
    fn encode(&self, text: &[u8]) -> Vec<u32> {
        let mut out = Vec::new();
        let modulus = (self.vocab_size - 1) as u64;
        let mut cursor = 0;
        while cursor < text.len() {
            let is_ws = is_whitespace(text[cursor]);
            let mut end = cursor + 1;
            while end < text.len() && is_whitespace(text[end]) == is_ws {
                end += 1;
            }
            let piece = &text[cursor..end];
            let digest = hash_bytes(piece);
            let mut first8 = [0u8; 8];
            first8.copy_from_slice(&digest[..8]);
            let id = (u64::from_le_bytes(first8) % modulus) + 1;
            out.push(id as u32);
            cursor = end;
        }
        out
    }
    fn config(&self) -> Value {
        serde_json::json!({
            "id": Self::ID,
            "vocab_size": self.vocab_size,
            "kind": "whitespace-hashed",
        })
    }
}

#[inline]
fn is_whitespace(b: u8) -> bool {
    // Match Python's `\s` for ASCII inputs: space, tab, newline, carriage
    // return, form feed, vertical tab. Multibyte UTF-8 whitespace
    // (e.g. NBSP) is *not* matched by Python's `re.findall(rb"...", text)`
    // either at the byte level — so don't expand here.
    matches!(b, b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c)
}

/// Build a v0.1 reproducibility proof matching the Python implementation.
pub fn reproducibility_test_vector<T: Tokenizer + ?Sized>(
    tokenizer: &T,
    documents: &std::collections::BTreeMap<Hash, Vec<u8>>,
    sample_size: usize,
) -> Value {
    if documents.is_empty() {
        return serde_json::json!({
            "doc_hashes": [],
            "expected_token_arrays_hash": "",
        });
    }
    let mut sampled: Vec<&Hash> = documents.keys().take(sample_size).collect();
    sampled.sort();
    let mut concat: Vec<u8> = Vec::new();
    let mut hex_hashes: Vec<String> = Vec::with_capacity(sampled.len());
    for h in &sampled {
        let content = documents.get(*h).expect("present by construction");
        let ids = tokenizer.encode(content);
        for id in ids {
            concat.extend_from_slice(&id.to_le_bytes());
        }
        hex_hashes.push(hex::encode(h));
    }
    let digest = hash_bytes(&concat);
    serde_json::json!({
        "doc_hashes": hex_hashes,
        "expected_token_arrays_hash": hex::encode(digest),
    })
}

/// Verify a reproducibility test vector against a tokenizer + the source
/// documents it claims to have tokenized. Returns Err if the tokenizer
/// produces different bytes than recorded.
pub fn verify_reproducibility<T: Tokenizer + ?Sized>(
    tokenizer: &T,
    test_vector: &Value,
    documents: &std::collections::HashMap<Hash, Vec<u8>>,
) -> TsetResult<()> {
    let expected = test_vector
        .get("expected_token_arrays_hash")
        .and_then(Value::as_str)
        .unwrap_or("");
    if expected.is_empty() {
        return Ok(());
    }
    let doc_hashes = test_vector
        .get("doc_hashes")
        .and_then(Value::as_array)
        .ok_or(TsetError::BadManifest("test_vector.doc_hashes"))?;
    let mut concat: Vec<u8> = Vec::new();
    for h in doc_hashes {
        let hex_str = h
            .as_str()
            .ok_or(TsetError::BadManifest("test_vector.doc_hashes[i]"))?;
        let bytes = hex::decode(hex_str)?;
        if bytes.len() != 32 {
            return Err(TsetError::BadManifest("test_vector.doc_hashes[i] length"));
        }
        let mut key = [0u8; 32];
        key.copy_from_slice(&bytes);
        let content = documents
            .get(&key)
            .ok_or(TsetError::BadManifest("test_vector references missing doc"))?;
        let ids = tokenizer.encode(content);
        for id in ids {
            concat.extend_from_slice(&id.to_le_bytes());
        }
    }
    let actual = hex::encode(hash_bytes(&concat));
    if actual != expected {
        return Err(TsetError::BadManifest("tokenizer reproducibility mismatch"));
    }
    Ok(())
}

/// Build a tokenizer from a manifest config dict: dispatches on the `id`
/// field. v0.2 only supports the two built-in tokenizer kinds.
pub fn tokenizer_from_config(cfg: &Value) -> TsetResult<Box<dyn Tokenizer>> {
    let id = cfg
        .get("id")
        .and_then(Value::as_str)
        .ok_or(TsetError::BadManifest("tokenizer_config.id"))?;
    match id {
        ByteLevelTokenizer::ID => Ok(Box::new(ByteLevelTokenizer)),
        WhitespaceTokenizer::ID => {
            let vocab = cfg
                .get("vocab_size")
                .and_then(Value::as_u64)
                .ok_or(TsetError::BadManifest("tokenizer_config.vocab_size"))?;
            if vocab > u32::MAX as u64 {
                return Err(TsetError::BadManifest("tokenizer_config.vocab_size > u32::MAX"));
            }
            Ok(Box::new(WhitespaceTokenizer::new(vocab as u32)?))
        }
        _ => Err(TsetError::BadManifest("unknown tokenizer_id")),
    }
}

/// Canonical JSON matching Python's `json.dumps(value, sort_keys=True,
/// separators=(",", ":"))`. Numbers are emitted via serde_json's Number
/// Display, which preserves the lexical form when `arbitrary_precision`
/// is enabled (load-bearing for audit log + tokenizer config hashing).
pub fn canonical_json(v: &Value) -> String {
    let mut out = String::new();
    write_canonical(v, &mut out);
    out
}

fn write_canonical(v: &Value, out: &mut String) {
    use std::fmt::Write;
    match v {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Number(n) => {
            let _ = write!(out, "{n}");
        }
        Value::String(s) => write_quoted_string(s, out),
        Value::Array(arr) => {
            out.push('[');
            for (i, item) in arr.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_canonical(item, out);
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
                write_canonical(map.get(*k).unwrap(), out);
            }
            out.push('}');
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_level_encode_matches_input_bytes() {
        let t = ByteLevelTokenizer;
        let ids = t.encode(b"abc");
        assert_eq!(ids, vec![97u32, 98, 99]);
    }

    #[test]
    fn whitespace_encode_is_deterministic() {
        let t = WhitespaceTokenizer::new(1024).unwrap();
        let a = t.encode(b"hello world");
        let b = t.encode(b"hello world");
        assert_eq!(a, b);
        assert_eq!(a.len(), 3); // "hello", " ", "world"
        for id in &a {
            assert!(*id >= 1 && *id < 1024);
        }
    }

    #[test]
    fn config_hash_is_canonical() {
        let t1 = ByteLevelTokenizer;
        let h1 = t1.config_hash();
        let h2 = t1.config_hash();
        assert_eq!(h1, h2);
    }
}
