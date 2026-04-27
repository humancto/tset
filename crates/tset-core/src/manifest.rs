//! Manifest is JSON, encoded with `sort_keys=True, separators=(",", ":")`
//! by the Python writer to produce a canonical byte representation hashed
//! into the header. The Rust reader hashes raw bytes and decodes a typed
//! `serde_json::Value` view that matches the Python dict shape.
//!
//! v0.3 will replace this with a strongly-typed protobuf schema.

use serde_json::Value;

use crate::error::{TsetError, TsetResult};

#[derive(Debug, Clone)]
pub struct Manifest {
    raw: Value,
}

impl Manifest {
    pub fn from_bytes(bytes: &[u8]) -> TsetResult<Self> {
        let raw: Value = serde_json::from_slice(bytes)?;
        Ok(Self { raw })
    }

    pub fn raw(&self) -> &Value {
        &self.raw
    }

    pub fn shard_id(&self) -> Option<&str> {
        self.raw.get("shard_id").and_then(Value::as_str)
    }

    pub fn shard_merkle_root_hex(&self) -> Option<&str> {
        self.raw.get("shard_merkle_root").and_then(Value::as_str)
    }

    pub fn smt_root_hex(&self) -> Option<&str> {
        self.raw.get("smt_root").and_then(Value::as_str)
    }

    pub fn audit_log(&self) -> Option<&Value> {
        self.raw.get("audit_log")
    }

    pub fn block_infos(&self) -> TsetResult<&Vec<Value>> {
        self.raw
            .pointer("/document_store/blocks")
            .and_then(Value::as_array)
            .ok_or(TsetError::BadManifest("document_store.blocks"))
    }

    pub fn doc_index(&self) -> TsetResult<&serde_json::Map<String, Value>> {
        self.raw
            .pointer("/document_store/document_index")
            .and_then(Value::as_object)
            .ok_or(TsetError::BadManifest("document_store.document_index"))
    }

    pub fn views(&self) -> TsetResult<&serde_json::Map<String, Value>> {
        self.raw
            .get("tokenization_views")
            .and_then(Value::as_object)
            .ok_or(TsetError::BadManifest("tokenization_views"))
    }

    pub fn view(&self, tokenizer_id: &str) -> TsetResult<&Value> {
        self.views()?
            .get(tokenizer_id)
            .ok_or_else(|| TsetError::ViewNotFound(tokenizer_id.to_string()))
    }
}
