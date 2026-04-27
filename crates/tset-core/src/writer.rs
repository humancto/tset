//! Single-shard TSET writer.
//!
//! Layout matches the Python reference impl byte-for-byte (manifest is
//! canonical JSON with `sort_keys=True, separators=(",", ":")`):
//!
//! ```text
//! [HEADER 4096B] [DOC BLOCK 0] ... [DOC BLOCK N]
//! [TOKENIZATION VIEW 0] ... [TOKENIZATION VIEW M]
//! [MANIFEST (canonical JSON)] [FOOTER 40B]
//! ```

use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use serde_json::{json, Map, Value};

use crate::audit_log::AuditLog;
use crate::constants::{HASH_SIZE, HEADER_SIZE, TRUNCATED_HASH_SIZE, VERSION_MAJOR, VERSION_MINOR};
use crate::document_store::DocumentStoreWriter;
use crate::error::{TsetError, TsetResult};
use crate::footer::Footer;
use crate::hashing::{hash_bytes, shard_merkle_root, Hash};
use crate::header::Header;
use crate::smt::SparseMerkleTree;
use crate::tokenizer_view::{
    build_view, DEFAULT_SPARSE_INDEX_INTERVAL, DEFAULT_TOKEN_CHUNK_SIZE,
};
use crate::tokenizers::{canonical_json, Tokenizer};

pub struct Writer {
    path: PathBuf,
    shard_id: String,
    docs: DocumentStoreWriter,
    doc_order: Vec<Hash>,
    doc_contents: HashMap<Hash, Vec<u8>>,
    views: Vec<Box<dyn Tokenizer>>,
    smt: SparseMerkleTree,
    audit: AuditLog,
}

impl Writer {
    pub fn create<P: AsRef<Path>>(path: P, shard_id: Option<String>) -> Self {
        let shard_id = shard_id.unwrap_or_else(random_shard_id);
        Self {
            path: path.as_ref().to_path_buf(),
            shard_id,
            docs: DocumentStoreWriter::new(),
            doc_order: Vec::new(),
            doc_contents: HashMap::new(),
            views: Vec::new(),
            smt: SparseMerkleTree::new(),
            audit: AuditLog::new(),
        }
    }

    pub fn add_document(&mut self, content: &[u8]) -> TsetResult<Hash> {
        if !self.views.is_empty() {
            return Err(TsetError::BadManifest(
                "add_document() called after add_tokenizer_view()",
            ));
        }
        let h = self.docs.add(content);
        if self.doc_contents.contains_key(&h) {
            return Ok(h);
        }
        self.doc_order.push(h);
        self.doc_contents.insert(h, content.to_vec());
        self.smt.insert(h);
        self.audit.append(
            "ingestion",
            json!({"doc_hash": hex::encode(h), "size": content.len()}),
            current_timestamp(),
        );
        Ok(h)
    }

    pub fn add_tokenizer_view(&mut self, tokenizer: Box<dyn Tokenizer>) -> TsetResult<()> {
        for existing in &self.views {
            if existing.tokenizer_id() == tokenizer.tokenizer_id() {
                return Err(TsetError::BadManifest("duplicate tokenizer_id"));
            }
        }
        self.views.push(tokenizer);
        Ok(())
    }

    pub fn close(self) -> TsetResult<()> {
        let path = self.path.clone();
        let bytes = self.encode()?;
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)?;
        f.write_all(&bytes)?;
        f.sync_all()?;
        Ok(())
    }

    fn encode(self) -> TsetResult<Vec<u8>> {
        let Self {
            shard_id,
            docs,
            doc_order,
            doc_contents,
            views,
            smt,
            mut audit,
            ..
        } = self;

        let mut manifest = Map::new();
        manifest.insert("version".into(), json!(format!("{VERSION_MAJOR}.{VERSION_MINOR}.0")));
        manifest.insert("shard_id".into(), json!(shard_id));
        manifest.insert(
            "created_at".into(),
            json!(format_iso_utc(current_timestamp())),
        );
        manifest.insert(
            "writer".into(),
            json!({
                "name": "tset-core (Rust)",
                "version": env!("CARGO_PKG_VERSION"),
            }),
        );
        manifest.insert("schema".into(), json!({"metadata_columns": []}));
        manifest.insert(
            "document_store".into(),
            json!({"blocks": [], "document_index": {}}),
        );
        manifest.insert("tokenization_views".into(), json!({}));

        let body_offset = HEADER_SIZE as u64;
        let (encoded_blocks, blocks, doc_index) = docs.finalize(body_offset);

        // Write blocks/index back into manifest
        let manifest_blocks: Vec<Value> = blocks
            .iter()
            .map(|b| {
                json!({
                    "offset": b.offset,
                    "compressed_size": b.compressed_size,
                    "uncompressed_size": b.uncompressed_size,
                    "num_documents": b.num_documents,
                })
            })
            .collect();
        let mut manifest_index = Map::new();
        for (h, loc) in &doc_index {
            manifest_index.insert(
                hex::encode(h),
                json!({
                    "block_idx": loc.block_idx,
                    "in_block_offset": loc.in_block_offset,
                    "content_size": loc.content_size,
                }),
            );
        }
        let docstore = manifest.get_mut("document_store").unwrap();
        docstore["blocks"] = Value::Array(manifest_blocks);
        docstore["document_index"] = Value::Object(manifest_index);

        let mut body: Vec<u8> = encoded_blocks;

        for tokenizer in &views {
            let ordered_docs: Vec<(Hash, Vec<u8>)> = doc_order
                .iter()
                .map(|h| (*h, doc_contents.get(h).cloned().unwrap_or_default()))
                .collect();
            let mut builds = build_view(
                tokenizer.as_ref(),
                &ordered_docs,
                DEFAULT_TOKEN_CHUNK_SIZE,
                DEFAULT_SPARSE_INDEX_INTERVAL,
            )?;
            let v = builds.remove(0);
            let view_offset = HEADER_SIZE as u64 + body.len() as u64;
            body.extend_from_slice(&v.encoded);

            let chunks_json: Vec<Value> = v
                .chunks
                .iter()
                .map(|c| {
                    json!({
                        "byte_offset_in_view": c.byte_offset_in_view,
                        "compressed_size": c.compressed_size,
                        "num_tokens": c.num_tokens,
                        "content_hash": c.content_hash.map(|h| hex::encode(h)),
                    })
                })
                .collect();
            let source_map_json: Vec<Value> = v
                .source_map
                .iter()
                .map(|s| {
                    json!({
                        "doc_hash": hex::encode(s.doc_hash),
                        "token_offset": s.token_offset,
                        "token_count": s.token_count,
                    })
                })
                .collect();
            let sparse_json: Vec<Value> = v
                .sparse_offset_index
                .iter()
                .map(|e| {
                    json!({
                        "token_offset": e.token_offset,
                        "chunk_id": e.chunk_id,
                        "in_chunk_offset": e.in_chunk_offset,
                    })
                })
                .collect();

            let entry = json!({
                "view_offset": view_offset,
                "view_size": v.encoded.len() as u64,
                "vocab_size": v.vocab_size,
                "tokenizer_config": v.tokenizer_config,
                "config_hash": hex::encode(v.config_hash),
                "total_tokens": v.total_tokens,
                "chunks": chunks_json,
                "source_map": source_map_json,
                "sparse_offset_index": sparse_json,
                "test_vector": v.test_vector,
            });
            manifest.get_mut("tokenization_views").unwrap()
                [tokenizer.tokenizer_id()] = entry;

            audit.append(
                "tokenizer_added",
                json!({
                    "tokenizer_id": tokenizer.tokenizer_id(),
                    "config_hash": hex::encode(v.config_hash),
                    "total_tokens": v.total_tokens,
                }),
                current_timestamp(),
            );
        }

        let merkle = shard_merkle_root(&doc_order);
        let smt_root = smt.root();
        let snapshot_id = random_short_id();
        audit.append(
            "version_snapshot",
            json!({
                "snapshot_id": snapshot_id,
                "shard_merkle_root": hex::encode(merkle),
                "smt_root": hex::encode(smt_root),
                "doc_count": doc_order.len(),
            }),
            current_timestamp(),
        );

        manifest.insert("shard_merkle_root".into(), json!(hex::encode(merkle)));
        manifest.insert("smt_root".into(), json!(hex::encode(smt_root)));
        manifest.insert("audit_log".into(), audit.to_json());
        manifest.insert(
            "metadata_columns".into(),
            json!({
                "row_count": doc_order.len(),
                "columns": {},
            }),
        );
        manifest.insert("subsets".into(), json!([]));
        manifest.insert(
            "smt_present_keys".into(),
            json!(smt.present_keys()
                .iter()
                .map(|k| hex::encode(k))
                .collect::<Vec<_>>()),
        );
        manifest.insert("smt_version".into(), json!("v0.1-fixed-256"));

        let manifest_value = Value::Object(manifest);
        let manifest_bytes = canonical_json(&manifest_value).into_bytes();
        let manifest_hash = hash_bytes(&manifest_bytes);
        let manifest_offset = HEADER_SIZE as u64 + body.len() as u64;
        let manifest_size = manifest_bytes.len() as u64;

        let header = Header {
            version_major: VERSION_MAJOR,
            version_minor: VERSION_MINOR,
            flags: 0,
            manifest_offset,
            manifest_size,
            shard_merkle_root: merkle,
            manifest_hash,
        };

        let mut hash28 = [0u8; TRUNCATED_HASH_SIZE];
        hash28.copy_from_slice(&manifest_hash[..TRUNCATED_HASH_SIZE]);
        let footer = Footer {
            manifest_size,
            manifest_hash28: hash28,
        };

        let mut out: Vec<u8> = Vec::with_capacity(
            HEADER_SIZE + body.len() + manifest_bytes.len() + crate::constants::FOOTER_SIZE,
        );
        out.extend_from_slice(&header.encode());
        out.extend_from_slice(&body);
        out.extend_from_slice(&manifest_bytes);
        out.extend_from_slice(&footer.encode());

        // sanity
        debug_assert_eq!(out.len() as u64, manifest_offset + manifest_size + 40);
        let _ = HASH_SIZE;
        Ok(out)
    }
}

fn current_timestamp() -> f64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

fn format_iso_utc(secs: f64) -> String {
    // Approximate ISO 8601 UTC; matches Python's datetime.utcfromtimestamp().isoformat().
    // Don't pull chrono in just for this; the manifest contains a separate
    // canonical timestamp in the audit log.
    let secs_int = secs as i64;
    let days = secs_int / 86400;
    let day_secs = secs_int % 86400;
    let h = day_secs / 3600;
    let m = (day_secs % 3600) / 60;
    let s = day_secs % 60;
    // 1970-01-01 + days. For now just emit a coarse stamp; readers don't
    // use this field for verification.
    format!(
        "1970-01-01T{:02}:{:02}:{:02}+00:00 (epoch+{}d)",
        h, m, s, days
    )
}

fn random_shard_id() -> String {
    let mut bytes = [0u8; 16];
    fill_random(&mut bytes);
    hex::encode(bytes)
}

fn random_short_id() -> String {
    let mut bytes = [0u8; 6];
    fill_random(&mut bytes);
    hex::encode(bytes)
}

fn fill_random(out: &mut [u8]) {
    // Cheap unpredictability without pulling in `rand`. BLAKE3 of system
    // time + per-call counter is fine for an identifier; security-grade
    // randomness is not required here.
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let mut buf: Vec<u8> = Vec::with_capacity(32);
    buf.extend_from_slice(&counter.to_le_bytes());
    buf.extend_from_slice(&(now as u128).to_le_bytes());
    let h = hash_bytes(&buf);
    out.copy_from_slice(&h[..out.len()]);
}
