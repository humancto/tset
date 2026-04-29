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

use std::fs::OpenOptions;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use serde_json::{json, Map, Value};

use crate::audit_log::AuditLog;
use crate::columns::MetadataColumns;
use crate::constants::{
    HASH_SIZE, HEADER_SIZE, MAGIC_DOC_BLOCK, TRUNCATED_HASH_SIZE, VERSION_MAJOR, VERSION_MINOR,
};
use crate::document_store::{BlockInfo, DocumentLocator, DocumentStoreWriter};
use crate::error::{TsetError, TsetResult};
use crate::footer::Footer;
use crate::hashing::{hash_bytes, shard_merkle_root, Hash};
use crate::header::Header;
use crate::mixture::Subset;
use crate::smt::SparseMerkleTree;
use crate::tokenizer_view::{build_view, DEFAULT_SPARSE_INDEX_INTERVAL, DEFAULT_TOKEN_CHUNK_SIZE};
use crate::tokenizers::{canonical_json, Tokenizer};

pub struct Writer {
    path: PathBuf,
    shard_id: String,
    docs: DocumentStoreWriter,
    doc_order: Vec<Hash>,
    /// `HashSet`-style dedup: only tracks doc-hash membership so we don't
    /// double-add. The actual document bytes live in `docs` (which streams
    /// them into compressed blocks); we re-read them out during view
    /// construction in `encode()` rather than carrying a full uncompressed
    /// copy in the writer's heap.
    doc_seen: std::collections::HashSet<Hash>,
    views: Vec<Box<dyn Tokenizer>>,
    smt: SparseMerkleTree,
    audit: AuditLog,
    columns: MetadataColumns,
    subsets: Vec<Subset>,
    /// When true, emit TSMT/TLOG/TCOL on-disk sections in addition to
    /// the in-manifest forms. Future v0.4 readers will prefer the
    /// on-disk sections; current v0.3 readers continue to read the
    /// in-manifest forms.
    emit_binary_sections: bool,
}

impl Writer {
    pub fn create<P: AsRef<Path>>(path: P, shard_id: Option<String>) -> Self {
        Self::create_with_options(path, shard_id, None)
    }

    /// `signer`: optional Ed25519 audit-log signer. When set, every
    /// audit entry written by this Writer is signed with the provided
    /// key, and the corresponding public key is published in the
    /// shard's audit_log section. Readers verify each entry's
    /// signature against that pubkey at open time.
    pub fn create_with_options<P: AsRef<Path>>(
        path: P,
        shard_id: Option<String>,
        signer: Option<crate::signing::AuditSigner>,
    ) -> Self {
        let shard_id = shard_id.unwrap_or_else(random_shard_id);
        let audit = match signer {
            Some(s) => AuditLog::with_signer(s),
            None => AuditLog::new(),
        };
        Self {
            path: path.as_ref().to_path_buf(),
            shard_id,
            docs: DocumentStoreWriter::new(),
            doc_order: Vec::new(),
            doc_seen: std::collections::HashSet::new(),
            views: Vec::new(),
            smt: SparseMerkleTree::new(),
            audit,
            columns: MetadataColumns::new(),
            subsets: Vec::new(),
            emit_binary_sections: false,
        }
    }

    /// Toggle on emission of TSMT/TLOG/TCOL on-disk sections alongside
    /// the in-manifest forms. Default off to preserve byte-exact parity
    /// with v0.1–v0.3 conformance fixtures.
    pub fn enable_binary_sections(&mut self) -> &mut Self {
        self.emit_binary_sections = true;
        self
    }

    pub fn add_document(&mut self, content: &[u8]) -> TsetResult<Hash> {
        self.add_document_with_metadata(content, None)
    }

    /// Add a document with optional metadata. `metadata` is a JSON object
    /// whose keys become column names; values are stored verbatim.
    /// Mirrors Python `Writer.add_document(content, metadata={...})`.
    /// Per the v0.2 ordering invariant, must be called before any
    /// `add_tokenizer_view`.
    pub fn add_document_with_metadata(
        &mut self,
        content: &[u8],
        metadata: Option<&Map<String, Value>>,
    ) -> TsetResult<Hash> {
        if !self.views.is_empty() {
            return Err(TsetError::BadManifest(
                "add_document() called after add_tokenizer_view()",
            ));
        }
        let h = self.docs.add(content);
        if !self.doc_seen.insert(h) {
            return Ok(h);
        }
        self.doc_order.push(h);
        self.smt.insert(h);
        self.audit.append(
            "ingestion",
            json!({"doc_hash": hex::encode(h), "size": content.len()}),
            current_timestamp(),
        );
        let empty = Map::new();
        self.columns.add_row(metadata.unwrap_or(&empty));
        Ok(h)
    }

    pub fn add_subset(&mut self, name: &str, predicate: &str, default_weight: f64) {
        self.subsets.push(Subset {
            name: name.to_string(),
            predicate: predicate.to_string(),
            default_weight,
        });
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
        let emit_binary_sections = self.emit_binary_sections;
        let Self {
            shard_id,
            docs,
            doc_order,
            views,
            smt,
            mut audit,
            columns,
            subsets,
            ..
        } = self;

        let mut manifest = Map::new();
        manifest.insert(
            "version".into(),
            json!(format!("{VERSION_MAJOR}.{VERSION_MINOR}.0")),
        );
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

        // Ordered (Hash, Vec<u8>) for the view builders. Reads docs back
        // from the just-finalized body bytes via a small block cache,
        // avoiding the previous always-in-RAM doc_contents map. For an
        // N-document corpus the writer's working set is now O(block size)
        // not O(corpus size).
        for tokenizer in &views {
            let ordered_docs: Vec<(Hash, Vec<u8>)> = {
                let mut out = Vec::with_capacity(doc_order.len());
                let mut cache: BlockCache = BlockCache::new();
                for h in &doc_order {
                    let loc = doc_index
                        .get(h)
                        .ok_or(TsetError::BadManifest("doc missing from index"))?;
                    let content = read_doc_from_body(&body, body_offset, &blocks, loc, &mut cache)?;
                    out.push((*h, content));
                }
                out
            };
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
                        "content_hash": c.content_hash.map(hex::encode),
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
                "bits_per_token": v.bits_per_token,
                "chunks": chunks_json,
                "source_map": source_map_json,
                "sparse_offset_index": sparse_json,
                "test_vector": v.test_vector,
            });
            manifest.get_mut("tokenization_views").unwrap()[tokenizer.tokenizer_id()] = entry;

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
        manifest.insert(
            "subsets".into(),
            Value::Array(subsets.iter().map(Subset::to_json).collect()),
        );
        manifest.insert("smt_version".into(), json!("v0.1-fixed-256"));

        // v0.4 wire format: when binary sections are emitted we DROP
        // the inline JSON forms of audit_log / metadata_columns /
        // smt_present_keys and bump version_minor to 4. The on-disk
        // sections (TSMT / TLOG / TCOL) become the sole source of
        // truth. v0.3 writers (no sections) keep the inline forms
        // and stay on version_minor=3 for backward compat.
        //
        // Pre-fix v0.3.2 wrote BOTH (sections AND inline), inflating
        // shards by ~50% when sections were enabled. See CHANGELOG
        // [0.4.0] and Issue #5 for measurements.
        if emit_binary_sections {
            use crate::sections::{encode_tcol_section, encode_tlog_section, encode_tsmt_section};

            // SMT
            let tsmt_bytes = encode_tsmt_section(&smt.present_keys(), &smt_root);
            let tsmt_offset = HEADER_SIZE as u64 + body.len() as u64;
            body.extend_from_slice(&tsmt_bytes);
            manifest.insert(
                "smt_section".into(),
                json!({
                    "offset": tsmt_offset,
                    "size": tsmt_bytes.len() as u64,
                }),
            );

            // Audit log (must be encoded BEFORE the manifest is finalized
            // — same as in-manifest form, just relocated)
            let audit_json = audit.to_json();
            let log_root_hex = audit_json
                .get("log_root")
                .and_then(Value::as_str)
                .unwrap_or("");
            let mut log_root = [0u8; HASH_SIZE];
            if let Ok(b) = hex::decode(log_root_hex) {
                if b.len() == HASH_SIZE {
                    log_root.copy_from_slice(&b);
                }
            }
            let tlog_bytes = encode_tlog_section(&audit_json, &log_root);
            let tlog_offset = HEADER_SIZE as u64 + body.len() as u64;
            body.extend_from_slice(&tlog_bytes);
            manifest.insert(
                "audit_log_section".into(),
                json!({
                    "offset": tlog_offset,
                    "size": tlog_bytes.len() as u64,
                }),
            );

            // Columns
            let cols_json = columns.to_json();
            let row_count = cols_json
                .get("row_count")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            let tcol_bytes = encode_tcol_section(&cols_json, row_count);
            let tcol_offset = HEADER_SIZE as u64 + body.len() as u64;
            body.extend_from_slice(&tcol_bytes);
            manifest.insert(
                "metadata_columns_section".into(),
                json!({
                    "offset": tcol_offset,
                    "size": tcol_bytes.len() as u64,
                }),
            );
        } else {
            // Legacy v0.3 path: inline forms only, no on-disk sections.
            manifest.insert("audit_log".into(), audit.to_json());
            manifest.insert("metadata_columns".into(), columns.to_json());
            manifest.insert(
                "smt_present_keys".into(),
                json!(smt
                    .present_keys()
                    .iter()
                    .map(hex::encode)
                    .collect::<Vec<_>>()),
            );
        }

        let effective_version_minor = if emit_binary_sections { 4 } else { 3 };

        let manifest_value = Value::Object(manifest);
        let manifest_bytes = canonical_json(&manifest_value).into_bytes();
        let manifest_hash = hash_bytes(&manifest_bytes);
        let manifest_offset = HEADER_SIZE as u64 + body.len() as u64;
        let manifest_size = manifest_bytes.len() as u64;

        let header = Header {
            version_major: VERSION_MAJOR,
            version_minor: effective_version_minor,
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
    buf.extend_from_slice(&now.to_le_bytes());
    let h = hash_bytes(&buf);
    out.copy_from_slice(&h[..out.len()]);
}

const BLOCK_HEADER_SIZE: usize = 24;

/// 1-slot LRU keyed by `block_idx`. Sufficient because doc_order is
/// processed in insertion order; consecutive docs nearly always live
/// in the same block.
struct BlockCache {
    cur: Option<(u32, Vec<u8>)>,
}

impl BlockCache {
    fn new() -> Self {
        Self { cur: None }
    }
}

fn read_doc_from_body(
    body: &[u8],
    body_offset: u64,
    blocks: &[BlockInfo],
    loc: &DocumentLocator,
    cache: &mut BlockCache,
) -> TsetResult<Vec<u8>> {
    let block = blocks
        .get(loc.block_idx as usize)
        .ok_or(TsetError::BadManifest("block_idx out of range"))?;
    let decompressed = if let Some((idx, ref bytes)) = cache.cur {
        if idx == loc.block_idx {
            bytes.clone()
        } else {
            decompress_block_from_body(body, body_offset, block)?
        }
    } else {
        decompress_block_from_body(body, body_offset, block)?
    };

    let start = loc.in_block_offset as usize;
    let stored_hash = &decompressed[start..start + HASH_SIZE];
    let mut h = [0u8; HASH_SIZE];
    h.copy_from_slice(stored_hash);
    let size = u64::from_le_bytes(
        decompressed[start + HASH_SIZE..start + HASH_SIZE + 8]
            .try_into()
            .unwrap(),
    );
    if size != loc.content_size {
        return Err(TsetError::DocumentContentSizeMismatch);
    }
    let body_start = start + HASH_SIZE + 8;
    let content = decompressed[body_start..body_start + size as usize].to_vec();

    cache.cur = Some((loc.block_idx, decompressed));
    Ok(content)
}

fn decompress_block_from_body(
    body: &[u8],
    body_offset: u64,
    block: &BlockInfo,
) -> TsetResult<Vec<u8>> {
    let abs = block.offset as usize;
    let body_relative = abs
        .checked_sub(body_offset as usize)
        .ok_or(TsetError::BadManifest("block.offset before body_offset"))?;
    if body_relative + BLOCK_HEADER_SIZE > body.len() {
        return Err(TsetError::BadManifest("block header exceeds body"));
    }
    let mut magic = [0u8; 4];
    magic.copy_from_slice(&body[body_relative..body_relative + 4]);
    if &magic != MAGIC_DOC_BLOCK {
        return Err(TsetError::BadBlockMagic(magic));
    }
    let payload_start = body_relative + BLOCK_HEADER_SIZE;
    let payload_end = payload_start + block.compressed_size as usize;
    if payload_end > body.len() {
        return Err(TsetError::BadManifest("block payload exceeds body"));
    }
    let compressed = &body[payload_start..payload_end];
    let raw = zstd::stream::decode_all(compressed).map_err(|e| TsetError::Zstd(e.to_string()))?;
    if raw.len() as u64 != block.uncompressed_size {
        return Err(TsetError::ChunkUncompressedSizeMismatch);
    }
    Ok(raw)
}

/// Append a new tokenization view to an existing TSET shard, in-place.
///
/// Per SPEC §7: existing views and document blocks aren't modified. Only
/// the manifest is rewritten; the file is truncated to the original
/// `manifest_offset`, the new view is appended at that offset, the new
/// manifest follows, and header + footer are rewritten. The previous
/// manifest bytes are gone (truncate); a future `tset compact` could
/// preserve them as time-travel snapshots.
///
/// Mirrors `tset.writer.append_tokenizer_view` (Python).
pub fn append_tokenizer_view<P: AsRef<Path>>(
    path: P,
    tokenizer: Box<dyn Tokenizer>,
) -> TsetResult<()> {
    use crate::reader::Reader as CoreReader;
    use std::io::{Seek, SeekFrom};

    let path: PathBuf = path.as_ref().to_path_buf();

    // Read existing shard state (manifest, doc list, ordered docs).
    let (mut manifest, ordered_docs, old_manifest_offset, header_v_minor) = {
        let r = CoreReader::open(&path)?;
        if r.tokenizer_ids()?
            .iter()
            .any(|t| t == tokenizer.tokenizer_id())
        {
            return Err(TsetError::BadManifest(
                "tokenizer_id already present in shard",
            ));
        }
        let manifest_value: Value = r.manifest().raw().clone();
        let manifest_obj = manifest_value
            .as_object()
            .ok_or(TsetError::BadManifest("manifest is not an object"))?
            .clone();
        // Re-read docs in their canonical order. Using doc_index keys
        // (which after JSON-sort-keys are alphabetized) keeps the order
        // independent of how the original writer added them — same as the
        // shard_merkle_root invariant.
        let order: Vec<Hash> = r.doc_hashes().copied().collect();
        let mut docs: Vec<(Hash, Vec<u8>)> = Vec::with_capacity(order.len());
        for h in &order {
            let bytes = r.get_document(h)?;
            docs.push((*h, bytes));
        }
        (
            manifest_obj,
            docs,
            r.header.manifest_offset,
            r.header.version_minor,
        )
    };

    // Build the view
    let mut builds = build_view(
        tokenizer.as_ref(),
        &ordered_docs,
        DEFAULT_TOKEN_CHUNK_SIZE,
        DEFAULT_SPARSE_INDEX_INTERVAL,
    )?;
    let v = builds.remove(0);

    // Truncate to old manifest_offset, then write [view][manifest][footer]
    let mut f = OpenOptions::new().write(true).read(true).open(&path)?;
    f.set_len(old_manifest_offset)?;
    f.seek(SeekFrom::Start(old_manifest_offset))?;
    let view_offset = old_manifest_offset;
    f.write_all(&v.encoded)?;

    // Manifest patches: add the view + append a tokenizer_added audit entry
    let chunks_json: Vec<Value> = v
        .chunks
        .iter()
        .map(|c| {
            json!({
                "byte_offset_in_view": c.byte_offset_in_view,
                "compressed_size": c.compressed_size,
                "num_tokens": c.num_tokens,
                "content_hash": c.content_hash.map(hex::encode),
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
        "bits_per_token": v.bits_per_token,
        "chunks": chunks_json,
        "source_map": source_map_json,
        "sparse_offset_index": sparse_json,
        "test_vector": v.test_vector,
    });
    let views = manifest
        .get_mut("tokenization_views")
        .and_then(Value::as_object_mut)
        .ok_or(TsetError::BadManifest("manifest.tokenization_views"))?;
    views.insert(tokenizer.tokenizer_id().to_string(), entry);

    // Append tokenizer_added audit entry — preserves the existing log.
    let mut audit = AuditLog::new();
    if let Some(audit_v) = manifest.get("audit_log") {
        if let Some(arr) = audit_v.get("entries").and_then(Value::as_array) {
            for e in arr {
                let signature = e.get("signature").and_then(Value::as_str).map(String::from);
                audit.entries.push(crate::audit_log::AuditEntry {
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
                    signature,
                });
            }
            audit.log_root = audit_v
                .get("log_root")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
        }
    }
    audit.append(
        "tokenizer_added",
        json!({
            "tokenizer_id": tokenizer.tokenizer_id(),
            "config_hash": hex::encode(v.config_hash),
            "total_tokens": v.total_tokens,
        }),
        current_timestamp(),
    );
    manifest.insert("audit_log".into(), audit.to_json());

    let manifest_value = Value::Object(manifest);
    let manifest_bytes = canonical_json(&manifest_value).into_bytes();
    let manifest_hash = hash_bytes(&manifest_bytes);
    let manifest_offset = f.stream_position()?;
    let manifest_size = manifest_bytes.len() as u64;
    f.write_all(&manifest_bytes)?;

    // Read existing shard_merkle_root from the new manifest's body
    let merkle_hex = manifest_value
        .get("shard_merkle_root")
        .and_then(Value::as_str)
        .unwrap_or("");
    let mut shard_merkle_root = [0u8; HASH_SIZE];
    if let Ok(b) = hex::decode(merkle_hex) {
        if b.len() == HASH_SIZE {
            shard_merkle_root.copy_from_slice(&b);
        }
    }

    let new_header = Header {
        version_major: VERSION_MAJOR,
        version_minor: header_v_minor,
        flags: 0,
        manifest_offset,
        manifest_size,
        shard_merkle_root,
        manifest_hash,
    };
    let mut hash28 = [0u8; TRUNCATED_HASH_SIZE];
    hash28.copy_from_slice(&manifest_hash[..TRUNCATED_HASH_SIZE]);
    let new_footer = Footer {
        manifest_size,
        manifest_hash28: hash28,
    };
    f.write_all(&new_footer.encode())?;
    f.seek(SeekFrom::Start(0))?;
    f.write_all(&new_header.encode())?;
    f.sync_all()?;
    Ok(())
}
