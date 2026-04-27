use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};

use memmap2::Mmap;
use serde_json::Value;

use crate::audit_log::verify_audit_log;
use crate::constants::{FOOTER_SIZE, HASH_SIZE, HEADER_SIZE, TRUNCATED_HASH_SIZE};
use crate::document_store::{BlockInfo, DocumentLocator, DocumentStoreReader};
use crate::error::{TsetError, TsetResult};
use crate::footer::Footer;
use crate::hashing::{hash_bytes, shard_merkle_root, Hash};
use crate::header::Header;
use crate::manifest::Manifest;
use crate::tokenizer_view::{
    read_chunk_with_bits, verify_view_header, ChunkInfo, SourceMapEntry,
};

fn required_u64(
    v: &Value,
    key: &str,
    label: &'static str,
) -> TsetResult<u64> {
    v.get(key)
        .and_then(Value::as_u64)
        .ok_or(TsetError::BadManifest(label))
}

fn required_str<'v>(v: &'v Value, key: &str, label: &'static str) -> TsetResult<&'v str> {
    v.get(key)
        .and_then(Value::as_str)
        .ok_or(TsetError::BadManifest(label))
}

fn parse_hash(hex_str: &str, label: &'static str) -> TsetResult<crate::hashing::Hash> {
    let bytes = hex::decode(hex_str)?;
    if bytes.len() != HASH_SIZE {
        return Err(TsetError::BadManifest(label));
    }
    let mut h = [0u8; HASH_SIZE];
    h.copy_from_slice(&bytes);
    Ok(h)
}

pub struct Reader {
    path: PathBuf,
    mmap: Mmap,
    pub header: Header,
    pub footer: Footer,
    manifest: Manifest,
    blocks: Vec<BlockInfo>,
    index: HashMap<Hash, DocumentLocator>,
}

/// Opt-in verification controls. Default = full verification (the
/// safe choice). Skipping the reproducibility check is only sound when
/// the caller has *already verified the shard once*; on a hot streaming
/// path you can re-open with `skip_reproducibility=true` to avoid
/// re-tokenizing the test_vector documents on every open.
#[derive(Debug, Clone, Copy)]
pub struct OpenOptions {
    pub skip_reproducibility: bool,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            skip_reproducibility: false,
        }
    }
}

impl Reader {
    pub fn open<P: AsRef<Path>>(path: P) -> TsetResult<Self> {
        Self::open_with_options(path, OpenOptions::default())
    }

    pub fn open_with_options<P: AsRef<Path>>(
        path: P,
        opts: OpenOptions,
    ) -> TsetResult<Self> {
        let r = Self::open_inner(path)?;
        r.verify_invariants_with(opts)?;
        Ok(r)
    }

    fn open_inner<P: AsRef<Path>>(path: P) -> TsetResult<Self> {
        let path: PathBuf = path.as_ref().to_path_buf();
        let file = File::open(&path)?;
        let mmap = unsafe { Mmap::map(&file)? };

        if mmap.len() < HEADER_SIZE + FOOTER_SIZE {
            return Err(TsetError::BadManifest("file shorter than header+footer"));
        }

        let header = Header::decode(&mmap[..HEADER_SIZE])?;
        let footer = Footer::decode(&mmap[mmap.len() - FOOTER_SIZE..])?;

        let manifest_off = usize::try_from(header.manifest_offset)
            .map_err(|_| TsetError::BadManifest("manifest_offset overflow"))?;
        let manifest_size = usize::try_from(header.manifest_size)
            .map_err(|_| TsetError::BadManifest("manifest_size overflow"))?;
        let manifest_end = manifest_off
            .checked_add(manifest_size)
            .ok_or(TsetError::BadManifest("manifest range overflow"))?;
        if manifest_end > mmap.len() {
            return Err(TsetError::BadManifest("manifest range exceeds file"));
        }
        let manifest_bytes = &mmap[manifest_off..manifest_end];

        if hash_bytes(manifest_bytes) != header.manifest_hash {
            return Err(TsetError::ManifestHashMismatch("header"));
        }
        if header.manifest_hash[..TRUNCATED_HASH_SIZE] != footer.manifest_hash28 {
            return Err(TsetError::ManifestHashMismatch("footer"));
        }
        if footer.manifest_size != header.manifest_size {
            return Err(TsetError::ManifestSizeMismatch);
        }

        let manifest = Manifest::from_bytes(manifest_bytes)?;

        let blocks = manifest
            .block_infos()?
            .iter()
            .map(|b| {
                let num_documents_u64 = required_u64(b, "num_documents", "block.num_documents")?;
                if num_documents_u64 > u32::MAX as u64 {
                    return Err(TsetError::BadManifest("block.num_documents > u32::MAX"));
                }
                Ok(BlockInfo {
                    offset: required_u64(b, "offset", "block.offset")?,
                    compressed_size: required_u64(b, "compressed_size", "block.compressed_size")?,
                    uncompressed_size: required_u64(
                        b,
                        "uncompressed_size",
                        "block.uncompressed_size",
                    )?,
                    num_documents: num_documents_u64 as u32,
                })
            })
            .collect::<TsetResult<Vec<_>>>()?;

        let mut index = HashMap::new();
        for (hex_h, v) in manifest.doc_index()? {
            let bytes = hex::decode(hex_h)?;
            if bytes.len() != HASH_SIZE {
                return Err(TsetError::BadManifest("doc index hash length"));
            }
            let mut key = [0u8; HASH_SIZE];
            key.copy_from_slice(&bytes);
            let block_idx_u64 = required_u64(v, "block_idx", "doc_index.block_idx")?;
            if block_idx_u64 > u32::MAX as u64 {
                return Err(TsetError::BadManifest("doc_index.block_idx > u32::MAX"));
            }
            index.insert(
                key,
                DocumentLocator {
                    block_idx: block_idx_u64 as u32,
                    in_block_offset: required_u64(
                        v,
                        "in_block_offset",
                        "doc_index.in_block_offset",
                    )?,
                    content_size: required_u64(v, "content_size", "doc_index.content_size")?,
                },
            );
        }

        Ok(Self {
            path,
            mmap,
            header,
            footer,
            manifest,
            blocks,
            index,
        })
    }

    fn verify_invariants_with(&self, opts: OpenOptions) -> TsetResult<()> {
        self.verify_invariants_core(opts.skip_reproducibility)
    }

    fn verify_invariants_core(&self, skip_reproducibility: bool) -> TsetResult<()> {
        // shard_merkle_root must agree with header AND with the manifest field
        // Order doesn't matter — root computed from doc_index keys (hex sort?
        // no — Python uses insertion order of doc_index dict). Replicate:
        let mut leaves: Vec<Hash> = Vec::with_capacity(self.index.len());
        for (hex_h, _) in self.manifest.doc_index()? {
            let bytes = hex::decode(hex_h)?;
            let mut k = [0u8; HASH_SIZE];
            k.copy_from_slice(&bytes);
            leaves.push(k);
        }
        let merkle = shard_merkle_root(&leaves);
        if let Some(hex) = self.manifest.shard_merkle_root_hex() {
            let m = hex::decode(hex)?;
            if m != merkle {
                return Err(TsetError::ShardMerkleRootMismatch("manifest"));
            }
        }
        if merkle != self.header.shard_merkle_root {
            return Err(TsetError::ShardMerkleRootMismatch("header"));
        }
        if let Some(audit) = self.manifest.audit_log() {
            if !verify_audit_log(audit) {
                return Err(TsetError::AuditLogIntegrityFailed);
            }
        }
        // Strict v0.2 enforcement: every chunk in every view MUST carry
        // a content_hash. Earlier this was checked only inside open_view,
        // so a malformed v0.2 shard could be opened and the per-view
        // mismatch would surface only when that view was used. Doing it
        // at file-open time is the fail-fast posture for v0.2+.
        if self.header.version_minor >= 2 {
            if let Ok(views) = self.manifest.views() {
                for (vid, view) in views {
                    let chunks = view
                        .get("chunks")
                        .and_then(Value::as_array)
                        .ok_or(TsetError::BadManifest("view.chunks"))?;
                    for c in chunks {
                        let has_hash = c
                            .get("content_hash")
                            .and_then(Value::as_str)
                            .map(|s| !s.is_empty())
                            .unwrap_or(false);
                        if !has_hash {
                            // Use BadManifest for parity with the existing
                            // open_view error; carry the view id as breadcrumb.
                            let _ = vid;
                            return Err(TsetError::BadManifest(
                                "v0.2 shard missing chunk.content_hash (file-open check)",
                            ));
                        }
                    }
                }
            }
        }

        // Full reproducibility check (SPEC §7 obligation #4): for each
        // view, rebuild a tokenizer from its config and re-tokenize the
        // test_vector documents, asserting the byte hash matches.
        //
        // `skip_reproducibility` short-circuits this — only sound on a
        // hot streaming open of a shard the caller has already verified.
        if skip_reproducibility {
            return Ok(());
        }
        if let Ok(views) = self.manifest.views() {
            for (_id, view) in views {
                if let Some(tv) = view.get("test_vector") {
                    let cfg = view
                        .get("tokenizer_config")
                        .ok_or(TsetError::BadManifest("view.tokenizer_config"))?;
                    let tokenizer = crate::tokenizers::tokenizer_from_config(cfg)?;
                    let mut docs_map: std::collections::HashMap<Hash, Vec<u8>> =
                        std::collections::HashMap::new();
                    let doc_hashes = tv
                        .get("doc_hashes")
                        .and_then(Value::as_array)
                        .ok_or(TsetError::BadManifest("view.test_vector.doc_hashes"))?;
                    for h in doc_hashes {
                        let hex_str = h.as_str().ok_or(TsetError::BadManifest(
                            "view.test_vector.doc_hashes[i]",
                        ))?;
                        let key = parse_hash(hex_str, "test_vector doc_hash")?;
                        let content = self.get_document(&key)?;
                        docs_map.insert(key, content);
                    }
                    crate::tokenizers::verify_reproducibility(
                        tokenizer.as_ref(),
                        tv,
                        &docs_map,
                    )?;
                }
            }
        }
        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Decode the on-disk TSMT section if the manifest references one
    /// (v0.4-style shards written with `Writer.enable_binary_sections`).
    /// Returns `Ok(None)` if no section pointer is in the manifest.
    pub fn on_disk_smt(&self) -> TsetResult<Option<crate::sections::TsmtSection>> {
        let raw = self.manifest.raw();
        let Some(ptr) = raw.get("smt_section") else {
            return Ok(None);
        };
        let off = ptr
            .get("offset")
            .and_then(Value::as_u64)
            .ok_or(TsetError::BadManifest("smt_section.offset"))? as usize;
        let size = ptr
            .get("size")
            .and_then(Value::as_u64)
            .ok_or(TsetError::BadManifest("smt_section.size"))? as usize;
        let end = off
            .checked_add(size)
            .ok_or(TsetError::BadManifest("smt_section range overflow"))?;
        if end > self.mmap.len() {
            return Err(TsetError::BadManifest("smt_section exceeds file"));
        }
        Ok(Some(crate::sections::decode_tsmt_section(
            &self.mmap[off..end],
        )?))
    }

    /// Decode the on-disk TLOG section if present.
    pub fn on_disk_audit_log(
        &self,
    ) -> TsetResult<Option<crate::sections::TlogSection>> {
        let raw = self.manifest.raw();
        let Some(ptr) = raw.get("audit_log_section") else {
            return Ok(None);
        };
        let off = ptr
            .get("offset")
            .and_then(Value::as_u64)
            .ok_or(TsetError::BadManifest("audit_log_section.offset"))? as usize;
        let size = ptr
            .get("size")
            .and_then(Value::as_u64)
            .ok_or(TsetError::BadManifest("audit_log_section.size"))? as usize;
        let end = off
            .checked_add(size)
            .ok_or(TsetError::BadManifest("audit_log_section range overflow"))?;
        if end > self.mmap.len() {
            return Err(TsetError::BadManifest("audit_log_section exceeds file"));
        }
        Ok(Some(crate::sections::decode_tlog_section(
            &self.mmap[off..end],
        )?))
    }

    /// Decode the on-disk TCOL section if present.
    pub fn on_disk_columns(
        &self,
    ) -> TsetResult<Option<crate::sections::TcolSection>> {
        let raw = self.manifest.raw();
        let Some(ptr) = raw.get("metadata_columns_section") else {
            return Ok(None);
        };
        let off = ptr
            .get("offset")
            .and_then(Value::as_u64)
            .ok_or(TsetError::BadManifest("metadata_columns_section.offset"))? as usize;
        let size = ptr
            .get("size")
            .and_then(Value::as_u64)
            .ok_or(TsetError::BadManifest("metadata_columns_section.size"))? as usize;
        let end = off
            .checked_add(size)
            .ok_or(TsetError::BadManifest(
                "metadata_columns_section range overflow",
            ))?;
        if end > self.mmap.len() {
            return Err(TsetError::BadManifest(
                "metadata_columns_section exceeds file",
            ));
        }
        Ok(Some(crate::sections::decode_tcol_section(
            &self.mmap[off..end],
        )?))
    }

    /// Reconstruct the shard's SMT root. Resolution order:
    /// 1. On-disk TSMT section (`smt_section` manifest pointer)
    /// 2. Manifest's `smt_root` hex field (current default)
    /// 3. Replay manifest's `smt_present_keys` array (fallback)
    pub fn smt_root(&self) -> Hash {
        if let Ok(Some(section)) = self.on_disk_smt() {
            return section.smt_root;
        }
        if let Some(s) = self.manifest.smt_root_hex() {  // legacy
            if let Ok(b) = hex::decode(s) {
                if b.len() == 32 {
                    let mut h = [0u8; 32];
                    h.copy_from_slice(&b);
                    return h;
                }
            }
        }
        // Fallback: rebuild from present-keys list
        let mut tree = crate::smt::SparseMerkleTree::new();
        if let Some(arr) = self
            .manifest
            .raw()
            .get("smt_present_keys")
            .and_then(serde_json::Value::as_array)
        {
            for v in arr {
                if let Some(s) = v.as_str() {
                    if let Ok(bytes) = hex::decode(s) {
                        if bytes.len() == 32 {
                            let mut h = [0u8; 32];
                            h.copy_from_slice(&bytes);
                            tree.insert(h);
                        }
                    }
                }
            }
        }
        tree.root()
    }

    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    pub fn shard_id(&self) -> Option<&str> {
        self.manifest.shard_id()
    }

    pub fn doc_hashes(&self) -> impl Iterator<Item = &Hash> {
        self.index.keys()
    }

    pub fn has_document(&self, doc_hash: &Hash) -> bool {
        self.index.contains_key(doc_hash)
    }

    pub fn get_document(&self, doc_hash: &Hash) -> TsetResult<Vec<u8>> {
        let docs = DocumentStoreReader::new(&self.mmap, self.blocks.clone(), self.index.clone());
        docs.get(doc_hash)
    }

    pub fn tokenizer_ids(&self) -> TsetResult<Vec<String>> {
        Ok(self.manifest.views()?.keys().cloned().collect())
    }

    pub fn view_total_tokens(&self, tokenizer_id: &str) -> TsetResult<u64> {
        required_u64(self.manifest.view(tokenizer_id)?, "total_tokens", "view.total_tokens")
    }

    pub fn open_view(&self, tokenizer_id: &str) -> TsetResult<TokenizationView<'_>> {
        let v = self.manifest.view(tokenizer_id)?;
        let view_offset = required_u64(v, "view_offset", "view.view_offset")?;
        let total_tokens = required_u64(v, "total_tokens", "view.total_tokens")?;
        let vocab_size_u64 = required_u64(v, "vocab_size", "view.vocab_size")?;
        if vocab_size_u64 > u32::MAX as u64 {
            return Err(TsetError::BadManifest("view.vocab_size > u32::MAX"));
        }
        let vocab_size = vocab_size_u64 as u32;
        let config_hash_hex = required_str(v, "config_hash", "view.config_hash")?;
        let config_hash = parse_hash(config_hash_hex, "view.config_hash length")?;

        let chunks_arr = v
            .get("chunks")
            .and_then(Value::as_array)
            .ok_or(TsetError::BadManifest("view.chunks"))?;
        let mut chunks = Vec::with_capacity(chunks_arr.len());
        for c in chunks_arr {
            // v0.1 shards: content_hash absent. v0.2+ shards: present and verified.
            // Strict v0.2: when the shard's version_minor is 2+, every chunk
            // MUST carry a content_hash (otherwise the integrity contract is
            // not actually enforced).
            //
            // Why this lives in `open_view` rather than `Reader::open`:
            // SPEC §7's reader obligations are stated per-tokenization-view
            // (because views are appendable independently of the rest of the
            // shard). Enforcing here means a malformed view can be added to
            // an otherwise-valid shard without breaking `Reader::open` for
            // unrelated views. Move to `Reader::open` if SPEC ever requires
            // file-open-time enforcement.
            let content_hash = match c.get("content_hash").and_then(Value::as_str) {
                Some(s) if !s.is_empty() => Some(parse_hash(s, "chunk.content_hash length")?),
                _ => {
                    if self.header.version_minor >= 2 {
                        return Err(TsetError::BadManifest(
                            "v0.2 shard missing chunk.content_hash",
                        ));
                    }
                    None
                }
            };
            chunks.push(ChunkInfo {
                byte_offset_in_view: required_u64(
                    c,
                    "byte_offset_in_view",
                    "chunk.byte_offset_in_view",
                )?,
                compressed_size: required_u64(c, "compressed_size", "chunk.compressed_size")?,
                num_tokens: required_u64(c, "num_tokens", "chunk.num_tokens")?,
                content_hash,
            });
        }
        verify_view_header(
            &self.mmap,
            view_offset,
            &config_hash,
            total_tokens,
            chunks.len() as u64,
        )?;

        let source_map_arr = v
            .get("source_map")
            .and_then(Value::as_array)
            .ok_or(TsetError::BadManifest("view.source_map"))?;
        let mut source_map = Vec::with_capacity(source_map_arr.len());
        for s in source_map_arr {
            let h_hex = required_str(s, "doc_hash", "source_map.doc_hash")?;
            let h = parse_hash(h_hex, "source_map.doc_hash length")?;
            source_map.push(SourceMapEntry {
                doc_hash: h,
                token_offset: required_u64(s, "token_offset", "source_map.token_offset")?,
                token_count: required_u64(s, "token_count", "source_map.token_count")?,
            });
        }

        // bits_per_token is v0.3+; default to 32 for v0.1/v0.2 shards
        // that don't carry the field.
        let bits_per_token = v
            .get("bits_per_token")
            .and_then(Value::as_u64)
            .unwrap_or(32) as u8;

        Ok(TokenizationView {
            mmap: &self.mmap,
            view_offset,
            vocab_size,
            bits_per_token,
            chunks,
            source_map,
            total_tokens,
        })
    }
}

pub struct TokenizationView<'a> {
    mmap: &'a Mmap,
    view_offset: u64,
    vocab_size: u32,
    bits_per_token: u8,
    chunks: Vec<ChunkInfo>,
    source_map: Vec<SourceMapEntry>,
    total_tokens: u64,
}

impl<'a> TokenizationView<'a> {
    pub fn total_tokens(&self) -> u64 {
        self.total_tokens
    }

    pub fn read_all(&self) -> TsetResult<Vec<u32>> {
        let mut out = Vec::with_capacity(self.total_tokens as usize);
        for chunk in &self.chunks {
            let arr = read_chunk_with_bits(
                &self.mmap,
                self.view_offset,
                chunk,
                Some(self.vocab_size),
                self.bits_per_token,
            )?;
            out.extend_from_slice(&arr);
        }
        Ok(out)
    }

    /// Eager all-at-once view. Kept for compatibility with PR 1 callers
    /// and the conformance suite. New code should prefer `iter_per_doc`
    /// (lazy iterator) — this method materializes the entire result up
    /// front and is O(shard size) in memory.
    pub fn iter_per_doc(&self) -> TsetResult<Vec<(Vec<u32>, Hash)>> {
        self.iter_per_doc_lazy()?.collect()
    }

    /// Lazy iterator — yields one `(tokens, doc_hash)` per source-map
    /// entry, decompressing chunks on demand and dropping previously-
    /// touched chunks once we've moved past them. Memory footprint is
    /// bounded to the current chunk + the previous one (for entries
    /// that span a chunk boundary).
    pub fn iter_per_doc_lazy(
        &self,
    ) -> TsetResult<impl Iterator<Item = TsetResult<(Vec<u32>, Hash)>> + '_> {
        let mut chunk_starts: Vec<u64> = Vec::with_capacity(self.chunks.len());
        let mut cum = 0u64;
        for c in &self.chunks {
            chunk_starts.push(cum);
            cum += c.num_tokens;
        }
        Ok(LazyDocIter {
            view: self,
            chunk_starts,
            sm_idx: 0,
            cache: BoundedChunkCache::new(),
        })
    }
}

/// Bounded two-slot LRU. Source-map entries are emitted in order, so a
/// streaming reader only ever touches the current chunk and (briefly)
/// the previous chunk while a doc spans the boundary.
struct BoundedChunkCache {
    a: Option<(usize, Vec<u32>)>,
    b: Option<(usize, Vec<u32>)>,
}

impl BoundedChunkCache {
    fn new() -> Self {
        Self { a: None, b: None }
    }
    fn get(&self, idx: usize) -> Option<&Vec<u32>> {
        if let Some((i, arr)) = &self.a {
            if *i == idx {
                return Some(arr);
            }
        }
        if let Some((i, arr)) = &self.b {
            if *i == idx {
                return Some(arr);
            }
        }
        None
    }
    fn put(&mut self, idx: usize, arr: Vec<u32>) {
        // Move current-a into b (drop old b), put new entry in a.
        self.b = self.a.take();
        self.a = Some((idx, arr));
    }
}

struct LazyDocIter<'a> {
    view: &'a TokenizationView<'a>,
    chunk_starts: Vec<u64>,
    sm_idx: usize,
    cache: BoundedChunkCache,
}

impl<'a> Iterator for LazyDocIter<'a> {
    type Item = TsetResult<(Vec<u32>, Hash)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let entry = self.view.source_map.get(self.sm_idx)?;
            self.sm_idx += 1;
            if entry.token_count == 0 {
                continue;
            }
            let mut cid = 0usize;
            while cid + 1 < self.chunk_starts.len()
                && self.chunk_starts[cid + 1] <= entry.token_offset
            {
                cid += 1;
            }
            let mut remaining = entry.token_count;
            let mut cur = entry.token_offset;
            let mut piece: Vec<u32> = Vec::with_capacity(entry.token_count as usize);
            while remaining > 0 {
                if self.cache.get(cid).is_none() {
                    let arr = match read_chunk_with_bits(
                        self.view.mmap,
                        self.view.view_offset,
                        &self.view.chunks[cid],
                        Some(self.view.vocab_size),
                        self.view.bits_per_token,
                    ) {
                        Ok(a) => a,
                        Err(e) => return Some(Err(e)),
                    };
                    self.cache.put(cid, arr);
                }
                let arr = self.cache.get(cid).unwrap();
                let off = (cur - self.chunk_starts[cid]) as usize;
                let take = std::cmp::min(arr.len() - off, remaining as usize);
                piece.extend_from_slice(&arr[off..off + take]);
                cur += take as u64;
                remaining -= take as u64;
                cid += 1;
            }
            return Some(Ok((piece, entry.doc_hash)));
        }
    }
}
