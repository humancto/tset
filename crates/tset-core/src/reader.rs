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
use crate::tokenizer_view::{read_chunk, verify_view_header, ChunkInfo, SourceMapEntry};

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

impl Reader {
    pub fn open<P: AsRef<Path>>(path: P) -> TsetResult<Self> {
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

        let reader = Self {
            path,
            mmap,
            header,
            footer,
            manifest,
            blocks,
            index,
        };
        reader.verify_invariants()?;
        Ok(reader)
    }

    fn verify_invariants(&self) -> TsetResult<()> {
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
        // Full reproducibility check (SPEC §7 obligation #4): for each
        // view, rebuild a tokenizer from its config and re-tokenize the
        // test_vector documents, asserting the byte hash matches.
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

    /// Reconstruct the shard's SMT root either from the manifest's
    /// `smt_root` field (cheap path) or by replaying `smt_present_keys`
    /// (fallback). The two MUST agree on a well-formed shard.
    pub fn smt_root(&self) -> Hash {
        if let Some(s) = self.manifest.smt_root_hex() {
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

        Ok(TokenizationView {
            mmap: &self.mmap,
            view_offset,
            vocab_size,
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
            let arr = read_chunk(&self.mmap, self.view_offset, chunk, Some(self.vocab_size))?;
            out.extend_from_slice(&arr);
        }
        Ok(out)
    }

    /// Yields `(tokens, doc_hash)` per source-map entry.
    pub fn iter_per_doc(&self) -> TsetResult<Vec<(Vec<u32>, Hash)>> {
        let mut chunk_starts: Vec<u64> = Vec::with_capacity(self.chunks.len());
        let mut cum = 0u64;
        for c in &self.chunks {
            chunk_starts.push(cum);
            cum += c.num_tokens;
        }
        let mut chunk_cache: HashMap<usize, Vec<u32>> = HashMap::new();
        let mut out = Vec::with_capacity(self.source_map.len());
        for entry in &self.source_map {
            if entry.token_count == 0 {
                continue;
            }
            // locate first chunk
            let mut cid = 0usize;
            while cid + 1 < chunk_starts.len() && chunk_starts[cid + 1] <= entry.token_offset {
                cid += 1;
            }
            let mut remaining = entry.token_count;
            let mut cur = entry.token_offset;
            let mut piece: Vec<u32> = Vec::with_capacity(entry.token_count as usize);
            while remaining > 0 {
                if !chunk_cache.contains_key(&cid) {
                    let arr = read_chunk(
                        self.mmap,
                        self.view_offset,
                        &self.chunks[cid],
                        Some(self.vocab_size),
                    )?;
                    chunk_cache.insert(cid, arr);
                }
                let arr = chunk_cache.get(&cid).unwrap();
                let off = (cur - chunk_starts[cid]) as usize;
                let take = std::cmp::min(arr.len() - off, remaining as usize);
                piece.extend_from_slice(&arr[off..off + take]);
                cur += take as u64;
                remaining -= take as u64;
                cid += 1;
            }
            out.push((piece, entry.doc_hash));
        }
        Ok(out)
    }
}
