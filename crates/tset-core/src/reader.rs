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

        let manifest_off = header.manifest_offset as usize;
        let manifest_size = header.manifest_size as usize;
        let manifest_bytes = &mmap[manifest_off..manifest_off + manifest_size];

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
            .map(|b| BlockInfo {
                offset: b.get("offset").and_then(Value::as_u64).unwrap_or(0),
                compressed_size: b.get("compressed_size").and_then(Value::as_u64).unwrap_or(0),
                uncompressed_size: b
                    .get("uncompressed_size")
                    .and_then(Value::as_u64)
                    .unwrap_or(0),
                num_documents: b
                    .get("num_documents")
                    .and_then(Value::as_u64)
                    .unwrap_or(0) as u32,
            })
            .collect::<Vec<_>>();

        let mut index = HashMap::new();
        for (hex_h, v) in manifest.doc_index()? {
            let bytes = hex::decode(hex_h)?;
            if bytes.len() != HASH_SIZE {
                return Err(TsetError::BadManifest("doc index hash length"));
            }
            let mut key = [0u8; HASH_SIZE];
            key.copy_from_slice(&bytes);
            index.insert(
                key,
                DocumentLocator {
                    block_idx: v.get("block_idx").and_then(Value::as_u64).unwrap_or(0) as u32,
                    in_block_offset: v
                        .get("in_block_offset")
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                    content_size: v.get("content_size").and_then(Value::as_u64).unwrap_or(0),
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
        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.path
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
        self.manifest
            .view(tokenizer_id)?
            .get("total_tokens")
            .and_then(Value::as_u64)
            .ok_or(TsetError::BadManifest("view.total_tokens"))
    }

    pub fn open_view(&self, tokenizer_id: &str) -> TsetResult<TokenizationView<'_>> {
        let v = self.manifest.view(tokenizer_id)?;
        let view_offset = v
            .get("view_offset")
            .and_then(Value::as_u64)
            .ok_or(TsetError::BadManifest("view.view_offset"))?;
        let total_tokens = v
            .get("total_tokens")
            .and_then(Value::as_u64)
            .ok_or(TsetError::BadManifest("view.total_tokens"))?;
        let vocab_size = v
            .get("vocab_size")
            .and_then(Value::as_u64)
            .ok_or(TsetError::BadManifest("view.vocab_size"))? as u32;
        let config_hash_hex = v
            .get("config_hash")
            .and_then(Value::as_str)
            .ok_or(TsetError::BadManifest("view.config_hash"))?;
        let config_hash_vec = hex::decode(config_hash_hex)?;
        if config_hash_vec.len() != HASH_SIZE {
            return Err(TsetError::BadManifest("view.config_hash length"));
        }
        let mut config_hash = [0u8; HASH_SIZE];
        config_hash.copy_from_slice(&config_hash_vec);

        let chunks_arr = v
            .get("chunks")
            .and_then(Value::as_array)
            .ok_or(TsetError::BadManifest("view.chunks"))?;
        let mut chunks = Vec::with_capacity(chunks_arr.len());
        for c in chunks_arr {
            let content_hash = match c.get("content_hash").and_then(Value::as_str) {
                Some(s) if !s.is_empty() => {
                    let bytes = hex::decode(s)?;
                    if bytes.len() != HASH_SIZE {
                        return Err(TsetError::BadManifest("chunk.content_hash length"));
                    }
                    let mut h = [0u8; HASH_SIZE];
                    h.copy_from_slice(&bytes);
                    Some(h)
                }
                _ => None,
            };
            chunks.push(ChunkInfo {
                byte_offset_in_view: c
                    .get("byte_offset_in_view")
                    .and_then(Value::as_u64)
                    .unwrap_or(0),
                compressed_size: c
                    .get("compressed_size")
                    .and_then(Value::as_u64)
                    .unwrap_or(0),
                num_tokens: c.get("num_tokens").and_then(Value::as_u64).unwrap_or(0),
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
            let h_hex = s
                .get("doc_hash")
                .and_then(Value::as_str)
                .ok_or(TsetError::BadManifest("source_map.doc_hash"))?;
            let bytes = hex::decode(h_hex)?;
            let mut h = [0u8; HASH_SIZE];
            h.copy_from_slice(&bytes);
            source_map.push(SourceMapEntry {
                doc_hash: h,
                token_offset: s.get("token_offset").and_then(Value::as_u64).unwrap_or(0),
                token_count: s.get("token_count").and_then(Value::as_u64).unwrap_or(0),
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
