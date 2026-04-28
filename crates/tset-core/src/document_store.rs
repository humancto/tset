use std::collections::HashMap;

use indexmap::IndexMap;

use crate::constants::{HASH_SIZE, MAGIC_DOC_BLOCK};
use crate::error::{TsetError, TsetResult};
use crate::hashing::{hash_bytes, Hash};

const BLOCK_HEADER_SIZE: usize = 4 + 4 + 8 + 8;
pub const DEFAULT_BLOCK_TARGET_BYTES: usize = 8 * 1024 * 1024;

use crate::constants::ZSTD_LEVEL;

#[derive(Debug, Clone)]
pub struct BlockInfo {
    pub offset: u64,
    pub compressed_size: u64,
    pub uncompressed_size: u64,
    pub num_documents: u32,
}

#[derive(Debug, Clone, Copy)]
pub struct DocumentLocator {
    pub block_idx: u32,
    pub in_block_offset: u64,
    pub content_size: u64,
}

pub struct DocumentStoreReader<'a> {
    file_bytes: &'a [u8],
    blocks: Vec<BlockInfo>,
    index: HashMap<Hash, DocumentLocator>,
    block_cache: std::cell::RefCell<HashMap<u32, Vec<u8>>>,
}

impl<'a> DocumentStoreReader<'a> {
    pub fn new(
        file_bytes: &'a [u8],
        blocks: Vec<BlockInfo>,
        index: HashMap<Hash, DocumentLocator>,
    ) -> Self {
        Self {
            file_bytes,
            blocks,
            index,
            block_cache: std::cell::RefCell::new(HashMap::new()),
        }
    }

    pub fn contains(&self, doc_hash: &Hash) -> bool {
        self.index.contains_key(doc_hash)
    }

    pub fn keys(&self) -> impl Iterator<Item = &Hash> {
        self.index.keys()
    }

    pub fn get(&self, doc_hash: &Hash) -> TsetResult<Vec<u8>> {
        let loc = self
            .index
            .get(doc_hash)
            .ok_or_else(|| TsetError::DocumentNotFound(hex::encode(doc_hash)))?;
        let block = self.read_block(loc.block_idx)?;
        let start = usize::try_from(loc.in_block_offset)
            .map_err(|_| TsetError::BadManifest("in_block_offset overflow"))?;
        let header_end = start
            .checked_add(HASH_SIZE + 8)
            .ok_or(TsetError::BadManifest("doc header range overflow"))?;
        if header_end > block.len() {
            return Err(TsetError::BadManifest("doc header exceeds block"));
        }
        let stored_hash: &[u8] = &block[start..start + HASH_SIZE];
        if stored_hash != doc_hash {
            return Err(TsetError::DocumentHashMismatch);
        }
        let size = u64::from_le_bytes(
            block[start + HASH_SIZE..start + HASH_SIZE + 8]
                .try_into()
                .unwrap(),
        );
        if size != loc.content_size {
            return Err(TsetError::DocumentContentSizeMismatch);
        }
        let size_usize =
            usize::try_from(size).map_err(|_| TsetError::BadManifest("doc size overflow"))?;
        let body_start = start + HASH_SIZE + 8;
        let body_end = body_start
            .checked_add(size_usize)
            .ok_or(TsetError::BadManifest("doc body range overflow"))?;
        if body_end > block.len() {
            return Err(TsetError::BadManifest("doc body exceeds block"));
        }
        Ok(block[body_start..body_end].to_vec())
    }

    fn read_block(&self, block_idx: u32) -> TsetResult<Vec<u8>> {
        if let Some(cached) = self.block_cache.borrow().get(&block_idx) {
            return Ok(cached.clone());
        }
        let info = self
            .blocks
            .get(block_idx as usize)
            .ok_or(TsetError::BadManifest("block_idx out of range"))?;
        let off = usize::try_from(info.offset)
            .map_err(|_| TsetError::BadManifest("block.offset overflow"))?;
        let compressed_size = usize::try_from(info.compressed_size)
            .map_err(|_| TsetError::BadManifest("block.compressed_size overflow"))?;
        let header_end = off
            .checked_add(BLOCK_HEADER_SIZE)
            .ok_or(TsetError::BadManifest("block header range overflow"))?;
        let payload_end = header_end
            .checked_add(compressed_size)
            .ok_or(TsetError::BadManifest("block payload range overflow"))?;
        if payload_end > self.file_bytes.len() {
            return Err(TsetError::BadManifest("block payload exceeds file"));
        }
        let header = &self.file_bytes[off..header_end];
        let mut magic = [0u8; 4];
        magic.copy_from_slice(&header[0..4]);
        if &magic != MAGIC_DOC_BLOCK {
            return Err(TsetError::BadBlockMagic(magic));
        }
        let compressed = &self.file_bytes[header_end..payload_end];
        let decompressed =
            zstd::stream::decode_all(compressed).map_err(|e| TsetError::Zstd(e.to_string()))?;
        if decompressed.len() as u64 != info.uncompressed_size {
            return Err(TsetError::ChunkUncompressedSizeMismatch);
        }
        self.block_cache
            .borrow_mut()
            .insert(block_idx, decompressed.clone());
        Ok(decompressed)
    }
}

/// Builds compressed, content-addressed document blocks. Identical
/// content (same BLAKE3) is stored once.
pub struct DocumentStoreWriter {
    block_target_bytes: usize,
    buffer: Vec<u8>,
    buffer_doc_count: u32,
    pending_locators: IndexMap<Hash, (u64, u64)>, // hash → (in_block_offset, content_size)
    index: IndexMap<Hash, DocumentLocator>,
    blocks: Vec<BlockInfo>,
    encoded_blocks: Vec<Vec<u8>>,
}

impl DocumentStoreWriter {
    pub fn new() -> Self {
        Self::with_block_target(DEFAULT_BLOCK_TARGET_BYTES)
    }

    pub fn with_block_target(block_target_bytes: usize) -> Self {
        Self {
            block_target_bytes,
            buffer: Vec::new(),
            buffer_doc_count: 0,
            pending_locators: IndexMap::new(),
            index: IndexMap::new(),
            blocks: Vec::new(),
            encoded_blocks: Vec::new(),
        }
    }

    pub fn add(&mut self, content: &[u8]) -> Hash {
        let h = hash_bytes(content);
        if self.index.contains_key(&h) || self.pending_locators.contains_key(&h) {
            return h;
        }
        let in_block_offset = self.buffer.len() as u64;
        self.buffer.extend_from_slice(&h);
        self.buffer
            .extend_from_slice(&(content.len() as u64).to_le_bytes());
        self.buffer.extend_from_slice(content);
        self.pending_locators
            .insert(h, (in_block_offset, content.len() as u64));
        self.buffer_doc_count += 1;
        if self.buffer.len() >= self.block_target_bytes {
            self.flush_block();
        }
        h
    }

    fn flush_block(&mut self) {
        if self.buffer.is_empty() {
            return;
        }
        let uncompressed_size = self.buffer.len() as u64;
        let compressed = zstd::stream::encode_all(&self.buffer[..], ZSTD_LEVEL)
            .expect("zstd encode should not fail on in-memory data");
        let block_idx = self.blocks.len() as u32;
        for (h, (off, size)) in self.pending_locators.drain(..) {
            self.index.insert(
                h,
                DocumentLocator {
                    block_idx,
                    in_block_offset: off,
                    content_size: size,
                },
            );
        }
        let mut encoded = Vec::with_capacity(BLOCK_HEADER_SIZE + compressed.len());
        encoded.extend_from_slice(MAGIC_DOC_BLOCK);
        encoded.extend_from_slice(&self.buffer_doc_count.to_le_bytes());
        encoded.extend_from_slice(&uncompressed_size.to_le_bytes());
        encoded.extend_from_slice(&(compressed.len() as u64).to_le_bytes());
        encoded.extend_from_slice(&compressed);
        self.blocks.push(BlockInfo {
            offset: 0,
            compressed_size: compressed.len() as u64,
            uncompressed_size,
            num_documents: self.buffer_doc_count,
        });
        self.encoded_blocks.push(encoded);
        self.buffer.clear();
        self.buffer_doc_count = 0;
    }

    /// Finalize the writer, assigning real file offsets starting at
    /// `body_offset`. Returns the concatenated encoded body, the block
    /// info table, and the doc → locator index (insertion order).
    pub fn finalize(
        mut self,
        body_offset: u64,
    ) -> (Vec<u8>, Vec<BlockInfo>, IndexMap<Hash, DocumentLocator>) {
        self.flush_block();
        let mut cursor = body_offset;
        for block in &mut self.blocks {
            block.offset = cursor;
            cursor += BLOCK_HEADER_SIZE as u64 + block.compressed_size;
        }
        let mut body = Vec::new();
        for enc in &self.encoded_blocks {
            body.extend_from_slice(enc);
        }
        (body, self.blocks, self.index)
    }
}

impl Default for DocumentStoreWriter {
    fn default() -> Self {
        Self::new()
    }
}
