use std::collections::HashMap;

use crate::constants::{HASH_SIZE, MAGIC_DOC_BLOCK};
use crate::error::{TsetError, TsetResult};
use crate::hashing::Hash;

const BLOCK_HEADER_SIZE: usize = 4 + 4 + 8 + 8;

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
        let decompressed = zstd::stream::decode_all(compressed)
            .map_err(|e| TsetError::Zstd(e.to_string()))?;
        if decompressed.len() as u64 != info.uncompressed_size {
            return Err(TsetError::ChunkUncompressedSizeMismatch);
        }
        self.block_cache
            .borrow_mut()
            .insert(block_idx, decompressed.clone());
        Ok(decompressed)
    }
}
