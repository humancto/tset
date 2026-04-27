use crate::constants::{CHUNK_HEADER_SIZE, MAGIC_VIEW};
use crate::error::{TsetError, TsetResult};
use crate::hashing::{hash_bytes, Hash};

#[derive(Debug, Clone)]
pub struct ChunkInfo {
    pub byte_offset_in_view: u64,
    pub compressed_size: u64,
    pub num_tokens: u64,
    /// BLAKE3 over the compressed payload (v0.2+); None for v0.1 shards.
    pub content_hash: Option<Hash>,
}

#[derive(Debug, Clone)]
pub struct SourceMapEntry {
    pub doc_hash: Hash,
    pub token_offset: u64,
    pub token_count: u64,
}

pub fn read_chunk(
    file_bytes: &[u8],
    view_offset: u64,
    chunk: &ChunkInfo,
    vocab_size: Option<u32>,
) -> TsetResult<Vec<u32>> {
    let abs_offset = view_offset
        .checked_add(chunk.byte_offset_in_view)
        .and_then(|x| usize::try_from(x).ok())
        .ok_or(TsetError::BadManifest("chunk offset overflow"))?;
    let header_end = abs_offset
        .checked_add(CHUNK_HEADER_SIZE)
        .ok_or(TsetError::BadManifest("chunk header range overflow"))?;
    if header_end > file_bytes.len() {
        return Err(TsetError::BadManifest("chunk header exceeds file"));
    }
    let header = &file_bytes[abs_offset..header_end];
    let uncompressed_size = u64::from_le_bytes(header[0..8].try_into().unwrap());
    let compressed_size = u64::from_le_bytes(header[8..16].try_into().unwrap());
    let num_tokens = u64::from_le_bytes(header[16..24].try_into().unwrap());
    if compressed_size != chunk.compressed_size {
        return Err(TsetError::ChunkCompressedSizeMismatch);
    }
    if num_tokens != chunk.num_tokens {
        return Err(TsetError::ChunkNumTokensMismatch);
    }
    let compressed_size_usize = usize::try_from(compressed_size)
        .map_err(|_| TsetError::BadManifest("chunk compressed_size overflow"))?;
    let payload_end = header_end
        .checked_add(compressed_size_usize)
        .ok_or(TsetError::BadManifest("chunk payload range overflow"))?;
    if payload_end > file_bytes.len() {
        return Err(TsetError::BadManifest("chunk payload exceeds file"));
    }
    let payload = &file_bytes[header_end..payload_end];

    if let Some(expected) = chunk.content_hash {
        if hash_bytes(payload) != expected {
            return Err(TsetError::ChunkContentHashMismatch);
        }
    }

    let raw = zstd::stream::decode_all(payload).map_err(|e| TsetError::Zstd(e.to_string()))?;
    if raw.len() as u64 != uncompressed_size {
        return Err(TsetError::ChunkUncompressedSizeMismatch);
    }
    if raw.len() % 4 != 0 {
        return Err(TsetError::ChunkUncompressedSizeMismatch);
    }
    let mut tokens = Vec::with_capacity(raw.len() / 4);
    for c in raw.chunks_exact(4) {
        let id = u32::from_le_bytes(c.try_into().unwrap());
        if let Some(v) = vocab_size {
            if id >= v {
                return Err(TsetError::TokenIdOutOfRange(id, v));
            }
        }
        tokens.push(id);
    }
    Ok(tokens)
}

pub fn verify_view_header(
    file_bytes: &[u8],
    view_offset: u64,
    expected_config_hash: &Hash,
    expected_total_tokens: u64,
    expected_num_chunks: u64,
) -> TsetResult<()> {
    let off = usize::try_from(view_offset)
        .map_err(|_| TsetError::BadManifest("view_offset overflow"))?;
    let header_end = off
        .checked_add(crate::constants::VIEW_HEADER_SIZE)
        .ok_or(TsetError::BadManifest("view header range overflow"))?;
    if header_end > file_bytes.len() {
        return Err(TsetError::BadManifest("view header exceeds file"));
    }
    let mut magic = [0u8; 4];
    magic.copy_from_slice(&file_bytes[off..off + 4]);
    if &magic != MAGIC_VIEW {
        return Err(TsetError::BadViewMagic(magic));
    }
    let config_hash: &[u8] = &file_bytes[off + 4..off + 4 + 32];
    if config_hash != expected_config_hash {
        return Err(TsetError::ViewConfigHashMismatch);
    }
    let total_on_disk = u64::from_le_bytes(file_bytes[off + 36..off + 44].try_into().unwrap());
    let chunks_on_disk = u64::from_le_bytes(file_bytes[off + 44..off + 52].try_into().unwrap());
    if total_on_disk != expected_total_tokens {
        return Err(TsetError::ViewTotalTokensMismatch {
            on_disk: total_on_disk,
            manifest: expected_total_tokens,
        });
    }
    if chunks_on_disk != expected_num_chunks {
        return Err(TsetError::ViewNumChunksMismatch {
            on_disk: chunks_on_disk,
            manifest: expected_num_chunks,
        });
    }
    Ok(())
}
