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

use crate::constants::{VIEW_HEADER_SIZE, ZSTD_LEVEL};
use crate::tokenizers::Tokenizer;
use serde_json::Value;

pub const DEFAULT_TOKEN_CHUNK_SIZE: usize = 65536;
pub const DEFAULT_SPARSE_INDEX_INTERVAL: usize = 65536;

#[derive(Debug, Clone)]
pub struct SparseIndexEntry {
    pub token_offset: u64,
    pub chunk_id: u32,
    pub in_chunk_offset: u32,
}

#[derive(Debug)]
pub struct TokenizationViewBuild {
    /// Encoded view bytes (header + chunk payloads). Append to the file body.
    pub encoded: Vec<u8>,
    pub chunks: Vec<ChunkInfo>,
    pub source_map: Vec<SourceMapEntry>,
    pub sparse_offset_index: Vec<SparseIndexEntry>,
    pub total_tokens: u64,
    pub test_vector: Value,
    pub config_hash: Hash,
    pub vocab_size: u32,
    pub tokenizer_config: Value,
}

/// Build a v0.2 tokenization view: chunked, zstd-compressed, content-hashed
/// little-endian-u32 token streams + per-document source map + sparse offset
/// index.
pub fn build_view<T: Tokenizer + ?Sized>(
    tokenizer: &T,
    documents: &[(Hash, Vec<u8>)],
    chunk_size_tokens: usize,
    sparse_interval: usize,
) -> TsetResult<Vec<TokenizationViewBuild>> {
    // Returns Vec because we may decide to split into multiple views in the
    // future; today we always return exactly one element. Keeping the shape
    // open avoids an API churn later.
    Ok(vec![build_view_one(
        tokenizer,
        documents,
        chunk_size_tokens,
        sparse_interval,
    )?])
}

fn build_view_one<T: Tokenizer + ?Sized>(
    tokenizer: &T,
    documents: &[(Hash, Vec<u8>)],
    chunk_size_tokens: usize,
    sparse_interval: usize,
) -> TsetResult<TokenizationViewBuild> {
    let mut chunks: Vec<ChunkInfo> = Vec::new();
    let mut chunk_payloads: Vec<Vec<u8>> = Vec::new();
    let mut source_map: Vec<SourceMapEntry> = Vec::new();
    let mut sparse_index: Vec<SparseIndexEntry> = Vec::new();
    let mut next_sparse_at: u64 = 0;

    let mut pending: Vec<u32> = Vec::new();
    let mut total_tokens: u64 = 0;
    let mut cursor_in_view: u64 = VIEW_HEADER_SIZE as u64;

    let flush_chunk = |pending: &mut Vec<u32>,
                       chunks: &mut Vec<ChunkInfo>,
                       chunk_payloads: &mut Vec<Vec<u8>>,
                       cursor_in_view: &mut u64| {
        if pending.is_empty() {
            return;
        }
        // pending values were range-checked during tokenization on encode
        let mut raw = Vec::with_capacity(pending.len() * 4);
        for id in pending.iter() {
            raw.extend_from_slice(&id.to_le_bytes());
        }
        let compressed = zstd::stream::encode_all(&raw[..], ZSTD_LEVEL)
            .expect("zstd encode in-memory should not fail");
        let content_hash = hash_bytes(&compressed);

        let mut chunk_payload = Vec::with_capacity(CHUNK_HEADER_SIZE + compressed.len());
        chunk_payload.extend_from_slice(&(raw.len() as u64).to_le_bytes());
        chunk_payload.extend_from_slice(&(compressed.len() as u64).to_le_bytes());
        chunk_payload.extend_from_slice(&(pending.len() as u64).to_le_bytes());
        chunk_payload.extend_from_slice(&compressed);

        chunks.push(ChunkInfo {
            byte_offset_in_view: *cursor_in_view,
            compressed_size: compressed.len() as u64,
            num_tokens: pending.len() as u64,
            content_hash: Some(content_hash),
        });
        chunk_payloads.push(chunk_payload);
        *cursor_in_view += CHUNK_HEADER_SIZE as u64 + compressed.len() as u64;
        pending.clear();
    };

    for (doc_hash, content) in documents {
        let ids = tokenizer.encode(content);
        if ids.is_empty() {
            continue;
        }
        let v = tokenizer.vocab_size();
        for id in &ids {
            if *id >= v {
                return Err(TsetError::TokenIdOutOfRange(*id, v));
            }
        }
        source_map.push(SourceMapEntry {
            doc_hash: *doc_hash,
            token_offset: total_tokens,
            token_count: ids.len() as u64,
        });

        let mut cursor = 0usize;
        while cursor < ids.len() {
            let space = chunk_size_tokens - pending.len();
            let take = space.min(ids.len() - cursor);
            let in_chunk_offset = pending.len();
            pending.extend_from_slice(&ids[cursor..cursor + take]);
            let global_first = total_tokens + cursor as u64;
            while next_sparse_at < global_first + take as u64 {
                let rel = if next_sparse_at >= global_first {
                    (next_sparse_at - global_first) as u32
                } else {
                    0
                };
                sparse_index.push(SparseIndexEntry {
                    token_offset: next_sparse_at,
                    chunk_id: chunks.len() as u32,
                    in_chunk_offset: in_chunk_offset as u32 + rel,
                });
                next_sparse_at += sparse_interval as u64;
            }
            cursor += take;
            if pending.len() >= chunk_size_tokens {
                flush_chunk(
                    &mut pending,
                    &mut chunks,
                    &mut chunk_payloads,
                    &mut cursor_in_view,
                );
            }
        }
        total_tokens += ids.len() as u64;
    }

    flush_chunk(
        &mut pending,
        &mut chunks,
        &mut chunk_payloads,
        &mut cursor_in_view,
    );

    let mut body = Vec::new();
    for p in &chunk_payloads {
        body.extend_from_slice(p);
    }

    let doc_lookup: std::collections::BTreeMap<Hash, Vec<u8>> = documents.iter().cloned().collect();
    let test_vector =
        crate::tokenizers::reproducibility_test_vector(tokenizer, &doc_lookup, 4);

    let config_hash = tokenizer.config_hash();
    let mut view_header = Vec::with_capacity(VIEW_HEADER_SIZE);
    view_header.extend_from_slice(MAGIC_VIEW);
    view_header.extend_from_slice(&config_hash);
    view_header.extend_from_slice(&total_tokens.to_le_bytes());
    view_header.extend_from_slice(&(chunks.len() as u64).to_le_bytes());
    debug_assert_eq!(view_header.len(), VIEW_HEADER_SIZE);

    let mut encoded = Vec::with_capacity(view_header.len() + body.len());
    encoded.extend_from_slice(&view_header);
    encoded.extend_from_slice(&body);

    Ok(TokenizationViewBuild {
        encoded,
        chunks,
        source_map,
        sparse_offset_index: sparse_index,
        total_tokens,
        test_vector,
        config_hash,
        vocab_size: tokenizer.vocab_size(),
        tokenizer_config: tokenizer.config(),
    })
}
