import struct
from dataclasses import dataclass

import numpy as np
import zstandard as zstd

from tset.constants import (
    DEFAULT_SPARSE_INDEX_INTERVAL,
    DEFAULT_TOKEN_CHUNK_SIZE,
    MAGIC_VIEW,
    ZSTD_LEVEL,
)
from tset.hashing import hash_bytes
from tset.tokenizers import Tokenizer, reproducibility_test_vector


VIEW_HEADER_SIZE = 52
CHUNK_HEADER_SIZE = 24
TOKEN_DTYPE = np.uint32
TOKEN_BYTES = 4


@dataclass
class ChunkInfo:
    byte_offset_in_view: int
    compressed_size: int
    num_tokens: int


@dataclass
class SourceMapEntry:
    doc_hash: bytes
    token_offset: int
    token_count: int


@dataclass
class SparseIndexEntry:
    token_offset: int
    chunk_id: int
    in_chunk_offset: int


@dataclass
class TokenizationViewBuild:
    encoded: bytes
    chunks: list[ChunkInfo]
    source_map: list[SourceMapEntry]
    sparse_offset_index: list[SparseIndexEntry]
    total_tokens: int
    test_vector: dict
    config_hash: bytes
    vocab_size: int
    tokenizer_config: dict


def build_view(
    tokenizer: Tokenizer,
    documents: list[tuple[bytes, bytes]],
    chunk_size_tokens: int = DEFAULT_TOKEN_CHUNK_SIZE,
    sparse_interval: int = DEFAULT_SPARSE_INDEX_INTERVAL,
) -> TokenizationViewBuild:
    """Tokenize an ordered sequence of `(doc_hash, content)` pairs into a
    chunked binary view. Returns the encoded bytes (header + chunks) plus the
    metadata that goes into the manifest."""
    compressor = zstd.ZstdCompressor(level=ZSTD_LEVEL)
    chunks: list[ChunkInfo] = []
    chunk_payloads: list[bytes] = []
    source_map: list[SourceMapEntry] = []
    sparse_index: list[SparseIndexEntry] = []
    next_sparse_at = 0

    pending = np.empty(0, dtype=TOKEN_DTYPE)
    pending_chunk_doc_offsets: list[tuple[int, int, int]] = []
    total_tokens = 0
    cursor_in_view = VIEW_HEADER_SIZE

    def flush_chunk():
        nonlocal pending, pending_chunk_doc_offsets, cursor_in_view
        if pending.size == 0:
            return
        if (pending >= tokenizer.vocab_size).any():
            raise ValueError("tokenizer emitted ID >= vocab_size")
        raw = pending.astype(TOKEN_DTYPE).tobytes()
        compressed = compressor.compress(raw)
        chunk_id = len(chunks)
        chunk_payload = (
            struct.pack(
                "<QQQ",
                len(raw),
                len(compressed),
                int(pending.size),
            )
            + compressed
        )
        chunks.append(
            ChunkInfo(
                byte_offset_in_view=cursor_in_view,
                compressed_size=len(compressed),
                num_tokens=int(pending.size),
            )
        )
        chunk_payloads.append(chunk_payload)
        cursor_in_view += CHUNK_HEADER_SIZE + len(compressed)
        pending = np.empty(0, dtype=TOKEN_DTYPE)
        pending_chunk_doc_offsets = []

    for doc_hash, content in documents:
        ids = tokenizer.encode(content).astype(TOKEN_DTYPE, copy=False)
        if ids.size == 0:
            continue
        source_map.append(
            SourceMapEntry(
                doc_hash=doc_hash,
                token_offset=total_tokens,
                token_count=int(ids.size),
            )
        )
        cursor = 0
        while cursor < ids.size:
            space = chunk_size_tokens - pending.size
            take = min(space, ids.size - cursor)
            in_chunk_offset = pending.size
            pending = np.concatenate([pending, ids[cursor : cursor + take]])
            global_first = total_tokens + cursor
            while next_sparse_at <= global_first + take - 1:
                rel = next_sparse_at - global_first
                if rel < 0:
                    rel = 0
                sparse_index.append(
                    SparseIndexEntry(
                        token_offset=next_sparse_at,
                        chunk_id=len(chunks),
                        in_chunk_offset=in_chunk_offset + rel,
                    )
                )
                next_sparse_at += sparse_interval
            cursor += take
            if pending.size >= chunk_size_tokens:
                flush_chunk()
        total_tokens += int(ids.size)

    flush_chunk()

    body = b"".join(chunk_payloads)
    doc_lookup = {h: c for h, c in documents}
    test_vector = reproducibility_test_vector(tokenizer, doc_lookup)

    # Domain-separated config_hash to avoid collision with manifest content
    config_hash = tokenizer.config_hash()
    view_header = (
        MAGIC_VIEW
        + config_hash
        + struct.pack("<Q", total_tokens)
        + struct.pack("<Q", len(chunks))
    )
    encoded = view_header + body

    return TokenizationViewBuild(
        encoded=encoded,
        chunks=chunks,
        source_map=source_map,
        sparse_offset_index=sparse_index,
        total_tokens=total_tokens,
        test_vector=test_vector,
        config_hash=config_hash,
        vocab_size=tokenizer.vocab_size,
        tokenizer_config=tokenizer.config(),
    )


def read_chunk(mm, view_offset: int, chunk: ChunkInfo) -> np.ndarray:
    abs_offset = view_offset + chunk.byte_offset_in_view
    header = bytes(mm[abs_offset : abs_offset + CHUNK_HEADER_SIZE])
    uncompressed_size, compressed_size, num_tokens = struct.unpack("<QQQ", header)
    if compressed_size != chunk.compressed_size:
        raise ValueError("chunk compressed_size mismatch with manifest")
    if num_tokens != chunk.num_tokens:
        raise ValueError("chunk num_tokens mismatch with manifest")
    payload = bytes(
        mm[
            abs_offset + CHUNK_HEADER_SIZE : abs_offset + CHUNK_HEADER_SIZE + compressed_size
        ]
    )
    raw = zstd.ZstdDecompressor().decompress(payload, max_output_size=uncompressed_size)
    if len(raw) != uncompressed_size:
        raise ValueError("chunk decompressed size mismatch")
    return np.frombuffer(raw, dtype=TOKEN_DTYPE)


def verify_view_header(mm, view_offset: int, expected_config_hash: bytes) -> None:
    magic = bytes(mm[view_offset : view_offset + 4])
    if magic != MAGIC_VIEW:
        raise ValueError(f"bad view magic at offset {view_offset}: {magic!r}")
    config_hash = bytes(mm[view_offset + 4 : view_offset + 4 + 32])
    if config_hash != expected_config_hash:
        raise ValueError("view config_hash on disk disagrees with manifest")
