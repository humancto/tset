import struct
from dataclasses import dataclass

import zstandard as zstd

from tset.constants import (
    DEFAULT_BLOCK_TARGET_BYTES,
    HASH_SIZE,
    MAGIC_DOC_BLOCK,
    ZSTD_LEVEL,
)
from tset.hashing import hash_bytes


@dataclass
class DocumentLocator:
    block_idx: int
    in_block_offset: int
    content_size: int


@dataclass
class BlockInfo:
    offset: int
    compressed_size: int
    uncompressed_size: int
    num_documents: int


class DocumentStoreWriter:
    """Buffers documents into compressed blocks. Identical content
    (same BLAKE3) is stored once."""

    def __init__(self, block_target_bytes: int = DEFAULT_BLOCK_TARGET_BYTES):
        self.block_target_bytes = block_target_bytes
        self._compressor = zstd.ZstdCompressor(level=ZSTD_LEVEL)
        self._buffer = bytearray()
        self._buffer_doc_count = 0
        self._pending_locators: dict[bytes, tuple[int, int]] = {}
        self._index: dict[bytes, DocumentLocator] = {}
        self._blocks: list[BlockInfo] = []
        self._encoded_blocks: list[bytes] = []

    def add(self, content: bytes) -> bytes:
        h = hash_bytes(content)
        if h in self._index or h in self._pending_locators:
            return h
        in_block_offset = len(self._buffer)
        self._buffer += h
        self._buffer += struct.pack("<Q", len(content))
        self._buffer += content
        self._pending_locators[h] = (in_block_offset, len(content))
        self._buffer_doc_count += 1
        if len(self._buffer) >= self.block_target_bytes:
            self._flush_block()
        return h

    def _flush_block(self) -> None:
        if not self._buffer:
            return
        uncompressed_size = len(self._buffer)
        compressed = self._compressor.compress(bytes(self._buffer))
        block_idx = len(self._blocks)
        for h, (off, size) in self._pending_locators.items():
            self._index[h] = DocumentLocator(
                block_idx=block_idx,
                in_block_offset=off,
                content_size=size,
            )
        encoded = (
            MAGIC_DOC_BLOCK
            + struct.pack(
                "<IQQ",
                self._buffer_doc_count,
                uncompressed_size,
                len(compressed),
            )
            + compressed
        )
        self._encoded_blocks.append(encoded)
        self._blocks.append(
            BlockInfo(
                offset=0,
                compressed_size=len(compressed),
                uncompressed_size=uncompressed_size,
                num_documents=self._buffer_doc_count,
            )
        )
        self._buffer = bytearray()
        self._buffer_doc_count = 0
        self._pending_locators = {}

    def finalize(self, body_offset: int) -> tuple[bytes, list[BlockInfo], dict[bytes, DocumentLocator]]:
        self._flush_block()
        cursor = body_offset
        for block in self._blocks:
            block.offset = cursor
            cursor += 24 + block.compressed_size
        encoded = b"".join(self._encoded_blocks)
        return encoded, self._blocks, self._index


def _decompress(payload: bytes, expected_uncompressed: int) -> bytes:
    decompressor = zstd.ZstdDecompressor()
    out = decompressor.decompress(payload, max_output_size=expected_uncompressed)
    if len(out) != expected_uncompressed:
        raise ValueError(
            f"decompressed size {len(out)} != expected {expected_uncompressed}"
        )
    return out


class DocumentStoreReader:
    """Reads documents from a memory-mapped TSET file given block + index info
    from the manifest. Decompresses each touched block at most once via cache."""

    def __init__(self, mm, blocks: list[BlockInfo], index: dict[bytes, DocumentLocator]):
        self._mm = mm
        self._blocks = blocks
        self._index = index
        self._block_cache: dict[int, bytes] = {}

    def has(self, doc_hash: bytes) -> bool:
        return doc_hash in self._index

    def get(self, doc_hash: bytes) -> bytes:
        loc = self._index[doc_hash]
        block_bytes = self._read_block(loc.block_idx)
        start = loc.in_block_offset
        stored_hash = block_bytes[start : start + HASH_SIZE]
        if stored_hash != doc_hash:
            raise ValueError("document hash mismatch on read")
        size = struct.unpack_from("<Q", block_bytes, start + HASH_SIZE)[0]
        if size != loc.content_size:
            raise ValueError("document content_size mismatch")
        content_start = start + HASH_SIZE + 8
        content = bytes(block_bytes[content_start : content_start + size])
        if hash_bytes(content) != doc_hash:
            raise ValueError("document content hash check failed")
        return content

    def _read_block(self, block_idx: int) -> bytes:
        cached = self._block_cache.get(block_idx)
        if cached is not None:
            return cached
        block = self._blocks[block_idx]
        magic = self._mm[block.offset : block.offset + 4]
        if bytes(magic) != MAGIC_DOC_BLOCK:
            raise ValueError(
                f"bad block magic at offset {block.offset}: {bytes(magic)!r}"
            )
        header_end = block.offset + 24
        payload = bytes(self._mm[header_end : header_end + block.compressed_size])
        decompressed = _decompress(payload, block.uncompressed_size)
        self._block_cache[block_idx] = decompressed
        return decompressed
