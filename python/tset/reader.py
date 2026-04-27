"""Single-shard TSET reader."""

from __future__ import annotations

import importlib.util
import mmap
import os
from functools import lru_cache
from typing import Iterator

import numpy as np


@lru_cache(maxsize=1)
def _rust_available() -> bool:
    return importlib.util.find_spec("tset_rs") is not None

from tset import manifest as M
from tset.audit_log import AuditLog
from tset.columns import MetadataColumns
from tset.constants import (
    FOOTER_SIZE,
    HEADER_SIZE,
    TRUNCATED_HASH_SIZE,
    HASH_SIZE,
)
from tset.document_store import BlockInfo, DocumentLocator, DocumentStoreReader
from tset.footer import Footer
from tset.hashing import hash_bytes, shard_merkle_root
from tset.header import Header
from tset.smt import SparseMerkleTree, InclusionProof, NonInclusionProof
from tset.tokenizer_view import (
    ChunkInfo,
    SourceMapEntry,
    SparseIndexEntry,
    read_chunk,
    verify_view_header,
)
from tset.tokenizers import Tokenizer, get_tokenizer_class, verify_reproducibility


class Reader:
    def __init__(self, path: str):
        self.path = path
        self._file = open(path, "rb")
        self._mm = mmap.mmap(self._file.fileno(), 0, access=mmap.ACCESS_READ)
        self.header = Header.decode(self._mm[:HEADER_SIZE])
        footer_bytes = self._mm[len(self._mm) - FOOTER_SIZE : len(self._mm)]
        self.footer = Footer.decode(bytes(footer_bytes))
        manifest_bytes = bytes(
            self._mm[
                self.header.manifest_offset : self.header.manifest_offset
                + self.header.manifest_size
            ]
        )
        if hash_bytes(manifest_bytes) != self.header.manifest_hash:
            raise ValueError("manifest hash mismatch (header)")
        if (
            self.header.manifest_hash[:TRUNCATED_HASH_SIZE]
            != self.footer.manifest_hash28
        ):
            raise ValueError("manifest hash mismatch (footer)")
        if self.footer.manifest_size != self.header.manifest_size:
            raise ValueError("manifest size mismatch between header and footer")
        self.manifest = M.decode_manifest(manifest_bytes)

        self._blocks = [
            BlockInfo(
                offset=b["offset"],
                compressed_size=b["compressed_size"],
                uncompressed_size=b["uncompressed_size"],
                num_documents=b["num_documents"],
            )
            for b in M.manifest_get_block_infos(self.manifest)
        ]
        self._index = {
            bytes.fromhex(h): DocumentLocator(
                block_idx=v["block_idx"],
                in_block_offset=v["in_block_offset"],
                content_size=v["content_size"],
            )
            for h, v in M.manifest_get_doc_index(self.manifest).items()
        }
        self._docs = DocumentStoreReader(self._mm, self._blocks, self._index)
        self._verify_invariants()

    def _verify_invariants(self) -> None:
        merkle = shard_merkle_root(list(self._index.keys()))
        manifest_root_hex = self.manifest.get("shard_merkle_root", "")
        if manifest_root_hex and bytes.fromhex(manifest_root_hex) != merkle:
            raise ValueError("shard_merkle_root in manifest does not match docs")
        if merkle != self.header.shard_merkle_root:
            raise ValueError("shard_merkle_root in header does not match docs")
        if "audit_log" in self.manifest:
            log = AuditLog.from_dict(self.manifest["audit_log"])
            if not log.verify():
                raise ValueError("audit log integrity check failed")
        for tid in self.tokenizer_ids():
            self.verify_tokenizer_view(tid)

    def close(self) -> None:
        try:
            self._mm.close()
        finally:
            self._file.close()

    def __enter__(self) -> "Reader":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def tokenizer_ids(self) -> list[str]:
        return list(M.manifest_views(self.manifest).keys())

    def has_document(self, doc_hash: bytes) -> bool:
        return doc_hash in self._index

    def get_document(self, doc_hash: bytes) -> bytes:
        return self._docs.get(doc_hash)

    def documents(self) -> Iterator[tuple[bytes, bytes]]:
        for h in self.doc_order_hex():
            hb = bytes.fromhex(h)
            yield hb, self.get_document(hb)

    def doc_order_hex(self) -> list[str]:
        # v0.1: order is determined by source_map of first view if present,
        # else by manifest insertion order which JSON dump preserves.
        views = M.manifest_views(self.manifest)
        if views:
            first = next(iter(views.values()))
            return [s["doc_hash"] for s in first["source_map"]]
        return list(M.manifest_get_doc_index(self.manifest).keys())

    def stream_tokens(
        self, tokenizer_id: str, batch_size: int = 1024
    ) -> Iterator[tuple[np.ndarray, bytes]]:
        """Stream tokens grouped per source document.

        Yields ``(tokens, doc_hash)`` where ``tokens`` is at most
        ``batch_size`` long and entirely belongs to ``doc_hash``. Iterating
        per-document lets dataset-level exclusion overlays drop tokens
        without leaking partial document content across batch boundaries.

        If the optional `tset_rs` PyO3 binding is installed and
        `TSET_PREFER_RUST` is not set to "0", the hot path delegates to
        the Rust reader for streaming. Set `TSET_PREFER_RUST=0` to force
        the pure-Python path (useful for differential testing).
        """
        if _rust_available() and os.environ.get("TSET_PREFER_RUST", "1") != "0":
            yield from self._stream_tokens_rust(tokenizer_id, batch_size)
            return
        yield from self._stream_tokens_py(tokenizer_id, batch_size)

    def _stream_tokens_rust(
        self, tokenizer_id: str, batch_size: int
    ) -> Iterator[tuple[np.ndarray, bytes]]:
        import tset_rs  # type: ignore[import-not-found]

        rs = tset_rs.Reader(self.path)
        for tokens_bytes, doc_hash in rs.stream_tokens(tokenizer_id):
            arr = np.frombuffer(tokens_bytes, dtype=np.uint32)
            for i in range(0, int(arr.size), batch_size):
                yield arr[i : i + batch_size], bytes(doc_hash)

    def _stream_tokens_py(
        self, tokenizer_id: str, batch_size: int = 1024
    ) -> Iterator[tuple[np.ndarray, bytes]]:
        view = self._open_view(tokenizer_id)
        view_offset = view["view_offset"]
        chunks = view["chunks"]
        source_map = view["source_map"]
        vocab_size = view["vocab_size"]
        chunk_arrays: dict[int, np.ndarray] = {}

        def chunk_arr(idx: int) -> np.ndarray:
            cached = chunk_arrays.get(idx)
            if cached is not None:
                return cached
            arr = read_chunk(
                self._mm, view_offset, ChunkInfo(**chunks[idx]), vocab_size=vocab_size
            )
            chunk_arrays[idx] = arr
            return arr

        chunk_starts: list[int] = []
        cum = 0
        for c in chunks:
            chunk_starts.append(cum)
            cum += c["num_tokens"]

        def read_range(token_offset: int, count: int) -> np.ndarray:
            if count <= 0:
                return np.empty(0, dtype=np.uint32)
            pieces: list[np.ndarray] = []
            cur = token_offset
            remaining = count
            cid = 0
            while cid + 1 < len(chunk_starts) and chunk_starts[cid + 1] <= cur:
                cid += 1
            while remaining > 0:
                arr = chunk_arr(cid)
                offset = cur - chunk_starts[cid]
                take = min(arr.size - offset, remaining)
                pieces.append(arr[offset : offset + take])
                cur += take
                remaining -= take
                cid += 1
            return pieces[0] if len(pieces) == 1 else np.concatenate(pieces)

        for entry in source_map:
            doc_hash = bytes.fromhex(entry["doc_hash"])
            tokens = read_range(entry["token_offset"], entry["token_count"])
            if tokens.size == 0:
                continue
            for i in range(0, int(tokens.size), batch_size):
                yield tokens[i : i + batch_size], doc_hash

    def _open_view(self, tokenizer_id: str) -> dict:
        views = M.manifest_views(self.manifest)
        if tokenizer_id not in views:
            raise KeyError(f"no such tokenization view: {tokenizer_id!r}")
        view = views[tokenizer_id]
        verify_view_header(
            self._mm,
            view["view_offset"],
            bytes.fromhex(view["config_hash"]),
            expected_total_tokens=view["total_tokens"],
            expected_num_chunks=len(view["chunks"]),
        )
        return view

    def verify_tokenizer_view(self, tokenizer_id: str, tokenizer: Tokenizer | None = None) -> None:
        view = self._open_view(tokenizer_id)
        if tokenizer is None:
            cls = get_tokenizer_class(tokenizer_id)
            tokenizer = cls.from_config(view["tokenizer_config"])
        if tokenizer.config_hash() != bytes.fromhex(view["config_hash"]):
            raise ValueError(
                f"tokenizer config hash mismatch for {tokenizer_id!r}"
            )
        docs = {
            bytes.fromhex(h): self.get_document(bytes.fromhex(h))
            for h in view["test_vector"]["doc_hashes"]
        }
        verify_reproducibility(tokenizer, view["test_vector"], docs)

    def view_total_tokens(self, tokenizer_id: str) -> int:
        return self._open_view(tokenizer_id)["total_tokens"]

    def smt(self) -> SparseMerkleTree:
        """Reconstruct the SMT from the present-keys list in the manifest."""
        tree = SparseMerkleTree()
        for hex_h in self.manifest.get("smt_present_keys", []):
            tree.insert(bytes.fromhex(hex_h))
        return tree

    def prove_inclusion(self, doc_hash: bytes) -> InclusionProof:
        proof = self.smt().prove(doc_hash)
        if not isinstance(proof, InclusionProof):
            raise ValueError(f"document {doc_hash.hex()} is not in this shard")
        return proof

    def prove_non_inclusion(self, doc_hash: bytes) -> NonInclusionProof:
        proof = self.smt().prove(doc_hash)
        if not isinstance(proof, NonInclusionProof):
            raise ValueError(f"document {doc_hash.hex()} IS in this shard")
        return proof

    def smt_root(self) -> bytes:
        root_hex = self.manifest.get("smt_root", "")
        if root_hex:
            return bytes.fromhex(root_hex)
        return self.smt().root()

    def metadata_columns(self) -> MetadataColumns:
        return MetadataColumns.from_dict(self.manifest.get("metadata_columns", {}))

    def audit_log(self) -> AuditLog:
        return AuditLog.from_dict(self.manifest.get("audit_log", {"entries": [], "log_root": ""}))
