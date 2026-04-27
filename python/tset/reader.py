"""Single-shard TSET reader."""

from __future__ import annotations

import mmap
from typing import Iterator

import numpy as np

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
from tset.tokenizers import Tokenizer, get_tokenizer, verify_reproducibility


class Reader:
    def __init__(self, path: str, verify: bool = True):
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
        if verify:
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
        for h in self._doc_order_hex():
            hb = bytes.fromhex(h)
            yield hb, self.get_document(hb)

    def _doc_order_hex(self) -> list[str]:
        # v0.1: order is determined by source_map of first view if present,
        # else by manifest insertion order which JSON dump preserves.
        views = M.manifest_views(self.manifest)
        if views:
            first = next(iter(views.values()))
            return [s["doc_hash"] for s in first["source_map"]]
        return list(M.manifest_get_doc_index(self.manifest).keys())

    def stream_tokens(
        self, tokenizer_id: str, batch_size: int = 1024
    ) -> Iterator[tuple[np.ndarray, bytes | None]]:
        """Stream `batch_size` token windows. Yields (tokens, doc_hash) where
        `doc_hash` is the source document hash if the entire batch lies inside
        one document; otherwise None."""
        view = self._open_view(tokenizer_id)
        chunks = view["chunks"]
        view_offset = view["view_offset"]
        source_map = view["source_map"]
        sm_idx = 0
        sm_consumed = 0
        global_offset = 0
        for chunk_meta in chunks:
            arr = read_chunk(self._mm, view_offset, ChunkInfo(**chunk_meta))
            cursor = 0
            while cursor < arr.size:
                take = min(batch_size, arr.size - cursor)
                batch = arr[cursor : cursor + take]
                doc_hash = self._dominant_doc(source_map, global_offset + cursor, take)
                yield batch, doc_hash
                cursor += take
            global_offset += int(arr.size)

    def _dominant_doc(
        self, source_map: list[dict], start: int, count: int
    ) -> bytes | None:
        end = start + count
        which: bytes | None = None
        for entry in source_map:
            es = entry["token_offset"]
            ec = entry["token_count"]
            ee = es + ec
            if ee <= start:
                continue
            if es >= end:
                break
            overlap_start = max(es, start)
            overlap_end = min(ee, end)
            if overlap_start < overlap_end:
                if which is None:
                    which = bytes.fromhex(entry["doc_hash"])
                else:
                    return None
        return which

    def _open_view(self, tokenizer_id: str) -> dict:
        views = M.manifest_views(self.manifest)
        if tokenizer_id not in views:
            raise KeyError(f"no such tokenization view: {tokenizer_id!r}")
        view = views[tokenizer_id]
        verify_view_header(self._mm, view["view_offset"], bytes.fromhex(view["config_hash"]))
        return view

    def verify_tokenizer_view(self, tokenizer_id: str, tokenizer: Tokenizer | None = None) -> None:
        view = self._open_view(tokenizer_id)
        if tokenizer is None:
            cfg = view["tokenizer_config"]
            ctor_kwargs = {k: v for k, v in cfg.items() if k != "id" and k != "kind"}
            try:
                tokenizer = get_tokenizer(tokenizer_id, **ctor_kwargs)
            except TypeError:
                tokenizer = get_tokenizer(tokenizer_id)
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
        return bytes.fromhex(self.manifest.get("smt_root", "")) if self.manifest.get("smt_root") else self.smt().root()

    def metadata_columns(self) -> MetadataColumns:
        return MetadataColumns.from_dict(self.manifest.get("metadata_columns", {}))

    def audit_log(self) -> AuditLog:
        return AuditLog.from_dict(self.manifest.get("audit_log", {"entries": [], "log_root": ""}))
