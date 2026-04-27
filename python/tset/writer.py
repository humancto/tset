"""Single-shard TSET writer.

Usage::

    from tset import Writer, ByteLevelTokenizer

    with Writer("corpus.tset") as w:
        h1 = w.add_document(b"hello world", metadata={"lang": "en"})
        h2 = w.add_document(b"foo bar baz")
        w.add_tokenizer_view(ByteLevelTokenizer())

After `__exit__` the file is fully written: header, body, manifest, footer.
A second tokenizer view can be appended in-place via
`tset.append_tokenizer_view(path, tokenizer)`.
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone
from typing import Any


def _now_iso() -> str:
    """Wall-clock ISO timestamp, or `TSET_DETERMINISTIC_CREATED_AT` if set
    (used by conformance fixture builds to keep manifest_hash byte-stable)."""
    det = os.environ.get("TSET_DETERMINISTIC_CREATED_AT")
    if det is not None:
        return det
    return datetime.now(timezone.utc).isoformat()


def _next_snapshot_id() -> str:
    """Random 12-hex snapshot ID, or a deterministic counter if
    `TSET_DETERMINISTIC_SNAPSHOT_ID` is set. The deterministic mode lets
    conformance fixtures be reproducible across machines + Python runs."""
    det = os.environ.get("TSET_DETERMINISTIC_SNAPSHOT_ID")
    if det is not None:
        return det
    return uuid.uuid4().hex[:12]

from tset import manifest as M
from tset.audit_log import AuditLog
from tset.columns import MetadataColumns
from tset.constants import HEADER_SIZE, TRUNCATED_HASH_SIZE, VERSION_MAJOR, VERSION_MINOR
from tset.document_store import DocumentStoreWriter
from tset.footer import Footer
from tset.hashing import hash_bytes, shard_merkle_root
from tset.header import Header
from tset.mixture import Subset
from tset.smt import SparseMerkleTree
from tset.tokenizer_view import build_view
from tset.tokenizers import Tokenizer


class Writer:
    def __init__(
        self,
        path: str,
        block_target_bytes: int | None = None,
        shard_id: str | None = None,
    ):
        self.path = path
        self.shard_id = shard_id or uuid.uuid4().hex
        self._docs: DocumentStoreWriter = (
            DocumentStoreWriter(block_target_bytes)
            if block_target_bytes
            else DocumentStoreWriter()
        )
        self._doc_order: list[bytes] = []
        self._doc_contents: dict[bytes, bytes] = {}
        self._views: list[tuple[Tokenizer, dict | None]] = []
        self._columns = MetadataColumns()
        self._subsets: list[Subset] = []
        self._audit = AuditLog()
        self._smt = SparseMerkleTree()
        self._closed = False

    def __enter__(self) -> "Writer":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is None:
            self.close()

    def add_document(
        self,
        content: bytes | str,
        metadata: dict[str, Any] | None = None,
    ) -> bytes:
        if self._views:
            raise RuntimeError(
                "add_document() called after add_tokenizer_view(); add all"
                " documents before registering any tokenization view"
            )
        if isinstance(content, str):
            content = content.encode("utf-8")
        h = self._docs.add(content)
        if h in self._doc_contents:
            return h
        self._doc_order.append(h)
        self._doc_contents[h] = content
        self._smt.insert(h)
        self._audit.append("ingestion", {"doc_hash": h.hex(), "size": len(content)})
        self._columns.add_row(metadata or {})
        return h

    def add_tokenizer_view(self, tokenizer: Tokenizer) -> None:
        if any(t.tokenizer_id == tokenizer.tokenizer_id for t, _ in self._views):
            raise ValueError(
                f"tokenizer_id {tokenizer.tokenizer_id!r} already added to this shard"
            )
        self._views.append((tokenizer, None))

    def add_subset(self, name: str, predicate: str, default_weight: float) -> None:
        self._subsets.append(
            Subset(name=name, predicate=predicate, default_weight=default_weight)
        )

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True

        manifest = M.empty_manifest(self.shard_id)
        manifest["created_at"] = _now_iso()

        body = bytearray()

        body_offset = HEADER_SIZE
        encoded_blocks, blocks, doc_index = self._docs.finalize(body_offset)
        body += encoded_blocks

        manifest_blocks = [
            {
                "offset": b.offset,
                "compressed_size": b.compressed_size,
                "uncompressed_size": b.uncompressed_size,
                "num_documents": b.num_documents,
            }
            for b in blocks
        ]
        manifest_index = {
            h.hex(): {
                "block_idx": loc.block_idx,
                "in_block_offset": loc.in_block_offset,
                "content_size": loc.content_size,
            }
            for h, loc in doc_index.items()
        }
        M.manifest_set_documents(manifest, manifest_blocks, manifest_index)

        for tokenizer, _ in self._views:
            ordered_docs = [(h, self._doc_contents[h]) for h in self._doc_order]
            view = build_view(tokenizer, ordered_docs)
            view_offset = HEADER_SIZE + len(body)
            body += view.encoded
            view_size = len(view.encoded)
            entry = {
                "view_offset": view_offset,
                "view_size": view_size,
                "vocab_size": view.vocab_size,
                "tokenizer_config": view.tokenizer_config,
                "config_hash": view.config_hash.hex(),
                "total_tokens": view.total_tokens,
                "chunks": [
                    {
                        "byte_offset_in_view": c.byte_offset_in_view,
                        "compressed_size": c.compressed_size,
                        "num_tokens": c.num_tokens,
                        "content_hash": c.content_hash,
                    }
                    for c in view.chunks
                ],
                "source_map": [
                    {
                        "doc_hash": s.doc_hash.hex(),
                        "token_offset": s.token_offset,
                        "token_count": s.token_count,
                    }
                    for s in view.source_map
                ],
                "sparse_offset_index": [
                    {
                        "token_offset": e.token_offset,
                        "chunk_id": e.chunk_id,
                        "in_chunk_offset": e.in_chunk_offset,
                    }
                    for e in view.sparse_offset_index
                ],
                "test_vector": view.test_vector,
            }
            M.manifest_add_view(manifest, tokenizer.tokenizer_id, entry)
            self._audit.append(
                "tokenizer_added",
                {
                    "tokenizer_id": tokenizer.tokenizer_id,
                    "config_hash": view.config_hash.hex(),
                    "total_tokens": view.total_tokens,
                },
            )

        merkle = shard_merkle_root(self._doc_order)
        smt_root = self._smt.root()
        snapshot_id = _next_snapshot_id()
        self._audit.append(
            "version_snapshot",
            {
                "snapshot_id": snapshot_id,
                "shard_merkle_root": merkle.hex(),
                "smt_root": smt_root.hex(),
                "doc_count": len(self._doc_order),
            },
        )

        M.manifest_set_shard_merkle_root(manifest, merkle.hex())
        M.manifest_set_smt_root(manifest, smt_root.hex())
        M.manifest_set_audit_log(manifest, [e.to_dict() for e in self._audit.entries], self._audit.log_root)
        M.manifest_set_columns(manifest, self._columns.to_dict())
        M.manifest_set_subsets(manifest, [s.to_dict() for s in self._subsets])
        manifest["smt_present_keys"] = [k.hex() for k in self._smt.present_keys()]
        manifest["smt_version"] = "v0.1-fixed-256"

        manifest_bytes = M.encode_manifest(manifest)
        manifest_hash = hash_bytes(manifest_bytes)
        manifest_offset = HEADER_SIZE + len(body)

        header = Header(
            version_major=VERSION_MAJOR,
            version_minor=VERSION_MINOR,
            flags=0,
            manifest_offset=manifest_offset,
            manifest_size=len(manifest_bytes),
            shard_merkle_root=merkle,
            manifest_hash=manifest_hash,
        )
        footer = Footer(
            manifest_size=len(manifest_bytes),
            manifest_hash28=manifest_hash[:TRUNCATED_HASH_SIZE],
        )

        os.makedirs(os.path.dirname(os.path.abspath(self.path)) or ".", exist_ok=True)
        with open(self.path, "wb") as f:
            f.write(header.encode())
            f.write(bytes(body))
            f.write(manifest_bytes)
            f.write(footer.encode())
            f.flush()
            os.fsync(f.fileno())


def append_tokenizer_view(path: str, tokenizer: Tokenizer) -> None:
    """Append a new tokenization view to an existing TSET shard, in-place.

    Per SPEC §7 conformance: existing views and document blocks are not
    modified; only the manifest is rewritten and the header/footer updated.
    The old manifest bytes remain in the file (garbage) but are no longer
    pointed at; v0.2 will introduce a compaction tool.
    """
    from tset.reader import Reader

    with Reader(path) as r:
        manifest = dict(r.manifest)
        if tokenizer.tokenizer_id in manifest.get("tokenization_views", {}):
            raise ValueError(
                f"tokenizer_id {tokenizer.tokenizer_id!r} already present in shard"
            )
        ordered_docs = [(bytes.fromhex(h), r.get_document(bytes.fromhex(h))) for h in r.doc_order_hex()]

    view = build_view(tokenizer, ordered_docs)

    with open(path, "rb") as f:
        old_header_bytes = f.read(HEADER_SIZE)
        f.seek(0, os.SEEK_END)
        end = f.tell()
    old_header = Header.decode(old_header_bytes)

    truncate_to = old_header.manifest_offset
    with open(path, "r+b") as f:
        f.truncate(truncate_to)
        f.seek(truncate_to)
        view_offset = truncate_to
        f.write(view.encoded)

        manifest["tokenization_views"][tokenizer.tokenizer_id] = {
            "view_offset": view_offset,
            "view_size": len(view.encoded),
            "vocab_size": view.vocab_size,
            "tokenizer_config": view.tokenizer_config,
            "config_hash": view.config_hash.hex(),
            "total_tokens": view.total_tokens,
            "chunks": [
                {
                    "byte_offset_in_view": c.byte_offset_in_view,
                    "compressed_size": c.compressed_size,
                    "num_tokens": c.num_tokens,
                    "content_hash": c.content_hash,
                }
                for c in view.chunks
            ],
            "source_map": [
                {
                    "doc_hash": s.doc_hash.hex(),
                    "token_offset": s.token_offset,
                    "token_count": s.token_count,
                }
                for s in view.source_map
            ],
            "sparse_offset_index": [
                {
                    "token_offset": e.token_offset,
                    "chunk_id": e.chunk_id,
                    "in_chunk_offset": e.in_chunk_offset,
                }
                for e in view.sparse_offset_index
            ],
            "test_vector": view.test_vector,
        }

        log = AuditLog.from_dict(manifest.get("audit_log", {"entries": [], "log_root": ""}))
        log.append(
            "tokenizer_added",
            {
                "tokenizer_id": tokenizer.tokenizer_id,
                "config_hash": view.config_hash.hex(),
                "total_tokens": view.total_tokens,
            },
        )
        manifest["audit_log"] = log.to_dict()

        manifest_bytes = M.encode_manifest(manifest)
        manifest_hash = hash_bytes(manifest_bytes)
        manifest_offset = f.tell()
        f.write(manifest_bytes)
        footer = Footer(
            manifest_size=len(manifest_bytes),
            manifest_hash28=manifest_hash[:TRUNCATED_HASH_SIZE],
        )
        f.write(footer.encode())

        new_header = Header(
            version_major=old_header.version_major,
            version_minor=old_header.version_minor,
            flags=old_header.flags,
            manifest_offset=manifest_offset,
            manifest_size=len(manifest_bytes),
            shard_merkle_root=old_header.shard_merkle_root,
            manifest_hash=manifest_hash,
        )
        f.seek(0)
        f.write(new_header.encode())
        f.flush()
        os.fsync(f.fileno())
