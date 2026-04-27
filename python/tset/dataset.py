"""Multi-shard dataset layout per RFC §5.8.

Layout on disk::

    my-dataset.tset/
      manifest.tset.json   <- dataset root manifest (JSON in v0.1)
      shards/
        part-00001.tset
        part-00002.tset
        ...
      exclusions.json      <- dataset-wide exclusion overlay

A single `.tset` file remains valid as a degenerate dataset of size 1.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterator

from tset.audit_log import AuditLog
from tset.hashing import hash_bytes, merkle_root
from tset.reader import Reader
from tset.smt import (
    EMPTY_ROOT,
    InclusionProof,
    NonInclusionProof,
    SparseMerkleTree,
)
from tset.writer import Writer


DATASET_MANIFEST_NAME = "manifest.tset.json"
EXCLUSIONS_NAME = "exclusions.json"
SHARDS_DIRNAME = "shards"


@dataclass
class ShardEntry:
    shard_id: str
    relpath: str
    shard_hash: str
    shard_smt_root: str
    doc_count: int
    total_tokens_per_view: dict[str, int]


def _shard_hash_for_dataset(shard_path: str) -> bytes:
    """Domain-separated hash of (manifest_hash || shard_merkle_root) for the
    dataset-level Merkle commitment. We deliberately don't hash the entire
    shard bytes — that would defeat single-shard updates, and the manifest
    hash already binds the entire shard contents."""
    with Reader(shard_path) as r:
        return hash_bytes(
            b"\x20" + r.header.manifest_hash + r.header.shard_merkle_root
        )


def _dataset_merkle_root(entries: list[ShardEntry]) -> bytes:
    leaves = [
        hash_bytes(
            b"\x21"
            + e.shard_id.encode("utf-8")
            + bytes.fromhex(e.shard_hash)
            + bytes.fromhex(e.shard_smt_root)
        )
        for e in sorted(entries, key=lambda x: x.shard_id)
    ]
    return merkle_root(leaves) if leaves else EMPTY_ROOT


class Dataset:
    """Read-only view over a multi-shard dataset directory (or a single-shard
    `.tset` file)."""

    def __init__(self, path: str):
        self.path = path
        self._single_file = path.endswith(".tset") and os.path.isfile(path)
        if self._single_file:
            self.manifest_path = None
            self._shard_paths = [path]
            self._exclusions: set[str] = set()
            self._dataset_manifest: dict | None = None
        else:
            self.manifest_path = os.path.join(path, DATASET_MANIFEST_NAME)
            with open(self.manifest_path, "r", encoding="utf-8") as f:
                self._dataset_manifest = json.load(f)
            self._shard_paths = [
                os.path.join(path, e["relpath"])
                for e in self._dataset_manifest["shards"]
            ]
            excl_path = os.path.join(path, EXCLUSIONS_NAME)
            if os.path.exists(excl_path):
                with open(excl_path, "r", encoding="utf-8") as f:
                    self._exclusions = set(json.load(f).get("excluded_doc_hashes", []))
            else:
                self._exclusions = set()

    def shard_paths(self) -> list[str]:
        return list(self._shard_paths)

    def shards(self) -> Iterator[Reader]:
        for p in self._shard_paths:
            yield Reader(p)

    def exclusions(self) -> set[str]:
        return set(self._exclusions)

    def is_excluded(self, doc_hash: bytes | str) -> bool:
        h = doc_hash.hex() if isinstance(doc_hash, bytes) else doc_hash
        return h in self._exclusions

    def stream_tokens(self, tokenizer_id: str, batch_size: int = 1024):
        for p in self._shard_paths:
            with Reader(p) as r:
                if tokenizer_id not in r.tokenizer_ids():
                    continue
                for batch, doc_hash in r.stream_tokens(tokenizer_id, batch_size):
                    if doc_hash.hex() in self._exclusions:
                        continue
                    yield batch, doc_hash

    def dataset_merkle_root(self) -> bytes:
        if self._single_file:
            with Reader(self.path) as r:
                return r.header.shard_merkle_root
        entries = [ShardEntry(**e) for e in self._dataset_manifest["shards"]]
        return _dataset_merkle_root(entries)

    def smt_root_per_shard(self) -> dict[str, bytes]:
        out: dict[str, bytes] = {}
        for p in self._shard_paths:
            with Reader(p) as r:
                out[p] = r.smt_root()
        return out

    def prove_inclusion(self, doc_hash: bytes) -> tuple[str, InclusionProof]:
        for p in self._shard_paths:
            with Reader(p) as r:
                if r.has_document(doc_hash):
                    if doc_hash.hex() in self._exclusions:
                        raise ValueError(
                            f"document {doc_hash.hex()} is dataset-level excluded"
                        )
                    return p, r.prove_inclusion(doc_hash)
        raise KeyError(f"document {doc_hash.hex()} not present in any shard")

    def prove_non_inclusion(self, doc_hash: bytes) -> dict:
        """Compose a dataset-level non-inclusion proof.

        For each shard, attach either:
        - a `NonInclusionProof` against the shard's SMT root (doc absent), or
        - an `InclusionProof` against the shard's SMT root (doc present, but
          dataset-level overlay must exclude it).

        Verification then binds *every* shard claim to its SMT root; the
        `present_but_excluded` flag never short-circuits without a proof.
        """
        excluded = doc_hash.hex() in self._exclusions
        per_shard = []
        for p in self._shard_paths:
            with Reader(p) as r:
                smt_root = r.smt_root().hex()
                if r.has_document(doc_hash):
                    if not excluded:
                        raise ValueError(
                            f"document {doc_hash.hex()} present in shard {p} and not excluded"
                        )
                    ip = r.prove_inclusion(doc_hash)
                    per_shard.append(
                        {
                            "shard": p,
                            "smt_root": smt_root,
                            "claim": "present_but_excluded",
                            "inclusion_proof": {"siblings": [s.hex() for s in ip.siblings]},
                        }
                    )
                else:
                    nip = r.prove_non_inclusion(doc_hash)
                    per_shard.append(
                        {
                            "shard": p,
                            "smt_root": smt_root,
                            "claim": "absent",
                            "non_inclusion_proof": {
                                "siblings": [s.hex() for s in nip.siblings]
                            },
                        }
                    )
        return {
            "doc_hash": doc_hash.hex(),
            "dataset_merkle_root": self.dataset_merkle_root().hex(),
            "shards": per_shard,
            "exclusion_overlay_includes": excluded,
        }

    def verify_non_inclusion_proof(self, proof: dict) -> bool:
        doc_hash = bytes.fromhex(proof["doc_hash"])
        for s in proof["shards"]:
            smt_root = bytes.fromhex(s["smt_root"])
            if s["claim"] == "absent":
                siblings = [bytes.fromhex(x) for x in s["non_inclusion_proof"]["siblings"]]
                if not NonInclusionProof(key=doc_hash, siblings=siblings).verify(smt_root):
                    return False
            elif s["claim"] == "present_but_excluded":
                if not proof.get("exclusion_overlay_includes", False):
                    return False
                siblings = [bytes.fromhex(x) for x in s["inclusion_proof"]["siblings"]]
                if not InclusionProof(key=doc_hash, siblings=siblings).verify(smt_root):
                    return False
            else:
                return False
        return True


class DatasetWriter:
    """Builder for a multi-shard dataset directory.

    The dataset writer creates the directory layout and a root manifest
    binding the shards. Individual shards are written via the standard
    `Writer`. Exclusions are added via `add_exclusion` and persisted to the
    `exclusions.json` overlay; the dataset manifest is rewritten on `close`.
    """

    def __init__(self, root: str, load_existing: bool = True):
        """If `load_existing` and `root/manifest.tset.json` exists, reload
        prior shard registrations + exclusions so the writer can extend
        an existing dataset instead of starting from scratch. Set False
        to ignore prior state (the previous behavior)."""
        self.root = root
        os.makedirs(os.path.join(root, SHARDS_DIRNAME), exist_ok=True)
        self._shards: list[ShardEntry] = []
        self._exclusions: set[str] = set()
        self._audit = AuditLog()
        self._closed = False
        if load_existing:
            self._load_existing_state()

    def _load_existing_state(self) -> None:
        manifest_path = os.path.join(self.root, DATASET_MANIFEST_NAME)
        excl_path = os.path.join(self.root, EXCLUSIONS_NAME)
        if os.path.exists(manifest_path):
            with open(manifest_path, "r", encoding="utf-8") as f:
                manifest = json.load(f)
            for s in manifest.get("shards", []):
                self._shards.append(ShardEntry(**s))
            audit = manifest.get("audit_log")
            if audit:
                self._audit = AuditLog.from_dict(audit)
        if os.path.exists(excl_path):
            with open(excl_path, "r", encoding="utf-8") as f:
                excl = json.load(f)
            self._exclusions.update(excl.get("excluded_doc_hashes", []))

    def shard_writer(self, name: str) -> Writer:
        relpath = os.path.join(SHARDS_DIRNAME, f"{name}.tset")
        return Writer(os.path.join(self.root, relpath))

    def register_shard(self, name: str) -> ShardEntry:
        relpath = os.path.join(SHARDS_DIRNAME, f"{name}.tset")
        # Idempotent: skip if already registered AND the on-disk shard
        # still matches what we recorded (same shard_id + manifest hash).
        # Detects accidental duplicate-register and silently allows
        # rebuild-then-register flows.
        for existing in self._shards:
            if existing.relpath == relpath:
                return existing
        shard_path = os.path.join(self.root, relpath)
        with Reader(shard_path) as r:
            shard_id = r.manifest["shard_id"]
            shard_smt_root = r.smt_root()
            doc_count = len(r._index)
            views = r.tokenizer_ids()
            totals = {v: r.view_total_tokens(v) for v in views}
        shard_hash = _shard_hash_for_dataset(shard_path)
        entry = ShardEntry(
            shard_id=shard_id,
            relpath=relpath,
            shard_hash=shard_hash.hex(),
            shard_smt_root=shard_smt_root.hex(),
            doc_count=doc_count,
            total_tokens_per_view=totals,
        )
        self._shards.append(entry)
        self._audit.append(
            "ingestion",
            {"shard_id": shard_id, "doc_count": doc_count, "shard_hash": shard_hash.hex()},
        )
        return entry

    def add_exclusion(self, doc_hash: bytes | str, reason: str = "") -> None:
        h = doc_hash.hex() if isinstance(doc_hash, bytes) else doc_hash
        if h in self._exclusions:
            return
        self._exclusions.add(h)
        self._audit.append("exclusion", {"doc_hash": h, "reason": reason})

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        snapshot_id = datetime.now(timezone.utc).strftime("snapshot-%Y%m%d-%H%M%S")
        ds_root = _dataset_merkle_root(self._shards)
        self._audit.append(
            "version_snapshot",
            {
                "snapshot_id": snapshot_id,
                "dataset_merkle_root": ds_root.hex(),
                "shard_count": len(self._shards),
            },
        )
        manifest = {
            "version": "0.1.0",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "shards": [s.__dict__ for s in self._shards],
            "dataset_merkle_root": ds_root.hex(),
            "audit_log": self._audit.to_dict(),
            "snapshot_id": snapshot_id,
        }
        with open(
            os.path.join(self.root, DATASET_MANIFEST_NAME), "w", encoding="utf-8"
        ) as f:
            json.dump(manifest, f, indent=2, sort_keys=True)
        with open(
            os.path.join(self.root, EXCLUSIONS_NAME), "w", encoding="utf-8"
        ) as f:
            json.dump(
                {
                    "snapshot_id": snapshot_id,
                    "excluded_doc_hashes": sorted(self._exclusions),
                },
                f,
                indent=2,
                sort_keys=True,
            )

    def __enter__(self) -> "DatasetWriter":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is None:
            self.close()
