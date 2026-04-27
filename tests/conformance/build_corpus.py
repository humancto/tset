#!/usr/bin/env python3
"""Generate the language-agnostic conformance corpus.

Writes three reference shards to tests/conformance/fixtures/ that any
conforming TSET reader must open + verify identically:

  fixture-empty.tset      — single empty document, byte-level view
  fixture-small.tset      — three docs with metadata, byte-level + whitespace views
  fixture-big.tset        — 100 docs with deterministic content, byte-level view

Plus a JSON sidecar (`expected.json`) per fixture listing the invariants
each reader must report (doc count, view total tokens, manifest hash hex,
shard merkle root hex). Readers across implementations open the shard and
assert their measured values match the sidecar.

Determinism: corpora use seed=0 for content. Timestamps in audit logs are
intentionally NOT in the sidecar — they're per-write nondeterministic and
not part of the conformance contract.

Usage:
    python tests/conformance/build_corpus.py

Idempotent: re-running rewrites the fixtures from scratch.
"""

from __future__ import annotations

import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "..", "python"))

from tset.reader import Reader  # noqa: E402
from tset.tokenizers import ByteLevelTokenizer, WhitespaceTokenizer  # noqa: E402
from tset.writer import Writer  # noqa: E402

FIXTURES = os.path.join(HERE, "fixtures")


def _make(name: str, path: str, build) -> dict:
    """Run `build(writer)` then snapshot the immutable invariants.

    Sets deterministic env vars (TSET_DETERMINISTIC_TIME, _CREATED_AT,
    _SNAPSHOT_ID) so the resulting shard is byte-stable across machines
    and Python runs — required for the committed fixtures to match a
    fresh `build_corpus.py` invocation. The shard_id is derived from
    the fixture name to keep that field deterministic too.
    """
    if os.path.exists(path):
        os.remove(path)
    os.environ["TSET_DETERMINISTIC_TIME"] = "1700000000.0"
    os.environ["TSET_DETERMINISTIC_CREATED_AT"] = "2023-11-14T22:13:20+00:00"
    os.environ["TSET_DETERMINISTIC_SNAPSHOT_ID"] = f"{name}-snap"
    shard_id = f"conformance-{name}-shard-id-padding".encode().hex()[:32]
    try:
        with Writer(path, shard_id=shard_id) as w:
            build(w)
    finally:
        for k in (
            "TSET_DETERMINISTIC_TIME",
            "TSET_DETERMINISTIC_CREATED_AT",
            "TSET_DETERMINISTIC_SNAPSHOT_ID",
        ):
            os.environ.pop(k, None)
    r = Reader(path)
    invariants = {
        "version_minor": r.header.version_minor,
        "shard_merkle_root": r.header.shard_merkle_root.hex(),
        "manifest_hash": r.header.manifest_hash.hex(),
        "manifest_size": r.header.manifest_size,
        # Count from the document store (source of truth for "what docs
        # exist in this shard"), not the per-view source_map (which omits
        # zero-token docs).
        "document_count": len(r.manifest["document_store"]["document_index"]),
        "tokenization_views": {
            tid: {
                "vocab_size": r.manifest["tokenization_views"][tid]["vocab_size"],
                "total_tokens": r.view_total_tokens(tid),
                "config_hash": r.manifest["tokenization_views"][tid]["config_hash"],
                "num_chunks": len(r.manifest["tokenization_views"][tid]["chunks"]),
            }
            for tid in r.tokenizer_ids()
        },
    }
    return invariants


def fixture_empty(w: Writer) -> None:
    w.add_document(b"")
    w.add_tokenizer_view(ByteLevelTokenizer())


def fixture_small(w: Writer) -> None:
    w.add_document(b"alpha document text", metadata={"lang": "en", "len": 19})
    w.add_document(b"beta", metadata={"lang": "fr", "len": 4})
    w.add_document(b"gamma payload here", metadata={"lang": "en", "len": 18})
    w.add_tokenizer_view(ByteLevelTokenizer())
    w.add_tokenizer_view(WhitespaceTokenizer(vocab_size=1024))


def fixture_big(w: Writer) -> None:
    # Deterministic content keyed by index — no PRNG to avoid platform drift
    for i in range(100):
        body = (f"document-{i:04d}-" + "x" * (i % 32 + 1)).encode("utf-8")
        w.add_document(body, metadata={"idx": i, "bucket": i % 4})
    w.add_tokenizer_view(ByteLevelTokenizer())


def main() -> None:
    os.makedirs(FIXTURES, exist_ok=True)
    cases = [
        ("fixture-empty", fixture_empty),
        ("fixture-small", fixture_small),
        ("fixture-big", fixture_big),
    ]
    for name, build in cases:
        shard = os.path.join(FIXTURES, f"{name}.tset")
        invariants = _make(name, shard, build)
        with open(os.path.join(FIXTURES, f"{name}.expected.json"), "w") as f:
            json.dump(invariants, f, indent=2, sort_keys=True)
            f.write("\n")
        print(f"  built {name}: {invariants['document_count']} docs, "
              f"manifest_size={invariants['manifest_size']}")


if __name__ == "__main__":
    main()
