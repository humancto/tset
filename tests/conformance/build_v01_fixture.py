#!/usr/bin/env python3
"""Build a v0.1 conformance fixture.

Runs the writer with `version_minor` forced to 1 (no chunk content_hash)
so we have a frozen reference for the "v0.2+ readers must read v0.1
shards" rule from RFC §5.6 #6.

This script is run ONCE and the output is committed. Re-running it
should produce identical bytes (deterministic env vars set).
"""

from __future__ import annotations

import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "..", "python"))

from tset import constants  # noqa: E402

# Force the writer to emit v0.1: patch the version constant the writer
# uses AND strip content_hash from each chunk before it reaches the
# manifest (v0.1 has no content_hash; v0.2 made it mandatory).
_orig_minor = constants.VERSION_MINOR
constants.VERSION_MINOR = 1

from tset.reader import Reader  # noqa: E402
from tset.tokenizers import ByteLevelTokenizer  # noqa: E402
import tset.writer as _writer_mod  # noqa: E402
import tset.tokenizer_view as _view_mod  # noqa: E402

_writer_mod.VERSION_MINOR = 1

_orig_build = _view_mod.build_view


def _build_v01(*args, **kwargs):
    res = _orig_build(*args, **kwargs)
    for c in res.chunks:
        c.content_hash = None
    return res


_view_mod.build_view = _build_v01
_writer_mod.build_view = _build_v01

from tset.writer import Writer  # noqa: E402

FIXTURES = os.path.join(HERE, "fixtures")


def _make(name: str, path: str, build) -> dict:
    if os.path.exists(path):
        os.remove(path)
    os.environ["TSET_DETERMINISTIC_TIME"] = "1700000000.0"
    os.environ["TSET_DETERMINISTIC_CREATED_AT"] = "2023-11-14T22:13:20+00:00"
    os.environ["TSET_DETERMINISTIC_SNAPSHOT_ID"] = f"{name}-snap"
    shard_id = f"v01-conformance-{name}-id-padding".encode().hex()[:32]
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
    return {
        "version_minor": r.header.version_minor,
        "shard_merkle_root": r.header.shard_merkle_root.hex(),
        "manifest_hash": r.header.manifest_hash.hex(),
        "manifest_size": r.header.manifest_size,
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


def fixture_v01_small(w: Writer) -> None:
    w.add_document(b"v0.1 alpha", metadata={"v": 1})
    w.add_document(b"v0.1 beta", metadata={"v": 1})
    w.add_tokenizer_view(ByteLevelTokenizer())


def main() -> None:
    os.makedirs(FIXTURES, exist_ok=True)
    name = "fixture-v01-small"
    shard = os.path.join(FIXTURES, f"{name}.tset")
    invariants = _make(name, shard, fixture_v01_small)
    assert invariants["version_minor"] == 1, "expected v0.1 shard"
    with open(os.path.join(FIXTURES, f"{name}.expected.json"), "w") as f:
        json.dump(invariants, f, indent=2, sort_keys=True)
        f.write("\n")
    print(f"  built {name}: v{invariants['version_minor']}, "
          f"{invariants['document_count']} docs, "
          f"manifest_size={invariants['manifest_size']}")


if __name__ == "__main__":
    try:
        main()
    finally:
        constants.VERSION_MINOR = _orig_minor
