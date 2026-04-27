"""Run the Python reader against the language-agnostic conformance corpus.

Each fixture is paired with an `expected.json` sidecar that lists the
invariants any conforming reader must observe. The Rust reader runs
the same fixtures in `crates/tset-core/tests/conformance.rs`.
"""

import json
import os

import pytest

from tset.reader import Reader

HERE = os.path.dirname(os.path.abspath(__file__))
FIXTURES = os.path.normpath(os.path.join(HERE, "..", "..", "tests", "conformance", "fixtures"))


def _cases():
    if not os.path.exists(FIXTURES):
        return []
    out = []
    for name in sorted(os.listdir(FIXTURES)):
        if not name.endswith(".tset"):
            continue
        stem = name[:-5]
        sidecar = os.path.join(FIXTURES, f"{stem}.expected.json")
        if os.path.exists(sidecar):
            out.append(stem)
    return out


@pytest.mark.parametrize("name", _cases())
def test_python_reader_matches_conformance_invariants(name):
    shard = os.path.join(FIXTURES, f"{name}.tset")
    with open(os.path.join(FIXTURES, f"{name}.expected.json")) as f:
        expected = json.load(f)

    r = Reader(shard)
    actual = {
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
    assert actual == expected, f"conformance mismatch for {name}"


def test_conformance_corpus_present():
    """If the corpus is missing, the build_corpus.py script needs to run."""
    assert _cases(), (
        f"no conformance fixtures found in {FIXTURES} — "
        "run `python tests/conformance/build_corpus.py`"
    )
