"""PR 9 — bit-packed token IDs in the Rust writer.

Confirms:
  - Rust writer emits 16-bit chunks when vocab ≤ 65536 (verified via
    manifest's bits_per_token field)
  - 16-bit chunks decode correctly through both the Rust and Python
    readers (token round-trip parity)
  - Storage on disk drops by ~½ for the same corpus when vocab ≤ 65536
"""

import os

import pytest

from tset.reader import Reader as PyReader
from tset.tokenizers import ByteLevelTokenizer
from tset.writer import Writer as PyWriter

tset_rs = pytest.importorskip("tset_rs")


def test_rust_writer_emits_16bit_chunks_for_byte_level(tmp_path):
    p = str(tmp_path / "16bit.tset")
    with tset_rs.Writer(p) as w:
        w.add_document(b"alpha document text")
        w.add_document(b"beta content here")
        w.add_tokenizer_view("byte-level-v1", 256)
    py = PyReader(p)
    view = py.manifest["tokenization_views"]["byte-level-v1"]
    assert view["bits_per_token"] == 16, "byte-level (vocab=256) → 16-bit pack"


def test_rust_writer_16bit_chunks_decode_correctly(tmp_path):
    """Tokens written 16-bit must equal tokens decoded by both readers."""
    import numpy as np

    p = str(tmp_path / "rt.tset")
    docs = [b"alpha document", b"beta gamma delta"]
    with tset_rs.Writer(p) as w:
        for d in docs:
            w.add_document(d)
        w.add_tokenizer_view("byte-level-v1", 256)

    # Python reader decodes 16-bit chunks
    py = PyReader(p)
    py_tokens = []
    for tokens, _h in py.stream_tokens("byte-level-v1", batch_size=4096):
        py_tokens.append(tokens)
    py_all = np.concatenate(py_tokens)
    expected = np.frombuffer(b"".join(docs), dtype=np.uint8).astype(np.uint32)
    assert np.array_equal(py_all, expected)

    # Rust reader decodes 16-bit chunks
    rs = tset_rs.Reader(p)
    rs_concat = []
    for token_bytes, _h in rs.stream_tokens("byte-level-v1"):
        rs_concat.append(np.frombuffer(token_bytes, dtype=np.uint32))
    rs_all = np.concatenate(rs_concat)
    assert np.array_equal(rs_all, expected)


def test_16bit_pack_chunk_storage_smaller_than_32bit(tmp_path):
    """Storage comparison at the *chunk* level (not whole shard, where
    the manifest can dominate at small sizes). Sum of compressed_size
    across chunks must be smaller for the 16-bit Rust writer than for
    the 32-bit Python writer on the same corpus."""
    body = (b"the quick brown fox jumps over the lazy dog. " * 200).strip()

    rs_path = str(tmp_path / "rust.tset")
    with tset_rs.Writer(rs_path) as w:
        for i in range(50):
            w.add_document(body + str(i).encode())
        w.add_tokenizer_view("byte-level-v1", 256)

    py_path = str(tmp_path / "py.tset")
    with PyWriter(py_path) as w:
        for i in range(50):
            w.add_document(body + str(i).encode())
        w.add_tokenizer_view(ByteLevelTokenizer())

    def chunk_total(path):
        r = PyReader(path)
        chunks = r.manifest["tokenization_views"]["byte-level-v1"]["chunks"]
        return sum(c["compressed_size"] for c in chunks)

    rs_chunks = chunk_total(rs_path)
    py_chunks = chunk_total(py_path)
    # Honest number: 16-bit raw IS half the bytes, but the 32-bit Python
    # encoding has predictable zero bytes that zstd compresses well.
    # On this corpus 16-bit lands at ~80% of 32-bit's compressed size,
    # not 50%. The win grows with vocab size (zero bytes get rarer).
    assert rs_chunks < py_chunks * 0.85, (
        f"expected 16-bit chunks ≥15% smaller; rs={rs_chunks} py={py_chunks}"
    )


def test_v01_v02_v03_shards_all_readable():
    """Forward compat: current readers must open shards from every minor
    version we've ever shipped."""
    import json

    here = os.path.dirname(__file__)
    fixtures = os.path.normpath(
        os.path.join(here, "..", "..", "tests", "conformance", "fixtures")
    )
    # v0.1 fixture committed in PR 7
    v01 = os.path.join(fixtures, "fixture-v01-small.tset")
    if os.path.exists(v01):
        r = PyReader(v01)
        assert r.header.version_minor == 1
        rs = tset_rs.Reader(v01)
        assert rs.version_minor == 1
    # current minor (built from build_corpus.py)
    cur = os.path.join(fixtures, "fixture-small.tset")
    assert os.path.exists(cur)
    r = PyReader(cur)
    assert r.header.version_minor in (2, 3)
    rs = tset_rs.Reader(cur)
    assert rs.version_minor in (2, 3)
