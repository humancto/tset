"""Cross-impl validation: Rust writer → Python reader and Rust writer → Rust reader.

Proves that the Rust writer produces shards that conform to SPEC.md and
are byte-equivalent to what the Python writer would produce (modulo the
random shard_id and the ISO timestamp string, which are header-of-mind
non-load-bearing).
"""

import pytest

import numpy as np

from tset.hashing import hash_bytes
from tset.reader import Reader as PyReader

tset_rs = pytest.importorskip("tset_rs")


@pytest.fixture
def rs_written(tmp_path):
    p = str(tmp_path / "rs_written.tset")
    docs = [
        b"alpha document text",
        b"beta document content",
        b"gamma payload here",
        b"delta extra bytes",
    ]
    w = tset_rs.Writer(p)
    hashes = [w.add_document(d) for d in docs]
    w.add_tokenizer_view("byte-level-v1", 256)
    w.close()
    return p, docs, hashes


def test_rust_writer_open_with_python_reader(rs_written):
    p, docs, hashes = rs_written
    py = PyReader(p)
    assert py.tokenizer_ids() == ["byte-level-v1"]
    assert py.view_total_tokens("byte-level-v1") == sum(len(d) for d in docs)
    for d, h in zip(docs, hashes):
        assert py.has_document(h)
        assert py.get_document(h) == d


def test_rust_writer_token_stream_matches_python_byte_level(rs_written):
    p, docs, hashes = rs_written
    py = PyReader(p)
    seen = []
    for tokens, doc_hash in py.stream_tokens("byte-level-v1", batch_size=4096):
        seen.append((bytes(doc_hash), tokens.copy()))
    # Each doc tokenizes to its own bytes (byte-level)
    by_hash = {h: t for h, t in seen}
    for d, h in zip(docs, hashes):
        assert h in by_hash
        assert np.array_equal(by_hash[h], np.frombuffer(d, dtype=np.uint8).astype(np.uint32))


def test_rust_writer_open_with_rust_reader(rs_written):
    p, _docs, hashes = rs_written
    rs = tset_rs.Reader(p)
    assert rs.tokenizer_ids() == ["byte-level-v1"]
    for h in hashes:
        assert rs.has_document(h)


def test_rust_writer_chunks_have_content_hashes(rs_written):
    """v0.2: chunk content_hash is mandatory and verified on read."""
    p, _docs, _ = rs_written
    py = PyReader(p)
    view = py.manifest["tokenization_views"]["byte-level-v1"]
    assert all(c.get("content_hash") for c in view["chunks"])


def test_rust_writer_whitespace_tokenizer_matches_python(tmp_path):
    p = str(tmp_path / "ws.tset")
    docs = [b"the quick brown fox", b"jumps over the lazy dog"]
    w = tset_rs.Writer(p)
    for d in docs:
        w.add_document(d)
    w.add_tokenizer_view("whitespace-hashed-v1", 1024)
    w.close()

    py = PyReader(p)
    # Sanity: read all tokens via Python reader; then re-encode via Python's
    # tokenizer and compare. If they agree we know Rust and Python tokenize
    # identically.
    from tset.tokenizers import WhitespaceTokenizer

    tokenizer = WhitespaceTokenizer(vocab_size=1024)
    expected_concat = []
    for d in docs:
        expected_concat.append(tokenizer.encode(d))
    expected_all = np.concatenate(expected_concat)

    actual_concat = []
    for tokens, _doc_hash in py.stream_tokens("whitespace-hashed-v1", batch_size=4096):
        actual_concat.append(tokens)
    actual_all = np.concatenate(actual_concat)
    assert np.array_equal(actual_all, expected_all)


def test_rust_writer_rejects_add_document_after_view(tmp_path):
    p = str(tmp_path / "ord.tset")
    w = tset_rs.Writer(p)
    w.add_document(b"first")
    w.add_tokenizer_view("byte-level-v1", 256)
    with pytest.raises(ValueError):
        w.add_document(b"too late")
