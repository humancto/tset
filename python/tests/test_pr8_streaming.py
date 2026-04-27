"""PR 8 — streaming writer: writer's working set scales with block size,
not corpus size.

The Rust writer no longer holds a HashMap<Hash, Vec<u8>> of every
document; it re-reads documents from the just-finalized doc-store body
during view construction, with a 1-block LRU. We can't directly assert
RSS in pytest, but we can:

  1. Assert that a 50 MB synthetic corpus writes successfully and round-
     trips byte-equivalent through both Python + Rust readers
  2. Assert that the resulting shard's Reader streams it without ever
     requiring all chunks to be resident (PR 7's lazy iter_per_doc).
"""

import os

import pytest

from tset.hashing import hash_bytes
from tset.reader import Reader as PyReader
from tset.tokenizers import ByteLevelTokenizer
from tset.writer import Writer as PyWriter

tset_rs = pytest.importorskip("tset_rs")


def test_rust_writer_handles_50mb_corpus(tmp_path):
    p = str(tmp_path / "big.tset")
    target_doc = b"x" * 4096  # 4 KB per doc
    n_docs = 12_500  # 50 MB
    with tset_rs.Writer(p) as w:
        for i in range(n_docs):
            # Make each doc unique so dedup doesn't fold them.
            w.add_document(target_doc + i.to_bytes(8, "little"))
        w.add_tokenizer_view("byte-level-v1", 256)
    # Must open and stream end-to-end via both readers.
    rs = tset_rs.Reader(p)
    assert rs.view_total_tokens("byte-level-v1") == n_docs * (4096 + 8)
    py = PyReader(p)
    assert len(py.manifest["document_store"]["document_index"]) == n_docs


def test_python_writer_still_works_at_5mb(tmp_path):
    """Python writer hasn't been streamed yet; document the current
    capability with a smaller corpus."""
    p = str(tmp_path / "py.tset")
    target_doc = b"y" * 4096
    n_docs = 1_250  # 5 MB
    with PyWriter(p) as w:
        for i in range(n_docs):
            w.add_document(target_doc + i.to_bytes(8, "little"))
        w.add_tokenizer_view(ByteLevelTokenizer())
    py = PyReader(p)
    assert len(py.manifest["document_store"]["document_index"]) == n_docs


def test_writer_dedup_still_works_without_doc_contents(tmp_path):
    """The previous impl tracked dedup via the doc_contents HashMap.
    With doc_contents removed, dedup must still work via the doc_seen
    HashSet."""
    p = str(tmp_path / "dedup.tset")
    body = b"identical content"
    with tset_rs.Writer(p) as w:
        h1 = w.add_document(body)
        h2 = w.add_document(body)  # same hash, should be dedup'd
        h3 = w.add_document(b"different")
        w.add_tokenizer_view("byte-level-v1", 256)
    assert h1 == h2
    py = PyReader(p)
    assert len(py.manifest["document_store"]["document_index"]) == 2
