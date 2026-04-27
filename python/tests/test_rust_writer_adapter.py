"""Smoke tests for `tset.rust_writer.RustWriter` — the Python-API-shaped
Writer that delegates to `tset_rs.Writer`. Validates the adapter passes
through every public method correctly."""

import pytest

from tset.hashing import hash_bytes
from tset.reader import Reader as PyReader
from tset.tokenizers import ByteLevelTokenizer, WhitespaceTokenizer

tset_rs = pytest.importorskip("tset_rs")
from tset.rust_writer import RustWriter  # noqa: E402


def test_rust_writer_adapter_basic_roundtrip(tmp_path):
    p = str(tmp_path / "rw.tset")
    with RustWriter(p) as w:
        w.add_document(b"alpha")
        w.add_document("beta as str")  # str → utf-8
        w.add_document(b"gamma", metadata={"lang": "en"})
        w.add_tokenizer_view(ByteLevelTokenizer())
    py = PyReader(p)
    assert py.tokenizer_ids() == ["byte-level-v1"]
    assert py.has_document(hash_bytes(b"alpha"))
    assert py.has_document(hash_bytes(b"beta as str"))
    assert py.has_document(hash_bytes(b"gamma"))


def test_rust_writer_adapter_whitespace_tokenizer(tmp_path):
    p = str(tmp_path / "ws.tset")
    with RustWriter(p) as w:
        w.add_document(b"the quick brown fox")
        w.add_tokenizer_view(WhitespaceTokenizer(vocab_size=512))
    py = PyReader(p)
    assert "whitespace-hashed-v1" in py.tokenizer_ids()
    view = py.manifest["tokenization_views"]["whitespace-hashed-v1"]
    assert view["vocab_size"] == 512


def test_rust_writer_adapter_subsets_persist(tmp_path):
    p = str(tmp_path / "subs.tset")
    with RustWriter(p) as w:
        w.add_document(b"x")
        w.add_subset("web", "lang = 'en'", 0.4)
        w.add_subset("code", "lang IN ('python')", 0.2)
        w.add_tokenizer_view(ByteLevelTokenizer())
    py = PyReader(p)
    names = sorted(s["name"] for s in py.manifest["subsets"])
    assert names == ["code", "web"]
