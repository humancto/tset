"""Tests for PR-5 converters: WebDataset and HF datasets adapter.

MDS converter is tested only as far as its import-error path because
mosaicml-streaming is a heavy dep that's not installed in CI.
"""

import io
import os
import tarfile

import pytest

from tset.converters import (
    hf_dataset_view,
    mds_to_tset,
    to_huggingface_dataset,
    webdataset_to_tset,
)
from tset.hashing import hash_bytes
from tset.reader import Reader as PyReader
from tset.tokenizers import ByteLevelTokenizer


def _make_webdataset(path: str, samples: list[tuple[str, bytes, dict]]) -> None:
    """Write a tar shard containing samples in WebDataset shape."""
    with tarfile.open(path, mode="w") as tf:
        for stem, body, meta in samples:
            for ext, data in [("txt", body), ("json", str(meta).replace("'", '"').encode())]:
                info = tarfile.TarInfo(f"{stem}.{ext}")
                info.size = len(data)
                tf.addfile(info, io.BytesIO(data))


def test_webdataset_to_tset_roundtrip(tmp_path):
    src = str(tmp_path / "shard.tar")
    dst = str(tmp_path / "out.tset")
    samples = [
        ("0001", b"alpha document text", {"label": 0}),
        ("0002", b"beta content here", {"label": 1}),
        ("0003", b"gamma payload", {"label": 0}),
    ]
    _make_webdataset(src, samples)
    result = webdataset_to_tset(src, dst, ByteLevelTokenizer())
    assert result["documents"] == 3
    py = PyReader(dst)
    assert py.tokenizer_ids() == ["byte-level-v1"]
    for _stem, body, _meta in samples:
        assert py.has_document(hash_bytes(body))


def test_webdataset_skip_samples_missing_content(tmp_path):
    src = str(tmp_path / "shard.tar")
    dst = str(tmp_path / "out.tset")
    # One sample is metadata-only — no .txt — should be skipped, not error.
    with tarfile.open(src, mode="w") as tf:
        info = tarfile.TarInfo("0001.txt")
        info.size = 5
        tf.addfile(info, io.BytesIO(b"hello"))
        info = tarfile.TarInfo("0002.json")
        info.size = 2
        tf.addfile(info, io.BytesIO(b"{}"))
    result = webdataset_to_tset(src, dst, ByteLevelTokenizer())
    assert result["documents"] == 1


def test_mds_to_tset_raises_clear_error_without_streaming(tmp_path, monkeypatch):
    # If mosaicml-streaming isn't installed (the typical case), the
    # converter must raise a runtime error with an install hint, not a
    # bare ImportError.
    import sys

    real_modules = sys.modules.copy()
    monkeypatch.setitem(sys.modules, "streaming", None)
    try:
        with pytest.raises(RuntimeError, match="mosaicml-streaming"):
            mds_to_tset(str(tmp_path), str(tmp_path / "x.tset"), ByteLevelTokenizer())
    finally:
        sys.modules.clear()
        sys.modules.update(real_modules)


def test_hf_dataset_view_yields_text_and_hash(tmp_path):
    p = str(tmp_path / "src.tset")
    docs = [b"foo bar", b"baz qux"]
    from tset.writer import Writer

    with Writer(p) as w:
        for d in docs:
            w.add_document(d)
        w.add_tokenizer_view(ByteLevelTokenizer())

    gen = hf_dataset_view(p)
    rows = list(gen())
    assert len(rows) == 2
    assert {r["text"] for r in rows} == {d.decode() for d in docs}
    assert all(len(r["doc_hash"]) == 64 for r in rows)


def test_to_huggingface_dataset_raises_clear_error_without_datasets(tmp_path, monkeypatch):
    import sys

    real_modules = sys.modules.copy()
    monkeypatch.setitem(sys.modules, "datasets", None)
    try:
        with pytest.raises(RuntimeError, match=r"`datasets`"):
            to_huggingface_dataset(str(tmp_path / "x.tset"))
    finally:
        sys.modules.clear()
        sys.modules.update(real_modules)
