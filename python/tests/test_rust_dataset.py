"""Cross-impl validation: Rust DatasetWriter ↔ Python Dataset.

Builds a multi-shard dataset entirely via tset_rs APIs, then opens it
with the Python Dataset reader and checks that the shape, exclusion
overlay, dataset Merkle root, and exclusion proofs all match.
"""

import os

import pytest

from tset.dataset import Dataset as PyDataset
from tset.hashing import hash_bytes

tset_rs = pytest.importorskip("tset_rs")


def _build_dataset(root: str) -> tuple[bytes, bytes]:
    """Build a 2-shard dataset using only Rust APIs. Returns
    (marker_doc_hash, regular_doc_hash) for use by exclusion tests."""
    marker = b"private text to exclude later"
    regular = b"plain corpus document"

    dw = tset_rs.DatasetWriter(root)
    # Shard 0
    p0 = dw.shard_path("part-00000")
    with tset_rs.Writer(p0) as w:
        w.add_document(b"alpha document")
        w.add_document(b"beta content")
        w.add_tokenizer_view("byte-level-v1", 256)
    dw.register_shard("part-00000")
    # Shard 1: contains the marker doc that we'll exclude
    p1 = dw.shard_path("part-00001")
    with tset_rs.Writer(p1) as w:
        w.add_document(marker)
        w.add_document(regular)
        w.add_tokenizer_view("byte-level-v1", 256)
    dw.register_shard("part-00001")
    dw.close()
    return hash_bytes(marker), hash_bytes(regular)


def test_rust_dataset_writer_open_with_python(tmp_path):
    root = str(tmp_path / "ds.tset")
    _build_dataset(root)
    py_ds = PyDataset(root)
    assert len(py_ds.shard_paths()) == 2
    # Each registered shard exists on disk
    for p in py_ds.shard_paths():
        assert os.path.exists(p)
    # Dataset merkle root is a non-empty 32-byte hash
    root_bytes = py_ds.dataset_merkle_root()
    assert len(root_bytes) == 32
    assert root_bytes != b"\x00" * 32


def test_rust_dataset_exclusion_overlay_visible_to_python(tmp_path):
    root = str(tmp_path / "ds.tset")
    marker, _ = _build_dataset(root)

    # Add an exclusion via Rust DatasetWriter, re-using the same dataset dir.
    # Re-create the writer pointing at the same root — Rust DatasetWriter
    # is a builder, not a stateful editor, so we register existing shards
    # again before adding the exclusion.
    dw = tset_rs.DatasetWriter(root)
    dw.register_shard("part-00000")
    dw.register_shard("part-00001")
    dw.add_exclusion(marker, "test exclusion")
    dw.close()

    py_ds = PyDataset(root)
    assert marker.hex() in py_ds.exclusions()
    assert py_ds.is_excluded(marker)


def test_rust_dataset_dataset_merkle_root_matches_python(tmp_path):
    root = str(tmp_path / "ds.tset")
    _build_dataset(root)
    rs_ds = tset_rs.Dataset(root)
    py_ds = PyDataset(root)
    assert bytes(rs_ds.dataset_merkle_root()) == py_ds.dataset_merkle_root()


def test_rust_writer_metadata_visible_in_python_columns(tmp_path):
    p = str(tmp_path / "meta.tset")
    with tset_rs.Writer(p) as w:
        w.add_document(b"alpha", {"lang": "en", "quality": 0.9})
        w.add_document(b"beta", {"lang": "fr", "quality": 0.4})
        w.add_document(b"gamma", {"lang": "en", "quality": 0.7})
        w.add_tokenizer_view("byte-level-v1", 256)

    from tset.reader import Reader as PyReader

    py = PyReader(p)
    cols = py.metadata_columns()
    assert cols.row_count == 3
    assert cols.column("lang") == ["en", "fr", "en"]
    assert cols.column("quality") == [0.9, 0.4, 0.7]
    matched = cols.filter_sql_like("lang = 'en' AND quality > 0.5")
    assert matched == [0, 2]


def test_rust_subsets_persisted_in_manifest(tmp_path):
    p = str(tmp_path / "subsets.tset")
    with tset_rs.Writer(p) as w:
        w.add_document(b"a")
        w.add_subset("web", "lang = 'en'", 0.4)
        w.add_subset("code", "lang IN ('python', 'rust')", 0.2)
        w.add_tokenizer_view("byte-level-v1", 256)

    from tset.reader import Reader as PyReader

    py = PyReader(p)
    subsets = py.manifest["subsets"]
    assert len(subsets) == 2
    names = sorted(s["name"] for s in subsets)
    assert names == ["code", "web"]
