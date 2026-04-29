"""Tests for ``tset.hf`` — HuggingFace ``datasets`` integration.

The adapter is the highest-leverage adoption surface for ML
practitioners (the people who already use ``datasets.load_dataset``).
These tests exercise both directions:

- TSET shard → HF ``Dataset``  (via ``from_tset``)
- TSET multi-shard → HF Dataset (via ``from_dataset``, with exclusions)
- HF ``Dataset`` → TSET shard (via ``to_tset``)

Plus the standard HF API usable on the result (``map``, ``filter``,
``select``, ``train_test_split``).

Skipped cleanly when the optional ``datasets`` dependency is absent.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

# Make the in-tree `tset` importable when running from repo root.
_HERE = Path(__file__).resolve().parent
_PYTHON_DIR = _HERE.parent
if str(_PYTHON_DIR) not in sys.path:
    sys.path.insert(0, str(_PYTHON_DIR))


datasets = pytest.importorskip("datasets")


# ── Fixtures ─────────────────────────────────────────────────────────


@pytest.fixture
def small_tset_shard(tmp_path: Path) -> Path:
    """Build a small TSET shard with metadata columns + two views."""
    from tset.tokenizers import ByteLevelTokenizer, WhitespaceTokenizer
    from tset.writer import Writer

    out = tmp_path / "small.tset"
    docs = [
        (b"hello world",  {"lang": "en", "rank": 1}),
        (b"bonjour monde",  {"lang": "fr", "rank": 2}),
        (b"ciao mondo",     {"lang": "it", "rank": 3}),
        (b"hola mundo",     {"lang": "es", "rank": 4}),
    ]
    with Writer(str(out)) as w:
        for content, md in docs:
            w.add_document(content, metadata=md)
        w.add_tokenizer_view(ByteLevelTokenizer())
        w.add_tokenizer_view(WhitespaceTokenizer(vocab_size=512))
    return out


# ── from_tset ────────────────────────────────────────────────────────


class TestFromTset:
    def test_returns_real_dataset(self, small_tset_shard: Path):
        from tset.hf import from_tset

        ds = from_tset(small_tset_shard)
        assert isinstance(ds, datasets.Dataset)
        assert len(ds) == 4
        assert set(ds.column_names) >= {"text", "doc_hash", "lang", "rank"}

    def test_metadata_columns_pass_through(self, small_tset_shard: Path):
        from tset.hf import from_tset

        ds = from_tset(small_tset_shard)
        langs = sorted(ds["lang"])
        assert langs == ["en", "es", "fr", "it"]
        assert sorted(ds["rank"]) == [1, 2, 3, 4]

    def test_with_tokens_includes_token_column(self, small_tset_shard: Path):
        from tset.hf import from_tset

        ds = from_tset(small_tset_shard, with_tokens=True, view="byte-level-v1")
        assert "tokens" in ds.column_names
        # ByteLevel: 1 token per byte
        rec = ds.filter(lambda r: r["text"] == "hello world")[0]
        assert len(rec["tokens"]) == len(b"hello world")

    def test_with_tokens_default_view(self, small_tset_shard: Path):
        """When view=None and with_tokens=True, picks the first view."""
        from tset.hf import from_tset

        ds = from_tset(small_tset_shard, with_tokens=True)
        assert "tokens" in ds.column_names
        # First registered view on the fixture is byte-level → 1 token / byte
        sample = ds[0]
        assert len(sample["tokens"]) == len(sample["text"].encode("utf-8"))

    def test_disable_metadata(self, small_tset_shard: Path):
        from tset.hf import from_tset

        ds = from_tset(small_tset_shard, with_metadata=False)
        assert set(ds.column_names) == {"text", "doc_hash"}

    def test_streaming_iterates_lazily(self, small_tset_shard: Path):
        from tset.hf import from_tset

        ds = from_tset(small_tset_shard, streaming=True)
        assert isinstance(ds, datasets.IterableDataset)
        rows = list(ds)
        assert len(rows) == 4
        assert {r["text"] for r in rows} == {
            "hello world", "bonjour monde", "ciao mondo", "hola mundo",
        }

    def test_standard_hf_api_works(self, small_tset_shard: Path):
        """Map, filter, select all work on the returned dataset."""
        from tset.hf import from_tset

        ds = from_tset(small_tset_shard)

        # filter
        en = ds.filter(lambda r: r["lang"] == "en")
        assert len(en) == 1
        assert en[0]["text"] == "hello world"

        # map
        upper = ds.map(lambda r: {"text_upper": r["text"].upper()})
        assert "text_upper" in upper.column_names
        assert "HELLO WORLD" in upper["text_upper"]

        # select
        sel = ds.select([0, 2])
        assert len(sel) == 2

    def test_empty_view_with_tokens_raises(self, tmp_path: Path):
        """with_tokens=True on a shard with no views fails clearly."""
        from tset.hf import from_tset
        from tset.writer import Writer

        out = tmp_path / "no-views.tset"
        with Writer(str(out)) as w:
            w.add_document(b"alpha")
            # no add_tokenizer_view
        with pytest.raises(Exception):
            list(from_tset(out, with_tokens=True, streaming=True))


# ── to_tset ──────────────────────────────────────────────────────────


class TestToTset:
    def test_round_trip_preserves_text_and_metadata(self, tmp_path: Path):
        from tset.hf import from_tset, to_tset

        src = datasets.Dataset.from_list(
            [
                {"text": "alpha", "lang": "en", "rank": 10},
                {"text": "beta",  "lang": "fr", "rank": 20},
                {"text": "gamma", "lang": "de", "rank": 30},
            ]
        )
        out = tmp_path / "round-trip.tset"
        info = to_tset(src, out, metadata_fields=["lang", "rank"])
        assert info["documents"] == 3

        ds_out = from_tset(out)
        assert len(ds_out) == 3
        assert sorted(ds_out["text"]) == ["alpha", "beta", "gamma"]
        assert sorted(ds_out["lang"]) == ["de", "en", "fr"]
        assert sorted(ds_out["rank"]) == [10, 20, 30]

    def test_metadata_star_keeps_everything(self, tmp_path: Path):
        from tset.hf import from_tset, to_tset

        src = datasets.Dataset.from_list(
            [{"text": "a", "x": 1, "y": "p"}, {"text": "b", "x": 2, "y": "q"}]
        )
        out = tmp_path / "star.tset"
        to_tset(src, out, metadata_fields="*")

        ds_out = from_tset(out)
        assert "x" in ds_out.column_names
        assert "y" in ds_out.column_names

    def test_missing_content_field_raises(self, tmp_path: Path):
        from tset.hf import to_tset

        src = datasets.Dataset.from_list([{"body": "text", "id": 1}])
        with pytest.raises(KeyError):
            to_tset(src, tmp_path / "x.tset", content_field="text")

    def test_non_str_content_raises(self, tmp_path: Path):
        from tset.hf import to_tset

        src = datasets.Dataset.from_list([{"text": 12345}])
        with pytest.raises(TypeError):
            to_tset(src, tmp_path / "x.tset")

    def test_accepts_plain_iterable(self, tmp_path: Path):
        """to_tset works on a list-of-dicts, not just `datasets.Dataset`."""
        from tset.hf import from_tset, to_tset

        records = [{"text": f"doc-{i}", "rank": i} for i in range(5)]
        out = tmp_path / "iterable.tset"
        to_tset(records, out, metadata_fields=["rank"])

        ds_out = from_tset(out)
        assert len(ds_out) == 5
        assert sorted(ds_out["rank"]) == [0, 1, 2, 3, 4]


# ── from_dataset (multi-shard) ───────────────────────────────────────


class TestFromDataset:
    def test_multi_shard_aggregates(self, tmp_path: Path):
        from tset.dataset import DatasetWriter
        from tset.hf import from_dataset
        from tset.tokenizers import ByteLevelTokenizer

        root = tmp_path / "multi"
        with DatasetWriter(str(root)) as dw:
            with dw.shard_writer("shard-001") as sw:
                sw.add_document(b"alpha", metadata={"src": "a"})
                sw.add_document(b"beta",  metadata={"src": "a"})
                sw.add_tokenizer_view(ByteLevelTokenizer())
            dw.register_shard("shard-001")
            with dw.shard_writer("shard-002") as sw:
                sw.add_document(b"gamma", metadata={"src": "b"})
                sw.add_document(b"delta", metadata={"src": "b"})
                sw.add_tokenizer_view(ByteLevelTokenizer())
            dw.register_shard("shard-002")

        ds = from_dataset(root)
        assert len(ds) == 4
        assert set(ds["text"]) == {"alpha", "beta", "gamma", "delta"}
        assert set(ds["src"]) == {"a", "b"}

    def test_exclusion_overlay_filters_records(self, tmp_path: Path):
        from tset.dataset import Dataset, DatasetWriter
        from tset.hf import from_dataset
        from tset.tokenizers import ByteLevelTokenizer

        root = tmp_path / "excl"
        with DatasetWriter(str(root)) as dw:
            with dw.shard_writer("only") as sw:
                sw.add_document(b"keep-me-1")
                sw.add_document(b"DROP-ME")
                sw.add_document(b"keep-me-2")
                sw.add_tokenizer_view(ByteLevelTokenizer())
            dw.register_shard("only")

        # Find the doc_hash for the document we want to drop
        ds_pre = Dataset(str(root))
        with next(iter(ds_pre.shards())) as r:
            target = next(h for h, c in r.documents() if c == b"DROP-ME")

        with DatasetWriter(str(root)) as dw:
            dw.add_exclusion(target, reason="test")

        ds = from_dataset(root)
        texts = ds["text"]
        assert "DROP-ME" not in texts
        assert {"keep-me-1", "keep-me-2"} == set(texts)
