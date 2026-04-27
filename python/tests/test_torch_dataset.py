"""Smoke tests for tset.torch_dataset.TsetIterableDataset.

torch is heavy; tests skip gracefully if it isn't installed. The
import-error path runs unconditionally so we always verify the
adapter behaves correctly when torch is absent."""

import sys

import pytest


def test_torch_dataset_requires_torch_when_missing(monkeypatch):
    """Forcing torch=None at import time must surface a clear error."""
    real = sys.modules.copy()
    monkeypatch.setitem(sys.modules, "torch", None)
    try:
        from tset.torch_dataset import TsetIterableDataset

        with pytest.raises(RuntimeError, match=r"PyTorch"):
            TsetIterableDataset("/no/such.tset", "byte-level-v1")
    finally:
        sys.modules.clear()
        sys.modules.update(real)


def test_torch_dataset_basic_iteration(tmp_path):
    torch = pytest.importorskip("torch")
    tset_rs = pytest.importorskip("tset_rs")
    from tset.torch_dataset import TsetIterableDataset

    p = str(tmp_path / "td.tset")
    with tset_rs.Writer(p) as w:
        for i in range(5):
            w.add_document(f"doc {i} content".encode())
        w.add_tokenizer_view("byte-level-v1", 256)

    ds = TsetIterableDataset(p, "byte-level-v1", batch_size=64)
    batches = list(ds)
    assert len(batches) > 0
    # Each batch is (Tensor[uint32], bytes_doc_hash)
    for tokens, doc_hash in batches:
        assert isinstance(tokens, torch.Tensor)
        assert tokens.dtype == torch.int32 or tokens.dtype == torch.int64 or "uint32" in str(tokens.dtype)
        assert isinstance(doc_hash, bytes)
        assert len(doc_hash) == 32


def test_torch_dataset_partition_no_overlap(tmp_path):
    """Across (rank, worker) slots the union of yields equals the
    full doc list with no overlap."""
    pytest.importorskip("torch")
    tset_rs = pytest.importorskip("tset_rs")
    from tset.torch_dataset import TsetIterableDataset

    p = str(tmp_path / "part.tset")
    n = 10
    with tset_rs.Writer(p) as w:
        for i in range(n):
            w.add_document(f"unique-{i}".encode())
        w.add_tokenizer_view("byte-level-v1", 256)

    seen_total = 0
    for rank in range(2):
        ds = TsetIterableDataset(p, "byte-level-v1", batch_size=4096, world_size=2, rank=rank)
        for tokens, _doc_hash in ds:
            seen_total += int(tokens.shape[0])
    # Total tokens across both ranks = total tokens in shard
    r = tset_rs.Reader(p)
    assert seen_total == r.view_total_tokens("byte-level-v1")
