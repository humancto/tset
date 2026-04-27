import os

import pytest

from tset.dataloader import DataLoader, _derive_seed
from tset.tokenizers import ByteLevelTokenizer
from tset.writer import Writer


@pytest.fixture
def shard(tmp_path):
    p = str(tmp_path / "loader.tset")
    with Writer(p) as w:
        for i in range(20):
            w.add_document(("abcdefghij" * 10 + f"-{i}").encode())
        w.add_tokenizer_view(ByteLevelTokenizer())
    return p


def test_dataloader_iterates(shard):
    loader = DataLoader(shard, "byte-level-v1", batch_size=64)
    batches = list(loader)
    assert len(batches) > 0
    assert all(b.shape[0] <= 64 for b in batches)


def test_dataloader_total_matches_view(shard):
    from tset.reader import Reader

    loader = DataLoader(shard, "byte-level-v1", batch_size=128)
    total = sum(int(b.shape[0]) for b in loader)
    with Reader(shard) as r:
        assert total == r.view_total_tokens("byte-level-v1")


def test_dataloader_partitioning(shard):
    a = DataLoader(shard, "byte-level-v1", batch_size=32, world_size=2, rank=0)
    b = DataLoader(shard, "byte-level-v1", batch_size=32, world_size=2, rank=1)
    ta = sum(int(x.shape[0]) for x in a)
    tb = sum(int(x.shape[0]) for x in b)
    from tset.reader import Reader

    with Reader(shard) as r:
        assert ta + tb == r.view_total_tokens("byte-level-v1")


def test_seed_derivation_is_stable():
    assert _derive_seed(42, 0, 0) == _derive_seed(42, 0, 0)
    assert _derive_seed(42, 0, 0) != _derive_seed(42, 0, 1)
    assert _derive_seed(42, 0, 0) != _derive_seed(43, 0, 0)
