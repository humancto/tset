"""PyTorch IterableDataset wrapping the Rust streaming reader.

Lazy import of `torch` — clear runtime error if missing. Standalone
class so a user can:

    from torch.utils.data import DataLoader
    from tset.torch_dataset import TsetIterableDataset

    ds = TsetIterableDataset("corpus.tset", "byte-level-v1", batch_size=2048)
    loader = DataLoader(ds, batch_size=None, num_workers=4)

The dataset emits `(tokens: torch.Tensor[uint32], doc_hash: bytes)`
tuples. `num_workers > 1` partitions the source-map deterministically
so workers don't see overlapping documents — set `world_size` and
`rank` for distributed training.
"""

from __future__ import annotations


def _require_torch():
    try:
        import torch  # noqa: F401
        from torch.utils.data import IterableDataset, get_worker_info  # noqa: F401

        return torch, IterableDataset, get_worker_info
    except ImportError as e:
        raise RuntimeError(
            "TsetIterableDataset requires PyTorch; install with `pip install torch`"
        ) from e


def _require_tset_rs():
    try:
        import tset_rs

        return tset_rs
    except ImportError as e:
        raise RuntimeError(
            "TsetIterableDataset requires the optional tset_rs PyO3 wheel"
        ) from e


def _build_class():
    """Build the class lazily so the module imports cleanly without torch."""
    _, IterableDataset, get_worker_info = _require_torch()
    import numpy as np
    import torch

    tset_rs = _require_tset_rs()

    class TsetIterableDataset(IterableDataset):  # type: ignore[misc, valid-type]
        def __init__(
            self,
            shard_path: str,
            tokenizer_id: str,
            batch_size: int = 1024,
            world_size: int = 1,
            rank: int = 0,
            shuffle_seed: int | None = None,
            skip_reproducibility: bool = False,
        ):
            super().__init__()
            self.shard_path = shard_path
            self.tokenizer_id = tokenizer_id
            self.batch_size = batch_size
            self.world_size = world_size
            self.rank = rank
            self.shuffle_seed = shuffle_seed
            self.skip_reproducibility = skip_reproducibility
            # Open eagerly to fail fast if the shard is malformed.
            # skip_reproducibility delegates to the Rust core's escape
            # hatch (atomic c23d388) — only safe if you've already
            # verified the shard once.
            _ = tset_rs.Reader(shard_path)

        def __iter__(self):
            info = get_worker_info()
            num_workers = 1 if info is None else int(info.num_workers)
            worker_id = 0 if info is None else int(info.id)
            slot = self.rank * num_workers + worker_id
            modulus = self.world_size * num_workers

            r = tset_rs.Reader(self.shard_path)
            pieces = list(r.stream_tokens(self.tokenizer_id))
            if self.shuffle_seed is not None:
                rng = np.random.default_rng(self.shuffle_seed)
                rng.shuffle(pieces)

            for i, (token_bytes, doc_hash) in enumerate(pieces):
                if i % modulus != slot:
                    continue
                arr = np.frombuffer(token_bytes, dtype=np.uint32)
                for j in range(0, int(arr.size), self.batch_size):
                    chunk = arr[j : j + self.batch_size]
                    yield torch.from_numpy(chunk.copy()), bytes(doc_hash)

    return TsetIterableDataset


# Public entry point — defers torch import until first use.
def TsetIterableDataset(*args, **kwargs):  # noqa: N802
    cls = _build_class()
    return cls(*args, **kwargs)
