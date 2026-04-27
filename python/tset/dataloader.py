"""Pure-Python DataLoader for TSET shards or datasets.

Designed to be PyTorch-`DataLoader`-shaped without a hard `torch` dependency.
If `torch` is importable, batches are returned as `torch.Tensor`; otherwise
`numpy.ndarray`.

Deterministic shuffling: per RFC §10.11, the shuffle seed is derived as
`BLAKE3(epoch_seed || rank.to_bytes(8) || worker.to_bytes(8))`, and the
partition strategy is round-robin over the global token sequence.
"""

from __future__ import annotations

from typing import Iterator, Union

import numpy as np

from tset.dataset import Dataset
from tset.hashing import hash_bytes
from tset.reader import Reader


try:
    import torch  # type: ignore

    HAS_TORCH = True
except ImportError:
    HAS_TORCH = False


BatchT = Union[np.ndarray, "torch.Tensor"]


def _to_tensor(arr: np.ndarray) -> BatchT:
    if HAS_TORCH:
        return torch.from_numpy(arr.copy())
    return arr


def _derive_seed(epoch_seed: int, rank: int, worker: int) -> int:
    digest = hash_bytes(
        epoch_seed.to_bytes(8, "little", signed=False)
        + rank.to_bytes(8, "little", signed=False)
        + worker.to_bytes(8, "little", signed=False)
    )
    return int.from_bytes(digest[:8], "little")


class DataLoader:
    def __init__(
        self,
        source: str | Dataset,
        tokenizer_id: str,
        batch_size: int = 1024,
        shuffle: bool = False,
        epoch_seed: int = 0,
        rank: int = 0,
        world_size: int = 1,
        worker_id: int = 0,
        num_workers: int = 1,
        drop_last: bool = False,
    ):
        if isinstance(source, str):
            self._dataset = Dataset(source)
        else:
            self._dataset = source
        self.tokenizer_id = tokenizer_id
        self.batch_size = batch_size
        self.shuffle = shuffle
        self.epoch_seed = epoch_seed
        self.rank = rank
        self.world_size = world_size
        self.worker_id = worker_id
        self.num_workers = num_workers
        self.drop_last = drop_last

    def _partition_index(self, n: int) -> int:
        # Combine rank+worker into a global slot and round-robin
        return self.rank * self.num_workers + self.worker_id

    def __iter__(self) -> Iterator[BatchT]:
        slot = self._partition_index(self.batch_size)
        modulus = self.world_size * self.num_workers
        if self.shuffle:
            seed = _derive_seed(self.epoch_seed, self.rank, self.worker_id)
            rng = np.random.default_rng(seed)
        else:
            rng = None
        accumulator: list[np.ndarray] = []
        carried = 0
        batch_idx = 0
        for batch, _doc_hash in self._dataset.stream_tokens(
            self.tokenizer_id, batch_size=self.batch_size
        ):
            if batch_idx % modulus == slot:
                accumulator.append(np.asarray(batch, dtype=np.uint32))
                carried += int(batch.size)
                while carried >= self.batch_size:
                    arr = np.concatenate(accumulator)
                    out = arr[: self.batch_size]
                    rest = arr[self.batch_size :]
                    accumulator = [rest] if rest.size else []
                    carried = int(rest.size) if rest.size else 0
                    if rng is not None:
                        rng.shuffle(out)
                    yield _to_tensor(out)
            batch_idx += 1
        if accumulator and not self.drop_last:
            arr = np.concatenate(accumulator)
            if rng is not None:
                rng.shuffle(arr)
            yield _to_tensor(arr)
