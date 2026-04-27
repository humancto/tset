"""Subset definitions and weighted sampling per RFC §5.5.

Subsets are predicate-defined slices of the corpus with default weights.
The `WeightedSampler` produces a deterministic, reproducible sequence of
document indices that follow the given weights without any physical
re-sharding of the underlying TSET shards.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from tset.columns import MetadataColumns


@dataclass
class Subset:
    name: str
    predicate: str
    default_weight: float

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "predicate": self.predicate,
            "default_weight": self.default_weight,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Subset":
        return cls(name=d["name"], predicate=d["predicate"], default_weight=float(d["default_weight"]))


class WeightedSampler:
    def __init__(
        self,
        subsets: list[Subset],
        columns: MetadataColumns,
        weights: dict[str, float] | None = None,
        seed: int = 0,
    ):
        if not subsets:
            raise ValueError("WeightedSampler requires at least one subset")
        self.subsets = subsets
        self.columns = columns
        self._weights = {s.name: (weights or {}).get(s.name, s.default_weight) for s in subsets}
        total = sum(self._weights.values())
        if total <= 0:
            raise ValueError("subset weights must sum to > 0")
        self._weights = {k: v / total for k, v in self._weights.items()}
        self._rows: dict[str, list[int]] = {}
        for s in subsets:
            self._rows[s.name] = columns.filter_sql_like(s.predicate)
        self._seed = seed

    def weights(self) -> dict[str, float]:
        return dict(self._weights)

    def members(self, subset_name: str) -> list[int]:
        return list(self._rows[subset_name])

    def sample(self, n: int, seed: int | None = None) -> list[int]:
        """Deterministic mixture sample. Uses a per-step seeded counter so
        the sequence is byte-identical across runs."""
        base_seed = seed if seed is not None else self._seed
        names = list(self._weights.keys())
        weights = [self._weights[n] for n in names]
        cum = []
        running = 0.0
        for w in weights:
            running += w
            cum.append(running)
        out: list[int] = []
        for step in range(n):
            digest = hashlib.blake2b(
                step.to_bytes(8, "little") + base_seed.to_bytes(8, "little", signed=False),
                digest_size=16,
            ).digest()
            r = int.from_bytes(digest[:8], "little") / 2**64
            idx = 0
            while idx < len(cum) - 1 and r > cum[idx]:
                idx += 1
            members = self._rows[names[idx]]
            if not members:
                continue
            pick = int.from_bytes(digest[8:16], "little") % len(members)
            out.append(members[pick])
        return out
