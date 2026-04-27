"""Per-document metadata columns with chunk-level statistics for predicate
pushdown.

v0.1 implements the in-memory + manifest-resident form. The on-disk columnar
section (`TCOL` magic) is reserved in constants.py but not emitted; columns
ride in the manifest for v0.1, with chunk statistics already enabling
pushdown. This is intentionally the smallest thing that demonstrates
predicate-driven document filtering for benchmarks D and the mixture sampler.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


@dataclass
class ColumnStats:
    min: Any
    max: Any
    null_count: int
    distinct_sample: list[Any]


class MetadataColumns:
    """Stores per-document metadata in column-major form.

    Documents are addressed by 0-based row index, parallel to the order of
    `Writer.add_document` calls. Each column has a logical type tag in
    {"string","categorical","int","float","bool"}. Chunk-level stats are
    computed on `compute_stats(chunk_size)`.
    """

    LOGICAL_TYPES = {"string", "categorical", "int", "float", "bool"}

    def __init__(self):
        self._columns: dict[str, list[Any]] = {}
        self._types: dict[str, str] = {}
        self._row_count = 0

    def declare(self, name: str, logical_type: str) -> None:
        if logical_type not in self.LOGICAL_TYPES:
            raise ValueError(f"unknown column type: {logical_type}")
        if name in self._columns:
            return
        self._columns[name] = [None] * self._row_count
        self._types[name] = logical_type

    def add_row(self, values: dict[str, Any]) -> int:
        for col, val in values.items():
            if col not in self._columns:
                self.declare(col, _infer_type(val))
        row_idx = self._row_count
        for col, ls in self._columns.items():
            ls.append(values.get(col))
        self._row_count += 1
        return row_idx

    @property
    def row_count(self) -> int:
        return self._row_count

    def names(self) -> list[str]:
        return list(self._columns.keys())

    def column(self, name: str) -> list[Any]:
        return self._columns[name]

    def filter(self, predicate: Callable[[dict], bool]) -> list[int]:
        out = []
        for i in range(self._row_count):
            row = {col: vals[i] for col, vals in self._columns.items()}
            if predicate(row):
                out.append(i)
        return out

    def filter_sql_like(self, expr: str) -> list[int]:
        """Tiny safe predicate language for benchmarks/D queries.

        Supported forms (joined by AND/OR, parenthesized):
            <col> = <literal>
            <col> != <literal>
            <col> IN (<lit>, <lit>, ...)
            <col> > <number>     (also <, >=, <=)
            <col> LIKE '<pattern>'   (% wildcard)

        Whitespace-insensitive; case-insensitive for keywords.
        """
        from tset._predicate import compile_predicate

        pred = compile_predicate(expr, self._types)
        return self.filter(pred)

    def compute_stats(self, chunk_size: int = 1024) -> dict[str, list[ColumnStats]]:
        out: dict[str, list[ColumnStats]] = {}
        for name, vals in self._columns.items():
            chunks = []
            for start in range(0, self._row_count, chunk_size):
                window = vals[start : start + chunk_size]
                non_null = [v for v in window if v is not None]
                stats = ColumnStats(
                    min=min(non_null) if non_null else None,
                    max=max(non_null) if non_null else None,
                    null_count=len(window) - len(non_null),
                    distinct_sample=list(set(non_null))[:32],
                )
                chunks.append(stats)
            out[name] = chunks
        return out

    def to_dict(self) -> dict:
        return {
            "row_count": self._row_count,
            "types": dict(self._types),
            "columns": {name: list(vals) for name, vals in self._columns.items()},
        }

    @classmethod
    def from_dict(cls, data: dict) -> "MetadataColumns":
        c = cls()
        c._row_count = data.get("row_count", 0)
        c._types = dict(data.get("types", {}))
        c._columns = {name: list(vals) for name, vals in data.get("columns", {}).items()}
        for name in c._columns:
            if name not in c._types:
                c._types[name] = "string"
        return c


def _infer_type(v: Any) -> str:
    if isinstance(v, bool):
        return "bool"
    if isinstance(v, int):
        return "int"
    if isinstance(v, float):
        return "float"
    return "string"
