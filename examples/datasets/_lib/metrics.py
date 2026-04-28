"""Tiny measurement helpers used by every showcase script."""

from __future__ import annotations

import time
from contextlib import contextmanager
from dataclasses import dataclass


def format_bytes(n: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    f = float(n)
    for u in units:
        if f < 1024.0 or u == units[-1]:
            return f"{f:.1f} {u}" if u != "B" else f"{int(f)} {u}"
        f /= 1024.0
    return f"{f} ?"


def format_duration(seconds: float) -> str:
    if seconds < 1e-3:
        return f"{seconds * 1e6:.0f} µs"
    if seconds < 1.0:
        return f"{seconds * 1e3:.1f} ms"
    if seconds < 60.0:
        return f"{seconds:.2f} s"
    m, s = divmod(seconds, 60.0)
    return f"{int(m)}m {s:.1f}s"


@dataclass
class Measurement:
    label: str
    seconds: float
    items: int = 0

    @property
    def per_second(self) -> float:
        return self.items / self.seconds if self.seconds > 0 else 0.0


@contextmanager
def measure(label: str):
    """Time a block of work. ``items`` is set after the fact:

    >>> with measure("ingest") as m:
    ...     n = ingest()
    ... m.items = n
    >>> print(m.seconds, m.per_second)
    """
    m = Measurement(label=label, seconds=0.0)
    t0 = time.perf_counter()
    try:
        yield m
    finally:
        m.seconds = time.perf_counter() - t0
