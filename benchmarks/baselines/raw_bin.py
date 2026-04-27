"""Raw .bin baseline (à la nanoGPT).

A flat uint32 token stream with no provenance, no metadata, no boundaries.
This is the lower-bound storage benchmark — TSET must demonstrate it doesn't
overshoot this by more than the §6 success metric (< 25% v0.2, < 15% v1).
"""

from __future__ import annotations

import os
import time

import numpy as np

from benchmarks.harness.corpus import CorpusRecord


def write_raw_bin(records: list[CorpusRecord], path: str) -> dict:
    os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
    arrs: list[np.ndarray] = []
    t0 = time.perf_counter()
    for r in records:
        arr = np.frombuffer(r.text.encode("utf-8"), dtype=np.uint8).astype(np.uint32, copy=True)
        arrs.append(arr)
    flat = np.concatenate(arrs)
    flat.tofile(path)
    t1 = time.perf_counter()
    return {
        "format": "raw_bin",
        "path": path,
        "size_bytes": os.path.getsize(path),
        "num_tokens": int(flat.size),
        "write_seconds": round(t1 - t0, 4),
    }
