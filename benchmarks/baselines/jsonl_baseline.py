"""JSONL baseline.

Plain JSONL on disk with metadata sidecar — the "before" state for many
teams considering TSET. Re-tokenization requires a full re-read.
"""

from __future__ import annotations

import os
import time

from benchmarks.harness.corpus import CorpusRecord, write_jsonl


def write_jsonl_baseline(records: list[CorpusRecord], path: str) -> dict:
    t0 = time.perf_counter()
    bytes_written = write_jsonl(records, path)
    t1 = time.perf_counter()
    return {
        "format": "jsonl",
        "path": path,
        "size_bytes": os.path.getsize(path),
        "documents": len(records),
        "write_seconds": round(t1 - t0, 4),
    }
