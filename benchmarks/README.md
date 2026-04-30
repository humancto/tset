# TSET benchmarks

End-to-end benchmarks for the Python streaming path. There are three
layers, each answering a different question:

| Layer | Question | How to run |
|---|---|---|
| `crates/tset-bench/` (Rust criterion) | how fast is one hot path on the Rust core? | `cargo bench -p tset-bench` |
| `benchmarks.harness` (Python) | how does TSET compare to JSONL / raw bin / Parquet on storage + throughput at fixed corpus sizes? | `python -m benchmarks.harness --quick` |
| `benchmarks.stream_throughput` (Python, this doc) | what's the peak streaming RSS, and does it scale sub-linearly with corpus size? | `python -m benchmarks.stream_throughput --mb 256` |

## Stream throughput

Measures write throughput, read throughput, compressed ratio, and
peak streaming RSS on a synthetic corpus.

```bash
# Quick smoke (~5 seconds, 64 MB raw)
python -m benchmarks.stream_throughput --mb 64

# CI-friendly default (~30 seconds, 256 MB raw)
python -m benchmarks.stream_throughput --mb 256

# Production-scale (~3-5 min, 1 GB raw)
python -m benchmarks.stream_throughput --mb 1024 --json bench-1gb.json
```

The script enforces a memory bound: streaming RSS must be less than
`4 × manifest_size + shard_size + 128 MB`. The bound is sub-linear
in corpus size — doubling the input doesn't double the RSS, only the
manifest fraction. See the script's module docstring for the full
derivation.

## Baseline numbers

Captured on a Linux x86_64 dev container (no swap) with
`TSET_PREFER_RUST=0` (pure Python streaming path). The Rust path is
typically 2-3× faster on read; both paths have the same memory
profile.

| target | docs | raw    | on-disk | ratio | write    | stream    | peak RSS |
|--------|------|--------|---------|-------|----------|-----------|----------|
| 32 MB  | 8 K  | 32 MB  | 6.3 MB  | 0.197 | 4.8 s    | 0.28 s    | 85 MB    |
| 64 MB  | 16 K | 64 MB  | 12.6 MB | 0.197 | 9.8 s    | 0.83 s    | 124 MB   |
| 128 MB | 33 K | 128 MB | 25.3 MB | 0.198 | 20.8 s   | 2.6 s     | 197 MB   |
| 256 MB | 66 K | 256 MB | 50.7 MB | 0.198 | 40.5 s   | 9.0 s     | 326 MB   |
| 512 MB | 131 K| 512 MB | 101 MB  | 0.198 | 99.3 s   | 33.1 s    | 588 MB   |

Throughput on a single thread holds around 6 MB/s raw write and
113 MB/s tokens read at the larger sizes. Numbers will look
considerably better with the Rust hot path enabled.

The streaming RSS scales roughly linearly with corpus *manifest
size*, not corpus *bytes* — at byte-level vocab the manifest is
dominated by the per-document `source_map`, which has one entry per
document. A tokenizer with longer chunks (BPE typically 3-5× shorter
than byte-level) shrinks the manifest proportionally.

## Why streaming isn't O(1)

The threat model says streaming RSS is "O(block_size + chunk_size)".
At small corpus sizes that holds; at hundreds of MB and up, the
manifest decode dominates. The current path is:

1. mmap the shard (no decode cost; pages fault in lazily)
2. Read manifest bytes from the mmap (one-shot copy)
3. `json.loads` the manifest into a Python dict
4. Iterate `source_map`; for each entry, decode the chunks it spans

Step 3 is where the memory inflates: Python's `dict` representation
of the JSON is ~3-4× the raw bytes. The fix path lives in v0.4 and
beyond — encode `source_map` and `chunks` as binary sections analogous
to TSMT/TLOG/TCOL, parse them lazily without materializing a full
Python dict. That brings the streaming bound back to true
O(block_size + chunk_size) regardless of corpus size.

For now: document it honestly, ship a benchmark that locks the
current behavior, and treat any RSS regression as a bug.
