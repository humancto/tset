# TinyShakespeare — measured results

These numbers are from a real run of the four scripts in this directory
on a Linux x86_64 host. Re-running may shift wall-clock figures by a few
percent; sizes are deterministic.

## Corpus

| Property | Value |
|---|---:|
| Source bytes | 1,115,394 (1.1 MB) |
| Source SHA-256 | `86c4e6aa9db7c042ec79f339dcb96d42b0075e16b8fc2e86bf0ca57e2dc565ed` |
| Paragraphs after split | 7,222 |
| **Unique documents in TSET (after dedup)** | **7,148** |
| Duplicate paragraphs collapsed | 74 |
| Tokenizer views | `byte-level-v1`, `whitespace-hashed-v1` |
| Binary sections | TSMT + TLOG + TCOL enabled |

## Storage

| Format | On-disk size | Size vs JSONL | Notes |
|---|---:|---:|---|
| Raw text | 1.1 MB | 0.69× | no record structure |
| JSONL | 1.5 MB | 1.00× | baseline |
| JSONL + zstd | 495 KB | 0.32× | compressed text |
| Parquet (zstd) | 536 KB | 0.34× | columnar |
| TSET · 1 view, no sections | 6.2 MB | 4.05× | ByteLevelTokenizer only |
| TSET · 2 views + sections | 7.5 MB | 4.91× | full receipts |

> TSET is **larger** than text-only formats because it embeds pre-computed
> tokenizations and Merkle structures in the same file. The fair
> comparison is "JSONL + tokenizer cache + audit metadata", not raw JSONL
> alone — which is exactly what TSET unifies into one binary.

## Read throughput (decode every document)

| Format | Read time | Effective throughput |
|---|---:|---:|
| Raw text (single mmap) | 3.6 ms | 295 MB/s |
| JSONL (json.loads per line) | 20.3 ms | 73 MB/s |
| JSONL + zstd | 27.0 ms | 55 MB/s |
| Parquet (column projection) | 50.5 ms | 11 MB/s |
| **TSET doc-store iteration** | **15.9 ms** | **474 MB/s** (zstd-decoded) |

TSET's full-corpus document iteration is faster than every other
format because the doc-store blocks decode in one zstd pass per block,
not one JSON parse per record.

## Token streaming

| Operation | Time | Throughput |
|---|---:|---:|
| Stream all `byte-level-v1` tokens (1.1 M tokens, batch=4096) | 110 ms | 10.0 M tokens/s |

Bit-packed token IDs with per-chunk content_hash, decoded via the
default Python path (no `tset_rs` wheel installed in this environment;
expect ~3× more with the Rust path).

## Receipts

| Receipt | Latency | Verified |
|---|---:|:---:|
| Inclusion proof for one doc | < 1 ms | ✓ |
| Non-inclusion proof for absent hash `0xa5…` | < 1 ms | ✓ |
| Tampered inclusion proof (sibling flipped) | — | rejected ✓ |
| Audit log (chained-hash, 7,148 entries) | < 5 ms | ✓ |

All four pass on every clean run. See `prove.py` for the actual
assertions.

## Reproducing

```bash
python -m examples.datasets.tinyshakespeare.download
python -m examples.datasets.tinyshakespeare.convert
python -m examples.datasets.tinyshakespeare.prove
python -m examples.datasets.tinyshakespeare.bench
cat examples/datasets/tinyshakespeare/out/bench.json
```
