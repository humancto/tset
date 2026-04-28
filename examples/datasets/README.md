# Showcase datasets

End-to-end demonstrations of the TSET binary format on real public corpora.
Each subdirectory ingests one dataset, converts it to TSET, exercises every
guarantee the format claims (inclusion proofs, non-inclusion proofs, signed
audit log, byte-identical re-tokenization, tamper detection), and writes a
`RESULTS.md` with measured numbers.

These are **examples**, not unit tests of the library. The unit tests live
under `python/tests/` and `crates/tset-core/tests/`. The new test files
under `python/tests/showcase/` exercise these examples against real data so
the receipts pitch is dramatized concretely.

## Available datasets

| Dataset | Source | License | Why it's here |
|---|---|---|---|
| [`tinyshakespeare/`](tinyshakespeare/) | karpathy/char-rnn on GitHub | Public domain / CC0-1.0 | Canonical small-scale ML text corpus (~1 MB) — natural language with strong line/structure signal. |
| [`click_source/`](click_source/) | Pallets/Click on GitHub | BSD-3-Clause | Real Python source code (~340 KB) — different vocabulary distribution from prose. |
| [`synthetic_stream/`](synthetic_stream/) | Generated locally | CC0-1.0 | 10k-document deterministic stream for throughput benchmarks. Reproducible offline. |

## What each subdirectory contains

Same shape across all three:

```
<dataset>/
  README.md         -- what this dataset is and why we picked it
  LICENSE.txt       -- attribution + SPDX
  download.py       -- idempotent fetch into the content-addressed cache
  convert.py        -- raw source → JSONL → TSET (with binary sections)
  prove.py          -- generate inclusion + non-inclusion proofs end-to-end
  bench.py          -- size + speed numbers vs JSONL/Parquet
  RESULTS.md        -- committed numbers from the latest run
  out/              -- gitignored build artifacts
```

## Running

```bash
# One dataset
python -m examples.datasets.tinyshakespeare.download
python -m examples.datasets.tinyshakespeare.convert
python -m examples.datasets.tinyshakespeare.prove
python -m examples.datasets.tinyshakespeare.bench

# All of them
make showcase
```

The first run downloads source files into `.cache/by-sha256/`; subsequent
runs are offline. SHA-256 is verified before any byte enters the cache, so
poisoned mirrors fail loudly.

## Adding a new dataset

1. Pick a source with a permissive, redistributable license.
2. Register the license in `_lib/licenses.py` (the registry is enforced;
   downloads without a registered license are refused).
3. Add a subdirectory with the six files above.
4. Add a row to the table here and to `python/tests/showcase/`.
