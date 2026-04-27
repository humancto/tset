# Rust Port — Plan & Status

This document tracks the migration of TSET's reference implementation from
pure Python to a Rust core with thin PyO3 bindings.

The RFC anticipates this in §9 risk table:

> *"Streaming throughput materially worse than MDS … mitigation: budget
> Rust core for v1 if needed."*

The v0.1 Python implementation validated the format design. The Rust core
delivers on the streaming throughput target and gives us a credible
language-agnostic reference impl.

## Architecture

```
tset/
├── crates/
│   ├── tset-core/        # pure Rust binary format (no_std-friendly, no Python)
│   │                     #   header, footer, hashing, manifest, document_store,
│   │                     #   tokenizer_view, smt, audit_log, reader, error
│   ├── tset-py/          # PyO3 bindings → tset_rs Python module
│   ├── tset-tokenizers/  # planned: Tokenizer trait + impls
│   ├── tset-converters/  # planned: jsonl, parquet, mds, webdataset
│   ├── tset-cli/         # planned: `tset convert | inspect | prove | verify`
│   └── tset-bench/       # planned: criterion benches A/B/C/D/E
├── python/
│   ├── tset/             # current pure-Python writer + reader (kept until
│   │                     # Rust writer lands; Python reader becomes the
│   │                     # fallback / cross-validation oracle)
│   └── tests/            # existing 56 tests + new test_rust_reader.py
│                         # cross-validation suite
└── target/               # cargo build artifacts (gitignored)
```

## What ships in this PR (PR 1 — reader skeleton)

- Cargo workspace with 6-crate plan; `tset-core` and `tset-py` populated
- `tset-core` reader-side primitives:
  - `header` + `footer` (encode + decode, version 0.1 / 0.2 compat)
  - `hashing` (BLAKE3 + domain-separated `shard_merkle_root`)
  - `manifest` (serde_json with `arbitrary_precision` so float lexical
    forms are byte-preserved across parse/serialize — load-bearing for
    audit-log canonical-JSON parity with Python)
  - `document_store` (zstd block decode, content-addressed lookup,
    block cache)
  - `tokenizer_view` (`read_chunk` with optional `content_hash`
    verification + vocab range check, `verify_view_header`)
  - `smt::verify_path` (proof verification side; insert/prove TBD with
    writer)
  - `audit_log::verify_audit_log` (chained-hash verifier with
    Python-byte-identical canonical JSON)
  - `Reader::open` (mmap-backed; verifies manifest hash, footer hash,
    shard merkle root, audit log integrity at open time)
- `tset-py` PyO3 binding exposing `Reader` with `documents()`,
  `stream_tokens()`, `has_document()`, `get_document()`,
  `view_total_tokens()`, `tokenizer_ids()`, `shard_merkle_root`
- `python/tests/test_rust_reader.py` — 5 cross-impl validation tests
  prove the Rust reader sees the same bytes the Python reader sees on
  the same shard, including tamper detection
- 10 Rust unit tests, 61 Python tests — all green

## What's next (PR 2 — writer + features)

- `tset-core::Writer` (single shard) + `DatasetWriter` (multi-shard)
- `tset-core::SMT` insert + prove (currently only verify is in-tree)
- `tset-core::AuditLog::append` (currently only verify is in-tree)
- `tset-core::MetadataColumns` + predicate compiler
- `tset-core::Mixture` + `WeightedSampler`
- Per-chunk content hashing on writer (closes the v0.2 gap from the
  prior self-review)
- Drop the old Python `tset/{reader,writer,document_store,…}.py`
  modules; `python/tset/__init__.py` re-exports from `tset_rs`
- All existing Python tests pass against the Rust impl unchanged

## What's after (PR 3 — converters, CLI, benches)

- `tset-converters` crate: jsonl, parquet (arrow-rs), mds, webdataset
- `tset-cli` binary: `tset convert <format> <src> <dst>`,
  `tset inspect <path>`, `tset prove <hash> <path>`, `tset verify <path>`
- `tset-bench` (criterion) + driver to populate `benchmarks/results/`
  with realistic numbers — honest target for Benchmark B is
  60%+ of `np.fromfile` raw-bin baseline (vs ~4% in pure Python).

## Build & test

```bash
# Rust unit tests
cargo test --workspace --release

# PyO3 wheel (run from crates/tset-py/, not workspace root —
# maturin doesn't support workspace roots without a [package])
cd crates/tset-py && maturin build --release

# Install the wheel + run cross-validation tests
pip install --force-reinstall --user target/wheels/tset_rs-*.whl
cd python && pytest tests/
```

## Format compatibility

The Rust reader accepts both `version_minor=1` and `version_minor=2`.
v0.1 shards have no `chunks[].content_hash`; v0.2 shards do, and the
Rust reader verifies them when present. This matches the
forward-compatibility rule in RFC §5.6 #6.

## Notes for reviewers

- **`serde_json` `arbitrary_precision` is load-bearing.** Without it,
  serde_json's default `f64` Display drops trailing digits for some
  Python-`time.time()` floats (e.g. `1777295241.8341959` →
  `1777295241.834196`). Both round-trip to the same `f64` but the byte
  representations differ, so any chained-hash structure (audit log,
  manifest hash) breaks.
- **Why no proto schema yet.** RFC §10 #1 leaves manifest schema
  governance open; protobuf migration is a v0.3+ item. Today the Rust
  reader treats the manifest as a `serde_json::Value` view — same
  shape as the Python dict, with the same canonicalization semantics.
- **No tokenizer in Rust yet.** Reader doesn't *need* a tokenizer for
  reproducibility verification yet — that's deferred until the Rust
  writer lands and we can choose between (a) calling out to the
  Python tokenizer via PyO3 from Rust tests, or (b) porting the
  byte-level + whitespace tokenizers (trivial) and leaving HF/tiktoken
  for a later integration crate.
