# Changelog

All notable changes to this project will be documented in this file.

## [0.2.0] — 2026-04-27

Rust port plus per-chunk content hashing.

### Format

- `version_minor = 2`. v0.2 readers accept v0.1 shards (forward-compat);
  v0.1 readers reject v0.2 (per RFC §5.6 #6).
- Per-chunk `content_hash` (BLAKE3 of compressed payload) is now mandatory
  in tokenization views. Readers MUST verify on read. Closes the chunk-body
  tamper gap from the v0.1 self-review.
- `python.tset.tokenizer_view.ChunkInfo` adds `content_hash: str | None`,
  defaulted None for v0.1 compat.

### Rust port (PR 2 — writer + features)

- `tset-core::Writer`, `DocumentStoreWriter`, `tokenizer_view::build_view`
  (chunked tokens + source map + sparse index + per-chunk content hashing)
- `tset-core::SparseMerkleTree` insert + prove (matches Python wire format
  exactly: `LEAF_PREFIX=0x10`, `INTERNAL_PREFIX=0x11`, MSB-first bit order)
- `tset-core::AuditLog::append` with chained-hash construction
- `tset-core::tokenizers`: `Tokenizer` trait + `ByteLevelTokenizer` +
  `WhitespaceTokenizer` + `reproducibility_test_vector` /
  `verify_reproducibility`
- `tset-core::Reader` now runs the **full** reproducibility check on open
  (replaces the partial check from PR 1)
- `tset-py` exposes `Writer` to Python (`add_document`,
  `add_tokenizer_view`, `close`)
- New cross-impl tests in `python/tests/test_rust_writer.py`:
  - Rust writer → Python reader (4 tests)
  - Rust writer → Rust reader (1 test)
  - Whitespace tokenizer parity, content-hash presence, ordering invariant
- `crates/tset-core/tests/roundtrip.rs`: Rust writer ↔ Rust reader

### Hashing

- `tset-core::shard_merkle_root` now sorts inputs (per SPEC §6),
  matching Python's order-independent semantics. The earlier Rust verifier
  hashed insertion order — that worked because PR 1 only verified
  Python-written shards (where the JSON-sort-keys made order alphabetical
  through a different path), but the Rust writer made the discrepancy
  visible.

### Test totals

- 25 Rust tests (18 unit + 5 malformed-input fuzz + 2 roundtrip)
- 68 Python tests (5 cross-impl Rust-reader + 7 cross-impl Rust-writer
  + 56 existing)
- All stable across 10 trials

### Deferred to PR 3

- DatasetWriter (multi-shard) in Rust
- MetadataColumns + predicate compiler in Rust
- Mixture / WeightedSampler in Rust
- Drop legacy Python writer/reader; re-export from tset_rs
- tset-converters (jsonl, parquet, mds, webdataset)
- tset-cli, tset-bench (criterion benches A/B/C/D/E)

## [0.1.0] — 2026-04-27

Initial public RFC v0.4 + reference implementation. Single batch covering the
v0.1 MVP scope (RFC §8.1) plus large portions of Gates 2 and 3.

### Spec & docs
- `RFC.md` — full v0.4 design pitch + PRD
- `SPEC.md` — normative v0.1 binary layout (header, footer, document blocks,
  tokenization views, manifest, conformance obligations)
- `spec/binary-layout.md`, `spec/smt-design.md`, `spec/glossary.md`
- `governance/GOVERNANCE.md`, `governance/RFC_PROCESS.md`
- `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`

### Gate 1 — Credibility (shipped)
- `python/tset/{header,footer,document_store,tokenizer_view,manifest,writer,reader}.py`
- Deterministic byte-level + whitespace-hashed tokenizers
- Tokenizer reproducibility proofs (config-hash + test-vector digest)
- `append_tokenizer_view` for in-place addition of a second view
- Benchmark harness with raw-bin and JSONL baselines
- Benchmarks A (storage efficiency) and C (tokenizer swap)

### Gate 2 — Differentiation (shipped)
- `python/tset/smt.py` — sparse Merkle tree with inclusion + non-inclusion
  proofs; manifest commits to `smt_root`
- `python/tset/audit_log.py` — append-only Merkle-chained provenance log
  (ingestion, exclusion, version snapshot, tokenizer added)
- `python/tset/columns.py` + `_predicate.py` — metadata columns with a small
  SQL-like predicate compiler for pushdown
- `python/tset/mixture.py` — predicate-defined subsets and `WeightedSampler`
- `python/tset/dataset.py` — multi-shard `Dataset` / `DatasetWriter` with
  dataset-level Merkle root and exclusion overlay; per-shard composition for
  dataset-wide non-inclusion proofs
- Benchmarks D (compliance query) and E (non-inclusion proof workflow)

### Gate 3 — Ecosystem (partial)
- `python/tset/dataloader.py` — pure-Python, PyTorch-`DataLoader`-shaped
  iterator; falls back to `numpy.ndarray` if `torch` is absent; deterministic
  shuffle seed derivation per RFC §10.11; rank/worker partitioning
- `python/tset/converters.py` — JSONL → TSET (built-in) and Parquet → TSET
  (optional `pyarrow`)
- MDS / WebDataset / HuggingFace `datasets` integrations are still pending —
  see roadmap section in `README.md`

### Tests
- 46 tests across `test_header_footer`, `test_smt`, `test_audit_log`,
  `test_writer_reader`, `test_dataset`, `test_columns_and_mixture`,
  `test_dataloader`, `test_converters`

### Known gaps vs. v0.2 targets
- Streaming throughput (Benchmark B) is far below the 70-80% of MDS target.
  v0.1 is single-process Python; multi-node S3 streaming is the v0.2 work.
- Tokenizer-swap speedup (Benchmark C) modest at small corpus size; the
  architectural win (no source re-read) remains, with bigger absolute deltas
  expected on multi-GB corpora and expensive tokenizers (BPE).
- SMT on-disk encoding is provisional — present-key set rides in the
  manifest pending the cryptography review called out in `spec/smt-design.md`.
