# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Gate 1 — Credibility (in progress)

- Initial repo scaffolding (RFC, SPEC, governance docs)
- Reference Python writer/reader for single-shard `.tset` files
- Document store with BLAKE3 content addressing
- Deterministic byte-level and whitespace tokenizers
- Tokenizer reproducibility proofs (drift detection)
- `add_tokenizer_view` append-only operation
- Benchmark harness with raw-bin and JSONL baselines
- Benchmarks A (storage efficiency) and C (tokenizer swap)

### Gate 2 — Differentiation (in progress)

- Sparse Merkle tree over document hashes (inclusion + non-inclusion proofs)
- Append-only Merkle audit log
- Metadata columns with predicate pushdown
- Multi-shard dataset layout + dataset-level exclusion overlay
- Mixture / subset definitions and `WeightedSampler`
- Benchmarks D (compliance query) and E (non-inclusion proof workflow)

### Gate 3 — Ecosystem (in progress)

- Pure-Python DataLoader (no hard `torch` dependency)
- JSONL converter (built-in via writer)
- Parquet converter (optional, requires `pyarrow`)

## [0.1.0] — 2026-04-27

Initial public RFC v0.4 + reference implementation per the §8.1 MVP scope.
