# Production-grade Roadmap

Tracking the 24 items from the post-PR-6 honest assessment toward
production readiness. Each row links to the PR that lands it.

| # | Tier | Item | Status | PR |
|---|------|------|--------|-----|
| 1 | T1 | Lazy streaming reader (no eager full-shard decode) | shipping | PR 7 |
| 2 | T1 | Streaming writer (flush during add_document, not at close) | planned | PR 8 |
| 3 | T1 | On-disk SMT (TSMT magic) | planned | PR 9 |
| 4 | T1 | On-disk audit log (TLOG) + metadata columns (TCOL) | planned | PR 10 |
| 5 | T1 | Audit log signing (Ed25519 + key rotation events) | planned | PR 10 |
| 6 | T1 | Drop Python duplicate impls; route through tset_rs | planned | PR 8 |
| 7 | T2 | `append_tokenizer_view` in Rust | planned | PR 11 |
| 8 | T2 | Bit-packed token IDs (17-bit / 16-bit fast path) | planned | PR 9 |
| 9 | T2 | Streaming over object storage (S3 reader) | planned | PR 11 |
| 10 | T2 | Real tokenizer registry (BPE / SentencePiece / tiktoken / HF) | planned | PR 11 |
| 11 | T2 | PyO3 bindings for proofs (prove_inclusion, etc.) | shipping | PR 7 |
| 12 | T2 | `tset → jsonl` / `tset → parquet` reverse converters | shipping | PR 7 |
| 13 | T2 | Rust DataLoader / IterableDataset for PyTorch | planned | PR 11 |
| 14 | T3 | CI pipeline | shipping | PR 7 |
| 15 | T3 | v0.1 frozen conformance fixture | shipping | PR 7 |
| 16 | T3 | cargo-fuzz target on Reader::open | planned | PR 7 (manual fuzz first) |
| 17 | T3 | Strict v0.2 enforcement at file open (not just open_view) | shipping | PR 7 |
| 18 | T3 | Predicate compiler: BETWEEN, NOT, IS NULL | shipping | PR 7 |
| 19 | T3 | DatasetWriter idempotent reopen | shipping | PR 7 |
| 20 | T3 | Cargo.lock policy | shipping | PR 7 |
| 21 | T4 | rustdoc + module-level design docs | planned | PR 11 |
| 22 | T4 | Verification modes (skip-reproducibility for hot streaming) | planned | PR 11 |
| 23 | T4 | tset-bench D (compliance) + E (exclusion workflow) | planned | PR 11 |
| 24 | T4 | Multi-modal extensions sketch | planned | PR 11 |

PR plan:
- **PR 7** (in flight): bounded set of T2/T3/T4 items that don't need format changes.
- **PR 8**: streaming writer + drop Python duplicates.
- **PR 9**: on-disk SMT (TSMT) + bit-packed token IDs (mandatory v0.3 features).
- **PR 10**: on-disk audit log (TLOG) + on-disk columns (TCOL) + audit signing.
- **PR 11**: object storage reader + Rust DataLoader + tokenizer registry + multi-modal + verification modes + remaining benches + docs.

Format version bumps:
- PR 9 introduces `version_minor=3` (on-disk SMT, bit-packed tokens).
- PR 10 introduces `version_minor=4` (TLOG, TCOL, signing).
- v0.4 readers MUST read v0.1, v0.2, v0.3 shards per RFC §5.6 #6.
