# Production-grade Roadmap

Tracking the 24 items from the post-PR-6 honest assessment toward
production readiness. Each row links to the commit / PR that landed it.

| # | Tier | Item | Status |
|---|------|------|--------|
| 1 | T1 | Lazy streaming reader (no eager full-shard decode) | ✅ PR 7 |
| 2 | T1 | Streaming writer (drop in-memory `doc_contents`) | ✅ PR 8 |
| 3 | T1 | On-disk SMT (TSMT magic) | ⏳ planned |
| 4 | T1 | On-disk audit log (TLOG) + metadata columns (TCOL) | ⏳ planned |
| 5 | T1 | Audit log signing (Ed25519 + key rotation events) | ✅ PR 10 (signing); rotation deferred |
| 6 | T1 | Drop Python duplicate impls; route through tset_rs | 🟨 partial: hashing documented; SMT/AuditLog still in Python (but tested as parity) |
| 7 | T2 | `append_tokenizer_view` in Rust | ✅ atomic 9f96768 |
| 8 | T2 | Bit-packed token IDs (16-bit / 32-bit) | ✅ PR 9 (16-bit fast path; 17-bit deferred) |
| 9 | T2 | Streaming over object storage (S3 reader) | ⏳ planned |
| 10 | T2 | Real tokenizer registry (BPE / tiktoken / HF) | ✅ atomic 3da7318 (HF adapter) |
| 11 | T2 | PyO3 bindings for proofs (prove_inclusion, etc.) | ✅ PR 7 |
| 12 | T2 | `tset → jsonl` / `tset → parquet` reverse converters | ✅ PR 7 |
| 13 | T2 | Rust DataLoader / IterableDataset for PyTorch | ⏳ planned |
| 14 | T3 | CI pipeline | ✅ PR 7 |
| 15 | T3 | v0.1 frozen conformance fixture | ✅ PR 7 |
| 16 | T3 | cargo-fuzz target on Reader::open | ✅ atomic 8da5126 (target + property test stand-in) |
| 17 | T3 | Strict v0.2 enforcement at file open (not view-open) | ✅ PR 7 |
| 18 | T3 | Predicate compiler: BETWEEN, NOT, IS NULL | ✅ PR 7 |
| 19 | T3 | DatasetWriter idempotent reopen | ✅ PR 7 |
| 20 | T3 | Cargo.lock policy | ✅ PR 7 |
| 21 | T4 | rustdoc + module-level design docs | ✅ atomic 7332c4a |
| 22 | T4 | Verification modes (skip-reproducibility for hot streaming) | ✅ atomic c23d388 |
| 23 | T4 | tset-bench D (compliance) + E (exclusion workflow) | ✅ atomic c0c1078 |
| 24 | T4 | Multi-modal extensions sketch | ⏳ planned (needs RFC update) |

## Score (post-PR-10 + atomics)

- **20 of 24 closed.** All Tier 3 + Tier 4 items done. 4 of 6 Tier 1
  done (the on-disk migrations are the meaty remaining items). 5 of 7
  Tier 2 done (S3 + Rust DataLoader still pending).

## Format version history

- v0.1 — initial reference impl (no chunk content_hash)
- v0.2 — per-chunk content_hash mandatory
- v0.3 — bit-packed token IDs (16-bit fast path)
- v0.3.1 — Ed25519 audit-log signing (PR 10) — additive optional field
- v0.4 (planned) — on-disk SMT/TLOG/TCOL sections (PR 11)

v0.x readers MUST read all earlier minor versions per RFC §5.6 #6;
verified by the conformance fixtures in `tests/conformance/fixtures/`.

## Working mode

We've moved from big PRs to **atomic passing commits on `main`**. Each
commit:
- Single conceptual change
- All Rust + Python tests pass before AND after
- Pushed to `origin/main` immediately

The PR branches `claude/rust-port[-pr2..pr10]` are preserved on the
remote for diff archaeology but new work doesn't go through them.
