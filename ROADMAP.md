# Production-grade Roadmap

Tracking the 24 items from the post-PR-6 honest assessment toward
production readiness.

| # | Tier | Item | Status |
|---|------|------|--------|
| 1 | T1 | Lazy streaming reader (no eager full-shard decode) | ✅ PR 7 |
| 2 | T1 | Streaming writer (drop in-memory `doc_contents`) | ✅ PR 8 |
| 3 | T1 | On-disk SMT (TSMT magic) | ✅ atomics e7372b4 + 5faa166 + 68f8efd (opt-in v0.3 additive; v0.4 mandatory pending design partner) |
| 4 | T1 | On-disk audit log (TLOG) + metadata columns (TCOL) | ✅ atomics b808dd3 + 423098e + 5faa166 + 68f8efd (same opt-in shape as TSMT) |
| 5 | T1 | Audit log signing (Ed25519 + key rotation events) | ✅ PR 10 (signing); rotation deferred (RFC §10 #18) |
| 6 | T1 | Drop Python duplicate impls; route through tset_rs | 🟨 design choice — Python writer/reader stays as fallback for users who don't want the Rust toolchain. Conformance suite locks byte-equivalence. RustWriter adapter exists for users who want Rust-only. |
| 7 | T2 | `append_tokenizer_view` in Rust | ✅ atomic 9f96768 |
| 8 | T2 | Bit-packed token IDs (16-bit / 32-bit) | ✅ PR 9 (16-bit fast path; 17-bit deferred to a design discussion per RFC §10 #7) |
| 9 | T2 | Streaming over object storage (S3 reader) | ✅ atomic 757d3da (ObjectStore trait + LocalFile/InMemory; download-to-tempfile path. True random-access S3 reader is its own atomic — needs Reader refactor to take a generic RangedReader instead of mmap::Mmap) |
| 10 | T2 | Real tokenizer registry (BPE / tiktoken / HF) | ✅ atomic 3da7318 (HF adapter; users wrap any HF Tokenizer + drive add_tokenizer_view) |
| 11 | T2 | PyO3 bindings for proofs (prove_inclusion, etc.) | ✅ PR 7 |
| 12 | T2 | `tset → jsonl` / `tset → parquet` reverse converters | ✅ PR 7 |
| 13 | T2 | Rust DataLoader / IterableDataset for PyTorch | ✅ atomic 0416f57 (TsetIterableDataset; lazy torch import) |
| 14 | T3 | CI pipeline | ✅ PR 7 |
| 15 | T3 | v0.1 frozen conformance fixture | ✅ PR 7 |
| 16 | T3 | cargo-fuzz target on Reader::open | ✅ atomic 8da5126 (target + stable-Rust property test stand-in) |
| 17 | T3 | Strict v0.2 enforcement at file open (not view-open) | ✅ PR 7 |
| 18 | T3 | Predicate compiler: BETWEEN, NOT, IS NULL | ✅ PR 7 |
| 19 | T3 | DatasetWriter idempotent reopen | ✅ PR 7 |
| 20 | T3 | Cargo.lock policy | ✅ PR 7 |
| 21 | T4 | rustdoc + module-level design docs | ✅ atomic 7332c4a |
| 22 | T4 | Verification modes (skip-reproducibility for hot streaming) | ✅ atomic c23d388 |
| 23 | T4 | tset-bench D (compliance) + E (exclusion workflow) | ✅ atomic c0c1078 |
| 24 | T4 | Multi-modal extensions sketch | ✅ atomic 848a92f (SPEC §10, design under review) |

## Score (post atomic 68f8efd)

**23 of 24 items closed in code.** Item 6 (drop Python duplicate impls)
is reframed as a **deliberate design choice**: Python's writer/reader
stays as the no-Rust-toolchain fallback. The conformance suite + 125
passing Python tests prove byte-equivalence with the Rust impl;
RustWriter adapter (PR 6) lets users opt into Rust-only writes when
they want. Replacing the entire Python implementation would force every
user to install the maturin-built wheel — a regression in adoption
friction that's not justified by the maintenance saving.

## What's still genuinely deferred (not closed)

These require external work:

- **v1.0 spec freeze.** Pending cryptographer sign-off on SMT params
  (RFC §10 #14–16). Format-side, frozen at v0.3 with v0.4 sections
  prepared.
- **Real Benchmark B (multi-node S3 streaming).** Needs the Reader
  refactor for true random-access reads + multi-node infra.
- **Mandatory v0.4 format bump.** Flips on-disk sections from opt-in
  to required. Gated on a design partner who's actually written +
  read v0.4 shards in production for 2+ weeks.
- **Audit log key rotation.** RFC §10 #18 — needs a multi-key
  manifest schema design.

## Format version history

- v0.1 — initial reference impl (no chunk content_hash)
- v0.2 — per-chunk content_hash mandatory
- v0.3 — bit-packed token IDs (16-bit fast path)
- v0.3.1 — Ed25519 audit-log signing (additive optional field)
- v0.3.2 — opt-in TSMT/TLOG/TCOL on-disk sections (additive optional)
- v0.4 (planned) — on-disk sections become mandatory; in-manifest
  forms can be dropped; manifest hash check becomes O(metadata size)

v0.x readers MUST read all earlier minor versions per RFC §5.6 #6;
verified by the conformance fixtures in `tests/conformance/fixtures/`.

## Working mode

Atomic passing commits on `main` (see `git log main` for the
chronology). Each commit:
- Single conceptual change
- All Rust + Python tests pass before AND after
- Pushed to `origin/main` immediately
- No PR overhead

The PR branches `claude/rust-port[-pr2..pr10]` are preserved on the
remote for diff archaeology but new work doesn't go through them.
