# Changelog

All notable changes to this project will be documented in this file.

## [0.3.1] — 2026-04-27

PR 10: Ed25519 audit-log signing.

### Threat model

Before PR 10 the audit log was tamper-evident only under the
assumption that the manifest itself wasn't rewritten. Anyone who
could rewrite the manifest could also rewrite the chained-hash log
to be self-consistent. PR 10 closes that gap by binding each entry
to a writer-controlled key. Tamper-evidence now holds even if the
manifest_hash + footer are recomputed by an adversary, as long as
the writer's secret key was never disclosed.

### `tset-core::signing` (new module)

- `AuditSigner::generate()` — fresh Ed25519 keypair from OS RNG.
- `AuditSigner::from_secret_bytes(&[u8])` — load existing key.
- `AuditSigner::sign(msg) -> [u8; 64]`
- `AuditSigner::public_key_bytes() -> [u8; 32]`
- `verify_signature(pk, msg, sig) -> bool` — fail-closed on
  malformed inputs (no panics).

### `AuditLog`

- `AuditLog::with_signer(AuditSigner)` — writer constructor that
  signs every appended entry.
- Each entry now carries an optional `signature: hex` field
  (Ed25519 over the entry_hash bytes).
- `writer_public_key: hex` published in the audit_log JSON when
  signing is enabled.
- `verify_audit_log` enforces:
  - if pubkey present, every entry MUST be signed and verify
  - if any entry signed but no pubkey, reject (signatures without
    a published key aren't trusted)
  - sig + chained-hash both validated per entry

### Writer

- `tset_core::Writer::create_with_options(path, shard_id, signer)`
  threads an optional signer through to the audit log.
- PyO3: `tset_rs.Writer(path, shard_id=None, signing_key=None)`
  accepts a 32-byte secret.
- New PyO3 module functions: `generate_signing_key()`,
  `signing_public_key(secret)`, `verify_audit_signature(pk, msg, sig)`.

### Python `AuditLog`

- `AuditLog.writer_public_key: str | None`
- `AuditEvent.signature: str | None`
- `verify()` calls into `tset_rs.verify_audit_signature` when a
  pubkey is published. (Pure-Python Ed25519 isn't bundled — using
  the FFI is the supported path.)

### Tests

- 6 new tests in `python/tests/test_pr10_signing.py`:
  signing-key shapes, signed-entry emission, tampered-signature
  rejection, drop-signatures downgrade rejection, signatures-without-
  pubkey rejection, unsigned writes still verify (backward compat).
- 3 new Rust unit tests in `signing.rs`.

### Test totals

- 39 Rust + 121 Python = 160

### What's next

- PR 11 will land on-disk binary sections (TSMT/TLOG/TCOL),
  S3 reader, Rust DataLoader, real tokenizer registry, multi-modal
  sketch, criterion benches D/E, rustdoc, and start dropping the
  Python duplicate impls now that signing is in place.

## [0.3.0] — 2026-04-27

PR 9: bit-packed token IDs — first format-version bump since v0.2.

### Format

- `version_minor = 3` (was 2). v0.3 readers MUST read v0.1, v0.2, v0.3.
- New per-view manifest field `bits_per_token: u8`. Currently `16` or
  `32`. The Rust writer chooses 16 when `vocab_size ≤ 65536` (covers
  all byte-level + most whitespace-hashed configs); else 32.
- v0.1/v0.2 shards have no `bits_per_token` field; readers default to
  32 (matches the historic on-disk layout).

### Rust

- `tset_core::tokenizer_view::bits_per_token_for_vocab(u32) -> u8`
  selects the smallest width that fits.
- `read_chunk_with_bits` is the new entry point; old `read_chunk`
  becomes a thin wrapper that hard-codes 32 (callers that don't have
  bits_per_token in scope).
- `build_view` packs as 16-bit u16 LE when vocab fits, otherwise the
  existing 32-bit u32 LE.
- `Reader::open_view` reads `bits_per_token` from the manifest with
  default 32.

### Python

- `tset.constants.VERSION_MINOR = 3` (was 2).
- Python writer emits `bits_per_token: 32` (it never bit-packs; the
  field is required for v0.3 conformance).
- `tset.tokenizer_view.read_chunk` accepts `bits_per_token={16,32}`
  and unpacks via `np.frombuffer(dtype=np.uint16).astype(np.uint32)`
  on the 16-bit path.
- Reader threads `bits_per_token` through to `read_chunk`.

### Tests

- `python/tests/test_pr9_bitpacking.py`:
  - Rust writer emits `bits_per_token=16` for byte-level
  - Round-trip parity (Python and Rust readers decode the same tokens)
  - Storage win: 16-bit chunks measured at ~19% smaller than 32-bit
    chunks on a 50-doc repeated corpus (the headline 2× shrink is
    bigger but zstd absorbs much of it on small vocabularies; lock
    threshold at ≥ 15%)
  - Forward compat: v0.1, v0.2-equivalent (current), v0.3 fixtures
    are all openable

### Test totals

- 36 Rust + 115 Python = 151

## [0.2.6] — 2026-04-27

PR 8: streaming-writer pass for the Rust core.

### Rust writer

- **Drop the `doc_contents: HashMap<Hash, Vec<u8>>` member.** This was
  the writer's largest memory hog — kept an uncompressed copy of every
  document until `close()` to feed `build_view`. Replaced with a
  `doc_seen: HashSet<Hash>` for dedup; documents are re-read from the
  just-finalized doc-store body during view construction via a 1-block
  LRU cache. Working set is now O(block target bytes) regardless of
  corpus size.
- New helpers `read_doc_from_body` + `decompress_block_from_body` +
  `BlockCache` in `crates/tset-core/src/writer.rs`.

### Python hashing

- Documented that `tset.hashing` mirrors `tset_core::hashing` exactly
  and is verified by the conformance suite. We deliberately don't
  delegate to `tset_rs` here because the FFI cost on small
  per-document calls dominates.

### Tests

- New `python/tests/test_pr8_streaming.py`:
  - 50 MB corpus through the Rust writer (12.5k × 4 KB docs)
  - 5 MB corpus through the Python writer (still in-memory)
  - dedup verification with `doc_contents` gone

### Test totals

- 36 Rust + 111 Python = 147

### What's still next

- Python writer still keeps documents in memory (mirrors the old Rust
  behavior). PR 9 will land the same simplification on the Python
  side, OR the route-through-tset_rs migration that obviates the
  Python writer entirely.

## [0.2.5] — 2026-04-27

PR 7: production-readiness pass — Tier 1/2/3 items from ROADMAP.md.

### Reader

- **Lazy streaming**. `iter_per_doc` is now a true iterator with a
  bounded 2-slot LRU cache. Previously held every chunk in memory after
  decompression — fatal for multi-GB shards. New eager `iter_per_doc()`
  is a thin wrapper kept for the conformance suite; all new code uses
  `iter_per_doc_lazy`.
- **Strict v0.2 enforcement at file open**. When `version_minor >= 2`
  the reader rejects shards with any chunk missing `content_hash` at
  `Reader::open` time, not only when the view is opened. Closes the
  PR-3 review item left as deferred.

### Writer (Python)

- **Python writer now actually emits v0.2.** The `content_hash` field
  on each chunk was declared on the dataclass in PR 2 but never
  computed by `build_view`. Result: every Python-written shard has been
  silently v0.1 (with a v0.2 stamp) since PR 2. Fixed: computes
  `BLAKE3(compressed_payload)` per chunk and writes it to the manifest.
- `tset.constants.VERSION_MINOR` is now 2 (was hardcoded to 1 in the
  `Writer.close` Header constructor; replaced with the imported
  constant).
- Conformance fixtures regenerated; the v0.1 fixture builder now
  patches `build_view` to strip `content_hash` after the fact, since
  the writer no longer emits v0.1 by default.

### PyO3 bindings

- `Reader.smt_root() -> bytes`
- `Reader.prove_inclusion(doc_hash) -> (doc_hash_hex, [sibling_hex; 256])`
- `Reader.prove_non_inclusion(doc_hash) -> (...)` (raises if doc IS in shard)
- Module-level `verify_inclusion_proof` and `verify_non_inclusion_proof`
  for offline verification without a Reader handle.

### Predicate compiler

- Added `NOT`, `BETWEEN ... AND ...`, `IS NULL`, `IS NOT NULL` in both
  the Rust (`tset-core::columns`) and Python (`tset._predicate`) impls.
  Keyword list expanded to reject these as column names.

### DatasetWriter

- `DatasetWriter(root, load_existing=True)` reloads prior shard
  registrations + exclusions from `manifest.tset.json` so an existing
  dataset can be extended (add a shard, add an exclusion) without
  re-registering everything from scratch.
- `register_shard(name)` is now idempotent — calling it twice with the
  same name is a no-op.

### Reverse converters

- `tset.converters.tset_to_jsonl(src, dst)` — newline-delimited JSON
  output with optional doc_hash + metadata columns.
- `tset.converters.tset_to_parquet(src, dst)` — pyarrow-based round-trip.

### Conformance suite

- New v0.1 fixture (`fixture-v01-small.tset`) committed to git. Locks
  the "v0.2+ readers MUST read v0.1 shards" rule from RFC §5.6 #6.
  Build script: `tests/conformance/build_v01_fixture.py`.

### CI

- New `.github/workflows/ci.yml`:
  - rust job: `cargo fmt --check`, `cargo clippy -D warnings`,
    `cargo test --workspace --release`
  - python job: builds the maturin wheel, rebuilds conformance corpus,
    runs `pytest`
  - cli job: builds + tests `tset-cli` standalone

### Test totals

- 36 Rust tests
- 108 Python tests (was 94: +13 in test_pr7.py + v0.1 conformance + assertion fix)
- 5/5 trial stable

## [0.2.4] — 2026-04-27

PR 6: language-agnostic conformance suite + Writer adapter + spec freeze
annotations + README usage docs.

### Conformance suite

- New `tests/conformance/` directory with `build_corpus.py` that
  generates 3 reference shards plus per-shard `*.expected.json`
  invariants (version, manifest_hash hex, shard_merkle_root hex,
  manifest_size, document_count, per-view config_hash + total_tokens
  + num_chunks).
- `python/tests/test_conformance.py` runs the Python reader against
  every fixture; `crates/tset-core/tests/conformance.rs` runs the
  Rust reader against the same fixtures. Both must report identical
  invariants — any divergence is a conformance failure.
- Three fixtures: `fixture-empty` (single empty doc), `fixture-small`
  (3 docs + metadata + 2 views), `fixture-big` (100 deterministic
  docs).

### Writer adapter

- `tset.rust_writer.RustWriter` — drop-in replacement for `tset.Writer`
  that delegates to `tset_rs.Writer`. Accepts a `Tokenizer` instance
  the way the Python writer does and translates it to the
  `(id, vocab_size)` pair the Rust binding takes.
- 3 new tests in `python/tests/test_rust_writer_adapter.py`.
- The legacy Python writer is NOT removed — it stays for users who
  want to avoid the Rust toolchain. Adapter is the migration path,
  not a hard switch.

### SPEC freeze annotations

- Top of `SPEC.md` now declares per-section stability:
  - §2 Header, §3 Footer, §4 Document store, §5 Tokenization view,
    §7 Reader/writer obligations, §8 Manifest, §9 Out of scope:
    **frozen at v0.2**
  - §6 SMT: **design under review** (RFC §10 #14–16; pending
    cryptographer sign-off)

### README

- Replaced the thin Quickstart with a full Usage section covering:
  Python install, optional Rust core via maturin, CLI subcommands,
  Python API (Reader / Writer / `append_tokenizer_view` / multi-shard
  datasets), Rust API, and format converters.

### Test totals

- 36 Rust (was 35; +1 conformance integration test running 3 fixtures)
- 92 Python (was 86; +4 conformance + 3 RustWriter adapter)
- All stable across 5 trials

### What's still left

- v1.0 spec freeze: pending cryptographer sign-off on SMT params
  (RFC §10 #14–16) and at least one production deployment per
  RFC §6 v1 success metrics. **Code-side, this is a documentation
  exercise; signoff is a humans-in-the-loop activity.**
- Drop legacy Python writer/reader entirely (replace with shim that
  always delegates to Rust). Not done in PR 6 because it removes a
  fallback path and the conformance suite proves the impls agree —
  flipping the default is a separate decision.

## [0.2.3] — 2026-04-27

PR 5: format converters + HuggingFace adapter + criterion benchmarks.

### Format converters

- `tset.converters.webdataset_to_tset(tar_path, dst, tokenizer)` —
  reads WebDataset `.tar` shards via stdlib `tarfile` (no external
  dep). Groups files by stem; treats `<stem>.txt` as the document
  body and `<stem>.json` / `<stem>.cls` as metadata fields.
- `tset.converters.mds_to_tset(mds_dir, dst, tokenizer)` — bridges
  MosaicML Streaming datasets via the official `mosaicml-streaming`
  reader (lazy import; raises a runtime error with an install hint
  if missing).
- Both converters reuse the existing `Writer` so the v0.2 format
  invariants (per-chunk content_hash, audit log, SMT) all flow
  through automatically.

### HuggingFace integration

- `tset.converters.hf_dataset_view(tset_path)` returns a generator
  function suitable for `datasets.Dataset.from_generator`.
- `tset.converters.to_huggingface_dataset(tset_path)` lazy-imports
  `datasets` and materializes the shard as a HF `Dataset` with
  `text` + `doc_hash` columns.
- HF is an optional dependency — clear runtime error with install
  hint when missing.

### tset-bench (new crate)

- `crates/tset-bench` with criterion benches for the hot paths:
  - `streaming` — Reader::open + view.iter_per_doc throughput
  - `writer` — full shard write throughput; tokenizer-swap (RFC
    Benchmark C) timing two views vs one
  - `smt` — SMT insert / prove inclusion / prove non-inclusion at
    n ∈ {100, 1k, 10k}
- Run with `cargo bench -p tset-bench --bench <name>`. Not a
  workspace member of `cargo test` runs; explicitly invoked.

### Tests

- 7 new Python tests in `python/tests/test_converters_pr5.py`:
  WebDataset round-trip; WebDataset handles dotted-extension stems
  (`0001.metadata.txt` → stem=`0001.metadata`, ext=`txt`); WebDataset
  preserves non-UTF-8 bodies; WebDataset skips samples missing content;
  MDS without `mosaicml-streaming` raises RuntimeError with install hint;
  HF generator yields `text + doc_hash`; HF without `datasets` raises
  clear error.
- All criterion benches compile and run a smoke pass.

### Test totals

- 35 Rust + 86 Python = 121, all stable

### Deferred to PR 6

- Drop legacy Python writer/reader (re-export from tset_rs). The
  Python implementation has features the Rust core hasn't matched
  yet — DataLoader, audit-log signing, mixture sampler — so wholesale
  replacement risks regressions.
- v1.0 spec freeze + conformance test suite. Needs cryptography
  reviewer sign-off on SMT params (RFC §10 14-18) before locking.

## [0.2.2] — 2026-04-27

PR 4: Rust DatasetWriter + MetadataColumns + Mixture / Subset.

### Rust additions

- `tset-core::columns::MetadataColumns` with chunk-level statistics, full
  insertion order, and a tiny SQL-like predicate compiler (`=`, `!=`,
  `>`, `<`, `>=`, `<=`, `IN (...)`, `LIKE`, `AND`, `OR`, `(...)`).
  Byte-equivalent JSON output to Python's columns module.
- `tset-core::mixture::Subset` for named `(predicate, default_weight)`
  pairs persisted in the manifest under `subsets`.
- `tset-core::dataset::Dataset` and `tset-core::dataset::DatasetWriter`
  for multi-shard layout per RFC §5.8: root manifest
  (`manifest.tset.json`, sort_keys + indent=2 to match Python),
  exclusion overlay (`exclusions.json`), dataset Merkle root over
  `(shard_id, shard_hash, shard_smt_root)` leaves, full SMT-bound
  inclusion + non-inclusion proofs.
- `tset-core::Reader::smt_root` (was previously private to dataset).
- `tset-core::hashing::merkle_root_unsorted` (matches Python's
  `merkle_root` separate from the order-independent
  `shard_merkle_root`).

### Writer wiring

- `Writer::add_document_with_metadata(content, Option<&Map>)` accepts a
  per-document metadata dict that lands in the columnar section.
  `add_document(content)` is a thin wrapper.
- `Writer::add_subset(name, predicate, default_weight)` registers
  named mixture subsets.
- The manifest's `metadata_columns` and `subsets` fields now carry
  real values instead of empty placeholders.

### PyO3 surface

- New: `tset_rs.Dataset(path)` and `tset_rs.DatasetWriter(root)` with
  `shard_path / register_shard / add_exclusion / close` plus context
  manager.
- `tset_rs.Writer.add_document(content, metadata=None)` accepts a
  Python dict (serialized via stdlib `json.dumps` to JSON, parsed
  with `serde_json` on the Rust side).
- `tset_rs.Writer.add_subset(name, predicate, default_weight)`.

### Tests

- 5 new cross-impl tests in `python/tests/test_rust_dataset.py`:
  Rust DatasetWriter → Python Dataset shape; exclusion overlay
  round-trip; dataset Merkle root parity; metadata columns parity
  (including `filter_sql_like`); subsets persisted in manifest.
- 3 Rust unit tests for the predicate compiler (basic, parens,
  `IN`/`LIKE`).

### Test totals

- 33 Rust (was 30: +3 column unit tests)
- 75 Python (was 70: +5 cross-impl dataset tests)
- 5/5 trial stable

### Deferred to PR 5

- Drop legacy Python writer/reader; re-export from `tset_rs`. Held
  back because there are still Python-only paths (mixture sampler in
  the `DataLoader`, predicate cleanup edge cases) that would silently
  break tests if removed.
- Format converters: parquet, mds, webdataset.
- `tset-bench` (criterion) covering all five RFC benchmarks.

## [0.2.1] — 2026-04-27

PR 3: streaming throughput win + CLI + strict v0.2.

### Reader

- **Strict v0.2 enforcement.** `Reader::open` rejects v0.2 shards
  (`version_minor=2`) that lack `chunk.content_hash` on any chunk. v0.1
  shards continue to be accepted.

### Python adapter

- `tset.Reader.stream_tokens` now delegates the hot path to the
  Rust reader via `tset_rs.Reader.stream_tokens` when the optional
  `tset_rs` wheel is installed. Set `TSET_PREFER_RUST=0` to force the
  pure-Python path (used by one test that intentionally mutates the
  in-memory manifest, and useful for differential testing).
- Zero-copy reinterpretation of the Rust `Vec<u32>` token buffer as
  little-endian bytes via `PyBytes::new_bound` over a raw byte view.
  The format is little-endian-only, so this is sound on every
  supported architecture.

### Benchmark B

- Rewritten as a head-to-head: Python reader vs Rust-backed reader vs
  raw `np.fromfile` on the same shard, all in one run. Persists both
  numbers in the JSON result.
- Measured on a 200 MB synthetic corpus: **2.85× speedup** for the
  Rust-backed adapter over pure Python streaming
  (~55M tok/s vs ~19M tok/s). At 50 MB and below the overhead of
  `Reader::open`'s reproducibility check dominates and the two paths
  trade blows. Real benchmark B (multi-node S3) is still v0.3+.

### tset-cli (new crate)

- `crates/tset-cli` produces a single static `tset` binary built into
  `target/release/tset`. Subcommands: `inspect`, `verify`,
  `convert jsonl`, `version`, `help`.
- `--text-field`, `--tokenizer`, `--vocab` flags on `convert jsonl`.
- 4 end-to-end integration tests covering the round-trip and error paths.
- Intentionally argparse-free (no clap) for now — the option surface
  is small enough that hand-rolled parsing keeps the binary lean.

### Test totals

- 29 Rust tests (18 unit + 5 malformed-input fuzz + 2 roundtrip + 4 CLI)
- 70 Python tests (unchanged surface; suite green with delegation on)
- All stable across 5 trials

### Deferred to PR 4

- `DatasetWriter` (multi-shard) in Rust
- `MetadataColumns` + predicate compiler in Rust
- `Mixture`/`WeightedSampler` in Rust
- Drop legacy Python writer/reader (`python/tset/__init__.py`
  re-exports from `tset_rs`)
- Format converters: parquet, mds, webdataset
- `tset-bench` (criterion) covering all five RFC benchmarks A/B/C/D/E

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
