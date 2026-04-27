//! Reference Rust implementation of the TSET binary format.
//!
//! TSET is an open binary format for LLM training data. A **shard** is a
//! single self-contained `.tset` file with header, document store,
//! tokenization views, manifest, and footer. A **dataset** is a logical
//! collection of shards under a root manifest with a dataset-wide
//! exclusion overlay. See [`SPEC.md`] for the normative wire format and
//! [`RFC.md`] for the design pitch.
//!
//! ## Module map
//!
//! - [`header`] / [`footer`] — fixed-size envelope at offset 0 / end-of-file
//! - [`document_store`] — content-addressed zstd-compressed text blocks
//! - [`tokenizer_view`] — chunked u16/u32 token streams + per-doc source map
//!   + per-chunk content hash + sparse offset index
//! - [`tokenizers`] — `Tokenizer` trait + ByteLevel + Whitespace
//! - [`smt`] — Sparse Merkle Tree over BLAKE3 doc hashes (insert + prove +
//!   verify, fixed depth 256)
//! - [`audit_log`] — append-only chained-hash log; optional Ed25519 signing
//!   per entry (PR 10)
//! - [`signing`] — Ed25519 helpers used by the audit log
//! - [`columns`] — per-document metadata + a small SQL-like predicate
//!   compiler (`=`, `!=`, `<`, `>`, `<=`, `>=`, `IN`, `LIKE`, `NOT`,
//!   `BETWEEN`, `IS [NOT] NULL`, `AND`, `OR`, parens)
//! - [`mixture`] — named subset definitions
//! - [`manifest`] — typed view over the JSON manifest (with serde_json's
//!   `arbitrary_precision` so float lexical forms round-trip exactly)
//! - [`reader`] — mmap-backed `Reader` (lazy chunk decode + bounded LRU)
//! - [`writer`] — `Writer` (streaming doc store; reads docs back from
//!   the body during view build to avoid double-buffering)
//! - [`dataset`] — multi-shard `Dataset` + `DatasetWriter`
//! - [`error`] — single `TsetError` enum, no panics on malformed input
//!
//! ## Format versioning
//!
//! Major-only-breaking. `version_minor` bumps add optional or mandatory
//! additive fields without changing existing byte layout. Current is
//! v0.3 (bit-packed token IDs, optional via `bits_per_token`). v0.3
//! readers MUST read v0.1 + v0.2 + v0.3 shards. See `SPEC.md` top.
//!
//! ## Crates in this workspace
//!
//! - `tset-core` (this crate) — the spec implementation
//! - `tset-py` — PyO3 bindings exposing this crate to Python as `tset_rs`
//! - `tset-cli` — `tset` binary with `inspect` / `verify` / `convert`
//! - `tset-bench` — criterion benchmarks for the hot paths

pub mod constants;
pub mod error;
pub mod hashing;
pub mod header;
pub mod footer;
pub mod manifest;
pub mod document_store;
pub mod tokenizers;
pub mod tokenizer_view;
pub mod smt;
pub mod audit_log;
pub mod signing;
pub mod columns;
pub mod mixture;
pub mod object_store;
pub mod sections;
pub mod reader;
pub mod writer;
pub mod dataset;

pub use error::{TsetError, TsetResult};
pub use reader::Reader;
pub use writer::Writer;
