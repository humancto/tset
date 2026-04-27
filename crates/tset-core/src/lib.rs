//! Reference Rust implementation of the TSET binary format.
//!
//! Reader-side primitives only in this crate revision. Writer support
//! lands in a follow-up. The crate has no Python dependency; PyO3
//! bindings live in the sibling `tset-py` crate.

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
pub mod columns;
pub mod mixture;
pub mod reader;
pub mod writer;
pub mod dataset;

pub use error::{TsetError, TsetResult};
pub use reader::Reader;
pub use writer::Writer;
