# tset-core

[![Crates.io](https://img.shields.io/crates/v/tset-core.svg)](https://crates.io/crates/tset-core)
[![docs.rs](https://docs.rs/tset-core/badge.svg)](https://docs.rs/tset-core)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/humancto/tset/blob/main/LICENSE)

The reference Rust implementation of the **TSET** binary format — the
*receipts layer* for LLM training data.

`tset-core` gives you `Reader`, `Writer`, `Dataset`, the sparse Merkle
tree, the signed audit log, and the v0.3.2 binary section codecs. It
has no Python dependency; the `tset_rs` PyO3 wheel and the `tset` CLI
sit on top of this crate.

## Install

```toml
[dependencies]
tset-core = "0.3"
```

## Quickstart

```rust
use tset_core::{Reader, Writer};
use tset_core::tokenizers::ByteLevelTokenizer;

let mut w = Writer::create("corpus.tset", None);
w.add_document(b"alpha document")?;
w.add_tokenizer_view(Box::new(ByteLevelTokenizer))?;
w.close()?;

let r = Reader::open("corpus.tset")?;
for (tokens, doc_hash) in r.open_view("byte-level-v1")?.iter_per_doc()? {
    // tokens: Vec<u32>, doc_hash: [u8; 32]
}
```

## What ships in this crate

- `Reader` / `Writer` for v0.3.2 shards
- `Dataset` / `DatasetWriter` for multi-shard corpora
- `SparseMerkleTree` with inclusion + non-inclusion proofs
- `AuditLog` with Ed25519 signing
- TSMT / TLOG / TCOL section codecs
- `ObjectStore` integration for cloud reads (S3, GCS)

## Resources

- **Homepage:** <https://humancto.github.io/tset/>
- **Spec:** [SPEC.md](https://github.com/humancto/tset/blob/main/SPEC.md)
- **Measured benchmarks:** [SCALING.md](https://github.com/humancto/tset/blob/main/examples/datasets/SCALING.md)

## License

MIT.
