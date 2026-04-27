# Glossary

(Mirrors RFC §13 with implementation-level cross-references.)

- **Shard** — A single self-contained `.tset` file. Has its own `Header`,
  `Footer`, manifest, and `shard_merkle_root`. Implemented in
  `python/tset/writer.py` + `reader.py`.

- **Dataset** — A directory of shards under a `manifest.tset.json` root
  manifest, plus an `exclusions.json` overlay. Implemented in
  `python/tset/dataset.py`.

- **View** — A specific tokenization of the corpus identified by
  `tokenizer_id`. Stored as a `TVEW`-magic body section + manifest entry.
  Implemented in `python/tset/tokenizer_view.py`.

- **Block** — A unit of zstd-compressed document storage (target 8-16 MB
  compressed). `DBLK` magic. Implemented in `python/tset/document_store.py`.

- **Chunk** — A unit of token stream data (default 64K tokens) inside a
  view. Each chunk is independently zstd-compressed.

- **Source map** — Per-view, per-document record mapping a token range to
  its source `doc_hash`. Stored in the manifest in v0.1.

- **Sparse offset index** — Per-view seek points (default every 64K tokens)
  enabling O(log N) random access.

- **Reproducibility proof** — Tokenizer config hash plus a small precomputed
  test vector (token-bytes digest over a deterministic sample of documents).
  The reader retokenizes and compares on open. Detects drift, not semantic
  errors.

- **SMT** — Sparse Merkle Tree over document hashes. Inclusion proof
  demonstrates a document IS in the shard's snapshot; non-inclusion proof
  demonstrates a document IS NOT. See [`smt-design.md`](smt-design.md) for
  parameters.

- **Conforming reader** — A reader that implements every obligation in
  `SPEC.md` §7. The format defines conformance; non-conforming readers
  cannot be compelled to honour exclusions.

- **Snapshot** — A named version of the corpus pinned by an audit-log
  `version_snapshot` event. Used as the cryptographic anchor for proofs.

- **Audit log** — Append-only Merkle-chained log of provenance events
  (ingestion, exclusion, version snapshot, tokenizer addition).

- **Subset** — Predicate-defined slice of the corpus with a default mixture
  weight. See `python/tset/mixture.py`.
