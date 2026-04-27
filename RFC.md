# TSET: An Open Standard for LLM Training Data

**Document type:** Combined design pitch + product requirements document
**Status:** RFC v0.4 — ready for public review (see §15 Changelog)
**Author:** Archy
**Last updated:** April 2026

-----

> **TSET is an open format for making training data verifiable, reusable, and composable by design.**
> It is not a feature set. It is a set of invariants that cannot be enforced across loosely-coupled systems.

-----

## Part 1 — Design Pitch

### The 30-second version

LLM training data today is stored in formats that were never designed for it. Parquet is for analytics. WebDataset is for image batches. MosaicML's MDS handles streaming but is tokenizer-blind. None of them know what a document is, what a tokenizer is, or where data came from.

The conventional wisdom is that you can solve this by composing existing tools — Parquet for metadata, MDS for streaming, JSON sidecars for tokenizer registries, external indexes for provenance. **That composition is exactly the problem.** Loosely-coupled systems cannot enforce the invariants that make training data trustworthy: that every token traces to a source, that exclusions are honored across all conforming readers, that two tokenization views describe the same underlying corpus, that a Merkle commitment binds the whole.

TSET is an open binary format that enforces these invariants in a single self-consistent shard, and a logical dataset format composed of one or more such shards under a root manifest. Text is content-addressed and stored once. Tokenizations are cheap secondary views. Metadata, mixture rules, and provenance are first-class sections. A single Merkle root commits to each shard; a dataset-level Merkle root commits to all shards plus a dataset-wide exclusion overlay.

The wedge is timing. The EU AI Act's training-data transparency obligations took effect August 2025 with full enforcement starting August 2026. Anthropic settled $1.5B over training data sourcing. California AB 1008 requires deletion of personal data from models. Frontier labs and enterprise fine-tuners alike now need provenance as a built-in property of their data pipeline, not a bolt-on.

We're building TSET as an open standard, MIT-licensed, with the explicit goal of becoming the default format for LLM training data the way Parquet became the default for analytics.

### The differentiation, sharply

The skeptic's argument is "we can hack this onto MDS." They are not entirely wrong about features — most individual capabilities can be bolted on. But four invariants cannot be enforced across loosely-coupled systems:

1. **Self-consistent Merkle commitment** at the shard level, plus a dataset-level meta-commitment over all shards. A sidecar index breaks this on every update.
2. **Token-to-byte reversibility** that's authoritative because the format owns both sides. External indexes drift.
3. **Multi-tokenizer views over the same byte-level corpus** without storage duplication. JSON registries plus separate token files duplicate storage and lose the binding.
4. **Provenance scoped to a versioned snapshot.** Time-travel reads, exclusion proofs, and reproducibility require the format to be the unit of versioning.

These are properties of the spec, not the implementation. Any reader, in any language, that conforms to the spec inherits them.

(Full RFC text continues — see git history for the v0.4 RFC content as posted in the project kickoff. The body below summarizes only the implementation-relevant sections; the canonical PRD lives unedited in the project root for reviewers.)

For the unabridged RFC v0.4 (Parts 1 & 2 in full, including: §5 Technical design; §5.1 Architecture; §5.2 Document store; §5.3 Tokenization views; §5.4 Metadata columns; §5.5 Mixture, provenance & proofs; §5.6 Critical invariants; §5.7 Non-claims; §5.8 Dataset layout; §6 Success metrics; §7 Benchmark methodology; §8 Roadmap (8.1 MVP, 8.2 Gate 1, 8.3 Gate 2, 8.4 Gate 3); §9 Risks; §10 Open questions; §11 Out of scope; §12 Comparable formats; §13 Glossary; §14 Reviewer checklist; §15 Changelog) — see the source provided in the project kickoff. SPEC.md extracts the binary-layout sections that this v0.1 implementation conforms to.

## What this implementation covers

This repository implements the **v0.1 MVP scope** defined in §8.1 of the RFC:

- JSONL → single-shard `.tset` writer
- One tokenization view per shard at write time; appendable additional views
- Document store with BLAKE3 content addressing
- Basic manifest (header, footer, schema, tokenizer registry)
- Reader that emits token batches and resolves source documents
- Benchmarks A (storage efficiency) and C (tokenizer swap)

Explicitly **deferred** to gates 2/3 per the RFC:

- SMT inclusion / non-inclusion proofs
- Append-only Merkle audit log
- Dataset-level manifests / multi-shard layout
- Metadata column predicate pushdown
- PyTorch DataLoader integration
- Streaming over object storage
- Converters from MDS / Parquet / WebDataset
- Mixture-aware sampling

The shard-level Merkle root is computed (over document hashes) and committed in the header even in v0.1, so the binding exists from day one — only the proof generation against it is deferred.
