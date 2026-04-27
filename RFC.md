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

### The problem, sharply

A typical AI lab's data pipeline today involves:

- Raw text in JSONL or Parquet on object storage
- A tokenization step that produces format-specific shards (MDS, raw bin, WebDataset tar)
- A separate metadata sidecar with quality scores, language IDs, dedup signatures
- An orchestration layer that mixes subsets at training time
- A compliance team that tries to reconstruct what was trained on, after the fact

Five practical pains follow:

1. **Re-tokenization is expensive and wasteful.** Switching from Llama 3 to Qwen tokenizer means re-reading a multi-terabyte corpus and recomputing token IDs.
2. **Provenance is a forensic exercise.** "Did model X train on document Y?" requires joining shards back to source records, often across deleted intermediate datasets.
3. **Mixture changes require re-sharding.** "Train on 30% code instead of 20%" means rebuilding the dataset.
4. **Document boundaries are lost in packed sequences.** Recovering them for evaluation, loss masking, or curriculum is awkward.
5. **Compliance reporting is manual.** Producing the EU AI Act's Public Summary Template requires bespoke tooling per dataset.

### The market opening

Parquet won the analytics format war because Hadoop needed columnar storage and nobody owned that intersection. Today, LLM training data has the same shape of opportunity:

- **No incumbent owns the full stack.** MDS owns streaming. Lance owns random access for ML. Parquet owns analytics. Raw bin owns researcher hacking. None own training-data-as-a-discipline.
- **A regulatory deadline is forcing the issue.** EU AI Act enforcement teeth come in August 2026.
- **The technical gaps are real and measurable.** Every problem listed above can be benchmarked, and the numbers are dramatic enough to drive adoption.

### Why open source, not a startup

The format only matters if it's adopted. Adoption requires the trust that only neutral, open governance provides. Frontier labs will not standardize on a vendor format. Regulators trust open formats more than proprietary ones. The path Confluent took with Kafka and Databricks took with Spark — open core, monetize later via managed offerings or consulting — is the right shape here, not a vertical SaaS.

### What success looks like in 24 months

- 10K+ GitHub stars
- Integrations in HuggingFace `datasets`, MosaicML `streaming`, PyTorch `DataLoader`
- 3+ frontier or near-frontier labs using TSET in production training pipelines
- A formal spec hosted at LF AI & Data or similar neutral foundation
- Cited as the reference format in at least one major regulator's technical guidance

### What success looks like in 6 months (the realistic near-term)

- A working v0.2 reference implementation with reproducible benchmarks against MDS, Parquet, and raw bin
- Two design partners running pilot training jobs on TSET
- A spec document detailed enough that a third-party engineer could build a compliant reader
- Public benchmarks showing 10x+ improvement on tokenizer-swap time and storage-with-provenance overhead under 20%

### Positioning statement

> TSET delivers competitive streaming throughput with strictly greater flexibility, verifiability, and compliance-workflow support than any existing format. It does this by enforcing in the spec what other systems require trust or convention to maintain.

-----

## Part 2 — Product Requirements Document

### 1. Overview

TSET (TrainSet) is an open binary format for LLM training data, defined at two levels:

- A **shard** is a single self-contained TSET file (described in §5.1) committing to its own Merkle root.
- A **dataset** is a logical collection of one or more shards under a root manifest (described in §5.8), committing to a dataset-level Merkle root and a dataset-wide exclusion overlay.

Single-shard datasets remain valid for small or single-author corpora; multi-shard datasets are the recommended layout at scale.

The format prioritizes, in this order:

1. **Provenance fidelity** — every token traceable to source, with formal inclusion and non-inclusion proofs at both shard and dataset level
2. **Tokenization flexibility** — text stored once, tokenized many ways, with reproducibility detection
3. **Mixture composability** — subset weights and filters at read time
4. **Streaming throughput** — competitive with MDS for distributed training
5. **Storage efficiency** — competitive with raw tokenized binaries

### 2. Goals and non-goals

**Goals:**

- Define an open binary format with a public, versioned specification
- Provide a reference Python writer/reader with PyTorch DataLoader integration
- Provide converters from MDS, Parquet, JSONL, and WebDataset
- Demonstrate measurable improvement on five reference benchmarks (defined in §7)
- Support cryptographic inclusion and non-inclusion proofs for the data pipeline
- Be extensible to multimodal data without breaking the v1 spec

**Non-goals (v1):**

- Not solving machine unlearning at the model-weight level — we support evidence for data-pipeline exclusion workflows only
- Not proving source authenticity (i.e., that data came from the claimed origin) — we prove integrity from ingestion onward
- Not replacing vector databases or embedding stores
- Not providing a general query engine — we provide reader-side predicate pushdown only
- Not handling streaming ingestion in the Kafka sense (TSET shards are append-mostly, not append-always)
- Not providing distributed write coordination beyond shard-level write isolation

### 3. Target users

**Primary (v1 design partners):**

- ML engineers at mid-market AI labs preparing pretraining or fine-tuning corpora
- Compliance and ML infrastructure teams at regulated enterprises (healthcare, fintech, legal) doing custom model training

**Secondary:**

- Frontier lab researchers running ablation experiments
- Open-source dataset publishers (RedPajama, FineWeb, etc.)
- Sovereign AI projects with statutory provenance requirements

**Tertiary:**

- Data marketplace operators needing verifiable delivery
- Academic researchers requiring reproducibility guarantees

### 4. User stories

**Story 1 — The tokenizer swap**

> As an ML engineer, I have a 500GB TSET dataset tokenized with Llama 3. I want to add a Qwen 3 tokenization view to each shard without re-reading source documents from S3.

**Story 2 — The compliance evidence workflow**

> As a compliance officer, I receive a GDPR Article 17 request and need to produce evidence that a specific document hash is not present in the most recent snapshot of our training corpus. I want to generate a non-inclusion proof from the dataset manifest within an hour, suitable as an input artifact to my organization's data-pipeline exclusion workflow. I do not expect TSET alone to satisfy any specific legal obligation; that determination is made by counsel.

**Story 3 — The mixture experiment**

> As a research scientist, I want to run an ablation that downweights code data from 20% to 10%. I expect this to be a config change, not a re-sharding job.

**Story 4 — The audit response**

> As an AI lab CTO, the EU AI Office requests our Public Summary Template for our latest GPAI model. I want to generate the technical fields from our TSET dataset manifest in one command. Legal review of the output remains with our compliance team.

**Story 5 — The dataset migration**

> As a platform engineer, my team has 10TB of training data in MDS shards. I want to migrate to TSET with a single CLI command and verify the migration losslessly.

### 5. Technical design

#### 5.1 Architecture (single shard)

A TSET shard is a self-contained, content-addressed file with the following sections, written in this order:

```
+--------------------------------------------------+
| HEADER                  (4 KB, fixed offset 0)   |
|   magic, version, manifest pointer,              |
|   shard Merkle root                              |
+--------------------------------------------------+
| DOCUMENT STORE                                   |
|   content-addressed, zstd-compressed text blocks |
|   primary unit: 8-16 MB compressed blocks        |
+--------------------------------------------------+
| TOKENIZATION VIEWS  (one or more)                |
|   per view: chunked token streams +              |
|   document-boundary streams + offset index +     |
|   tokenizer reproducibility proof                |
+--------------------------------------------------+
| METADATA COLUMNS                                 |
|   columnar storage with chunk-level statistics   |
|   and bloom filters for predicate pushdown       |
+--------------------------------------------------+
| MIXTURE + PROVENANCE LAYER (shard-local)         |
|   subset definitions, exclusion records,         |
|   sparse Merkle tree of doc hashes,              |
|   append-only audit log                          |
+--------------------------------------------------+
| MANIFEST                                         |
|   schema, offsets, tokenizer registry,           |
|   subset definitions (protobuf-encoded)          |
+--------------------------------------------------+
| FOOTER                  (40 bytes, fixed)        |
|   manifest size, manifest hash, magic            |
+--------------------------------------------------+
```

The header sits at byte 0 so streaming readers can begin parsing immediately. The manifest is referenced from the header and reachable from the footer for compatibility with seek-to-end readers.

#### 5.2 Document store

Documents are content-addressed by BLAKE3 hash. Identical documents ingested from multiple sources are stored once and referenced from each source record. Documents are grouped into compressed blocks of 8-16 MB compressed size to amortize zstd dictionary training and S3 GET overhead.

The document index maps `doc_hash → (block_id, offset_in_block, length)` and is itself stored as a sorted, mmap-friendly structure (a static B-tree or Eytzinger array).

#### 5.3 Tokenization views

Each view is identified by a `tokenizer_id` (e.g., `"llama3-128k-v1"`) registered in the manifest. A view contains:

- **Token stream:** bit-packed token IDs (17 bits for vocabularies up to 128K, configurable), grouped into chunks of ~64K tokens, zstd-compressed
- **Boundary stream:** RLE-encoded run lengths marking document boundaries within the packed stream
- **Source map:** for every chunk, an entry `(global_token_offset, doc_hash, byte_offset_in_doc)` enabling O(log N) seek and O(1) provenance lookup
- **Sparse offset index:** every Nth token, a record `(global_offset → chunk_id)` for fast random access
- **Tokenizer reproducibility proof:** a hash of the tokenizer config plus a fixed test-vector — N reference documents whose tokenization is precomputed and verified at read time.

**Scope of the reproducibility proof.** The proof detects configuration drift and common implementation mismatch. It does not prove semantic correctness of the tokenizer for every input, and it does not protect against an adversary who controls both the test vector and the tokenizer. Its purpose is to surface accidental divergence (different library versions, wrong config file, corrupted vocabulary) at read time rather than at audit time.

Adding a tokenizer to an existing shard appends a new view section and rewrites the manifest. The document store is untouched.

**Determinism requirement:** TSET only supports deterministic tokenizers. Tokenizers that depend on runtime state, randomness, or non-pinned dependencies are out of scope for v1.

#### 5.4 Metadata columns

Per-document metadata is stored columnar with predicate pushdown support. v1 reserves the following columns; users can add custom columns with arbitrary schemas:

- `source_url` (string, dictionary-encoded)
- `license` (categorical, dictionary-encoded)
- `lang` (categorical, dictionary-encoded)
- `quality_score` (float32)
- `dedup_cluster_id` (uint64)
- `ingestion_ts` (uint64, unix epoch)
- `perplexity_ref` (float32, optional)
- `capability_tags` (bit-packed flags)

Each column chunk carries min/max/null-count statistics and an optional bloom filter, mirroring Parquet's pushdown machinery.

#### 5.5 Mixture, provenance, and proofs

**This section describes the design direction. Specific cryptographic parameters are listed as open items in §10 and are explicitly marked "design under review" until resolved with a cryptography reviewer.**

The mixture log defines named subsets as predicates over metadata columns:

```yaml
subsets:
  - name: web
    predicate: "source_type = 'web' AND quality_score > 0.5"
    default_weight: 0.40
  - name: code
    predicate: "lang IN ('python', 'rust', 'go', 'typescript')"
    default_weight: 0.20
```

Readers expose a `WeightedSampler` that respects these weights without requiring physical re-sharding.

**Provenance structure.** The provenance layer is composed of two distinct cryptographic structures, deliberately separated because they answer different questions:

1. **Sparse Merkle tree (SMT) over the document hash space.** Keys are document hashes; leaves are presence bits. This structure supports both efficient inclusion proofs ("doc X is in this corpus") and efficient non-inclusion proofs ("doc X is *not* in this corpus"). The SMT root is committed in the shard header. *Design under review:* tree depth, empty-branch representation, sparse-tree compression strategy, and snapshot-versus-rolling semantics — see §10.
2. **Append-only Merkle log of provenance events** (Certificate Transparency-style). Events include `ingestion`, `exclusion`, and `version_snapshot`. Each event is signed by the writer's key. *Design under review:* signing scope, key rotation policy, log compaction — see §10.

A formal **non-inclusion proof** for a document hash is the SMT non-inclusion proof at a specific version snapshot, plus the log entry binding that version to the snapshot's SMT root.

**What provenance proves and does not prove:**

- ✅ A specific document hash *is* or *is not* present in a specific snapshot of the corpus (SMT)
- ✅ The corpus has not been silently modified since a given version snapshot (Merkle log)
- ✅ A token at a given offset maps to a specific document hash (source map)
- ❌ The data originally came from the claimed source URL (source authenticity is *not* in scope)
- ❌ The model trained on this corpus has "forgotten" any specific document (model-level unlearning is *not* in scope)

#### 5.6 Critical invariants

These are properties of the spec that any conforming reader must enforce. The format defines conformance; non-conforming readers cannot be compelled by the format.

1. **Content-addressing is total:** every document is keyed by its BLAKE3 hash; no document appears in a shard without an entry in the document index.
2. **Token-to-document mapping is total:** every token in every view maps to a `(doc_hash, byte_range)` in O(log N).
3. **Exclusion is monotonic:** once a document hash is marked excluded in a snapshot's SMT (at shard or dataset level), no conforming reader may return its tokens in any view at that snapshot or later.
4. **Tokenizer determinism is verifiable:** every view carries a reproducibility proof; conforming readers verify it on open.
5. **The manifest is the only mutable section:** appends to views, columns, or the provenance log result in a new manifest version; old manifests remain valid for time-travel reads.
6. **Versioning is forward-compatible:** v1 readers must reject v2+ files cleanly; v2+ readers must read v1 files with v1 semantics.

#### 5.7 What TSET does not prove (explicit non-claims)

To protect against misuse and overreach in compliance contexts, the spec explicitly disclaims:

- **Source authenticity.** TSET proves data hasn't been modified since ingestion. It does not prove data came from the source URL recorded in metadata. Users requiring source authenticity must layer signed provenance attestations from publishers.
- **Model-level unlearning.** TSET supports evidence for data-pipeline exclusion workflows. Whether a model trained on a TSET corpus has functionally forgotten a document is a model-weight question outside the format's scope.
- **Legal compliance with any specific regulation.** TSET provides primitives that *enable* compliance workflows. Whether a particular use satisfies GDPR Article 17, EU AI Act Article 53, or any other regulation is a determination for legal counsel and the relevant regulator.
- **Tokenizer semantic correctness.** The reproducibility proof detects configuration drift, not semantic errors in the tokenizer itself.

#### 5.8 Dataset layout

A TSET dataset is a logical collection of one or more shards under a root manifest. Recommended layout on object storage:

```
my-dataset.tset/
  manifest.tset.pb           # root dataset manifest (protobuf)
  shards/
    part-00001.tset          # self-consistent TSET shard
    part-00002.tset
    part-00003.tset
    ...
  proofs/                    # optional, cached precomputed proofs
    snapshot-2026-04-26/
      smt-roots.bin
      exclusion-overlay.bin
      ...
  benchmarks/                # optional, build-time benchmark artifacts
    storage.json
    throughput.json
```

**Key properties:**

1. **Each shard is independently readable.** A reader can open `part-00042.tset` and consume it without touching other shards or the dataset manifest. This preserves the single-shard "self-consistent file with a Merkle root" property.
2. **The root manifest commits to all shards.** The dataset-level Merkle root is computed over `(shard_id, shard_hash, shard_smt_root)` tuples, plus a dataset-level exclusion overlay. The dataset root is the cryptographic anchor for dataset-wide proofs.
3. **Exclusion is dataset-scoped.** A document hash excluded at the dataset level overrides inclusion at any shard level. Conforming readers consult the dataset manifest's exclusion overlay before returning any token, regardless of which shard they're streaming.
4. **Parallel writes are shard-isolated.** Each writer owns one or more shards; the root manifest is rewritten atomically when a write completes (analogous to Iceberg's manifest list pattern). v1 does not support concurrent writers to the same shard.
5. **Lazy materialization is supported.** A consumer that only needs 10% of the corpus can stream the relevant shards based on shard-level metadata predicates exposed in the dataset manifest, without downloading the whole dataset.
6. **Single-shard mode remains valid.** A single `.tset` file with no surrounding directory is a valid TSET dataset of size 1. The dataset manifest in that case is implicit and equals the shard manifest.

**Dataset-level proofs.** A non-inclusion proof at the dataset level is the composition of:

- The dataset Merkle proof showing which shards exist in the snapshot
- For each shard, an SMT non-inclusion proof showing the document hash is not present in that shard
- The dataset exclusion overlay showing the document hash is not subsequently included via overlay

This composition is verifiable in O(log S × log D) where S is the number of shards and D is the document hash space size.

### 6. Success metrics

Metrics are split into **v0.2 achievable** (first public release) and **v1 target** (frozen spec release, ~12 months).

**Technical metrics:**

| Metric                                        | v0.2 achievable | v1 target  |
|-----------------------------------------------|-----------------|------------|
| Storage overhead vs raw bin (with provenance) | < 25%           | < 15%      |
| Streaming throughput vs MDS                   | 70-80%          | 85-90%     |
| Tokenizer swap time vs re-export              | 10x faster      | 20x faster |
| Inclusion proof generation                    | < 100ms         | < 50ms     |
| Non-inclusion proof generation (dataset-level)| < 1s            | < 200ms    |
| Compliance query (full corpus scan)           | < 2 min         | < 60s      |
| Random sample access (over network)           | < 50ms p50      | < 20ms p50 |

The streaming throughput target is deliberately moderated. Hitting MDS parity on first release is unrealistic given the abstraction layers TSET introduces. The honest pitch is **competitive throughput with strictly greater flexibility and verifiability** — not equal-or-better throughput in isolation.

**Adoption metrics (12 month):**

| Metric                                  | Target    |
|-----------------------------------------|-----------|
| GitHub stars                            | 5,000+    |
| Production deployments (any scale)      | 10+       |
| Design partners with named case studies | 3+        |
| Upstream integrations (HF, MDS, Lance)  | 2+        |
| Spec document at neutral foundation     | Initiated |

### 7. Benchmark methodology

The benchmark harness is the single most important artifact for adoption. It must be built **before** the format itself and run on every PR.

**Reference corpus:** A 10GB subset of RedPajama-V2 covering web, code, books, and academic sources, with metadata sidecars including language ID, quality score, and source URL.

**Comparison formats:** MDS, Parquet (with sidecar metadata), WebDataset, raw tokenized binary, and TSET v0.x.

**The five reference benchmarks:**

**Benchmark A — Storage efficiency**
Compressed bytes per source byte, with and without metadata. Target: TSET within 25% of raw bin while carrying full provenance (v0.2); 15% (v1).

**Benchmark B — Streaming throughput**
Tokens/sec to a simulated GPU consumer over S3 with 8 workers per node, 4 nodes. Measure with and without shuffling. Target: 70-80% of MDS (v0.2); 85-90% (v1).

**Benchmark C — Tokenizer swap**
Wall-clock time to add a second tokenization view to an existing 10GB dataset across all shards. Compare against full re-export from JSONL. Target: 10x faster (v0.2); 20x (v1).

**Benchmark D — Compliance query**
Three queries:

1. "Find all tokens derived from documents matching `source_url LIKE '%nyt.com%'`"
2. "Produce a training stream excluding all docs with `quality_score < 0.3`"
3. "Generate the EU AI Act Public Summary Template fields from the dataset manifest"

Target: query 1 in under 5s, query 2 in under 2 min, query 3 fully automated.

**Benchmark E — Non-inclusion proof workflow**
End-to-end demonstration: ingest a multi-shard dataset including a marker document, train a tiny model, receive an exclusion request, produce a dataset-level non-inclusion proof, regenerate the training stream from a new snapshot, retrain, verify the marker document's tokens are absent. Target: end-to-end demo in under 10 minutes for a 1GB dataset across 4 shards.

Each benchmark emits a JSON result file consumed by a public dashboard rebuilt on every commit.

### 8. Roadmap

The roadmap has been restructured into three sequential gates. Each gate has a single goal; the next gate doesn't begin until the previous one is met. This is realistic for a solo part-time maintainer at ~15 hours/week.

#### 8.1 MVP surface area

This subsection pins what the *first runnable version* of TSET will and will not include. It is the answer to "what do I run first" for a GitHub visitor.

**The first runnable version (v0.1) supports:**

- JSONL → single-shard `.tset` writer
- One tokenization view per shard
- Document store with BLAKE3 content addressing
- Basic manifest (header, footer, schema, tokenizer registry)
- Reader that emits token batches
- Benchmarks A (storage efficiency) and C (tokenizer swap) only

**The first runnable version explicitly does *not* include:**

- SMT inclusion or non-inclusion proofs
- Append-only Merkle audit log
- Dataset-level manifests or multi-shard layout
- Metadata column predicate pushdown
- PyTorch DataLoader integration
- Streaming over object storage
- Converters from MDS, Parquet, or WebDataset
- Mixture-aware sampling

These are deferred to gates 2 and 3 below. The MVP exists to validate the format's storage and tokenizer-swap claims with real numbers, not to demonstrate the full vision.

**Recommended initial repository structure:**

```
tset/
  README.md              # one-page pitch + quickstart
  RFC.md                 # this document
  SPEC.md                # formal binary layout spec (extracted from RFC)
  CHANGELOG.md
  LICENSE                # MIT or Apache-2.0
  CONTRIBUTING.md
  CODE_OF_CONDUCT.md

  benchmarks/            # the comparison harness, built first
    harness/             # format-agnostic runner
    baselines/           # MDS, Parquet, raw bin
    results/             # JSON outputs + dashboard data

  python/                # reference implementation
    tset/
      __init__.py
      writer.py
      reader.py
      tokenizer_view.py
      manifest.py
    tests/
    examples/
      jsonl_to_tset.py
      tokenizer_swap_demo.py

  spec/                  # spec docs separated from RFC
    binary-layout.md
    smt-design.md        # marked "design under review"
    glossary.md

  governance/            # to be populated during gate 2
    GOVERNANCE.md
    RFC_PROCESS.md
```

A first-time visitor should be able to clone the repo, run `python -m benchmarks.harness --format raw_bin` against the included sample corpus, and see a baseline number — even before TSET itself is functional. That's the credibility-first ordering Gate 1 below codifies.

#### 8.2 Gate 1 — Credibility (Months 1-2)

**Single goal:** publishable benchmarks against existing formats, even before TSET exists in working form.

- Build the comparison harness in Python with format-agnostic interfaces
- Implement baseline runners for MDS, Parquet, raw bin
- Publish initial baseline numbers as a blog post — establishes credibility before TSET ships
- Begin minimal TSET writer/reader (single-shard, single view, no provenance)
- Run benchmarks A and C with v0.1
- **Exit criterion:** a published benchmark repo and one blog post that shows up in ML Twitter discussion

#### 8.3 Gate 2 — Differentiation (Months 3-5)

**Single goal:** demonstrate the invariants that no other format provides.

- Multi-shard dataset layout and root manifest
- Sparse Merkle tree provenance layer (per-shard and dataset-level)
- Tokenizer reproducibility proofs
- Append-only audit log
- Metadata columns with predicate pushdown
- Run benchmarks D and E
- Spec document v0.5 draft, posted publicly for comment
- Cryptography review of SMT design parameters
- Governance proposal drafted (not just deferred)
- **Exit criterion:** working dataset-level non-inclusion proof demo; spec draft has external comments from at least 3 reviewers including one cryptographer

#### 8.4 Gate 3 — Ecosystem (Months 6-12)

**Single goal:** make adoption frictionless.

- PyTorch DataLoader integration with deterministic shuffling
- Converters: MDS → TSET, Parquet → TSET, JSONL → TSET
- HuggingFace `datasets` integration
- 2-3 design partners onboarded
- Foundation engagement (LF AI & Data or equivalent)
- v1.0 spec frozen
- **Exit criterion:** at least one production training run on TSET that isn't run by the maintainer

**Honest scoping note:** gate 3 may take longer than 6 months solo. If integrations stall, the right move is to recruit a co-maintainer or scope down to PyTorch + one converter, not to compromise on the format itself.

### 9. Risks and mitigations

| Risk                                               | Likelihood | Impact | Mitigation                                                                                         |
|----------------------------------------------------|------------|--------|----------------------------------------------------------------------------------------------------|
| Databricks ships provenance in MDS                 | Medium     | High   | Move fast; design TSET as additive layer; engage upstream early                                    |
| Streaming throughput materially worse than MDS     | High       | High   | Honest framing as "competitive with greater flexibility"; budget Rust core for v1 if needed        |
| Adoption stalls due to switching costs             | High       | Medium | Disproportionate investment in converters and drop-in API compatibility                            |
| Cryptographic claims overreach exposing legal risk | Medium     | High   | Explicit non-claims section; legal review of compliance positioning before any case study          |
| SMT design flaws discovered post-spec freeze       | Medium     | High   | Mark SMT specifics as "design under review"; require cryptography reviewer in gate 2 exit criteria |
| Spec design errors discovered post-v1              | Medium     | High   | Public RFC process; design partner review before v1 freeze; conformance test suite                 |
| Compliance regulations soften unexpectedly         | Low        | Medium | Don't make compliance the only pitch; lead with tokenizer-swap value                               |
| Lance absorbs this direction                       | Medium     | Medium | Position as complementary; consider Lance as a backend option                                      |
| Solo maintainer burnout                            | High       | High   | Find co-maintainer by gate 2; sustainable schedule; don't quit day job until production signal     |
| Governance vacuum delays standards adoption        | Medium     | Medium | Draft governance proposal in gate 2, not gate 3                                                    |

### 10. Open questions

These are deliberately unresolved and need design partner input or further research. Items 1-8 are general design questions; items 14-18 are SMT and signing specifics flagged "design under review."

1. **Tokenizer registry governance.** Who maintains the canonical mapping of `tokenizer_id` to tokenizer config? Is it a separate repo, a section of the spec, or decentralized via content hashes of tokenizer configs?
2. **Multi-writer coordination.** v1 supports shard-isolated writes. Should v2 support concurrent writers to the same shard via something like Apache Iceberg's optimistic concurrency model?
3. **Multimodal extensions.** What's the right abstraction for image-text and audio-text data? Treat modalities as additional views, or as a separate blob store with sample-level alignment?
4. **Differential snapshots.** Is "v2 = v1 + diff" worth building into v1, or layered on top later?
5. **Encryption.** Column-level encryption for sensitive metadata is desirable. Adopt Parquet's modular encryption design or build native?
6. **Bloom filter parameters.** What false-positive rate is right for predicate pushdown? Probably tunable per column, but defaults need empirical justification.
7. **Token bit-packing strategy.** Fixed 17-bit vs variable-length? Trade-off between read speed and storage.
8. **Partial reads from S3.** What's the right chunk size for cost-effective range requests?
9. **Backward and forward compatibility guarantees.** What changes are breaking? What's the deprecation policy? How long are old reader versions supported? This needs a written compatibility policy before v1 freeze.
10. **Failure mode semantics.** What does a reader do on partial writes, corrupted blocks, missing manifests, mismatched Merkle roots? Each failure mode needs a defined behavior — fail-closed by default, but with explicit recovery paths.
11. **Distributed training semantics.** Deterministic shuffling across N nodes × M workers must be correct *and* reproducible across reruns. Specify the seed derivation and the partitioning algorithm in the spec, not just the library.
12. **Cost model.** S3 GET amplification, CPU-vs-IO trade-offs, prefetch buffer sizing. A published cost model helps users choose chunk sizes for their workload.
13. **Governance model.** Who approves spec changes? RFC process? Voting rights? Foundation target (LF AI & Data, Apache, independent)? This is critical for adoption and must be settled by gate 2, not deferred to gate 3.

**Cryptographic specifics — design under review (gate 2 cryptography reviewer needed):**

14. **SMT depth and key derivation.** Is the SMT a fixed-depth tree over the full BLAKE3 output (256 bits), or a variable-depth structure? How are keys derived from document hashes — directly, or via a separate domain-separation step?
15. **Empty branch representation.** Pre-computed zero-hashes per level vs. sparse-tree compression vs. an alternative accumulator (Verkle, RSA)? Trade-off between proof size, write performance, and verification cost.
16. **Snapshot semantics.** Is each snapshot an immutable copy of the SMT root, or a rolling structure with version pointers? How are old snapshots garbage-collected, if ever?
17. **Signing scope.** What exactly is signed in each Merkle log entry — the SMT root only, or the SMT root plus a metadata digest? What signature scheme (Ed25519 vs ECDSA vs post-quantum)? How are public keys distributed?
18. **Key rotation.** Out of scope for v1 (single key per writer for the lifetime of the dataset), or supported via a key-rotation event in the Merkle log? If the latter, how do verifiers reconcile signatures across rotations?

### 11. Out of scope for v1 (deferred to v2+)

- Native vector / embedding storage with ANN indexes
- Streaming ingestion (Kafka-style)
- Time-travel queries beyond named version snapshots
- Cross-file foreign keys
- Embedded learned compression
- GPU-direct decompression
- Source authenticity proofs (requires upstream publisher signing)
- Model-level unlearning verification (requires separate weight-level techniques)
- Concurrent writers to the same shard
- Post-quantum signatures (revisit when standardized)

### 12. Appendix A — Comparable formats

| Format             | Strengths                                           | Weaknesses for LLM training                              |
|--------------------|-----------------------------------------------------|----------------------------------------------------------|
| Parquet            | Mature, columnar, predicate pushdown                | Footer-at-end; not streaming-friendly; no token semantics |
| MosaicML MDS       | Streaming, fast random access, shuffling            | Tokenizer-blind; weak metadata; no provenance            |
| WebDataset         | Simple tar-based, good for images                   | No random access; no metadata story                      |
| Lance              | Random access, vector-native                        | Not streaming-optimized for training                     |
| Raw .bin (nanoGPT) | Maximally simple, fast                              | Zero metadata; lossy                                     |
| Apache Iceberg     | Versioning, schema evolution, manifest-list pattern | Designed for analytics tables, not training              |

TSET deliberately borrows: content-addressing from Git/IPFS, columnar metadata from Parquet, streaming primitives from MDS, dataset-level manifest design from Iceberg, sparse Merkle trees from blockchain state proofs, append-only logs from Certificate Transparency. Nothing here is novel computer science. The contribution is composition under enforced invariants.

### 13. Appendix B — Glossary

- **Shard:** A single self-contained TSET file with its own Merkle root and SMT
- **Dataset:** A logical collection of one or more shards under a root manifest with a dataset-level Merkle root and exclusion overlay
- **View:** A specific tokenization of the corpus identified by tokenizer ID
- **Block:** A unit of zstd-compressed document storage, typically 8-16 MB compressed
- **Chunk:** A unit of token stream or column data, typically 64K-256K elements
- **Subset:** A named, predicate-defined slice of the corpus with a default mixture weight
- **Provenance log:** Append-only Merkle-committed record of ingestion, exclusion, and version events
- **SMT:** Sparse Merkle tree over the document hash space, supporting inclusion and non-inclusion proofs
- **Manifest:** The protobuf-encoded structural metadata of a TSET shard or dataset
- **Source map:** Per-chunk record mapping token offsets to source documents
- **Snapshot:** A named version of the corpus, binding an SMT root to a Merkle log entry
- **Reproducibility proof:** Tokenizer config hash plus precomputed test vectors that detect configuration drift and implementation mismatch (not a proof of semantic correctness)
- **Conforming reader:** A reader that implements all enforcement obligations defined in §5.6

### 14. Appendix C — Review checklist for the next reviewer

Items the author specifically wants critique on in the next round:

1. **Is the dataset/shard split clean?** Are there cases where the shard-level vs dataset-level distinction creates ambiguity, particularly around exclusion overlays and proof composition?
2. **Is the SMT specification list (§10 items 14-18) complete**, or are there additional cryptographic parameters that need pinning before a reviewer would feel comfortable signing off?
3. **Is the tokenizer reproducibility proof scope** (drift detection, not semantic correctness) clearly enough disclaimed to survive an adversarial use case?
4. **Are the v0.2 streaming targets (70-80% of MDS) achievable** in pure Python, or does even v0.2 require a Rust core?
5. **Is the gate-based roadmap realistic** for a solo part-time maintainer at ~15 hours/week?
6. **Is the governance question resolvable** in gate 2 without external funding, or does it require foundation engagement that pushes it later?
7. **What's missing from the failure-mode semantics** that would block enterprise adoption?
8. **Is the explicit non-claims section sufficient** to protect against compliance overreach, or does it need legal review before publication?
9. **Where would a senior cryptography reviewer** attack the SMT + signing design as currently described?
10. **Is single-shard mode adequately specified** as a degenerate case of dataset mode, or does it need its own subsection?

### 15. Changelog

**v0.3 → v0.4 (this version):** Polish for public RFC release based on third-round review:

- **Status promoted from Draft to RFC.** Document now labeled v0.4 RFC for public review, not internal-draft v0.3.
- **Tagline softened** from "TSET is the first format where…" to "TSET is an open format for making…" — avoids unnecessary attack surface around primacy claims.
- **Removed redundant "conforming reader, in any language, that conforms"** wording in Part 1 differentiation — now reads "any reader, in any language, that conforms to the spec inherits them."
- **Added §8.1 MVP surface area** explicitly scoping the first runnable version: JSONL → single-shard writer, one tokenization view, BLAKE3 document store, basic manifest, reader, benchmarks A and C only. Lists what's deferred and provides a recommended initial repo structure.
- **Renumbered Roadmap subsections** as 8.1 (MVP) and 8.2/8.3/8.4 (Gates 1-3) for clearer hierarchy.

**v0.1 → v0.2:** Reframed differentiation around invariants; added formal SMT + Merkle log provenance; added tokenizer determinism; added explicit non-claims; split metrics into v0.2/v1 targets; restructured roadmap into three sequential gates; added five missing open questions; added cryptographic-claims-overreach risk.

**v0.2 → v0.3:** Substantive changes incorporating second-round review:

- **Added §5.8 Dataset Layout** describing TSET as a logical dataset of one or more shards under a root manifest, with shard isolation, dataset-level exclusion overlay, parallel writes, and lazy materialization. Single-shard mode preserved as a degenerate case.
- **Updated §1 Overview, §2 Goals, and Part 1** to consistently distinguish shard from dataset.
- **§5.3 Tokenizer reproducibility proof:** Softened scope language to "detects configuration drift and common implementation mismatch; does not prove semantic correctness."
- **§5.5 Provenance:** Marked SMT specifics explicitly as "design under review"; pulled cryptographic parameters into §10 items 14-18 awaiting cryptography reviewer.
- **§5.6 Critical invariants:** Changed "no reader will return" to "no conforming reader may return" to clarify the format defines conformance, not enforcement against arbitrary readers. Added introductory clarification.
- **§5.7 Non-claims:** Added tokenizer semantic correctness to the disclaimed-claims list.
- **User Story 2:** Rewrote to use evidence-workflow framing rather than "verify exclusion" — produces non-inclusion proofs as input to compliance workflows rather than satisfying legal obligations directly.
- **User Story 4:** Added legal-review caveat for AI Act template generation.
- **§9 Risks:** Added SMT design flaw risk separately from general spec design errors.
- **§10 Open questions:** Added items 14-18 listing specific cryptographic parameters as "design under review."
- **§11 Out of scope:** Added concurrent writers to the same shard and post-quantum signatures.
- **Gate 2 exit criteria:** Now requires cryptography reviewer engagement and governance proposal drafted (not just deferred).
- **Glossary:** Added shard, dataset, conforming reader, and tightened reproducibility proof definition.

-----

*End of v0.4 RFC. Public reviewer feedback welcomed at the linked issue tracker once the repo is published.*
