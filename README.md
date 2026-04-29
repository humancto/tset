<div align="center">

<img src="docs/assets/favicon.svg" alt="TSET" width="72" height="72" />

# TSET

### Training data your model can prove it actually saw.

**An open binary format for LLM training corpora.**
Store text once. Tokenize many ways. Verify everywhere.

[![Crates.io · tset-core](https://img.shields.io/crates/v/tset-core?label=crates.io%2Ftset-core&color=orange&logo=rust)](https://crates.io/crates/tset-core)
[![Crates.io · tset-cli](https://img.shields.io/crates/v/tset-cli?label=crates.io%2Ftset-cli&color=orange&logo=rust)](https://crates.io/crates/tset-cli)
[![PyPI](https://img.shields.io/pypi/v/tset?label=pypi%2Ftset&color=3776ab&logo=python&logoColor=white)](https://pypi.org/project/tset/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Format](https://img.shields.io/badge/format-v0.3.2-7dd3fc)](SPEC.md)
[![Tests](https://img.shields.io/badge/tests-42%20rust%20%C2%B7%20145%20py%20%C2%B7%2027%20showcase-success)](#status)
[![Clippy](https://img.shields.io/badge/clippy-0%20warnings-success)](#status)
[![RFC](https://img.shields.io/badge/RFC-v0.4-a78bfa)](RFC.md)

[**Website**](https://humancto.github.io/tset/) ·
[**Showcase**](https://humancto.github.io/tset/showcase/) ·
[**RFC**](RFC.md) ·
[**Spec**](SPEC.md) ·
[**SCALING**](examples/datasets/SCALING.md) ·
[**Changelog**](CHANGELOG.md)

</div>

---

## Install

**Rust (the canonical implementation):**

```bash
# CLI binary
cargo install tset-cli

# Library
cargo add tset-core
```

**Python (auto-includes the prebuilt Rust wheel):**

```bash
pip install tset
```

The Python package ships the same Rust core as a precompiled wheel
(via `tset-py`) and falls back to a pure-Python reference impl when
the wheel isn't available. One install command, native speed by default.

---

## What is TSET?

Training pipelines today juggle **JSONL, Parquet, WebDataset, MDS,** and a
dozen tokenizer caches — each with its own mental model for provenance.
TSET (**T**okenized **Set**) is one binary format that solves the whole problem:

- **One copy of the text.** Documents are stored once, content-addressed by BLAKE3.
- **Many tokenizations.** Append a new tokenizer view in place — no re-shuffling raw bytes.
- **One Merkle root.** The entire shard is bound to a single hash you can publish.
- **Provable inclusion *and* non-inclusion.** Prove a document is in the corpus, or prove it isn't (GDPR Art. 17 friendly).
- **Signed audit trail.** Every ingest, exclusion, and tokenizer addition is appended to an Ed25519-signed log.
- **Reproducible by construction.** Re-tokenizing the same shard yields byte-identical output. Cross-impl conformance fixtures lock the wire format.

It's MIT-licensed, has a Rust core, Python bindings, a CLI, and 42 + 145 tests.

---

## How it's laid out

```
┌─ MAGIC + version_minor ──────────────────────────────┐
│  shard_merkle_root  ·  manifest_hash                 │
├─ DOC STORE ──────────────────────────────────────────┤
│  zstd-compressed blocks · per-doc BLAKE3 addressed   │
├─ TOKENIZER VIEWS (one per view) ─────────────────────┤
│  bit-packed token IDs · per-chunk content_hash       │
├─ OPTIONAL on-disk sections (v0.3.2) ─────────────────┤
│  TSMT  Sparse Merkle Tree                            │
│  TLOG  Append-only audit log (Ed25519 signed)        │
│  TCOL  Columnar metadata index                       │
├─ MANIFEST (canonical JSON) ──────────────────────────┤
│  document index · views · subsets · weights          │
└─ FOOTER (mirrors header for tail-first verify) ──────┘
```

Full normative layout: [SPEC.md](SPEC.md). Design pitch: [RFC.md](RFC.md).

---

## Quickstart

### Python

```bash
pip install tset            # ships the Rust wheel as `tset_rs`; native speed by default
```

For a development install from source:

```bash
git clone https://github.com/humancto/tset && cd tset
pip install maturin
maturin develop --manifest-path crates/tset-py/Cargo.toml
pip install -e python/
```

```python
from tset import Reader, Writer
from tset.tokenizers import ByteLevelTokenizer, WhitespaceTokenizer

# Write
with Writer("corpus.tset") as w:
    w.add_document(b"alpha document", metadata={"lang": "en"})
    w.add_document(b"beta",           metadata={"lang": "fr"})
    w.add_subset("english", "lang = 'en'", default_weight=0.7)
    w.add_tokenizer_view(ByteLevelTokenizer())
    w.enable_binary_sections()  # opt-in v0.3.2 on-disk TSMT/TLOG/TCOL

# Add a second tokenization in place — does NOT re-read source documents
from tset.writer import append_tokenizer_view
append_tokenizer_view("corpus.tset", WhitespaceTokenizer(vocab_size=1024))

# Read
r = Reader("corpus.tset")
print(r.tokenizer_ids())  # ['byte-level-v1', 'whitespace-hashed-v1']
for tokens, doc_hash in r.stream_tokens("byte-level-v1", batch_size=1024):
    train_step(tokens)  # numpy.ndarray of uint32, plus the source doc_hash
```

### Rust

```rust
use tset_core::{Reader, Writer};
use tset_core::tokenizers::ByteLevelTokenizer;

let mut w = Writer::create("corpus.tset", None);
w.enable_binary_sections();
w.add_document(b"alpha document")?;
w.add_tokenizer_view(Box::new(ByteLevelTokenizer))?;
w.close()?;

let r = Reader::open("corpus.tset")?;
let view = r.open_view("byte-level-v1")?;
for (tokens, doc_hash) in view.iter_per_doc()? {
    // tokens: Vec<u32>, doc_hash: [u8; 32]
}
```

### CLI

```bash
# Convert a JSONL corpus to TSET (with on-disk binary sections)
tset convert jsonl input.jsonl out.tset --binary-sections

# Inspect a shard (header, manifest, sections, signing pubkey)
tset inspect out.tset

# Full integrity check: manifest hash, footer, Merkle root,
# audit log signature, per-view reproducibility
tset verify out.tset
```

---

## Multi-shard datasets + exclusion overlay

```python
from tset.dataset import Dataset, DatasetWriter
from tset.tokenizers import ByteLevelTokenizer

with DatasetWriter("my-corpus.tset/") as dw:
    with dw.shard_writer("part-0001") as sw:
        sw.add_document(b"doc 1")
        sw.add_tokenizer_view(ByteLevelTokenizer())
    dw.register_shard("part-0001")
    # … repeat for additional shards …
    dw.add_exclusion(b"\x00" * 32, reason="GDPR Art. 17 request")

ds = Dataset("my-corpus.tset/")
print(ds.dataset_merkle_root().hex())          # one root over all shards + exclusions
for tokens, doc_hash in ds.stream_tokens("byte-level-v1"):
    pass  # excluded docs are filtered automatically by every conforming reader
```

The dataset Merkle root binds **shards + exclusions + subset weights** into a
single hash. Add or revoke an exclusion → the root changes → readers detect it.

---

## Drop-in for `datasets.load_dataset` users

`tset.hf` is the on-ramp for anyone with a HuggingFace dataset
workflow. Both directions work in one line:

```python
from datasets import load_dataset
from tset.hf import to_tset, from_tset

# (1) Convert any HF dataset to TSET. Metadata fields and tokens come along.
hf = load_dataset("Salesforce/wikitext", "wikitext-2-raw-v1", split="train")
to_tset(hf, "wikitext-2.tset", metadata_fields="*")

# (2) Open a TSET shard back as a HuggingFace `Dataset`.
ds = from_tset("wikitext-2.tset", with_tokens=True)
ds = ds.filter(lambda r: len(r["text"]) > 200)
ds.train_test_split(test_size=0.1)   # standard HF API works
```

Multi-shard datasets with deletion overlays applied automatically:

```python
from tset.hf import from_dataset
ds = from_dataset("./my-corpus.tset/")  # excluded docs are filtered out
```

`pip install tset[hf]` pulls `datasets` in.

## Other format converters

```python
from tset.converters import (
    jsonl_to_tset, parquet_to_tset, webdataset_to_tset, mds_to_tset,
)
from tset.tokenizers import ByteLevelTokenizer

jsonl_to_tset("input.jsonl", "out.tset", ByteLevelTokenizer(),
              metadata_fields=["lang", "source"])
webdataset_to_tset("shard.tar", "out.tset", ByteLevelTokenizer())
# parquet/mds require their respective optional deps
```

PyTorch users get a lazy-imported `IterableDataset` adapter:

```python
from tset.torch_dataset import TsetIterableDataset
from torch.utils.data import DataLoader

ds = TsetIterableDataset("corpus.tset", view="byte-level-v1", batch_size=1024)
for tokens, doc_hash in DataLoader(ds, batch_size=None):
    ...
```

---

## Verify a published corpus in 10 seconds

A real, deterministically-built TSET shard ships in this repo and is
served by GitHub raw. Anyone can fetch and verify it without trusting
us:

```bash
pip install tset
python -m examples.published.verify \
  https://raw.githubusercontent.com/humancto/tset/main/examples/published/corpus.tset \
  --expected-smt-root=369cf1fbacb1af433d2ea84ead6aa326eba6bd4698f872304a533444a5815444
```

The verifier opens the shard, generates an inclusion proof for one
document and a non-inclusion proof for an absent hash, rejects a
tampered proof, verifies the audit-log chain, and pins the SMT root.
30 lines of code, no library dependencies beyond `tset`. See
[`examples/published/`](examples/published/) for the source.

---

## What this format is *not* (read this before pitching it as compliance)

TSET makes **integrity** claims, not **authenticity** claims. See [RFC §5.7](RFC.md).

- It does **not** prove a document came from the URL recorded in metadata.
- It does **not** prove a model trained on a TSET corpus has "forgotten" any document.
- It does **not** by itself satisfy any specific regulation. It produces
  artifacts (non-inclusion proofs, Merkle commitments, signed audit logs) that
  compliance workflows can use as inputs. Whether your particular use satisfies
  GDPR Art. 17, the EU AI Act, or any other regulation is for your counsel to determine.

---

## Status

| Surface                        | Status                                                                  |
|--------------------------------|-------------------------------------------------------------------------|
| RFC                            | v0.4 — public review                                                    |
| Binary spec                    | v0.3.2 (frozen body; v0.4 mandatory-sections planned)                   |
| Rust core (`tset-core`)        | Reader · Writer · Dataset · ObjectStore · 39 unit + 9 integration tests |
| Python (`tset`)                | Reference impl · 145 tests · auto-delegates hot paths to `tset_rs`      |
| PyO3 binding (`tset_rs`)       | Reader · Writer · Dataset · DatasetWriter · on-disk section accessors   |
| CLI (`tset`)                   | `inspect` · `verify` · `convert jsonl --binary-sections`                |
| Provenance proofs              | SMT inclusion + non-inclusion + dataset-overlay binding                 |
| Audit log                      | Ed25519-signed (additive optional, downgrade-attack rejected)           |
| Conformance                    | v0.1, v0.3, v0.3.2 fixtures locked — both readers cross-verified        |
| Lint                           | `cargo clippy --all-targets` — zero warnings                            |
| Format                         | `cargo fmt --check` — clean                                             |
| License                        | MIT                                                                     |

24 of 24 production-grade items closed + 6 polish atomics on top. See [ROADMAP.md](ROADMAP.md).

### Format version history

| Version | Notes                                                                  |
|---------|------------------------------------------------------------------------|
| v0.1    | Initial reference impl                                                 |
| v0.2    | Per-chunk `content_hash` mandatory                                     |
| v0.3    | Bit-packed token IDs (16-bit fast path)                                |
| v0.3.1  | Ed25519 audit-log signing (additive optional field)                    |
| v0.3.2  | Opt-in TSMT / TLOG / TCOL on-disk sections; eager content-hash verify  |
| v0.4    | Planned — on-disk sections become mandatory, in-manifest forms drop    |

v0.x readers MUST read all earlier minor versions per RFC §5.6 #6 — verified by
the conformance fixtures in [`tests/conformance/fixtures/`](tests/conformance/fixtures).

---

## Layout

```
RFC.md                  -- design pitch + PRD v0.4
SPEC.md                 -- binary layout (normative)
ROADMAP.md              -- production-grade tracker (24/24)
CHANGELOG.md            -- version-by-version notes
spec/                   -- additional spec docs (SMT design, glossary)
crates/tset-core/       -- Rust core (Reader/Writer/Dataset/ObjectStore)
crates/tset-py/         -- PyO3 bindings (`tset_rs`)
crates/tset-cli/        -- `tset` binary
crates/tset-bench/      -- criterion benchmarks
python/tset/            -- Python reference implementation
python/tests/           -- pytest suite
benchmarks/             -- format-agnostic benchmark harness
tests/conformance/      -- cross-impl conformance fixtures
fuzz/                   -- cargo-fuzz target on Reader::open
docs/                   -- GitHub Pages landing site
governance/             -- governance + RFC process
```

---

## Contributing

Open an issue before you build on top — the format is frozen at v0.3.2 with v0.4
sections prepared, but the manifest schema still has give. See
[CONTRIBUTING.md](CONTRIBUTING.md) and [governance/](governance/).

---

## License

MIT — see [LICENSE](LICENSE).

<div align="center">
<sub>Built for the people who actually have to ship the data pipeline.</sub>
</div>
