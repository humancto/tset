# TSET

> An open standard for LLM training data — verifiable, reusable, composable.

TSET is a binary format that stores text once, lets you tokenize it many ways,
and binds the whole shard with a Merkle root. Every token traces back to a
source document. Every tokenization view carries a reproducibility proof.
Exclusions are honored across all conforming readers.

See [`RFC.md`](RFC.md) for the full design pitch and PRD (v0.4) and
[`SPEC.md`](SPEC.md) for the v0.1 binary-layout specification.

## Status

| Surface             | Status                  |
|---------------------|-------------------------|
| RFC                 | v0.4 — public review    |
| Binary spec         | v0.2 (frozen body; SMT under review) |
| Python reference    | v0.2.x                  |
| Rust core (`tset-core`) | feature-parity for shards + datasets |
| PyO3 binding (`tset_rs`) | Reader, Writer, Dataset, DatasetWriter |
| CLI (`tset`)        | inspect, verify, convert jsonl |
| Benchmarks          | A, C (Gate 1), D, E (Gate 2), B head-to-head Py/Rust (Gate 3); criterion benches in `tset-bench` |
| Provenance proofs   | SMT inclusion + non-inclusion + dataset overlay binding |
| Multi-shard dataset | Yes (Python + Rust)     |
| Converters          | JSONL, Parquet, WebDataset, MDS, HuggingFace |
| Conformance suite   | Yes — `tests/conformance/` (Python + Rust readers run identical fixtures) |
| DataLoader          | Pure-Python (deterministic shuffling) |

## Quickstart

```bash
# Python reference impl
pip install -e python/

# Optional: Rust core via PyO3 (gives a 2.87× streaming speedup at 200 MB).
# Built into the same Python module — `tset.Reader` will delegate to the
# Rust path automatically when the wheel is installed.
cd crates/tset-py && maturin build --release && pip install --force-reinstall \
    ../../target/wheels/tset_rs-*.whl
```

### CLI

The `tset` binary (in `target/release/tset`) handles the common cases:

```bash
# Convert JSONL → TSET (byte-level by default)
tset convert jsonl input.jsonl output.tset

# whitespace-hashed tokenizer with a 1024-vocab
tset convert jsonl input.jsonl ws.tset --tokenizer=whitespace-hashed-v1 --vocab=1024

# Inspect a shard (header, manifest, views, doc count)
tset inspect output.tset

# Full integrity check (verifies manifest hash, footer, merkle root,
# audit log, and reproducibility proof for every view)
tset verify output.tset
```

### Python API

```python
from tset import Reader, Writer
from tset.tokenizers import ByteLevelTokenizer, WhitespaceTokenizer

# Write
with Writer("corpus.tset") as w:
    w.add_document(b"alpha document", metadata={"lang": "en"})
    w.add_document(b"beta", metadata={"lang": "fr"})
    w.add_subset("english", "lang = 'en'", default_weight=0.7)
    w.add_tokenizer_view(ByteLevelTokenizer())

# Add a second tokenization in-place — does NOT re-read source documents
from tset.writer import append_tokenizer_view
append_tokenizer_view("corpus.tset", WhitespaceTokenizer(vocab_size=1024))

# Read
r = Reader("corpus.tset")
print(r.tokenizer_ids())  # ['byte-level-v1', 'whitespace-hashed-v1']
for tokens, doc_hash in r.stream_tokens("byte-level-v1", batch_size=1024):
    pass  # numpy.ndarray of uint32, plus the source doc_hash
```

### Rust API

```rust
use tset_core::{Reader, Writer};
use tset_core::tokenizers::ByteLevelTokenizer;

let mut w = Writer::create("corpus.tset", None);
w.add_document(b"alpha document")?;
w.add_tokenizer_view(Box::new(ByteLevelTokenizer))?;
w.close()?;

let r = Reader::open("corpus.tset")?;
let view = r.open_view("byte-level-v1")?;
for (tokens, doc_hash) in view.iter_per_doc()? {
    // tokens: Vec<u32>, doc_hash: [u8; 32]
}
```

### Multi-shard datasets

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
print(ds.dataset_merkle_root().hex())
for tokens, doc_hash in ds.stream_tokens("byte-level-v1"):
    pass  # excluded docs are filtered automatically
```

### Format converters

```python
from tset.converters import (
    jsonl_to_tset, parquet_to_tset, webdataset_to_tset,
    mds_to_tset, to_huggingface_dataset,
)
from tset.tokenizers import ByteLevelTokenizer

jsonl_to_tset("input.jsonl", "out.tset", ByteLevelTokenizer(),
              metadata_fields=["lang", "source"])

webdataset_to_tset("shard.tar", "out.tset", ByteLevelTokenizer())
# parquet/mds/hf require their respective optional deps installed
```

## Run the benchmarks

```bash
# Generate a 10 MB synthetic corpus and run the storage benchmark
python -m benchmarks.harness --benchmark storage --corpus-size-mb 10

# Tokenizer-swap benchmark
python -m benchmarks.harness --benchmark tokenizer_swap --corpus-size-mb 10

# Compliance query + non-inclusion proof workflow
python -m benchmarks.harness --benchmark compliance --corpus-size-mb 1
python -m benchmarks.harness --benchmark exclusion --corpus-size-mb 1
```

Benchmark JSON outputs go to `benchmarks/results/`.

## What this format is *not* (read this before pitching it as a compliance
solution)

TSET makes integrity claims, not authenticity claims. See [`RFC.md` §5.7](RFC.md).

- It does **not** prove a document came from the URL recorded in metadata.
- It does **not** prove a model trained on a TSET corpus has "forgotten" any
  document.
- It does **not** by itself satisfy any specific regulation. It produces
  artifacts (non-inclusion proofs, Merkle commitments) that compliance
  workflows can use as inputs. Whether your particular use satisfies GDPR Art.
  17, the EU AI Act, or any other regulation is for your counsel to determine.

## Layout

```
RFC.md                  -- design pitch + PRD v0.4
SPEC.md                 -- binary layout v0.1 (normative)
spec/                   -- additional spec docs (SMT design, glossary)
python/tset/            -- reference implementation
python/tests/           -- pytest test suite
python/examples/        -- runnable examples
benchmarks/             -- format-agnostic benchmark harness
governance/             -- governance + RFC process
```

## License

MIT — see [`LICENSE`](LICENSE).
