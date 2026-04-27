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
| Binary spec         | v0.1                    |
| Python reference    | v0.1.0                  |
| Benchmarks          | A, C (Gate 1) + D, E (Gate 2) |
| Provenance proofs   | SMT inclusion + non-inclusion |
| Multi-shard dataset | Yes (Gate 2)            |
| DataLoader          | Pure-Python (Gate 3)    |

## Quickstart

```bash
pip install -e python/

# Convert a JSONL corpus into a single-shard .tset file
python python/examples/jsonl_to_tset.py \
    --input corpus.jsonl \
    --output corpus.tset \
    --tokenizer byte-level

# Add a second tokenization view in-place (does not re-read source)
python python/examples/tokenizer_swap_demo.py \
    --shard corpus.tset \
    --add-tokenizer whitespace

# Read and stream tokens back
python -c "
from tset import Reader
r = Reader('corpus.tset')
print('views:', r.tokenizer_ids())
for tokens, doc_hash in r.stream_tokens('byte-level', batch_size=1024):
    print(len(tokens), doc_hash[:8])
    break
"
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
