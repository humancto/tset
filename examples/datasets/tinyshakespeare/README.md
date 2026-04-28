# TinyShakespeare → TSET

End-to-end demonstration of the TSET binary format on a small public-domain
text corpus. TinyShakespeare is the ~1 MB corpus popularised by Andrej
Karpathy's [`char-rnn`][1]; it concatenates several Shakespeare plays and
sees heavy use as a smoke-test corpus in the ML community.

[1]: https://github.com/karpathy/char-rnn

## Pipeline

```text
input.txt           raw 1.1 MB plain-text file from karpathy/char-rnn
   │  split on blank lines (paragraph boundary)
   ▼
out/corpus.jsonl    7,222 paragraphs · 1.5 MB
   │  tset.converters.jsonl_to_tset (ByteLevelTokenizer)
   │  + tset.writer.append_tokenizer_view (WhitespaceTokenizer)
   ▼
out/corpus.tset     7,148 unique docs · 2 tokenizer views · TSMT/TLOG/TCOL
```

The 7,222 → 7,148 drop is **content-addressed deduplication**: 74 paragraphs
in TinyShakespeare share BLAKE3 hashes (recurring stage directions, common
refrains). TSET collapses them to a single document automatically and the
audit log records 7,148 ingest events.

## Files

| File | Purpose |
|---|---|
| `download.py` | Idempotent fetch into the content-addressed cache. |
| `convert.py`  | Raw → JSONL → TSET (with binary sections + 2 views). |
| `prove.py`    | Inclusion + non-inclusion proofs, tamper rejection, audit-log signature. |
| `bench.py`    | Storage and read-throughput vs JSONL/JSONL+zstd/Parquet. |
| `RESULTS.md`  | Committed numbers from the latest run. |
| `out/`        | Build artifacts (gitignored). |

## Reproducing

```bash
python -m examples.datasets.tinyshakespeare.download
python -m examples.datasets.tinyshakespeare.convert
python -m examples.datasets.tinyshakespeare.prove
python -m examples.datasets.tinyshakespeare.bench
```

The first command downloads `input.txt` into
`examples/datasets/.cache/by-sha256/<sha>` and verifies the SHA-256 before
storing it. Subsequent runs are offline.

## Tests

The full eight-class test matrix for this dataset lives at
[`python/tests/showcase/test_tinyshakespeare.py`](../../../python/tests/showcase/test_tinyshakespeare.py)
and gates on the cache being populated. Run:

```bash
pytest python/tests/showcase/test_tinyshakespeare.py -v
```

## License

TinyShakespeare derives from public-domain Shakespeare plays. The corpus
file as distributed by `karpathy/char-rnn` is treated here as CC0-1.0; see
[`LICENSE.txt`](LICENSE.txt) for attribution.
