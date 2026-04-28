# Click 8.1.7 source → TSET

A small Python codebase converted to TSET. Demonstrates the format on
**non-natural-language data** (very different vocabulary distribution
from prose) and on a corpus where average doc size is large enough that
TSET ends up **smaller than JSONL** — see `RESULTS.md`.

## Pipeline

```text
click-8.1.7.tar.gz       BSD-3 source tarball from GitHub
   │  iter_python_files()
   ▼
out/corpus.jsonl         71 python files · 577 KB
   │  Writer + ByteLevelTokenizer
   ▼
out/corpus.tset          TSET shard (1 view, no sections)
```

## Reproducing

```bash
python -m examples.datasets.click_source.download
python -m examples.datasets.click_source.convert
```

## License

Click is © Pallets and contributors, BSD-3-Clause. See `LICENSE.txt`.
