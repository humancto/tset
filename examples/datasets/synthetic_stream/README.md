# Synthetic stream — scaling experiments

A deterministic, generated-locally corpus used for **scaling and
competitive benchmarks**. The generator (``generate.py``) takes a
target byte size and produces JSONL with realistic-ish English-shaped
records. Same seed → byte-identical output.

This dataset is **not** a real-world test corpus. It exists to answer
two specific questions from `SCALING.md`:

1. Does the TSET-vs-JSONL ratio hold at scale, or does it grow?
2. Where does TSET sit against JSONL+zstd / Parquet / WebDataset / MDS
   when given the same bytes?

## Files

| File | Purpose |
|---|---|
| `generate.py` | Deterministic JSONL generator (Zipfian word freq). |
| `scale_bench.py` | Builds 1 / 10 / 100 MB JSONL → 3 TSET configs each, profiles regions, dumps `out/scale_bench.json`. |
| `competitive.py` | Same 10 MB corpus across JSONL / +zstd / Parquet / WebDataset / MDS / TSET (3 configs); dumps `out/competitive.json`. |

## Headline numbers

| Scale | TSET · 1 view ratio vs JSONL |
|---|---:|
| 1 MB (1,171 docs) | 1.56× |
| 10 MB (11,830 docs) | 1.57× |
| 100 MB (118,336 docs) | 1.57× |

The ratio is constant to two decimal places across a 100× scale jump
because synthetic-stream docs are roughly the same size at every scale.
See `SCALING.md` for the full breakdown.

## Reproducing

```bash
python -m examples.datasets.synthetic_stream.scale_bench
python -m examples.datasets.synthetic_stream.competitive
```

The 100 MB row of `scale_bench` takes about two minutes (Python writer
is the bottleneck). Skip it with `python -c 'from examples.datasets.synthetic_stream.scale_bench import SCALES; SCALES[:] = SCALES[:2]'` if you're iterating.
