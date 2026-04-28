# Click source — measured results

Real run on a Linux x86_64 host. Reproduces with the commands in
`README.md`.

## Corpus

| Property | Value |
|---|---:|
| Source | github.com/pallets/click @ 8.1.7 (BSD-3-Clause) |
| Tarball SHA-256 | `89251974dba8552b4e22990ca34adfb93a47ba7deb27fe7358a6661a09ca8793` |
| Python files | 71 |
| Total source bytes | 555,457 |
| JSONL bytes (one record per file) | 591,144 |
| **TSET bytes (1 view, no sections)** | **376,747** |

## Storage ratio

| Format | Size | × JSONL |
|---|---:|---:|
| Raw concatenated source | 555 KB | 0.94× |
| JSONL | 591 KB | 1.00× |
| **TSET** | **377 KB** | **0.64×** |

> TSET is **36 % smaller than JSONL** on this corpus. Zstd compression
> on the doc-store blocks (highly compressible Python source) more than
> compensates for the manifest overhead because there are only 71
> documents to index. This is the inverse of the Shakespeare result and
> is exactly what the per-doc overhead model predicts.

See `examples/datasets/SCALING.md` for the full discussion.
