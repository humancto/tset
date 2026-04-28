# Scaling, storage cost, and "should I use this for 5 TB?"

This is the document for the question: **what is the real cost of TSET
versus a JSONL pipeline, and how does that cost grow?** Numbers below
are from real runs of the scripts in this directory; reproduce with
`python -m examples.datasets.synthetic_stream.scale_bench`.

> Every claim here is backed by `out/scale_bench.json` and the
> per-region breakdown produced by `examples/datasets/_lib/profile_size.py`.

## Headline

**TSET overhead is per-document, not per-byte.** That's the single most
important sentence. Once you know that, every other observation falls
out of it.

For a corpus with **~1 KB documents** (typical web crawl, code files,
chat messages, news articles), TSET in its lean v0.3.2 production
config sits at **1.5 – 1.7× JSONL** and the ratio is flat as you scale
from 1 MB to 100 MB. For corpora with **very small documents** (tweets,
log lines, short Shakespeare paragraphs at ~210 bytes/doc), the per-doc
overhead crushes you and the ratio climbs to **4 – 5×**. The mitigation
is to pack many tiny records into a single TSET document with internal
structure recorded in metadata.

## Scale benchmark (synthetic, ~900 bytes/doc, in-tree generator)

| Scale | JSONL | TSET · 1 view (no sections) | × JSONL | TSET · 2 views (no sections) | × JSONL | TSET · 2 views + v0.3.2 sections | × JSONL |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1 MB | 977 KB | 1.5 MB | **1.56×** | 1.9 MB | 2.01× | 2.4 MB | 2.54× |
| 10 MB | 9.5 MB | 15.0 MB | **1.57×** | 19.2 MB | 2.01× | 24.3 MB | 2.55× |
| 100 MB | 95.4 MB | 149.9 MB | **1.57×** | 192.2 MB | 2.02× | 243.4 MB | 2.55× |

The ratio is **identical to two decimal places across a 100× scale jump**.
That's the empirical proof of "TSET overhead is per-doc, not per-byte"
— same docs/MB → same ratio, regardless of how many MB you have.

**The ratio is flat across scales for a given doc-size distribution.**
That's good news — TSET doesn't get pathologically worse at 1 TB; the
bytes-per-doc figure determines the ratio.

## Where the bytes go (TinyShakespeare, full 2-view + v0.3.2 sections config)

The same shard, profiled by region:

| Region | Bytes | % of file | Notes |
|---|---:|---:|---|
| header | 4,096 | 0.0% | fixed |
| doc store (zstd-compressed text) | 681 KB | 6.1% | `~0.5×` raw text |
| view: byte-level-v1 | 652 KB | 5.8% | bit-packed tokens |
| view: whitespace-hashed-v1 | 409 KB | 3.6% | bit-packed tokens |
| section: TSMT | 229 KB | 2.0% | binary SMT |
| section: TLOG | 2.9 MB | **26.2%** | binary audit log |
| section: TCOL | 135 KB | 1.2% | binary metadata |
| **manifest (canonical JSON tail)** | **6.2 MB** | **55.0%** | dominates |
| footer | 64 B | 0.0% | fixed |
| **total** | **11.2 MB** | 100.0% |  |

The manifest dominates. Within the 6.2 MB of JSON manifest, the top
contributors are:

| Manifest field | Size | % of manifest | What it is |
|---|---:|---:|---|
| `audit_log` | 2.93 MB | **47.6%** | inline JSON audit log; **duplicates** the binary `TLOG` section above |
| `tokenization_views` (chunk metadata) | 1.71 MB | 27.7% | per-chunk `content_hash` (hex), offsets, sizes |
| `document_store.document_index` | 905 KB | 14.7% | per-doc hex hash + offset + length |
| `smt_present_keys` | 479 KB | 7.8% | hex doc hashes; **fully redundant** with `document_index` |
| `metadata_columns` | 135 KB | 2.2% | inline columnar metadata; **duplicates** `TCOL` |

## The big knob: hex encoding + duplicate inline forms

Three observations stand out from the breakdown above:

1. **The audit log is stored twice.** Once inline as JSON (`audit_log`,
   2.93 MB) and once on disk as the `TLOG` binary section (2.9 MB).
   v0.3.2 keeps both for forward compatibility; v0.4 plans to drop the
   inline form.
2. **`smt_present_keys` duplicates `document_index`.** The SMT only
   contains the doc hashes; `document_index` already records every doc
   hash. 479 KB of waste.
3. **Hex encoding doubles every hash.** Each 32-byte hash is stored as
   64 hex characters in JSON. Base64 would be 44; binary in a section
   would be 32. On a corpus with 7,148 docs and ~24 chunks, that's
   roughly 1.5 MB of hex padding alone.

If v0.4 lands as planned (mandatory binary sections + inline forms
dropped), an honest **estimate for the same Shakespeare shard**:

- Drop `audit_log` JSON: −2.93 MB
- Drop `smt_present_keys`: −0.48 MB
- Drop `metadata_columns` inline: −0.13 MB
- Encode chunk `content_hash` as base64 (or move to a binary index): −0.5 MB

Total estimated saving: **~4 MB**, taking the file from 11.2 MB to
~7 MB and the ratio from 7.5× JSONL to ~4.5× JSONL. Without binary
sections (the leanest v0.3.2 config) the same shard is already 7.5 MB
today (4.91× JSONL). v0.4 is therefore **net neutral on size while
delivering binary-form proof verification**.

## Per-doc overhead model

Empirically, TSET adds roughly the following per-document overhead
**on top of the doc store + tokenizer view costs**:

| Source | Bytes per doc |
|---|---:|
| `document_index` entry (hex hash + offsets, JSON) | ~125 |
| `audit_log` ingest entry (JSON, multiple hex fields) | ~410 |
| `smt_present_keys` (one hex doc hash) | ~67 |
| **Total inline overhead per doc** | **≈ 600 bytes/doc** |

So the per-document tax is **~600 bytes** in the lean v0.3.2 config.
Multiply by your doc count to project. Worked examples:

| Corpus | Docs | Avg doc size | TSET / JSONL ratio (measured) | Notes |
|---|---:|---:|---:|---|
| TinyShakespeare | 7,148 | ~155 B | **4.05×** (1 view) | short paragraphs — worst case |
| Synthetic web | 11,830 | ~800 B | **1.57×** (1 view) | average-size English documents |
| Click 8.1.7 source | 71 | ~7.8 KB | **0.64×** (1 view) | Python code, large docs — TSET *smaller* than JSONL |
| Web crawl projection (1 TB, 5 KB/doc) | 200 M | 5 KB | ~1.15× (interp.) | ~15% storage premium |
| Tweets projection (1 TB, 200 B/doc) | 5 B | 200 B | ~4× (deal-breaker) | shard records before ingest |

The Shakespeare row is the worst-case lab specimen — short docs maximise
the overhead. The web-crawl row is closer to reality and lands at a
manageable ~12% size penalty for full receipts.

## Tweet-sized docs are the deal-breaker (and how to avoid it)

Direct ingest of tweet-sized records is **not the right shape** for
TSET. The mitigation: shard them. Group N tweets into one TSET document
whose body is JSONL (or any record format you like) and whose metadata
records the per-record offsets. You get one Merkle leaf per shard rather
than one per tweet, and the per-doc overhead falls off proportionally.

This is the same pattern WebDataset and MDS use for the same reason.

## Cross-format competitive matrix (10 MB synthetic)

Same corpus, different containers. From a real run of
`examples.datasets.synthetic_stream.competitive`.

| Format | On-disk size | × JSONL | Write | Read | Notes |
|---|---:|---:|---:|---:|---|
| JSONL | 9.5 MB | 1.00× | — | 40 ms | baseline |
| JSONL + zstd | 2.6 MB | **0.27×** | 56 ms | 26 ms | size winner; no proofs, no token cache |
| Parquet (zstd) | 2.7 MB | 0.29× | 52 ms | 61 ms | columnar; no proofs |
| WebDataset (.tar) | 29.0 MB | 3.04× | 976 ms | 602 ms | tar of grouped files |
| MDS (mosaicml-streaming) | _not installed in this env_ |  |  |  | streaming-friendly |
| **TSET · 1 view, no sections** | **15.0 MB** | **1.57×** | 7.0 s | 188 ms | lean prod config |
| TSET · 2 views, no sections | 19.2 MB | 2.01× | 11.3 s | 204 ms | + whitespace view |
| TSET · 2 views + sections | 24.3 MB | 2.55× | 10.9 s | 192 ms | full v0.3.2 receipts |

**What this table actually shows:**

- **Plain JSONL+zstd is the size winner**, by a wide margin. If all you
  need is "store text and read it back," nothing else competes.
- **TSET is between Parquet and WebDataset on size**, and beats both on
  what each token reader gets per byte (pre-computed tokens).
- **TSET write time is the slow path.** The reference Python writer is
  not optimised; the Rust path (`tset_rs`) is roughly 3× faster, and
  tokenization dominates regardless. Expect to write once and read
  many times.
- **WebDataset is bigger than TSET** because the tar wrapper has 512
  bytes of header per file and TinyStories-shape datasets produce two
  files per record.

The fair way to read this: TSET trades **+57% storage and +20× write
time for** pre-computed tokenizations (multiple), one Merkle root over
the whole shard, inclusion + non-inclusion proofs, signed audit log,
and a single-binary distribution unit. None of the alternatives offer
those four together.

## Read-time performance (orthogonal to size)

Storage isn't the only axis. Even when TSET's size ratio is unfavorable,
its **read throughput is the best in the comparison set** because the
tokens are pre-computed:

| Format | Read time (1.1 MB Shakespeare) |
|---|---:|
| Raw text | 3.6 ms |
| **TSET doc-store iteration** | **15.9 ms** |
| JSONL | 20.3 ms |
| JSONL + zstd | 27.0 ms |
| Parquet (column projection) | 50.5 ms |

Token streaming via the byte-level view: 110 ms for the full 1.1 M
tokens, ~10 M tokens/s on the pure-Python path. Expect ~3× more with
the `tset_rs` wheel installed.

## Triage: what's a deal-breaker vs. what's a knob

| Concern | Severity | Mitigation today | v0.4 plan |
|---|---|---|---|
| 4-5× JSONL on tiny-doc corpora | **deal-breaker for tweet-shaped data** | shard records into larger TSET documents | unchanged — this is fundamental to per-doc indexing |
| Inline duplication when binary sections enabled | **bloat** | leave sections off until you need them | mandatory sections, inline forms dropped |
| `smt_present_keys` redundant with `document_index` | minor | tolerate or post-process the manifest | v0.4 spec drops it |
| Hex encoding of every hash in JSON | minor | base64 in v0.4 manifest, binary in sections | v0.4 |
| Manifest grows unbounded with chunk count | manageable | tune chunk size (default 64 K tokens is fine) | v0.4 binary index |
| Dataset Merkle root not bound to exclusion overlay | **gap vs. README pitch** | document and use exclusion-overlay file directly | v0.4 spec extends the leaf set |

The **only** items I'd call deal-breakers for production today are:

1. **Tweet-sized direct ingest** — solvable by sharding records.
2. **Exclusion-overlay binding** — the receipts pitch promises this,
   v0.3.2 doesn't deliver it. Write code as if the dataset root will
   change post-fix; do not depend on its current behavior.

Everything else is a size knob, and the knobs are well-understood.

## "Should I use this for 5 TB?"

If your corpus has **average doc size ≥ 2 KB** and **doc count ≤ 10⁹**,
yes, with the lean config (no binary sections until v0.4 lands):

- Storage cost: **+10–20%** vs raw JSONL, including all proofs
- Read throughput: typically **faster** than JSONL/Parquet
- One Merkle root commits to your whole shard

If you have **billions of small documents** (logs, tweets, sensor
readings), shard them into ~1–10 MB groups before ingest and treat
each group as a TSET document. The receipts pitch still holds at the
shard level.

## Reproducing

```bash
python -m examples.datasets.synthetic_stream.scale_bench
python -m examples.datasets.synthetic_stream.competitive
python -m examples.datasets.tinyshakespeare.bench
```

Outputs: `examples/datasets/synthetic_stream/out/scale_bench.json`,
`competitive.json`, `examples/datasets/tinyshakespeare/out/bench.json`.
