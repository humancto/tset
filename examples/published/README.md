# Published TSET corpus + verifier

This directory contains a small TSET shard that is **publicly hosted**
on the open web and a **30-line verifier** anyone can run to confirm
the receipts. It is the single most concrete demonstration of the
format: real bytes, on the open internet, with a published Merkle
root that you can pin in your own code and watch fail loudly if the
artefact is ever tampered with.

## The artefact

| | |
|---|---|
| **Hosted at** | <https://raw.githubusercontent.com/humancto/tset/main/examples/published/corpus.tset> |
| **Source** | First 200 paragraphs of TinyShakespeare (`karpathy/char-rnn`) — public domain |
| **Size** | 188,540 bytes |
| **Format** | TSET v0.3.2, single shard, `byte-level-v1` view, no binary sections |
| **Receipts** | See [`PUBLISHED-ROOT.txt`](PUBLISHED-ROOT.txt) |

The published `smt_root` for the current artefact is:

```
369cf1fbacb1af433d2ea84ead6aa326eba6bd4698f872304a533444a5815444
```

If you ever fetch the URL and compute a different `smt_root`, the file
has been tampered with, mirror is wrong, or the build script has been
updated and `PUBLISHED-ROOT.txt` should reflect that.

## Verify it from your laptop in 10 seconds

```bash
pip install tset

python -m examples.published.verify \
  https://raw.githubusercontent.com/humancto/tset/main/examples/published/corpus.tset \
  --expected-smt-root=369cf1fbacb1af433d2ea84ead6aa326eba6bd4698f872304a533444a5815444
```

Output (on success):

```
opened              : https://raw.githubusercontent.com/...
size                : 188,540 bytes
shard_merkle_root   : 9aaf829b410a26085a5e0fd30b8c130c58771dec25fb5f760f4d4a5494b623ab
smt_root            : 369cf1fbacb1af433d2ea84ead6aa326eba6bd4698f872304a533444a5815444
document_count      : 200
audit_log_entries   : 202
  ✓ inclusion proof   912c26a1450a…  (256 siblings)
  ✓ non-incl. proof   abababababab…
  ✓ tampered proof rejected
  ✓ audit log chain   (202 entries)
  ✓ smt_root matches expected pin

All receipts verified.
```

That's the receipts pitch in one terminal session: an *outsider* with
no access to your build pipeline can verify what's in your published
corpus, prove what isn't, and detect any bytes that were changed
between you and them.

## Reproducing the build

The corpus is **byte-identical across builds** when the determinism
environment is set. Anyone can re-run the build script and confirm
the same Merkle root from the same source:

```bash
TSET_DETERMINISTIC_CREATED_AT=2026-01-01T00:00:00+00:00 \
TSET_DETERMINISTIC_SNAPSHOT_ID=tset-published-shakespeare-v0001 \
TSET_DETERMINISTIC_TIME=1735689600.0 \
python -m examples.published.build
```

`build.py` sets these automatically, so:

```bash
python -m examples.published.build
```

is enough. The output `corpus.tset` will be byte-identical to the
committed file on the same Python version.

## What this directory contains

| File | Purpose |
|---|---|
| `build.py` | Deterministic builder. Re-run anytime; produces byte-identical output. |
| `verify.py` | 30-line verifier. Takes a path or URL + optional pinned `smt_root`. |
| `corpus.tset` | The published shard (committed). |
| `PUBLISHED-ROOT.txt` | Human-readable receipt with all the hashes. |

## Writing your own verifier

`verify.py` calls just five public Python APIs:

```python
from tset.reader import Reader

r = Reader(path)
r.smt_root()                      # 32-byte SMT root
r.header.shard_merkle_root        # 32-byte shard Merkle root
r.prove_inclusion(doc_hash)       # → InclusionProof, .verify(root) → bool
r.prove_non_inclusion(doc_hash)   # → NonInclusionProof, .verify(root) → bool
r.audit_log().verify()            # → bool, chained-hash + signature check
```

Auditors who want to verify a published TSET corpus offline against a
published root need exactly this much code. Read the file, it really is
that short.
