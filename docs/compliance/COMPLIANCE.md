# Reference compliance workflow

This document is the maintainer guide for **using TSET to answer
compliance questions** — the "show your work" layer between a
training pipeline and an outside party (an auditor, a regulator, or a
user filing a deletion request).

It does not constitute legal advice. Whether your specific use of TSET
satisfies any specific regulation is for your counsel to determine.
What this doc gives you is the **operational pattern**: which TSET
artefact answers which question, who needs to see what, and what a
verifier needs to accept the answer.

## The four questions

Compliance work for an LLM training pipeline boils down to four
questions, asked by different audiences with different evidence
thresholds. TSET produces a specific receipt for each.

| # | Question | Asked by | TSET receipt | Doc |
|---|---|---|---|---|
| 1 | "What did you train on?" | regulator, internal review | published `dataset_merkle_root` + the binary | §1 below |
| 2 | "Was *this* document in your training set?" | regulator, downstream user | inclusion proof against `smt_root` | §2 |
| 3 | "Was *this* document **excluded** from your training set?" | user filing GDPR Art. 17 / DPDPA / RTBF; auditor checking deletion-overlay claim | non-inclusion proof against `smt_root` (or composite root with overlay) | §3 |
| 4 | "Did you record every change to the corpus?" | regulator examining process discipline; auditor doing chain-of-custody | signed `audit_log` | §4 |

The runnable counterpart of this doc is
[`examples/compliance/audit.py`](../../examples/compliance/audit.py),
a single-file reference verifier that produces every receipt for a
given shard.

## §1. Publishing a Merkle root

The first thing you do is **commit to a snapshot of your corpus** and
publish the commitment. TSET's commitment is the
`shard_merkle_root` (single shard) or `dataset_merkle_root`
(multi-shard dataset). You publish it once; thereafter any party can
demand: "give me a TSET shard whose root matches this hash, plus the
proofs I asked for."

```python
from tset.reader import Reader
r = Reader("training-corpus.tset")
print(r.smt_root().hex())            # the SMT root over all doc hashes
print(r.header.shard_merkle_root.hex())  # shard-level Merkle root
```

Pin both values somewhere outside TSET: a commit on your model card,
a notarised PDF, a signed press release. Once published, the values
are receipts for every claim below.

For multi-shard datasets:

```python
from tset.dataset import Dataset
ds = Dataset("training-corpus.tset/")
print(ds.dataset_merkle_root().hex())  # composite root (shards + exclusions)
```

The composite root binds **shards + exclusions + subset weights**
into a single value (see SPEC §8a and §8a.1). Changing any of those
changes the root; readers verifying against your published value
detect it.

## §2. Inclusion proof — "yes, it was in"

When a regulator points at a document and asks "was this in your
training set?", you produce an **inclusion proof** against your
published `smt_root`:

```python
from tset.reader import Reader
import hashlib
r = Reader("training-corpus.tset")

# The doc hash they're asking about — they computed it themselves;
# you don't need to read their copy.
doc_hash = bytes.fromhex("a55d…")

if r.has_document(doc_hash):
    proof = r.prove_inclusion(doc_hash)
    # proof.key, proof.siblings — opaque blob to send to the auditor
    serialized = {
        "claim": "doc_present_in_smt",
        "doc_hash": doc_hash.hex(),
        "smt_root": r.smt_root().hex(),
        "key": proof.key.hex(),
        "siblings": [s.hex() for s in proof.siblings],
    }
```

The auditor independently re-derives the SMT root from `(key,
siblings)` and asserts equality with your published `smt_root`. They
do not need access to the rest of your corpus, your tokenizer
configs, your audit log — only:

- the doc hash they care about,
- the proof bundle,
- your published `smt_root`.

That property — proof verification without seeing the corpus — is
why the SMT exists at all.

## §3. Non-inclusion proof — "no, it was excluded"

The deletion case is the harder one and the more legally interesting.
A user files a GDPR Art. 17 (or equivalent) request asking you to
prove their data is **not** in your training set. With TSET:

```python
absent_hash = bytes.fromhex("a55d…")  # the user's doc hash

# (a) Single-shard, never-ingested:
proof = r.prove_non_inclusion(absent_hash)
assert proof.verify(r.smt_root())

# (b) Multi-shard dataset, where the document was ingested earlier
#     and you've since added it to the exclusion overlay:
ds = Dataset("training-corpus.tset/")
bundle = ds.prove_non_inclusion(absent_hash)
# bundle is a JSON object with one entry per shard:
#   - shards where the doc was never ingested → standalone non-incl proof
#   - shards where the doc IS ingested but the dataset overlay excludes
#     it → inclusion proof + an attestation that the doc_hash appears in
#     the published exclusion list (which is itself bound by the
#     composite dataset_merkle_root, see SPEC §8a)
```

Both forms verify against your published root. The second form is
the one a deletion-request response should produce: it cryptographically
records (a) you have the document, (b) you've placed it on the
exclusion overlay, and (c) every conforming reader will skip it
during training.

## §4. Audit log

Process discipline is its own evidence. The TSET audit log is a
chained-hash log of every meaningful event during shard construction:

- `ingestion` — every `add_document` call
- `tokenizer_added` — every `add_tokenizer_view` call
- `exclusion` — every `add_exclusion` call (dataset-level)
- `version_snapshot` — close-time receipt with the snapshot id and
  the dataset Merkle root

Optionally, with PR-10's signing landed, every entry is **Ed25519
signed** by a key the writer controls. The public key is published
in the manifest so a verifier can validate signatures without your
secrets. That makes the chain tamper-evident even if an attacker
rewrites the manifest hash.

```python
log = r.audit_log()
assert log.verify()  # chained-hash + signatures
for ev in log.entries:
    print(ev.event_type, ev.payload, ev.signature[:16] if ev.signature else "(unsigned)")
```

## What the auditor receives

Bundle for a typical compliance response:

| File | Purpose |
|---|---|
| `corpus.tset` (or a hosted URL) | The shard itself |
| `smt_root.txt` | The published SMT root, in your trust path (model card, repo tag, etc.) |
| `proof_<hash>.json` | One inclusion or non-inclusion proof bundle per requested doc |
| `audit_log.json` | The full audit log (extracted via `r.audit_log().to_dict()`) |
| `writer_pubkey.txt` | The Ed25519 public key the writer used to sign the audit log |
| `verifier.py` | A copy of `examples/compliance/audit.py` so the auditor doesn't need to install TSET to validate |

The auditor's checklist is short:

```
[ ] verifier opens corpus.tset
[ ] verifier prints smt_root and it matches smt_root.txt
[ ] for each proof_*.json: verifier asserts proof.verify(smt_root) is True
[ ] audit_log.verify() returns True
[ ] writer_pubkey.txt matches the manifest's writer_public_key
```

If all five hold, the auditor has cryptographic certainty about your
claims — not legal certainty about your conclusions, but a hard
floor under what's possible.

## What this doc is *not*

- Not a substitute for a privacy-impact assessment, data-protection
  agreement, or any other instrument your jurisdiction requires.
- Not a guarantee that an "excluded" document was never seen by an
  earlier model checkpoint. The exclusion overlay applies at
  read-time for every conforming reader; if your weights file was
  trained against the corpus before the exclusion was added, that's
  a separate question (typically answered by retraining or a
  fine-tuning patch — TSET helps you re-establish the corpus state,
  not the model state).
- Not a replacement for your incident-response process. A
  cryptographic receipt is evidence; it's not the response itself.

The full RFC §5.7 honesty section also applies: TSET makes
**integrity** claims, not authenticity claims. A non-inclusion proof
shows the document is not in *this* shard; it doesn't prove the
document never existed in some earlier shard or some other artefact
of your pipeline.

## Reproducing this from the published example

The verifier in `examples/compliance/audit.py` runs against the
published TSET corpus this repo ships in
`examples/published/corpus.tset`. To exercise the full flow:

```bash
pip install tset
python -m examples.compliance.audit examples/published/corpus.tset
```

Output: SMT root, an inclusion proof for one real document, a
non-inclusion proof for an absent hash, the audit-log status, and
exit 0. That's the entire compliance flow on real bytes from a
fresh terminal.
