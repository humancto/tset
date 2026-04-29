# TSET threat model

This document is the maintainer-side honest accounting of **what TSET
actually defends against, what it doesn't, and which assumptions are
load-bearing**. It is the companion to `SPEC.md` (what is on disk)
and `docs/compliance/COMPLIANCE.md` (which receipt answers which
question). If you are about to recommend TSET to a regulator, an
auditor, or a security-conscious dataset publisher, read this first.

It is not legal advice. It is the security model the format was
designed against.

## Audiences and what they get

TSET is a **commit-and-prove** layer over an LLM training corpus. The
commitments are:

- `shard_merkle_root` — Merkle root over every document in a shard.
- `smt_root` — Sparse Merkle Tree root over the *set* of document hashes.
- `dataset_merkle_root` — composite root binding shards + exclusions.
- `manifest_hash` — content hash of the manifest JSON, embedded in the
  fixed-position header.
- `audit_log.log_root` — Ed25519-signed hash chain over every
  ingest/exclude/view-add event.

A verifier holding any of those roots, plus the shard bytes and the
appropriate proof, can answer one of the four compliance questions
(`docs/compliance/COMPLIANCE.md` §1–§4) **without trusting the
publisher to replay anything**. This document explains what that
guarantee depends on.

## Trust boundaries

TSET makes a hard distinction between **the publisher** (the party
who built the shard) and **the verifier** (anyone else holding the
public roots).

- The publisher is **trusted to choose what goes into the shard**.
  TSET does not, and cannot, vouch that the documents they ingested
  are the documents they claim to have ingested in the real world.
  Garbage-in / garbage-out applies to the *contents*.
- The publisher is **not trusted not to lie about it later**. Once the
  roots are published, every claim ("doc X was in the training set",
  "doc Y was deleted on date Z", "the corpus was tokenized this way")
  is a hash-checked proof against an immutable commitment.

Concretely: the format protects against post-hoc rewrites by the
publisher. It does not protect against fraud at ingest time.

## Threats in scope

### T1. Silent corpus rewriting after publication

**Attack.** Publisher posts `smt_root = R`. Months later, after a
deletion request or a controversy, they swap a document, regenerate
the shard, and serve the new bytes under the same name.

**Defence.** The new shard has a different `smt_root`. Any verifier
who pinned `R` from the original announcement (model card, press
release, on-chain notarisation, archived link) sees the mismatch on
first open: `Reader.__init__` rejects the shard before yielding any
data. There is no "best effort" path that ignores the root. (See
`reader.py:_verify_invariants`.)

**What's load-bearing.** The verifier must have pinned the root
**outside** the bytes they later receive. TSET cannot help if the
only copy of `R` lives on the publisher's own website.

### T2. Bit-flips, partial downloads, transport corruption

**Attack.** A shard is corrupted in transit, on disk, or by a buggy
mirror.

**Defence.** Every layer is hash-checked on read. The manifest is
hashed and compared against both the header copy and the truncated
footer copy. Each compressed block's uncompressed BLAKE3 is the
document's content address — corruption changes the address, which
changes the SMT root, which fails verification before any tokens are
yielded.

### T3. Partial-disclosure inclusion lies

**Attack.** Publisher claims doc X is in the corpus and serves only
the leaf bytes, hoping the verifier accepts it without proof.

**Defence.** Inclusion is asserted via `prove_inclusion(doc_hash)`,
which returns a Merkle path against `smt_root`. The verifier reruns
`InclusionProof.verify(root)` locally; without the full sibling chain
the proof is unforgeable under collision-resistance of BLAKE3.

### T4. Selective non-disclosure

**Attack.** Publisher quietly drops a document from a shard but
continues to claim it was trained on.

**Defence.** Asymmetric: a verifier cannot detect the drop unless
they already had the published root from before the drop. With the
old root, `prove_non_inclusion` against the *new* shard succeeds for
the dropped document, exposing the discrepancy.

This is exactly the pattern used for legitimate deletion (T7) — the
mechanism is the same, only the audit log entry distinguishes
"GDPR-Art-17 erasure" from "we removed it because we didn't like the
question". TSET makes that entry mandatory but cannot enforce that
its `reason` field is honest. See `audit_log.py`.

### T5. Tokenizer-view tampering

**Attack.** Publisher swaps the tokens served for a document, hoping
nobody re-tokenizes from raw bytes.

**Defence.** Each view stores `tokenizer_config` plus a deterministic
`test_vector`. Opening a Reader re-encodes the test vector against
the registered tokenizer class and rejects mismatches. Custom
adapters (e.g. `HfTokenizer`) skip the *class-level* re-encode but
still enforce per-chunk `content_hash`, so swapped tokens are caught.

**What's load-bearing.** The verifier must have a tokenizer
implementation that's bit-identical to the publisher's. The
conformance fixtures (`tests/conformance/fixtures/`) lock this for
the bundled tokenizers; for custom adapters, the verifier needs to
audit the adapter itself.

### T6. Audit-log truncation, reordering, or forgery

**Attack.** Publisher ingests doc X, regrets it, deletes the
corresponding audit entry, and re-signs the log.

**Defence.** Every entry chains: `entry_i.prev_hash =
H(entry_{i-1})`. Removing an entry breaks the chain at the next
entry, which fails `AuditLog.verify()`. The whole log is sealed by
`log_root`, which is exposed (a) inside the manifest and (b) on disk
in the TLOG section in v0.4 shards. A tamper has to forge **every**
hash from the deletion point forward and produce a new signature.

The Ed25519 signature is the second wall: even with the chain
recomputed, an attacker cannot mint a valid signature without the
private key.

### T7. Right-to-erasure compliance

**Attack.** Not actually an attack — but a frequent worry from
security review: "How do you delete a document under GDPR Art. 17
without invalidating every previously published root?"

**Mechanism.** TSET treats erasure as an **overlay**, not a rewrite.
The original shard's `smt_root` stays valid; an exclusion record
(signed and dated) is added to the dataset's exclusion overlay; the
**composite** `dataset_merkle_root` rolls forward. A verifier
checking inclusion of the deleted document gets a non-inclusion
proof against the new dataset root, while still being able to verify
the old shard root for any historical claim.

This pattern means the deletion is **permanent and provable**, not
that the bytes are wiped from every disk that ever held the shard.
That's by design: cryptographically committing to "we will no longer
train on this" is exactly what the regulator asks for; physically
chasing every replica is a storage problem TSET deliberately doesn't
solve.

## Threats out of scope

### O1. Pre-ingest lies

If the publisher ingests a synthetic document and labels it as a
real CommonCrawl URL, TSET records the synthetic content
faithfully. The format binds **bytes to roots**, not bytes to
real-world provenance. Provenance metadata in the metadata column
is only as trustworthy as the publisher's collection pipeline.

### O2. Sidechannel inference about excluded documents

If a verifier can observe model behaviour and infer that an
excluded document was *almost certainly* in training data, that's a
membership-inference attack on the *model* — outside TSET's
boundary. TSET commits to *what was assembled*, not to what the
gradient ultimately memorised.

### O3. Compute / quality fraud

TSET does not prove that a model was actually trained on the
committed corpus, only that the corpus is what it claims to be.
Linking a model checkpoint to a training-data root is a separate
problem (training-step receipts, gradient-tape attestation,
hardware-attested logs) that is out of scope here.

### O4. Live tampering of an open Reader

If an attacker can write to the open shard file while a Reader is
streaming from it, mmap reads will see torn state. TSET assumes
ordinary filesystem semantics: the shard is read-only after publish.
Use OS-level file permissions and immutable storage if you need to
defend against this.

### O5. Side-channel timing of proofs

`SparseMerkleTree.prove` is not constant-time. A network attacker
who can time proof generation may infer information about which
keys are present. If you serve proofs over a network, batch them
and add jitter, or generate proofs offline.

## Cryptographic assumptions

In rough order of how badly we'd be hosed if any one of them broke:

| Primitive | Used for | Source | Status |
|---|---|---|---|
| BLAKE3 | content addressing, manifest hash, Merkle leaves, audit chain | `blake3` crate (Rust), `blake3` PyPI (Python) | unbroken; designed for tree hashing |
| Ed25519 | audit-log signature | `ed25519-dalek` (Rust), `cryptography` (Python) | unbroken in the academic literature; use the deterministic-nonce construction (RFC 8032) |
| Sparse Merkle Tree (depth 256) | inclusion / non-inclusion proofs | `tset.smt` | constructed over BLAKE3; security reduces to BLAKE3 collision resistance |
| zstd | block compression | `zstd` crate, `zstandard` PyPI | not security-critical: hashes are over **uncompressed** content |

If BLAKE3 falls, the format must be re-versioned (header magic
change, new SMT). The audit-log signature alone would survive a
hash break only as long as the entry payload is independently
authenticated, which it isn't — so a hash break would break the
chain too.

## Key-management assumptions

The audit log is signed with an Ed25519 key chosen at write time.
TSET makes **no** assertion about how that key is generated, stored,
or rotated. Three deployment patterns we recommend:

1. **Per-shard ephemeral key, public key in manifest.** Strongest
   non-repudiation: the shard contains its own verifier. Loses
   nothing if the key leaks afterwards because the audit log is
   sealed.
2. **Long-lived org key, public key pinned out-of-band.** Cheapest;
   sensible for orgs that already publish a signing key for releases.
   A leaked key invalidates the trust model going forward but cannot
   retroactively forge sealed logs (the `log_root` is published).
3. **HSM-backed key.** Required if you intend to publish under a
   regulatory regime with key-storage requirements.

If you do not need non-repudiation, you may use a constant test key.
TSET will still detect tampering as long as the verifier checks the
chain; the signature only adds the "this was you" property.

## What a verifier should actually do

The minimum diligence we'd defend in a code review:

1. **Pin the roots out-of-band.** `smt_root`, `manifest_hash`, and
   (if cross-shard) `dataset_merkle_root` come from somewhere the
   publisher cannot retroactively edit.
2. **Open the shard with `Reader(...)`.** This rejects on first byte
   if the manifest, shard root, or audit log are tampered.
3. **Re-derive at least one inclusion proof.** Pick a document at
   random, re-hash its bytes, run `prove_inclusion`, verify against
   the pinned root. Catches partial substitutions that pass the
   structural checks but corrupt one document.
4. **Re-tokenize one chunk.** Run `verify_tokenizer_view` for each
   declared tokenizer. Catches any view-section tamper that the
   chunk-level `content_hash` would otherwise still let through.
5. **Walk the audit log.** Confirm `log_root` matches the manifest
   and (v0.4+) the on-disk TLOG section. Confirm the signing key is
   the one you expect.

`examples/compliance/audit.py` does all five against any shard.

## Continuous fuzz coverage

Two `cargo-fuzz` targets run nightly via
[`.github/workflows/fuzz.yml`](../../.github/workflows/fuzz.yml):

- **`reader_open`** — feeds arbitrary bytes to `Reader::open`. The
  contract: never panic. Either succeed (input was a valid TSET
  shard) or return a `TsetError`. Any panic is a security bug.
- **`sections_decode`** — feeds arbitrary bytes to the three v0.3.2
  on-disk section decoders (`decode_tsmt_section`,
  `decode_tlog_section`, `decode_tcol_section`). These are the
  parser surface a verifier hits when it follows a manifest pointer
  into hostile bytes.

The corpus is cached across runs, so coverage compounds nightly. A
crash during a run uploads the reproducer as an artefact and fails
the workflow — see the run log if you spot a failure on the
**Fuzz** badge.

## Reporting issues

Security issues: open a private advisory on
[`github.com/humancto/tset/security`](https://github.com/humancto/tset/security/advisories/new),
not a public issue. We commit to responding within 7 days.
