# SMT Design — under review

> **Status:** design under review (RFC §10 items 14-18). Cryptography reviewer
> sign-off required before this document is normative. Until then, the on-disk
> SMT serialisation is **not** part of the v0.1 binary contract — only the
> in-manifest `smt_root` and (transitional) `smt_present_keys` fields are.

## Current parameters (placeholder)

- **Key derivation.** Direct use of the document's BLAKE3 hash as the SMT
  key. No additional domain separation. *Open:* whether to apply
  `BLAKE3(b"tset/smt/key" || doc_hash)` for cross-protocol safety.
- **Depth.** Fixed at 256. *Open:* variable-depth structures could shrink
  proofs at the cost of complexity.
- **Empty branch representation.** Pre-computed `EMPTY[d] = hash(internal ||
  EMPTY[d-1] || EMPTY[d-1])` for d in [0, 256], anchored at
  `EMPTY[0] = hash(leaf || 0x00)`.
- **Leaf hashing.** `PRESENT_LEAF = hash(leaf || 0x01)`,
  `ABSENT_LEAF = hash(leaf || 0x00)`. Domain prefixes `0x10` / `0x11` for
  leaf vs internal.
- **Snapshot semantics.** Each shard's manifest commits to a single
  `smt_root`. Dataset manifest includes the per-shard SMT root + a dataset
  exclusion overlay, both committed under the dataset Merkle root.

## What v0.1 does NOT yet do

- Verkle / RSA accumulator alternatives (RFC §10 item 15).
- Signing of the SMT root or audit log entries (RFC §10 item 17).
- Key rotation events in the audit log (RFC §10 item 18).
- On-disk sparse-tree serialisation (currently the manifest carries the full
  present-key set so the reader can reconstruct the tree; this scales to
  ~10⁶ documents per shard and will be replaced with a sparse on-disk
  encoding in v0.2).

## What we want from a cryptography reviewer

1. Are the domain-separation prefixes (`0x10` / `0x11`) sufficient to
   prevent collision attacks across leaf and internal levels?
2. Is direct use of the BLAKE3 hash safe as the SMT key, or do we need
   additional domain separation?
3. What is the recommended path forward for signing the audit log and the
   SMT root — Ed25519, ECDSA, or post-quantum (e.g. ML-DSA)?
4. Are there published references for snapshot management and rolling SMT
   garbage collection that we should adopt?
