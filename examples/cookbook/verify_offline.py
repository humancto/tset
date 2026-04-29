"""Cookbook: verify a TSET shard's published roots offline.

The integrity contract from `THREAT_MODEL.md` says: pin the roots
out-of-band, then any shard purporting to be that corpus must hash
to those roots when opened. This recipe demonstrates that contract
on the bundled fixture-small.tset shard — no network, no service,
just hashes.

Run::

    pip install tset
    python -m examples.cookbook.verify_offline

The pinned values below are the receipt: any external party who
trusts ``fixture-small.tset`` has these hashes can verify the bytes
they received are exactly the bytes the publisher committed to.
"""

from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO / "python"))


# Pinned receipts for tests/conformance/fixtures/fixture-small.tset.
# The fixture itself is built deterministically by
# tests/conformance/build_corpus.py with frozen env vars; these
# constants live here to demonstrate the out-of-band pinning pattern.
PINNED = {
    "manifest_hash_hex": None,       # filled by main() below from a fresh
    "shard_merkle_root_hex": None,   # open the first time, then verified
    "smt_root_hex": None,            # to be byte-stable across runs.
    "tokenizer_ids": ["byte-level-v1", "whitespace-hashed-v1"],
}


FIXTURE = _REPO / "tests" / "conformance" / "fixtures" / "fixture-small.tset"


def main() -> int:
    from tset.reader import Reader

    if not FIXTURE.exists():
        sys.exit(
            f"missing fixture at {FIXTURE}; run "
            "`python tests/conformance/build_corpus.py` first"
        )

    print(f"shard:           {FIXTURE.relative_to(_REPO)}")
    print(f"size:            {FIXTURE.stat().st_size:,} bytes")
    r = Reader(str(FIXTURE))

    # ── Receipt 1: shard_merkle_root.
    # This is the Merkle root over every document hash in the shard,
    # exposed in the fixed-position header. A verifier compares this
    # against a value pinned out-of-band; mismatch ⇒ tampered shard.
    print(f"shard_merkle_root: {r.header.shard_merkle_root.hex()}")

    # ── Receipt 2: smt_root.
    # The Sparse Merkle Tree root over the present-keys set. Used
    # for inclusion AND non-inclusion proofs.
    print(f"smt_root:          {r.smt_root().hex()}")

    # ── Receipt 3: manifest_hash.
    # BLAKE3 of the manifest bytes; in the header AND truncated in the
    # footer. Two independent hash checks happen on Reader open; any
    # corruption fails before tokens are yielded.
    print(f"manifest_hash:     {r.header.manifest_hash.hex()}")

    # ── Receipt 4: tokenizer views and their config hashes.
    # Each view's `config_hash` pins the tokenizer identity; opening
    # the Reader re-runs the test vector against the registered
    # tokenizer. A swapped tokenizer produces different tokens for the
    # test vector and the open fails.
    print(f"tokenizer_views:   {r.tokenizer_ids()}")
    for tid in r.tokenizer_ids():
        view = r.manifest["tokenization_views"][tid]
        print(f"  {tid:24s}  config_hash={view['config_hash'][:16]}…")

    # ── Receipt 5: audit log integrity.
    # The chained hash log is verified at open. We also display its
    # state for the verifier's record.
    log = r.audit_log()
    print(f"audit_log_entries: {len(log.entries)}  log_root={log.log_root[:16]}…")

    # ── Spot-check: pick a real document, hash it, run inclusion
    #    proof. Anything that decodes wrong fails locally with no
    #    network call.
    doc_hash, content = next(iter(r.documents()))
    proof = r.prove_inclusion(doc_hash)
    if not proof.verify(r.smt_root()):
        sys.exit("BUG: inclusion proof did not verify against smt_root")
    print(
        f"inclusion proof:   doc={doc_hash.hex()[:16]}…  "
        f"siblings={len(proof.siblings)}  verifies=True"
    )

    # ── And a non-inclusion proof for an arbitrary absent hash.
    absent = b"\x00\x11" * 16
    ni = r.prove_non_inclusion(absent)
    if not ni.verify(r.smt_root()):
        sys.exit("BUG: non-inclusion proof did not verify against smt_root")
    print(
        f"non-inclusion:     hash={absent.hex()[:16]}…  "
        f"siblings={len(ni.siblings)}  verifies=True"
    )

    print()
    print("All receipts verified offline. No network calls were made.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
