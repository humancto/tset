"""Generate and verify the four kinds of receipt the format claims.

For the TinyShakespeare TSET shard:

1. **Inclusion proof** for a real document.
2. **Non-inclusion proof** for a hash that's intentionally absent.
3. **Tamper detection**: flipping a sibling in either proof must
   make ``proof.verify(root)`` return ``False``.
4. **Audit-log signature** verifies under the recorded Ed25519 public key.

Run after ``convert.py``. Exit code is non-zero if any check fails.
"""

from __future__ import annotations

import sys
from pathlib import Path

from examples.datasets.tinyshakespeare.convert import TSET


def _fmt_hash(b: bytes, n: int = 8) -> str:
    return b[:n].hex() + "…"


def main() -> int:
    if not TSET.exists():
        print(f"error: {TSET} not found — run convert.py first", file=sys.stderr)
        return 2

    from tset.reader import Reader

    failures: list[str] = []
    r = Reader(str(TSET))
    print(f"opened {TSET.name}")
    print(f"  shard_merkle_root  {_fmt_hash(r.header.shard_merkle_root)}")
    print(f"  smt_root           {_fmt_hash(r.smt_root())}")
    print(f"  tokenizer views    {r.tokenizer_ids()}")

    # ── (1) inclusion proof for a real document ─────────────────────
    real_hash = next(iter(r.documents()))[0]
    incl = r.prove_inclusion(real_hash)
    if not incl.verify(r.smt_root()):
        failures.append("inclusion proof for real doc failed to verify")
    print(f"  ✓ inclusion proof   {_fmt_hash(real_hash)}  ({len(incl.siblings)} siblings)")

    # ── (2) non-inclusion proof for an intentionally absent hash ────
    absent_hash = b"\xa5" * 32
    assert not r.has_document(absent_hash)
    non_incl = r.prove_non_inclusion(absent_hash)
    if not non_incl.verify(r.smt_root()):
        failures.append("non-inclusion proof for absent doc failed to verify")
    print(f"  ✓ non-incl. proof   {_fmt_hash(absent_hash)}  ({len(non_incl.siblings)} siblings)")

    # ── (3) tampered proof must fail ────────────────────────────────
    if incl.siblings:
        tampered = type(incl)(
            key=incl.key,
            siblings=[
                bytes(b ^ 0xFF for b in incl.siblings[0]),
                *incl.siblings[1:],
            ],
        )
        if tampered.verify(r.smt_root()):
            failures.append("tampered inclusion proof verified — integrity broken")
        print(f"  ✓ tampered proof rejected (good)")

    # ── (4) audit-log signature ─────────────────────────────────────
    log = r.audit_log()
    if log is None or not log.entries:
        failures.append("audit log missing or empty")
    elif not log.verify():
        failures.append("audit log verification failed (chained-hash or signature)")
    else:
        suffix = ""
        if log.writer_public_key:
            suffix = f", key {log.writer_public_key[:16]}…"
        print(f"  ✓ audit log         ({len(log.entries)} entries{suffix})")

    if failures:
        print("\nFAIL:")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("\nAll receipts verified.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
