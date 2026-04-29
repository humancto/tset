"""Verify a published TSET corpus end-to-end.

Open a TSET shard from a URL or local path, generate the four receipts
this format claims, and (optionally) compare the SMT root against an
expected value. Prints a short receipt table on success; non-zero exit
on any failure.

Usage::

    # Verify a local shard
    python -m examples.published.verify examples/published/corpus.tset

    # Verify a published shard from the open web (no download tooling required)
    python -m examples.published.verify \\
        https://raw.githubusercontent.com/humancto/tset/main/examples/published/corpus.tset

    # Pin against a published root (fails loudly if the bytes were tampered)
    python -m examples.published.verify \\
        https://raw.githubusercontent.com/humancto/tset/main/examples/published/corpus.tset \\
        --expected-smt-root=369cf1fbacb1af433d2ea84ead6aa326eba6bd4698f872304a533444a5815444

This file is intentionally short. Anyone can read it end-to-end and
confirm what verification actually does.
"""

from __future__ import annotations

import argparse
import sys
import tempfile
import urllib.request
from pathlib import Path


def _resolve(path_or_url: str) -> Path:
    """Return a local path. Downloads to a temp file if given an URL."""
    if path_or_url.startswith(("http://", "https://")):
        tmp = tempfile.NamedTemporaryFile(suffix=".tset", delete=False)
        tmp.close()
        urllib.request.urlretrieve(path_or_url, tmp.name)
        return Path(tmp.name)
    p = Path(path_or_url).expanduser().resolve()
    if not p.is_file():
        sys.exit(f"error: {p} is not a file")
    return p


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("path", help="Path or URL to a .tset shard")
    ap.add_argument(
        "--expected-smt-root",
        default=None,
        help="Hex SMT root to require; verifier exits non-zero on mismatch",
    )
    args = ap.parse_args(argv)

    repo_root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(repo_root / "python"))
    from tset.reader import Reader

    local = _resolve(args.path)
    r = Reader(str(local))

    smt_root = r.smt_root()
    shard_root = r.header.shard_merkle_root
    log = r.audit_log()

    print(f"opened              : {args.path}")
    print(f"size                : {local.stat().st_size:,} bytes")
    print(f"shard_merkle_root   : {shard_root.hex()}")
    print(f"smt_root            : {smt_root.hex()}")
    print(f"document_count      : {len(list(r.doc_order_hex()))}")
    print(f"audit_log_entries   : {len(log.entries)}")

    failures: list[str] = []

    # 1. Inclusion proof for a real document
    real_hash, _ = next(iter(r.documents()))
    incl = r.prove_inclusion(real_hash)
    if incl.verify(smt_root):
        print(f"  ✓ inclusion proof   {real_hash.hex()[:12]}…  ({len(incl.siblings)} siblings)")
    else:
        failures.append("inclusion proof failed verification")

    # 2. Non-inclusion proof for an intentionally absent hash
    absent = b"\xab" * 32
    assert not r.has_document(absent)
    non_incl = r.prove_non_inclusion(absent)
    if non_incl.verify(smt_root):
        print(f"  ✓ non-incl. proof   {absent.hex()[:12]}…")
    else:
        failures.append("non-inclusion proof failed verification")

    # 3. Tampered proof must be rejected
    if incl.siblings:
        tampered = type(incl)(
            key=incl.key,
            siblings=[bytes(b ^ 0xFF for b in incl.siblings[0])] + incl.siblings[1:],
        )
        if tampered.verify(smt_root):
            failures.append("tampered proof verified (integrity broken)")
        else:
            print("  ✓ tampered proof rejected")

    # 4. Audit-log chain
    if not log.verify():
        failures.append("audit log chained-hash failed verification")
    else:
        print(f"  ✓ audit log chain   ({len(log.entries)} entries)")

    # 5. Expected-root pin
    if args.expected_smt_root:
        want = bytes.fromhex(args.expected_smt_root)
        if want != smt_root:
            failures.append(
                f"expected smt_root {args.expected_smt_root}, got {smt_root.hex()}"
            )
        else:
            print(f"  ✓ smt_root matches expected pin")

    if failures:
        print("\nFAIL:")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("\nAll receipts verified.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
