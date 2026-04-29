"""Reference compliance verifier: a single self-contained script an
external auditor can run against a TSET shard to validate the four
receipts the format provides:

  1. Published-root match
  2. Inclusion proof for a real document
  3. Non-inclusion proof for an intentionally absent hash
  4. Audit-log integrity (chained-hash; Ed25519 if signed)

Usage::

    pip install tset
    python -m examples.compliance.audit <path-or-url>.tset \\
        [--expected-smt-root <hex>] \\
        [--check-doc-hash <hex>] \\
        [--check-absent-hash <hex>]

Exit 0 if every receipt verifies. Exit non-zero with a per-receipt
failure list otherwise.

This script is the runnable counterpart of
``docs/compliance/COMPLIANCE.md``. Read both — the doc covers the
process and audience for each receipt; the script shows the exact
five public APIs you need to validate them.
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
import urllib.request
from dataclasses import asdict, dataclass, field
from pathlib import Path


@dataclass
class AuditReport:
    smt_root: str = ""
    shard_merkle_root: str = ""
    document_count: int = 0
    audit_log_entries: int = 0
    writer_public_key: str | None = None
    receipts: list[dict] = field(default_factory=list)
    failures: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.failures

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2, sort_keys=True)


def _resolve(path_or_url: str) -> tuple[Path, bool]:
    """Return ``(local_path, is_temp)``. Cleans up temp files in the
    caller's ``finally`` block when ``is_temp=True``. Same shape as
    ``examples/published/verify.py``."""
    if path_or_url.startswith(("http://", "https://")):
        tmp = tempfile.NamedTemporaryFile(suffix=".tset", delete=False)
        tmp.close()
        urllib.request.urlretrieve(path_or_url, tmp.name)
        return Path(tmp.name), True
    p = Path(path_or_url).expanduser().resolve()
    if not p.is_file():
        sys.exit(f"error: {p} is not a file")
    return p, False


def _bootstrap() -> None:
    """Make ``tset`` importable when running from the repo source tree."""
    repo_root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(repo_root / "python"))


def audit(
    path: Path,
    *,
    expected_smt_root: str | None = None,
    check_doc_hash: bytes | None = None,
    check_absent_hash: bytes | None = None,
) -> AuditReport:
    """Run all four receipts against the shard. Returns an AuditReport
    with the verification record."""
    _bootstrap()
    from tset.reader import Reader

    r = Reader(str(path))
    rep = AuditReport(
        smt_root=r.smt_root().hex(),
        shard_merkle_root=r.header.shard_merkle_root.hex(),
        document_count=len(list(r.doc_order_hex())),
    )

    # ── (1) published-root pin ─────────────────────────────────────
    if expected_smt_root is not None:
        if expected_smt_root.lower() == rep.smt_root.lower():
            rep.receipts.append({
                "type": "published_root_match",
                "expected": expected_smt_root,
                "verified": True,
            })
        else:
            rep.failures.append(
                f"smt_root mismatch: published={expected_smt_root}, "
                f"observed={rep.smt_root}"
            )

    # ── (2) inclusion proof for a real document ────────────────────
    target_hash = check_doc_hash
    if target_hash is None:
        target_hash = next(iter(r.documents()))[0]
    elif not r.has_document(target_hash):
        rep.failures.append(
            f"--check-doc-hash {target_hash.hex()} not in this shard "
            "(use --check-absent-hash for non-inclusion checks)"
        )
        target_hash = None

    if target_hash is not None:
        proof = r.prove_inclusion(target_hash)
        if proof.verify(r.smt_root()):
            rep.receipts.append({
                "type": "inclusion_proof",
                "doc_hash": target_hash.hex(),
                "siblings": len(proof.siblings),
                "verified": True,
            })
        else:
            rep.failures.append(
                f"inclusion proof for {target_hash.hex()} did not verify"
            )

    # ── (3) non-inclusion proof for an intentionally absent hash ───
    absent = check_absent_hash if check_absent_hash is not None else b"\xab" * 32
    if r.has_document(absent):
        rep.failures.append(
            f"--check-absent-hash {absent.hex()} is actually present in shard; "
            "pick a different hash or use --check-doc-hash"
        )
    else:
        ni_proof = r.prove_non_inclusion(absent)
        if ni_proof.verify(r.smt_root()):
            rep.receipts.append({
                "type": "non_inclusion_proof",
                "doc_hash": absent.hex(),
                "siblings": len(ni_proof.siblings),
                "verified": True,
            })
        else:
            rep.failures.append(
                f"non-inclusion proof for {absent.hex()} did not verify"
            )

    # ── (4) audit-log integrity ────────────────────────────────────
    log = r.audit_log()
    if log is None or not log.entries:
        rep.failures.append("audit log missing or empty")
    else:
        rep.audit_log_entries = len(log.entries)
        rep.writer_public_key = log.writer_public_key
        if log.verify():
            rep.receipts.append({
                "type": "audit_log",
                "entries": len(log.entries),
                "signed": bool(log.writer_public_key),
                "verified": True,
            })
        else:
            rep.failures.append("audit-log chained-hash verification failed")

    return rep


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description="TSET reference compliance verifier"
    )
    p.add_argument("path", help="Path or HTTPS URL to a .tset shard")
    p.add_argument(
        "--expected-smt-root",
        help="Hex SMT root the corpus's owner has published; verifier exits "
        "non-zero on mismatch.",
    )
    p.add_argument(
        "--check-doc-hash",
        help="Hex doc hash to assert IS in the shard. Default: pick the "
        "first ingested doc.",
    )
    p.add_argument(
        "--check-absent-hash",
        help="Hex doc hash to assert is NOT in the shard. Default: 0xab*32.",
    )
    p.add_argument(
        "--json",
        action="store_true",
        help="Emit the AuditReport as JSON instead of human text.",
    )
    args = p.parse_args(argv)

    local, is_temp = _resolve(args.path)
    try:
        rep = audit(
            local,
            expected_smt_root=args.expected_smt_root,
            check_doc_hash=bytes.fromhex(args.check_doc_hash) if args.check_doc_hash else None,
            check_absent_hash=bytes.fromhex(args.check_absent_hash) if args.check_absent_hash else None,
        )
    finally:
        if is_temp:
            try:
                local.unlink()
            except OSError:
                pass

    if args.json:
        print(rep.to_json())
    else:
        print(f"opened              : {args.path}")
        print(f"shard_merkle_root   : {rep.shard_merkle_root}")
        print(f"smt_root            : {rep.smt_root}")
        print(f"document_count      : {rep.document_count}")
        print(f"audit_log_entries   : {rep.audit_log_entries}")
        if rep.writer_public_key:
            print(f"writer_public_key   : {rep.writer_public_key}")
        for r in rep.receipts:
            kind = r["type"].replace("_", "-")
            extra = ""
            if r["type"] == "inclusion_proof":
                extra = f"  ({r['siblings']} siblings, doc {r['doc_hash'][:12]}…)"
            elif r["type"] == "non_inclusion_proof":
                extra = f"  ({r['siblings']} siblings, absent {r['doc_hash'][:12]}…)"
            elif r["type"] == "audit_log":
                extra = (
                    f"  ({r['entries']} entries, "
                    f"{'signed' if r['signed'] else 'unsigned'})"
                )
            print(f"  ✓ {kind}{extra}")
        if rep.failures:
            print("\nFAIL:")
            for f in rep.failures:
                print(f"  - {f}")
        else:
            print("\nAll receipts verified.")

    return 0 if rep.ok else 1


if __name__ == "__main__":
    sys.exit(main())
