"""Smoke test for ``examples/compliance/audit.py``.

The reference verifier is the runnable counterpart of
``docs/compliance/COMPLIANCE.md`` — an external auditor's
self-contained way to validate a TSET shard's receipts. The test
exercises the four-receipt path against a committed conformance
fixture so a regression in any of the five public APIs the verifier
calls (Reader, smt_root, prove_inclusion, prove_non_inclusion,
audit_log().verify()) surfaces immediately.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
FIXTURE = REPO / "tests" / "conformance" / "fixtures" / "fixture-small.tset"


def test_audit_succeeds_against_known_good_shard():
    """All four receipts verify on a known-good v0.3.2 fixture; exit 0."""
    pytest.importorskip("tset")
    sys.path.insert(0, str(REPO))
    from examples.compliance.audit import audit

    rep = audit(FIXTURE)
    assert rep.ok, f"audit failures: {rep.failures}"
    assert rep.smt_root  # non-empty hex
    assert rep.shard_merkle_root  # non-empty hex
    assert rep.document_count >= 1
    assert rep.audit_log_entries >= 1
    types = {r["type"] for r in rep.receipts}
    assert "inclusion_proof" in types
    assert "non_inclusion_proof" in types
    assert "audit_log" in types


def test_audit_pinned_smt_root_match():
    """When the published SMT root matches the shard, the audit
    reports a published_root_match receipt."""
    pytest.importorskip("tset")
    sys.path.insert(0, str(REPO))
    from examples.compliance.audit import audit
    from tset.reader import Reader

    actual = Reader(str(FIXTURE)).smt_root().hex()
    rep = audit(FIXTURE, expected_smt_root=actual)
    assert rep.ok
    assert any(r["type"] == "published_root_match" for r in rep.receipts)


def test_audit_pinned_smt_root_mismatch_fails():
    """A wrong published root must surface as a failure (the whole
    point of the pin). The verifier does not silently accept it."""
    pytest.importorskip("tset")
    sys.path.insert(0, str(REPO))
    from examples.compliance.audit import audit

    rep = audit(FIXTURE, expected_smt_root="00" * 32)
    assert not rep.ok
    assert any("smt_root mismatch" in f for f in rep.failures)


def test_audit_check_doc_hash_runs_inclusion_proof():
    """Pointing the verifier at a specific present doc produces an
    inclusion proof for that doc. Catches regressions where the
    --check-doc-hash flag drifts away from prove_inclusion."""
    pytest.importorskip("tset")
    sys.path.insert(0, str(REPO))
    from examples.compliance.audit import audit
    from tset.reader import Reader

    target = next(iter(Reader(str(FIXTURE)).documents()))[0]
    rep = audit(FIXTURE, check_doc_hash=target)
    assert rep.ok
    incl = next(r for r in rep.receipts if r["type"] == "inclusion_proof")
    assert incl["doc_hash"] == target.hex()


def test_audit_rejects_check_doc_hash_for_absent_doc():
    """Asking for an inclusion proof on a doc that isn't in the shard
    is operator error — the verifier surfaces it as a failure rather
    than silently swap to a non-inclusion proof."""
    pytest.importorskip("tset")
    sys.path.insert(0, str(REPO))
    from examples.compliance.audit import audit

    rep = audit(FIXTURE, check_doc_hash=b"\xee" * 32)
    assert not rep.ok
    assert any("not in this shard" in f for f in rep.failures)
