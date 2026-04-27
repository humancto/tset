"""PR 10 — Ed25519 audit-log signing.

Covers:
  - generate_signing_key returns a (32-byte secret, 32-byte public) pair
  - signing_public_key derives the public key from a secret
  - Writer(... signing_key=secret) emits signed audit entries
  - Reader rejects a tampered signature
  - Reader rejects a downgrade attempt (drop signatures from a signed log)
  - Reader rejects a manifest where signatures appear without a pubkey
"""

import json
import os

import pytest

from tset.audit_log import AuditLog
from tset.hashing import hash_bytes
from tset.reader import Reader as PyReader

tset_rs = pytest.importorskip("tset_rs")


def test_generate_signing_key_shapes():
    sk, pk = tset_rs.generate_signing_key()
    assert len(sk) == 32
    assert len(pk) == 32
    assert tset_rs.signing_public_key(sk) == pk


def test_writer_with_signing_key_emits_signed_entries(tmp_path):
    sk, pk = tset_rs.generate_signing_key()
    p = str(tmp_path / "signed.tset")
    with tset_rs.Writer(p, None, sk) as w:
        w.add_document(b"alpha")
        w.add_document(b"beta")
        w.add_tokenizer_view("byte-level-v1", 256)

    py = PyReader(p)
    audit = py.manifest["audit_log"]
    assert audit.get("writer_public_key") == pk.hex()
    for entry in audit["entries"]:
        assert "signature" in entry
        # 64-byte Ed25519 signature, 128 hex chars
        assert len(entry["signature"]) == 128
        msg = bytes.fromhex(entry["entry_hash"])
        sig = bytes.fromhex(entry["signature"])
        assert tset_rs.verify_audit_signature(pk, msg, sig)


def test_reader_rejects_tampered_signature(tmp_path):
    """Tampering a signature in-memory and re-verifying via AuditLog
    should fail. (Tampering on disk would also fail the manifest hash
    check; this test isolates the signature check.)"""
    sk, _pk = tset_rs.generate_signing_key()
    p = str(tmp_path / "tamp.tset")
    with tset_rs.Writer(p, None, sk) as w:
        w.add_document(b"alpha")
        w.add_tokenizer_view("byte-level-v1", 256)

    py = PyReader(p)
    audit = py.manifest["audit_log"]
    # Flip a byte in entry 0's signature
    sig0 = audit["entries"][0]["signature"]
    audit["entries"][0]["signature"] = "00" + sig0[2:]
    log = AuditLog.from_dict(audit)
    assert not log.verify(), "tampered signature must fail verify"


def test_reader_rejects_downgrade_drop_signatures(tmp_path):
    """Cannot strip signatures from a signed log: keeping the
    writer_public_key but removing entry signatures must fail."""
    sk, _pk = tset_rs.generate_signing_key()
    p = str(tmp_path / "down.tset")
    with tset_rs.Writer(p, None, sk) as w:
        w.add_document(b"alpha")
        w.add_tokenizer_view("byte-level-v1", 256)
    py = PyReader(p)
    audit = py.manifest["audit_log"]
    for e in audit["entries"]:
        e.pop("signature", None)
    log = AuditLog.from_dict(audit)
    assert not log.verify(), "drop-signatures downgrade must fail"


def test_reader_rejects_signatures_without_pubkey(tmp_path):
    """Signatures without a published pubkey are not trusted."""
    sk, _pk = tset_rs.generate_signing_key()
    p = str(tmp_path / "nopk.tset")
    with tset_rs.Writer(p, None, sk) as w:
        w.add_document(b"alpha")
        w.add_tokenizer_view("byte-level-v1", 256)
    py = PyReader(p)
    audit = py.manifest["audit_log"]
    audit.pop("writer_public_key")
    log = AuditLog.from_dict(audit)
    assert not log.verify()


def test_unsigned_writes_still_work(tmp_path):
    """Writing without a signing key produces an unsigned audit log
    that verifies cleanly (backward compat with v0.1–v0.3 shards)."""
    p = str(tmp_path / "plain.tset")
    with tset_rs.Writer(p) as w:
        w.add_document(b"alpha")
        w.add_tokenizer_view("byte-level-v1", 256)
    py = PyReader(p)
    audit = py.manifest["audit_log"]
    assert "writer_public_key" not in audit
    for entry in audit["entries"]:
        assert "signature" not in entry
    log = AuditLog.from_dict(audit)
    assert log.verify()
