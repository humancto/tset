"""tset_rs.append_tokenizer_view (Python parity for the Rust core).

Smoke-tests that a v0.3 shard written by Rust can be re-opened by both
readers, that a second tokenization view appended via the Rust path
shows up cleanly, and that the resulting shard still verifies.
"""

import pytest

from tset.reader import Reader as PyReader

tset_rs = pytest.importorskip("tset_rs")


def test_rust_append_tokenizer_view_basic(tmp_path):
    p = str(tmp_path / "two-views.tset")
    with tset_rs.Writer(p) as w:
        w.add_document(b"alpha document text")
        w.add_document(b"beta gamma")
        w.add_tokenizer_view("byte-level-v1", 256)

    # Append a second view via the Rust function
    tset_rs.append_tokenizer_view(p, "whitespace-hashed-v1", 1024)

    py = PyReader(p)
    ids = sorted(py.tokenizer_ids())
    assert ids == ["byte-level-v1", "whitespace-hashed-v1"]
    # Audit log should now have two tokenizer_added events
    audit = py.manifest["audit_log"]
    types = [e["event_type"] for e in audit["entries"]]
    assert types.count("tokenizer_added") == 2
    # Reader still verifies (manifest hash + per-chunk content_hash all valid)
    rs = tset_rs.Reader(p)
    assert "whitespace-hashed-v1" in rs.tokenizer_ids()


def test_rust_append_tokenizer_view_rejects_duplicate(tmp_path):
    p = str(tmp_path / "dup.tset")
    with tset_rs.Writer(p) as w:
        w.add_document(b"alpha")
        w.add_tokenizer_view("byte-level-v1", 256)
    with pytest.raises(ValueError):
        tset_rs.append_tokenizer_view(p, "byte-level-v1", 256)
