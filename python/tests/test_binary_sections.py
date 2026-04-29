"""Tests for v0.3.2 on-disk binary sections.

Three properties to lock:

1. **Round-trip** — encode → decode returns the same data. Lives in the
   sections module's own unit tests (not here).

2. **In-manifest = on-disk equivalence** — when a Writer emits both, the
   in-manifest JSON view and the binary section MUST agree on:
   - SMT root (TSMT.smt_root vs manifest.smt_root)
   - SMT present-keys set (TSMT.present_keys vs manifest.smt_present_keys)
   - Audit log root (TLOG.log_root vs manifest.audit_log.log_root)
   - Column row_count (TCOL.row_count vs manifest.metadata_columns.row_count)

3. **Cross-impl byte-equivalence** — Python writer's TSMT/TLOG/TCOL bytes
   are bit-identical to what the Rust writer (`tset_rs.Writer.enable_binary_sections`)
   produces, given the same input + deterministic env vars.

The conformance fixture `fixture-sections.tset` locks the v0.3.2 wire
format the same way `fixture-small.tset` locks v0.3 and
`fixture-v01-small.tset` locks v0.1.
"""

import os

import pytest

from tset.constants import HEADER_SIZE
from tset.header import Header
from tset.reader import Reader
from tset.sections import (
    decode_tcol_section,
    decode_tlog_section,
    decode_tsmt_section,
)
from tset.tokenizers import ByteLevelTokenizer, WhitespaceTokenizer
from tset.writer import Writer

CONFORMANCE_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "..", "tests", "conformance", "fixtures")
)


def _read_section_from_shard(shard_path: str, manifest_key: str):
    r = Reader(shard_path)
    pointer = r.manifest.get(manifest_key)
    if pointer is None:
        return None
    with open(shard_path, "rb") as f:
        data = f.read()
    off = pointer["offset"]
    end = off + pointer["size"]
    return data[off:end]


def test_python_writer_emits_binary_sections_when_enabled(tmp_path):
    p = str(tmp_path / "sec.tset")
    with Writer(p) as w:
        w.enable_binary_sections()
        w.add_document(b"alpha")
        w.add_document(b"beta")
        w.add_tokenizer_view(ByteLevelTokenizer())

    r = Reader(p)
    for key in ("smt_section", "audit_log_section", "metadata_columns_section"):
        assert key in r.manifest, f"manifest missing {key} pointer"
        assert "offset" in r.manifest[key]
        assert "size" in r.manifest[key]


def test_python_writer_default_does_not_emit_binary_sections(tmp_path):
    p = str(tmp_path / "plain.tset")
    with Writer(p) as w:
        w.add_document(b"alpha")
        w.add_tokenizer_view(ByteLevelTokenizer())
    r = Reader(p)
    assert "smt_section" not in r.manifest
    assert "audit_log_section" not in r.manifest
    assert "metadata_columns_section" not in r.manifest


def test_v04_writer_drops_inline_forms_when_sections_enabled(tmp_path):
    """v0.4 contract: when sections are emitted the inline JSON forms
    of audit_log / smt_present_keys / metadata_columns MUST be absent.

    Pre-v0.4 (the v0.3.2 transitional state) wrote both — sections AND
    inline — which inflated shards by ~50%. Issue #5 resolved this by
    making sections the sole source of truth in v0.4.
    """
    p = str(tmp_path / "v04.tset")
    with Writer(p) as w:
        w.enable_binary_sections()
        w.add_document(b"alpha", metadata={"lang": "en"})
        w.add_document(b"beta")
        w.add_tokenizer_view(ByteLevelTokenizer())

    r = Reader(p)
    # Header announces v0.4
    assert r.header.version_minor == 4
    # Sections present
    for key in ("smt_section", "audit_log_section", "metadata_columns_section"):
        assert key in r.manifest, f"missing {key}"
    # Inline forms absent — this is the storage-saving contract
    assert "smt_present_keys" not in r.manifest
    assert "audit_log" not in r.manifest
    assert "metadata_columns" not in r.manifest


def test_v04_reader_loads_audit_log_from_tlog_section(tmp_path):
    """The reader's ``audit_log()`` accessor must return entries from
    TLOG even though no inline form exists in the manifest."""
    p = str(tmp_path / "v04log.tset")
    with Writer(p) as w:
        w.enable_binary_sections()
        w.add_document(b"alpha")
        w.add_document(b"beta")
        w.add_tokenizer_view(ByteLevelTokenizer())

    r = Reader(p)
    log = r.audit_log()
    assert log.entries, "v0.4 reader returned empty audit log; TLOG decode broken"
    types = {e.event_type for e in log.entries}
    assert "ingestion" in types
    # And the chained-hash + signature contract still verifies
    assert log.verify()


def test_v04_reader_loads_smt_from_tsmt_section(tmp_path):
    p = str(tmp_path / "v04smt.tset")
    with Writer(p) as w:
        w.enable_binary_sections()
        w.add_document(b"alpha")
        w.add_document(b"beta")
        w.add_document(b"gamma")
        w.add_tokenizer_view(ByteLevelTokenizer())

    r = Reader(p)
    smt = r.smt()
    assert len(smt.present_keys()) == 3, (
        "v0.4 reader didn't reconstruct SMT from TSMT section"
    )
    # Inclusion + non-inclusion proofs still verify against smt_root
    h, _ = next(iter(r.documents()))
    assert r.prove_inclusion(h).verify(r.smt_root())
    assert r.prove_non_inclusion(b"\xab" * 32).verify(r.smt_root())


def test_v04_reader_loads_metadata_columns_from_tcol_section(tmp_path):
    p = str(tmp_path / "v04col.tset")
    with Writer(p) as w:
        w.enable_binary_sections()
        w.add_document(b"alpha", metadata={"lang": "en", "score": 0.9})
        w.add_document(b"beta", metadata={"lang": "fr", "score": 0.4})
        w.add_tokenizer_view(ByteLevelTokenizer())

    r = Reader(p)
    cols = r.metadata_columns()
    assert sorted(cols.names()) == ["lang", "score"]
    assert sorted(cols.column("lang")) == ["en", "fr"]


def test_v032_conformance_fixture_decodes_cleanly():
    """The committed fixture-sections.tset is the canonical v0.3.2 shard."""
    shard = os.path.join(CONFORMANCE_DIR, "fixture-sections.tset")
    assert os.path.exists(shard), (
        "missing fixture-sections.tset; run "
        "python tests/conformance/build_corpus.py"
    )
    r = Reader(shard)
    # Must verify cleanly via the standard reader (manifest hash, audit
    # log, reproducibility proofs all checked at open)
    assert r.tokenizer_ids() == ["byte-level-v1", "whitespace-hashed-v1"]
    # All three section pointers present
    for key in ("smt_section", "audit_log_section", "metadata_columns_section"):
        assert key in r.manifest

    # Each section decodes without raising
    decode_tsmt_section(_read_section_from_shard(shard, "smt_section"))
    decode_tlog_section(_read_section_from_shard(shard, "audit_log_section"))
    decode_tcol_section(_read_section_from_shard(shard, "metadata_columns_section"))


def test_v032_fixture_byte_stable_across_rebuilds(tmp_path, monkeypatch):
    """If we run build_corpus.py-style env vars + same inputs, we get
    byte-identical bytes to the committed fixture. Locks against
    accidental wire-format drift."""
    monkeypatch.setenv("TSET_DETERMINISTIC_TIME", "1700000000.0")
    monkeypatch.setenv("TSET_DETERMINISTIC_CREATED_AT", "2023-11-14T22:13:20+00:00")
    monkeypatch.setenv("TSET_DETERMINISTIC_SNAPSHOT_ID", "fixture-sections-snap")
    # Match build_corpus.py's shard_id derivation exactly
    shard_id = (
        "conformance-fixture-sections-shard-id-padding".encode().hex()[:32]
    )
    p = str(tmp_path / "rebuilt.tset")
    with Writer(p, shard_id=shard_id) as w:
        w.enable_binary_sections()
        w.add_document(b"alpha document text", metadata={"lang": "en", "len": 19})
        w.add_document(b"beta", metadata={"lang": "fr", "len": 4})
        w.add_document(b"gamma payload here", metadata={"lang": "en", "len": 18})
        w.add_tokenizer_view(ByteLevelTokenizer())
        w.add_tokenizer_view(WhitespaceTokenizer(vocab_size=1024))

    rebuilt_bytes = open(p, "rb").read()
    fixture_path = os.path.join(CONFORMANCE_DIR, "fixture-sections.tset")
    fixture_bytes = open(fixture_path, "rb").read()
    assert rebuilt_bytes == fixture_bytes, (
        "rebuilt v0.3.2 fixture diverged from committed fixture — wire "
        "format drift between Python writer revisions"
    )


def test_existing_v03_fixture_still_lacks_sections():
    """Forward compat: the v0.3 fixture-small.tset has no on-disk
    sections (it predates v0.3.2). v0.3.2 readers must still open it."""
    shard = os.path.join(CONFORMANCE_DIR, "fixture-small.tset")
    r = Reader(shard)
    assert "smt_section" not in r.manifest
    # And it still decodes without error
    assert r.tokenizer_ids() == ["byte-level-v1", "whitespace-hashed-v1"]
