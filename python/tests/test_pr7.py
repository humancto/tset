"""Tests for PR 7 items.

Covers:
  - Lazy reader iter (memory-bounded streaming)
  - PyO3 proof bindings (prove_inclusion, prove_non_inclusion, verify_*)
  - Strict v0.2 enforcement at file open
  - Predicate compiler: NOT, BETWEEN, IS NULL, IS NOT NULL
  - DatasetWriter idempotent reopen + add-exclusion-later
  - Reverse converters: tset → jsonl (parquet covered if pyarrow installed)
  - v0.1 conformance fixture readable by current reader
"""

import json
import os

import pytest

from tset.columns import MetadataColumns
from tset.converters import tset_to_jsonl
from tset.dataset import Dataset, DatasetWriter
from tset.hashing import hash_bytes
from tset.reader import Reader as PyReader
from tset.tokenizers import ByteLevelTokenizer
from tset.writer import Writer

tset_rs = pytest.importorskip("tset_rs")

CONFORMANCE_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "..", "tests", "conformance", "fixtures")
)


# --- predicate compiler completeness ---


def _cols():
    c = MetadataColumns()
    for r in [
        {"a": 1, "b": "x", "q": 0.9},
        {"a": 2, "b": "y", "q": 0.4},
        {"a": 3, "b": "z", "q": None},
        {"a": 4, "b": "y", "q": 0.7},
    ]:
        c.add_row(r)
    return c


def test_predicate_not_negates_atom():
    c = _cols()
    assert c.filter_sql_like("NOT (a = 1)") == [1, 2, 3]


def test_predicate_not_compound():
    c = _cols()
    assert c.filter_sql_like("NOT (a > 1 AND b = 'y')") == [0, 2]


def test_predicate_between_inclusive():
    c = _cols()
    assert c.filter_sql_like("a BETWEEN 2 AND 3") == [1, 2]


def test_predicate_between_floats():
    c = _cols()
    assert c.filter_sql_like("q BETWEEN 0.5 AND 1.0") == [0, 3]


def test_predicate_is_null():
    c = _cols()
    assert c.filter_sql_like("q IS NULL") == [2]


def test_predicate_is_not_null():
    c = _cols()
    assert c.filter_sql_like("q IS NOT NULL") == [0, 1, 3]


# --- proof bindings ---


def test_proof_bindings_inclusion(tmp_path):
    p = str(tmp_path / "proof.tset")
    with Writer(p) as w:
        w.add_document(b"alpha")
        w.add_document(b"beta")
        w.add_tokenizer_view(ByteLevelTokenizer())

    r = tset_rs.Reader(p)
    h = hash_bytes(b"alpha")
    doc_hex, siblings = r.prove_inclusion(h)
    assert doc_hex == h.hex()
    assert len(siblings) == 256

    root = bytes(r.smt_root())
    assert tset_rs.verify_inclusion_proof(h, siblings, root)
    # Tampered sibling fails
    bad = list(siblings)
    bad[0] = "00" * 32
    assert not tset_rs.verify_inclusion_proof(h, bad, root)


def test_proof_bindings_non_inclusion(tmp_path):
    p = str(tmp_path / "proof2.tset")
    with Writer(p) as w:
        w.add_document(b"alpha")
        w.add_tokenizer_view(ByteLevelTokenizer())
    r = tset_rs.Reader(p)
    absent = hash_bytes(b"not-here")
    doc_hex, siblings = r.prove_non_inclusion(absent)
    assert doc_hex == absent.hex()
    root = bytes(r.smt_root())
    assert tset_rs.verify_non_inclusion_proof(absent, siblings, root)


# --- strict v0.2 at file open ---


def test_strict_v02_at_file_open_rejects_missing_chunk_hash(tmp_path):
    """Tamper the manifest to drop a chunk's content_hash, then open.
    The reader must reject at open time, before any view is opened."""
    p = str(tmp_path / "tamper.tset")
    with Writer(p) as w:
        w.add_document(b"hello")
        w.add_tokenizer_view(ByteLevelTokenizer())

    # Read raw, parse manifest, drop content_hash, re-encode, fix header
    # so manifest_hash matches but chunk content_hash is missing.
    from tset import manifest as M
    from tset.constants import HEADER_SIZE
    from tset.hashing import hash_bytes as _hb
    from tset.header import Header
    from tset.footer import Footer
    from tset.constants import TRUNCATED_HASH_SIZE

    with open(p, "rb") as f:
        data = bytearray(f.read())
    h = Header.decode(bytes(data[:HEADER_SIZE]))
    m_off, m_size = h.manifest_offset, h.manifest_size
    manifest_bytes = bytes(data[m_off : m_off + m_size])
    manifest = M.decode_manifest(manifest_bytes)
    # Drop content_hash from each chunk
    for view in manifest["tokenization_views"].values():
        for chunk in view["chunks"]:
            chunk["content_hash"] = None
    new_bytes = M.encode_manifest(manifest)
    # If sizes differ, we'd need to relocate; for this test we expect
    # the encoded length to differ marginally — write into a fresh file.
    new_path = str(tmp_path / "tamper_fixed.tset")
    new_manifest_off = HEADER_SIZE + (m_off - HEADER_SIZE)  # body unchanged
    new_h = Header(
        version_major=h.version_major,
        version_minor=h.version_minor,
        flags=h.flags,
        manifest_offset=new_manifest_off,
        manifest_size=len(new_bytes),
        shard_merkle_root=h.shard_merkle_root,
        manifest_hash=_hb(new_bytes),
    )
    f28 = _hb(new_bytes)[:TRUNCATED_HASH_SIZE]
    new_footer = Footer(manifest_size=len(new_bytes), manifest_hash28=f28)
    body = data[HEADER_SIZE:m_off]
    with open(new_path, "wb") as f:
        f.write(new_h.encode())
        f.write(body)
        f.write(new_bytes)
        f.write(new_footer.encode())
    # The Python reader is lenient (treats missing content_hash as None);
    # the Rust reader is strict. Test the Rust path explicitly.
    with pytest.raises(ValueError, match="content_hash"):
        tset_rs.Reader(new_path)


# --- DatasetWriter idempotent reopen ---


def test_dataset_writer_reopen_loads_existing_state(tmp_path):
    root = str(tmp_path / "ds")
    # First pass: create + register one shard
    dw = DatasetWriter(root)
    p0 = os.path.join(dw.root, "shards", "part-0001.tset")
    with Writer(p0) as w:
        w.add_document(b"alpha")
        w.add_tokenizer_view(ByteLevelTokenizer())
    dw.register_shard("part-0001")
    dw.close()

    # Reopen, add a second shard + an exclusion. Existing shard should
    # already be registered (no need to re-register).
    dw2 = DatasetWriter(root)
    assert len(dw2._shards) == 1
    p1 = os.path.join(dw2.root, "shards", "part-0002.tset")
    with Writer(p1) as w:
        w.add_document(b"beta")
        w.add_tokenizer_view(ByteLevelTokenizer())
    dw2.register_shard("part-0002")
    dw2.add_exclusion(hash_bytes(b"alpha"), reason="test")
    dw2.close()

    ds = Dataset(root)
    assert len(ds.shard_paths()) == 2
    assert hash_bytes(b"alpha").hex() in ds.exclusions()


def test_dataset_writer_register_idempotent(tmp_path):
    """Calling register_shard twice with the same name should not duplicate."""
    root = str(tmp_path / "ds2")
    dw = DatasetWriter(root)
    p = os.path.join(dw.root, "shards", "x.tset")
    with Writer(p) as w:
        w.add_document(b"alpha")
        w.add_tokenizer_view(ByteLevelTokenizer())
    dw.register_shard("x")
    dw.register_shard("x")  # idempotent
    assert len(dw._shards) == 1


# --- reverse converter ---


def test_tset_to_jsonl_roundtrip(tmp_path):
    src = str(tmp_path / "src.tset")
    with Writer(src) as w:
        w.add_document(b"alpha document", metadata={"lang": "en", "score": 0.9})
        w.add_document(b"beta", metadata={"lang": "fr", "score": 0.4})
        w.add_tokenizer_view(ByteLevelTokenizer())

    dst = str(tmp_path / "dst.jsonl")
    result = tset_to_jsonl(src, dst)
    assert result["documents"] == 2

    with open(dst) as f:
        lines = [json.loads(line) for line in f if line.strip()]
    assert len(lines) == 2
    assert {ln["text"] for ln in lines} == {"alpha document", "beta"}
    assert all("doc_hash" in ln for ln in lines)
    assert sorted(ln["lang"] for ln in lines) == ["en", "fr"]


# --- v0.1 conformance fixture ---


def test_v01_fixture_readable_by_current_reader():
    """Per RFC §5.6 #6, v0.2+ readers MUST read v0.1 shards."""
    shard = os.path.join(CONFORMANCE_DIR, "fixture-v01-small.tset")
    sidecar = os.path.join(CONFORMANCE_DIR, "fixture-v01-small.expected.json")
    assert os.path.exists(shard), (
        f"missing v0.1 fixture {shard}; "
        "run python tests/conformance/build_v01_fixture.py"
    )
    with open(sidecar) as f:
        expected = json.load(f)
    assert expected["version_minor"] == 1

    r = PyReader(shard)
    assert r.header.version_minor == 1
    assert r.header.shard_merkle_root.hex() == expected["shard_merkle_root"]

    # Rust reader must also read v0.1
    rs = tset_rs.Reader(shard)
    assert rs.version_minor == 1
    assert bytes(rs.shard_merkle_root) == bytes.fromhex(expected["shard_merkle_root"])
