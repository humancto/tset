"""Regression tests for the items called out in the self-review pass."""

import os

import pytest

from tset.audit_log import AuditLog
from tset.dataset import Dataset, DatasetWriter
from tset.hashing import hash_bytes
from tset.reader import Reader
from tset.smt import InclusionProof
from tset.tokenizer_view import TOKEN_DTYPE
from tset.tokenizers import ByteLevelTokenizer, WhitespaceTokenizer
from tset.writer import Writer, append_tokenizer_view


def _build_two_shard_dataset(root: str, marker: bytes) -> bytes:
    docs_a = [b"alpha doc.", b"beta doc."]
    docs_b = [marker, b"gamma doc."]
    dw = DatasetWriter(root)
    for name, docs in [("part-00000", docs_a), ("part-00001", docs_b)]:
        with dw.shard_writer(name) as sw:
            for d in docs:
                sw.add_document(d)
            sw.add_tokenizer_view(ByteLevelTokenizer())
        dw.register_shard(name)
    dw.close()
    return hash_bytes(marker)


def test_dataset_proof_cannot_be_forged_for_excluded_doc(tmp_path):
    root = str(tmp_path / "ds.tset")
    marker = b"private text to exclude"
    h = _build_two_shard_dataset(root, marker)

    # Record exclusion against the second shard
    dw = DatasetWriter(root)
    for p in Dataset(root).shard_paths():
        dw.register_shard(os.path.splitext(os.path.basename(p))[0])
    dw.add_exclusion(h, "test")
    dw.close()

    ds = Dataset(root)
    proof = ds.prove_non_inclusion(h)
    assert ds.verify_non_inclusion_proof(proof)

    # Tampered: drop the inclusion proof for the present-but-excluded shard
    forged = {**proof, "shards": [dict(s) for s in proof["shards"]]}
    for s in forged["shards"]:
        if s["claim"] == "present_but_excluded":
            s["inclusion_proof"]["siblings"][0] = "00" * 32
    assert not ds.verify_non_inclusion_proof(forged)

    # Tampered: claim "present_but_excluded" with no overlay flag
    forged2 = {**proof, "exclusion_overlay_includes": False}
    assert not ds.verify_non_inclusion_proof(forged2)


def test_dataset_inclusion_proof_in_multi_shard(tmp_path):
    root = str(tmp_path / "ds.tset")
    _build_two_shard_dataset(root, b"unique marker")
    ds = Dataset(root)
    h = hash_bytes(b"gamma doc.")
    shard_path, proof = ds.prove_inclusion(h)
    assert isinstance(proof, InclusionProof)
    with Reader(shard_path) as r:
        assert proof.verify(r.smt_root())


def test_writer_rejects_add_document_after_view(tmp_path):
    p = str(tmp_path / "ordered.tset")
    with Writer(p) as w:
        w.add_document(b"first")
        w.add_tokenizer_view(ByteLevelTokenizer())
        with pytest.raises(RuntimeError, match="add_document"):
            w.add_document(b"too late")


def test_duplicate_content_does_not_skew_metadata_rows(tmp_path):
    p = str(tmp_path / "dup.tset")
    with Writer(p) as w:
        w.add_document(b"hello", metadata={"lang": "en"})
        w.add_document(b"hello", metadata={"lang": "fr"})
        w.add_tokenizer_view(ByteLevelTokenizer())
    with Reader(p) as r:
        assert len(r.doc_order_hex()) == 1
        cols = r.metadata_columns()
        assert cols.row_count == 1
        assert cols.column("lang") == ["en"]


def test_audit_log_intact_after_append_view(tmp_path):
    p = str(tmp_path / "appended.tset")
    with Writer(p) as w:
        w.add_document(b"hello world")
        w.add_tokenizer_view(ByteLevelTokenizer())
    append_tokenizer_view(p, WhitespaceTokenizer(vocab_size=512))
    with Reader(p) as r:
        log = r.audit_log()
        assert log.verify()
        kinds = [e.event_type for e in log.entries]
        assert kinds.count("tokenizer_added") == 2


def test_reader_rejects_oob_token_id_on_read(tmp_path):
    p = str(tmp_path / "vocab.tset")
    with Writer(p) as w:
        w.add_document(b"abc")
        w.add_tokenizer_view(ByteLevelTokenizer())
    # Surgically lower vocab_size in the manifest so a present token id
    # exceeds the new bound; reader must refuse to serve it.
    with Reader(p) as r:
        view = r.manifest["tokenization_views"]["byte-level-v1"]
        assert view["vocab_size"] == 256
        view["vocab_size"] = 10  # 'a' = 97 > 10 → out of range
        # Bypass manifest hash check by using the in-memory manifest only
        with pytest.raises(ValueError, match="token id >= vocab_size"):
            list(r.stream_tokens("byte-level-v1", 4))


def test_reader_rejects_corrupted_chunk_header(tmp_path):
    """The v0.1 chunk-integrity layer is the chunk's own size+count fields
    cross-checked against the manifest. Corrupting a chunk header makes
    streaming fail. (Per-chunk content hashing is a v0.2 item.)"""
    p = str(tmp_path / "tampered.tset")
    with Writer(p) as w:
        for i in range(20):
            w.add_document(f"payload-{i}".encode() * 5)
        w.add_tokenizer_view(ByteLevelTokenizer())
    # Locate the first chunk's num_tokens header field via the manifest
    with Reader(p) as r:
        view = r.manifest["tokenization_views"]["byte-level-v1"]
        chunk0 = view["chunks"][0]
        # chunk header is at view_offset + byte_offset_in_view; num_tokens at +16
        target = view["view_offset"] + chunk0["byte_offset_in_view"] + 16
    with open(p, "rb") as f:
        data = bytearray(f.read())
    data[target] ^= 0xFF
    with open(p, "wb") as f:
        f.write(bytes(data))
    with Reader(p) as r:
        with pytest.raises(ValueError, match="num_tokens mismatch"):
            list(r.stream_tokens("byte-level-v1", batch_size=128))


def test_predicate_compound_parens_and_in():
    from tset.columns import MetadataColumns

    cols = MetadataColumns()
    for r in [
        {"a": 1, "b": "x"},
        {"a": 2, "b": "y"},
        {"a": 3, "b": "z"},
        {"a": 4, "b": "y"},
    ]:
        cols.add_row(r)
    # Row 3 has a=4, b='y' — fails both clauses; only rows 1 and 2 match
    assert cols.filter_sql_like("(a > 1 AND a < 4) OR b = 'z'") == [1, 2]
    # Inclusive bound brings row 3 in
    assert cols.filter_sql_like("(a > 1 AND a <= 4) OR b = 'z'") == [1, 2, 3]
    assert cols.filter_sql_like("b IN ('x', 'y')") == [0, 1, 3]


def test_dataloader_partition_union_equals_full_stream(tmp_path):
    p = str(tmp_path / "loader.tset")
    with Writer(p) as w:
        for i in range(40):
            w.add_document(("abcdefghij" * 5 + f"-{i}").encode())
        w.add_tokenizer_view(ByteLevelTokenizer())
    from tset.dataloader import DataLoader

    seen = []
    for rank in range(2):
        for worker in range(2):
            ld = DataLoader(
                p,
                "byte-level-v1",
                batch_size=32,
                world_size=2,
                rank=rank,
                num_workers=2,
                worker_id=worker,
            )
            for batch in ld:
                seen.append(int(batch.shape[0]))
    with Reader(p) as r:
        assert sum(seen) == r.view_total_tokens("byte-level-v1")


def test_v02_per_chunk_hash_detects_body_tampering(tmp_path):
    """v0.2 mandates chunk content_hash, which catches body-byte tampering
    even when the chunk header is left intact."""
    p = str(tmp_path / "v02.tset")
    with Writer(p) as w:
        for i in range(40):
            w.add_document(("payload " * 30 + str(i)).encode())
        w.add_tokenizer_view(ByteLevelTokenizer())
    with Reader(p) as r:
        assert r.header.version_minor == 2
        view = r.manifest["tokenization_views"]["byte-level-v1"]
        chunk0 = view["chunks"][0]
        assert chunk0["content_hash"], "v0.2 must record content_hash on every chunk"
        # Flip a byte deep inside the compressed payload (body, not header)
        body_off = (
            view["view_offset"] + chunk0["byte_offset_in_view"] + 24 + chunk0["compressed_size"] // 2
        )
    with open(p, "rb") as f:
        data = bytearray(f.read())
    data[body_off] ^= 0xFF
    with open(p, "wb") as f:
        f.write(bytes(data))
    with Reader(p) as r:
        with pytest.raises(ValueError, match="content_hash mismatch"):
            list(r.stream_tokens("byte-level-v1", batch_size=64))


def test_header_rejects_unknown_flags():
    from tset.constants import HEADER_SIZE
    from tset.header import Header

    h = Header(0, 1, 0, 100, 200, b"\x00" * 32, b"\x00" * 32)
    enc = bytearray(h.encode())
    enc[8:12] = (0x00000001).to_bytes(4, "little")
    with pytest.raises(ValueError, match="header flags"):
        Header.decode(bytes(enc))
