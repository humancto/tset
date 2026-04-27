import os
import tempfile

import pytest

from tset.hashing import hash_bytes
from tset.reader import Reader
from tset.tokenizers import ByteLevelTokenizer, WhitespaceTokenizer
from tset.writer import Writer, append_tokenizer_view


@pytest.fixture
def shard_path(tmp_path):
    return str(tmp_path / "test.tset")


@pytest.fixture
def small_shard(shard_path):
    with Writer(shard_path) as w:
        w.add_document(b"hello world", metadata={"lang": "en", "qs": 0.9})
        w.add_document(b"foo bar baz", metadata={"lang": "en", "qs": 0.5})
        w.add_document(b"lorem ipsum dolor sit amet", metadata={"lang": "la", "qs": 0.3})
        w.add_tokenizer_view(ByteLevelTokenizer())
    return shard_path


def test_round_trip(small_shard):
    with Reader(small_shard) as r:
        assert r.tokenizer_ids() == ["byte-level-v1"]
        contents = {h.hex(): c for h, c in r.documents()}
        assert b"hello world" in contents.values()
        assert b"foo bar baz" in contents.values()


def test_streaming_byte_count_matches_total(small_shard):
    with Reader(small_shard) as r:
        total = sum(int(b.size) for b, _ in r.stream_tokens("byte-level-v1", 4))
        assert total == r.view_total_tokens("byte-level-v1")
        assert total == sum(len(c) for c in [b"hello world", b"foo bar baz", b"lorem ipsum dolor sit amet"])


def test_reproducibility_proof_passes(small_shard):
    with Reader(small_shard) as r:
        r.verify_tokenizer_view("byte-level-v1")


def test_reproducibility_proof_fails_on_swapped_tokenizer(small_shard):
    with Reader(small_shard) as r:
        with pytest.raises(ValueError, match="config hash mismatch"):
            r.verify_tokenizer_view("byte-level-v1", tokenizer=WhitespaceTokenizer())


def test_append_tokenizer_view(small_shard):
    append_tokenizer_view(small_shard, WhitespaceTokenizer(vocab_size=4096))
    with Reader(small_shard) as r:
        ids = sorted(r.tokenizer_ids())
        assert ids == ["byte-level-v1", "whitespace-hashed-v1"]
        r.verify_tokenizer_view("whitespace-hashed-v1")
        # Documents are unchanged
        contents = [c for _, c in r.documents()]
        assert b"hello world" in contents


def test_append_same_tokenizer_id_rejected(small_shard):
    with pytest.raises(ValueError, match="already present"):
        append_tokenizer_view(small_shard, ByteLevelTokenizer())


def test_inclusion_proof_for_present_doc(small_shard):
    with Reader(small_shard) as r:
        h = hash_bytes(b"hello world")
        proof = r.prove_inclusion(h)
        assert proof.verify(r.smt_root())


def test_non_inclusion_proof_for_absent_doc(small_shard):
    with Reader(small_shard) as r:
        absent = hash_bytes(b"never ingested")
        proof = r.prove_non_inclusion(absent)
        assert proof.verify(r.smt_root())


def test_audit_log_integrity(small_shard):
    with Reader(small_shard) as r:
        log = r.audit_log()
        assert log.verify()
        kinds = [e.event_type for e in log.entries]
        assert "ingestion" in kinds
        assert "tokenizer_added" in kinds
        assert "version_snapshot" in kinds


def test_metadata_predicate_pushdown(small_shard):
    with Reader(small_shard) as r:
        cols = r.metadata_columns()
        latin = cols.filter_sql_like("lang = 'la'")
        assert latin == [2]
        high = cols.filter_sql_like("qs >= 0.5")
        assert high == [0, 1]


def test_mismatched_manifest_hash_detected(small_shard):
    # corrupt one byte inside the manifest payload
    with open(small_shard, "rb") as f:
        data = bytearray(f.read())
    from tset.header import Header

    h = Header.decode(bytes(data[:4096]))
    # flip a byte in the middle of the manifest
    data[h.manifest_offset + 5] ^= 0x01
    with open(small_shard, "wb") as f:
        f.write(bytes(data))
    with pytest.raises(ValueError, match="manifest hash mismatch"):
        Reader(small_shard)


def test_unknown_tokenizer_id_keyerror(small_shard):
    with Reader(small_shard) as r:
        with pytest.raises(KeyError):
            list(r.stream_tokens("nonexistent-v1"))
