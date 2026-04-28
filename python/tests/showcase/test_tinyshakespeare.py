"""TinyShakespeare end-to-end test matrix.

Eight test classes, each exercising one specific guarantee TSET makes
against a real public corpus. The session-scoped ``shakespeare_corpus``
fixture from ``conftest.py`` runs ``download`` + ``convert`` once and
hands back paths.

Skipped only on network unavailability (see ``_try_or_skip_network`` in
conftest). Integrity failures fail the test.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import pytest

from tset.audit_log import AuditLog
from tset.constants import HEADER_SIZE
from tset.reader import Reader
from tset.smt import InclusionProof, NonInclusionProof
from tset.tokenizers import ByteLevelTokenizer, WhitespaceTokenizer


# ────────────────────────────────────────────────────────────────────
# A. Round-trip integrity
# ────────────────────────────────────────────────────────────────────


class TestA_RoundTrip:
    def test_unique_doc_count_matches_dedup(self, shakespeare_corpus):
        """JSONL paragraph count − duplicates == TSET document count."""
        para = shakespeare_corpus["paragraphs"]
        # Manually compute dedup count using the same hash the writer uses.
        from tset.hashing import hash_bytes

        seen = {hash_bytes(p.encode("utf-8")) for p in para}
        r = Reader(str(shakespeare_corpus["tset"]))
        assert len(seen) == len(list(r.doc_order_hex())), (
            "TSET doc count must equal unique paragraphs by BLAKE3"
        )

    def test_every_unique_paragraph_round_trips(self, shakespeare_corpus):
        """Every distinct paragraph survives JSONL → TSET → bytes."""
        para = shakespeare_corpus["paragraphs"]
        r = Reader(str(shakespeare_corpus["tset"]))
        unique_payloads = {p.encode("utf-8") for p in para}
        recovered = {content for _h, content in r.documents()}
        assert recovered == unique_payloads


# ────────────────────────────────────────────────────────────────────
# B. Tokenization correctness
# ────────────────────────────────────────────────────────────────────


class TestB_Tokenization:
    def test_byte_level_view_tokens_match_direct_tokenize(self, shakespeare_corpus):
        """For every doc, tokens in the TSET view equal direct tokenize()."""
        r = Reader(str(shakespeare_corpus["tset"]))
        tok = ByteLevelTokenizer()
        # Sample 50 docs (full corpus check would be slow; this is more
        # than enough to catch any systemic mismatch and is deterministic
        # given the corpus content).
        samples = list(r.documents())[:50]
        for doc_hash, content in samples:
            expected = list(tok.encode(content))
            actual: list[int] = []
            for batch, dh in r.stream_tokens("byte-level-v1", batch_size=2_000_000):
                if dh == doc_hash:
                    actual.extend(int(x) for x in batch)
                    break
            assert actual == expected, f"mismatch on doc {doc_hash.hex()[:12]}"

    def test_whitespace_view_tokens_match_direct_tokenize(self, shakespeare_corpus):
        """Whitespace tokens via the TSET view match the tokenizer's own
        ``encode()`` of the same content. This is the integrity contract;
        comparing to ``str.split()`` would be misleading because the
        tokenizer collapses internal whitespace runs differently than
        ``split()`` does on edge cases.
        """
        r = Reader(str(shakespeare_corpus["tset"]))
        tok = WhitespaceTokenizer(vocab_size=4096)
        for doc_hash, content in list(r.documents())[:25]:
            expected = list(tok.encode(content))
            actual: list[int] = []
            for batch, dh in r.stream_tokens(
                "whitespace-hashed-v1", batch_size=2_000_000
            ):
                if dh == doc_hash:
                    actual.extend(int(x) for x in batch)
                    break
            assert actual == expected, (
                f"whitespace tokens mismatch on doc {doc_hash.hex()[:12]}"
            )


# ────────────────────────────────────────────────────────────────────
# C. View independence
# ────────────────────────────────────────────────────────────────────


class TestC_ViewIndependence:
    def test_two_views_present_after_convert(self, shakespeare_corpus):
        r = Reader(str(shakespeare_corpus["tset"]))
        ids = sorted(r.tokenizer_ids())
        assert ids == sorted(["byte-level-v1", "whitespace-hashed-v1"])

    def test_each_view_has_independent_token_count(self, shakespeare_corpus):
        """Byte-level produces roughly one token per byte; whitespace
        produces one token per word. On English text the average word
        length is ~4-5 chars, so byte-level totals are ~3-7× the
        whitespace totals — assert the order-of-magnitude relation."""
        r = Reader(str(shakespeare_corpus["tset"]))
        n_byte = r.view_total_tokens("byte-level-v1")
        n_ws = r.view_total_tokens("whitespace-hashed-v1")
        assert n_byte > 0 and n_ws > 0
        ratio = n_byte / n_ws
        # Shakespeare's average word length is ~3.7 chars (lots of "the",
        # "and", "I", short character names); on this corpus the
        # byte/whitespace ratio measures ~2.76. Other prose corpora hit
        # ~5×. Assert a generous band that catches collapse-to-zero or
        # explosion regressions without baking in dataset-specific values.
        assert 2.0 < ratio < 12.0, (
            f"byte/ws ratio {ratio:.2f} outside expected band"
        )

    def test_each_view_self_verifies(self, shakespeare_corpus):
        """``Reader.verify_tokenizer_view`` re-tokenizes and compares to
        the on-disk chunks — byte-identical or it raises."""
        r = Reader(str(shakespeare_corpus["tset"]))
        r.verify_tokenizer_view("byte-level-v1", ByteLevelTokenizer())
        r.verify_tokenizer_view(
            "whitespace-hashed-v1", WhitespaceTokenizer(vocab_size=4096)
        )


# ────────────────────────────────────────────────────────────────────
# D. Provenance receipts
# ────────────────────────────────────────────────────────────────────


class TestD_Receipts:
    def test_inclusion_proof_for_real_doc_verifies(self, shakespeare_corpus):
        r = Reader(str(shakespeare_corpus["tset"]))
        for h, _ in list(r.documents())[:25]:
            p = r.prove_inclusion(h)
            assert isinstance(p, InclusionProof)
            assert p.verify(r.smt_root()), f"inclusion proof failed for {h.hex()[:12]}"

    def test_non_inclusion_proof_for_absent_hash_verifies(self, shakespeare_corpus):
        r = Reader(str(shakespeare_corpus["tset"]))
        # Three intentionally absent hashes (corpus has none of these)
        for absent in (b"\x00" * 32, b"\xa5" * 32, b"\xff" * 32):
            assert not r.has_document(absent)
            p = r.prove_non_inclusion(absent)
            assert isinstance(p, NonInclusionProof)
            assert p.verify(r.smt_root()), f"non-incl. proof failed for {absent.hex()[:12]}"

    def test_tampered_inclusion_proof_rejected(self, shakespeare_corpus):
        r = Reader(str(shakespeare_corpus["tset"]))
        h, _ = next(iter(r.documents()))
        p = r.prove_inclusion(h)
        if not p.siblings:
            pytest.skip("proof has no siblings to flip")
        flipped = InclusionProof(
            key=p.key,
            siblings=[bytes(b ^ 0xFF for b in p.siblings[0])] + p.siblings[1:],
        )
        assert not flipped.verify(r.smt_root())

    def test_tampered_non_inclusion_proof_rejected(self, shakespeare_corpus):
        r = Reader(str(shakespeare_corpus["tset"]))
        absent = b"\xa5" * 32
        p = r.prove_non_inclusion(absent)
        if not p.siblings:
            pytest.skip("proof has no siblings to flip")
        flipped = NonInclusionProof(
            key=p.key,
            siblings=[bytes(b ^ 0xFF for b in p.siblings[0])] + p.siblings[1:],
        )
        assert not flipped.verify(r.smt_root())


# ────────────────────────────────────────────────────────────────────
# E. Audit log integrity
# ────────────────────────────────────────────────────────────────────


class TestE_AuditLog:
    def test_every_unique_doc_has_ingest_entry(self, shakespeare_corpus):
        r = Reader(str(shakespeare_corpus["tset"]))
        log: AuditLog | None = r.audit_log()
        assert log is not None and log.entries
        ingests = [e for e in log.entries if e.event_type == "ingestion"]
        n_unique = len(list(r.doc_order_hex()))
        assert len(ingests) == n_unique, (
            f"audit log has {len(ingests)} ingest entries, "
            f"expected {n_unique} (one per unique doc)"
        )

    def test_audit_log_chained_hash_verifies(self, shakespeare_corpus):
        r = Reader(str(shakespeare_corpus["tset"]))
        log = r.audit_log()
        assert log is not None
        assert log.verify(), "audit log chained-hash verification failed"

    def test_tokenizer_view_addition_recorded(self, shakespeare_corpus):
        """Both views appear in the audit log."""
        r = Reader(str(shakespeare_corpus["tset"]))
        log = r.audit_log()
        view_events = [e for e in log.entries if e.event_type == "tokenizer_added"]
        ids = {e.payload.get("tokenizer_id") for e in view_events}
        assert "byte-level-v1" in ids
        assert "whitespace-hashed-v1" in ids


# ────────────────────────────────────────────────────────────────────
# F. Tamper detection (file-level byte flips)
# ────────────────────────────────────────────────────────────────────


class TestF_TamperDetection:
    def _flip_byte(self, src: Path, dst: Path, offset: int) -> None:
        data = bytearray(src.read_bytes())
        if offset < 0:
            offset = len(data) + offset
        data[offset] ^= 0xFF
        dst.write_bytes(bytes(data))

    def test_flip_doc_store_byte_fails_open_or_iter(
        self, shakespeare_corpus, tmp_path
    ):
        """Flipping a byte well inside the doc store must be detected.

        Per-doc BLAKE3 is recomputed when documents are read; the
        manifest's per-block content hash also catches it. Either path
        must surface the corruption."""
        bad = tmp_path / "tampered.tset"
        # Pick an offset deep inside the doc store: well past the
        # 64-byte header but before the manifest tail.
        size = shakespeare_corpus["tset"].stat().st_size
        target = HEADER_SIZE + (size - HEADER_SIZE) // 4
        self._flip_byte(shakespeare_corpus["tset"], bad, target)

        with pytest.raises(Exception):
            r = Reader(str(bad))
            # Force iteration in case open is lazy
            for _ in r.documents():
                pass

    def test_flip_manifest_byte_fails_open(self, shakespeare_corpus, tmp_path):
        """The footer mirrors a manifest hash; flipping a byte in the
        manifest must fail manifest-hash verification at open time."""
        bad = tmp_path / "tampered.tset"
        size = shakespeare_corpus["tset"].stat().st_size
        # Manifest sits near the tail (before the footer's 64 bytes).
        target = size - 256
        self._flip_byte(shakespeare_corpus["tset"], bad, target)
        with pytest.raises(Exception):
            Reader(str(bad))

    def test_flip_header_magic_fails_open(self, shakespeare_corpus, tmp_path):
        bad = tmp_path / "tampered.tset"
        # Magic lives in the first 8 bytes
        self._flip_byte(shakespeare_corpus["tset"], bad, 0)
        with pytest.raises(Exception):
            Reader(str(bad))


# ────────────────────────────────────────────────────────────────────
# G. Cross-impl conformance (Python vs Rust path)
# ────────────────────────────────────────────────────────────────────


class TestG_CrossImpl:
    def _have_rust(self) -> bool:
        try:
            import tset_rs  # noqa: F401

            return True
        except ImportError:
            return False

    def test_rust_reader_produces_same_smt_root(self, shakespeare_corpus):
        if not self._have_rust():
            pytest.skip("tset_rs wheel not installed")
        import tset_rs  # type: ignore

        path = str(shakespeare_corpus["tset"])
        py = Reader(path)
        rs = tset_rs.Reader(path)
        assert bytes(rs.smt_root()) == py.smt_root()

    def test_rust_reader_doc_count_matches(self, shakespeare_corpus):
        if not self._have_rust():
            pytest.skip("tset_rs wheel not installed")
        import tset_rs  # type: ignore

        py = Reader(str(shakespeare_corpus["tset"]))
        rs = tset_rs.Reader(str(shakespeare_corpus["tset"]))
        # Rust binding exposes doc_hashes_hex(), Python uses doc_order_hex()
        assert len(list(py.doc_order_hex())) == len(rs.doc_hashes_hex())


# ────────────────────────────────────────────────────────────────────
# H. End-to-end RTBF (right-to-be-forgotten) receipt
# ────────────────────────────────────────────────────────────────────


class TestH_RTBFReceipt:
    def test_present_doc_can_be_excluded_at_dataset_level(
        self, shakespeare_corpus, tmp_path
    ):
        """The full receipts flow:

        1. A document is present in the shard (inclusion proof verifies).
        2. We add a dataset-level exclusion overlay against its hash.
        3. The dataset Merkle root changes.   ← KNOWN GAP, see below.
        4. Reading via Dataset filters the excluded doc out of iteration.

        Step 3 is currently *not* enforced by either the Python or Rust
        implementation: ``_dataset_merkle_root()`` only commits to the
        list of registered shards, not to the exclusion overlay.

        That's a real gap between the pitch ("shards + exclusions +
        weights bind to the root") and the v0.3.2 binary, tracked as a
        v0.4 follow-up. We assert (4) — exclusions still take effect at
        read time — and assert (3) only via the exclusion-overlay file
        contents rather than the root.
        """
        from tset.dataset import Dataset, DatasetWriter

        # Build a small dataset with the existing shard. DatasetWriter
        # expects shards to live under <root>/shards/<name>.tset.
        dataset_root = tmp_path / "shake-dataset"
        (dataset_root / "shards").mkdir(parents=True)
        import shutil

        shard_path = dataset_root / "shards" / "shard-0001.tset"
        shutil.copy(shakespeare_corpus["tset"], shard_path)

        with DatasetWriter(str(dataset_root)) as dw:
            dw.register_shard("shard-0001")

        ds_before = Dataset(str(dataset_root))
        root_before = ds_before.dataset_merkle_root()

        # Pick a real document hash from the first shard.
        with next(iter(ds_before.shards())) as shard:
            target_h = next(iter(shard.documents()))[0]
            assert shard.has_document(target_h)

        # Add the dataset-level exclusion
        with DatasetWriter(str(dataset_root)) as dw:
            dw.add_exclusion(target_h, reason="RTBF showcase")

        ds_after = Dataset(str(dataset_root))

        # Receipt 1: the dataset's exclusion overlay now lists this hash
        assert ds_after.is_excluded(target_h)

        # Receipt 2 (read-time enforcement): streaming via the Dataset
        # never yields a batch whose doc_hash equals the excluded one.
        for _batch, dh in ds_after.stream_tokens("byte-level-v1", batch_size=4096):
            assert dh != target_h, (
                "excluded doc surfaced through Dataset.stream_tokens"
            )

        # Receipt 3 (commitment to the exclusion overlay) is currently
        # NOT enforced by the dataset Merkle root. See the docstring; we
        # capture today's behavior in an xfail-worthy assertion so a
        # future fix flips it green.
        # _dataset_merkle_root() leaves are derived from ShardEntry only,
        # so the root does not change when exclusions are added.
        assert root_before == ds_after.dataset_merkle_root(), (
            "behavior probe: today's root is not bound to exclusions; "
            "if this assertion starts failing, a fix has landed and the "
            "test should be inverted to assert change instead."
        )
