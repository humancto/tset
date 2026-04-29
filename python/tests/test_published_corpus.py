"""Lock the published TSET corpus's Merkle root.

If the build environment, source data, or writer code drifts in a way
that changes the bytes of `examples/published/corpus.tset`, this test
fires. The published root is publicized and pinned in
`examples/published/PUBLISHED-ROOT.txt` and the README; a silent shift
would invalidate every external auditor's pin.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))


CORPUS = REPO / "examples" / "published" / "corpus.tset"

# Published values from PUBLISHED-ROOT.txt. If you intentionally bump
# the published artefact, regenerate via `python -m examples.published.build`
# and update both the README, PUBLISHED-ROOT.txt, and these constants.
EXPECTED = {
    "size_bytes": 188540,
    "document_count": 200,
    "audit_log_entries": 202,
    "shard_merkle_root_hex": "9aaf829b410a26085a5e0fd30b8c130c58771dec25fb5f760f4d4a5494b623ab",
    "smt_root_hex": "369cf1fbacb1af433d2ea84ead6aa326eba6bd4698f872304a533444a5815444",
    "manifest_hash_hex": "7e787dc584ba7a4769414a81415dbc172c89a529ddc941405e9a3629f6449328",
    "first_doc_hash_hex": "912c26a1450aa0860809fff28ad9b34b2c6779b336f6c28f809276f571f3aece",
}


def test_corpus_is_committed():
    assert CORPUS.is_file(), (
        f"expected committed corpus at {CORPUS}; "
        "run `python -m examples.published.build` to regenerate"
    )


def test_published_root_matches():
    """Pin every receipt the README publicizes. Any drift is a real
    bug — either the bytes were edited or the writer changed."""
    pytest.importorskip("tset")
    from tset.reader import Reader

    assert CORPUS.stat().st_size == EXPECTED["size_bytes"]
    r = Reader(str(CORPUS))
    assert len(list(r.doc_order_hex())) == EXPECTED["document_count"]
    assert r.smt_root().hex() == EXPECTED["smt_root_hex"]
    assert r.header.shard_merkle_root.hex() == EXPECTED["shard_merkle_root_hex"]
    assert r.header.manifest_hash.hex() == EXPECTED["manifest_hash_hex"]
    assert next(iter(r.doc_order_hex())) == EXPECTED["first_doc_hash_hex"]
    log = r.audit_log()
    assert len(log.entries) == EXPECTED["audit_log_entries"]
    assert log.verify()


def test_inclusion_and_non_inclusion_proofs_verify():
    pytest.importorskip("tset")
    from tset.reader import Reader

    r = Reader(str(CORPUS))
    h = next(iter(r.documents()))[0]
    assert r.prove_inclusion(h).verify(r.smt_root())
    assert r.prove_non_inclusion(b"\xab" * 32).verify(r.smt_root())


def test_tampered_proof_is_rejected():
    pytest.importorskip("tset")
    from tset.reader import Reader

    r = Reader(str(CORPUS))
    h = next(iter(r.documents()))[0]
    p = r.prove_inclusion(h)
    if not p.siblings:
        pytest.skip("proof has no siblings to flip")
    flipped = type(p)(
        key=p.key,
        siblings=[bytes(b ^ 0xFF for b in p.siblings[0])] + p.siblings[1:],
    )
    assert not flipped.verify(r.smt_root())
