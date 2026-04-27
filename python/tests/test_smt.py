from tset.hashing import hash_bytes
from tset.smt import (
    EMPTY_ROOT,
    InclusionProof,
    NonInclusionProof,
    SparseMerkleTree,
)


def test_empty_tree_root():
    t = SparseMerkleTree()
    assert t.root() == EMPTY_ROOT
    assert len(t) == 0


def test_insert_and_inclusion():
    t = SparseMerkleTree()
    keys = [hash_bytes(f"doc-{i}".encode()) for i in range(50)]
    for k in keys:
        t.insert(k)
    r = t.root()
    for k in keys:
        p = t.prove(k)
        assert isinstance(p, InclusionProof)
        assert p.verify(r)


def test_non_inclusion():
    t = SparseMerkleTree()
    for i in range(20):
        t.insert(hash_bytes(f"doc-{i}".encode()))
    r = t.root()
    for i in range(50, 60):
        k = hash_bytes(f"missing-{i}".encode())
        p = t.prove(k)
        assert isinstance(p, NonInclusionProof)
        assert p.verify(r)


def test_non_inclusion_on_empty():
    t = SparseMerkleTree()
    p = t.prove(hash_bytes(b"x"))
    assert isinstance(p, NonInclusionProof)
    assert p.verify(EMPTY_ROOT)


def test_tampered_proof_fails():
    t = SparseMerkleTree()
    k = hash_bytes(b"present")
    t.insert(k)
    r = t.root()
    p = t.prove(k)
    p.siblings[5] = b"\x42" * 32
    assert not p.verify(r)


def test_idempotent_insert():
    t = SparseMerkleTree()
    k = hash_bytes(b"x")
    t.insert(k)
    r1 = t.root()
    t.insert(k)
    assert t.root() == r1
    assert len(t) == 1
