"""Verify Python's tset.hashing.merkle_root / shard_merkle_root return
the same bytes whether delegated to tset_rs or computed in pure Python.
This is the conformance lock against silent drift between impls."""

import pytest

from tset.hashing import hash_bytes, merkle_root, shard_merkle_root


def _pure_python_merkle_root(leaves):
    if not leaves:
        return b"\x00" * 32
    level = [hash_bytes(b"\x00" + leaf) for leaf in leaves]
    while len(level) > 1:
        if len(level) % 2 == 1:
            level.append(level[-1])
        level = [
            hash_bytes(b"\x01" + level[i] + level[i + 1])
            for i in range(0, len(level), 2)
        ]
    return level[0]


@pytest.mark.parametrize(
    "leaves",
    [
        [],
        [hash_bytes(b"x")],
        [hash_bytes(b"x"), hash_bytes(b"y")],
        [hash_bytes(f"doc-{i}".encode()) for i in range(7)],
        [hash_bytes(f"doc-{i}".encode()) for i in range(100)],
    ],
)
def test_merkle_root_matches_pure_python(leaves):
    assert merkle_root(leaves) == _pure_python_merkle_root(leaves)


def test_shard_merkle_root_matches_sorted_pure_python():
    leaves = [hash_bytes(f"doc-{i}".encode()) for i in range(50)]
    # shard_merkle_root sorts before hashing
    assert shard_merkle_root(leaves) == _pure_python_merkle_root(sorted(leaves))


def test_delegation_active_when_tset_rs_installed():
    """If tset_rs is importable, hashing module must have wired up _RUST."""
    pytest.importorskip("tset_rs")
    from tset import hashing

    assert hashing._RUST is not None
