"""Sparse Merkle Tree over BLAKE3 document hashes.

v0.1 implementation: fixed-depth (256) binary SMT with precomputed empty
subtree hashes and a pointer-based sparse representation that materialises
only the paths leading to present leaves.

Design status: per RFC §10 items 14-18 the on-disk SMT layout, key
derivation, and snapshot semantics are explicitly under cryptographic review.
This module is the runtime implementation only — the manifest stores the SMT
*root* (and a serialised present-key set, for v0.1 reproducibility) under a
versioned `smt_version` field so on-disk encoding can change without breaking
older readers.
"""

from __future__ import annotations

from dataclasses import dataclass

from tset.hashing import hash_bytes


SMT_DEPTH = 256
LEAF_PREFIX = b"\x10"
INTERNAL_PREFIX = b"\x11"
ABSENT_LEAF = hash_bytes(LEAF_PREFIX + b"\x00")
PRESENT_LEAF = hash_bytes(LEAF_PREFIX + b"\x01")


def _build_empty() -> list[bytes]:
    """EMPTY[d] = hash of an empty subtree of depth d (containing 2^d leaves)."""
    levels = [ABSENT_LEAF]
    cur = ABSENT_LEAF
    for _ in range(SMT_DEPTH):
        cur = hash_bytes(INTERNAL_PREFIX + cur + cur)
        levels.append(cur)
    return levels


EMPTY = _build_empty()
EMPTY_ROOT = EMPTY[SMT_DEPTH]


def _bit(key: bytes, i: int) -> int:
    """Bit at position i counted MSB-first across the whole key."""
    return (key[i >> 3] >> (7 - (i & 7))) & 1


def _verify_path(key: bytes, leaf_hash: bytes, siblings: list[bytes]) -> bytes:
    """Recompute the root from a leaf hash + siblings ordered top-down
    (siblings[0] is the sibling of the root's child, siblings[-1] is the
    leaf's sibling)."""
    if len(siblings) != SMT_DEPTH:
        raise ValueError(f"siblings length must be {SMT_DEPTH}")
    node = leaf_hash
    for level in range(SMT_DEPTH):
        depth = SMT_DEPTH - 1 - level
        sibling = siblings[depth]
        if _bit(key, depth) == 0:
            node = hash_bytes(INTERNAL_PREFIX + node + sibling)
        else:
            node = hash_bytes(INTERNAL_PREFIX + sibling + node)
    return node


@dataclass
class InclusionProof:
    key: bytes
    siblings: list[bytes]

    def root(self) -> bytes:
        return _verify_path(self.key, PRESENT_LEAF, self.siblings)

    def verify(self, expected_root: bytes) -> bool:
        return self.root() == expected_root


@dataclass
class NonInclusionProof:
    key: bytes
    siblings: list[bytes]

    def root(self) -> bytes:
        return _verify_path(self.key, ABSENT_LEAF, self.siblings)

    def verify(self, expected_root: bytes) -> bool:
        return self.root() == expected_root


class _Internal:
    __slots__ = ("left", "right")

    def __init__(self):
        self.left: _Internal | None = None
        self.right: _Internal | None = None


class SparseMerkleTree:
    def __init__(self):
        self._present: set[bytes] = set()
        self._root: _Internal | None = None
        self._hash_cache: dict[int, bytes] = {}

    def __len__(self) -> int:
        return len(self._present)

    def has(self, key: bytes) -> bool:
        return key in self._present

    def insert(self, key: bytes) -> None:
        if len(key) * 8 != SMT_DEPTH:
            raise ValueError(f"key must be {SMT_DEPTH // 8} bytes")
        if key in self._present:
            return
        self._present.add(key)
        self._hash_cache.clear()
        if self._root is None:
            self._root = _Internal()
        node = self._root
        for depth in range(SMT_DEPTH - 1):
            bit = _bit(key, depth)
            if bit == 0:
                if node.left is None:
                    node.left = _Internal()
                node = node.left
            else:
                if node.right is None:
                    node.right = _Internal()
                node = node.right
        last_bit = _bit(key, SMT_DEPTH - 1)
        if last_bit == 0:
            node.left = _LEAF_MARKER
        else:
            node.right = _LEAF_MARKER

    def _hash_subtree(self, node, depth: int) -> bytes:
        if node is None:
            return EMPTY[SMT_DEPTH - depth]
        if node is _LEAF_MARKER:
            return PRESENT_LEAF
        cached = self._hash_cache.get(id(node))
        if cached is not None:
            return cached
        left = self._hash_subtree(node.left, depth + 1)
        right = self._hash_subtree(node.right, depth + 1)
        h = hash_bytes(INTERNAL_PREFIX + left + right)
        self._hash_cache[id(node)] = h
        return h

    def root(self) -> bytes:
        if self._root is None:
            return EMPTY_ROOT
        return self._hash_subtree(self._root, 0)

    def prove(self, key: bytes) -> InclusionProof | NonInclusionProof:
        if len(key) * 8 != SMT_DEPTH:
            raise ValueError(f"key must be {SMT_DEPTH // 8} bytes")
        siblings: list[bytes] = []
        node = self._root
        for depth in range(SMT_DEPTH):
            bit = _bit(key, depth)
            if node is None or node is _LEAF_MARKER:
                siblings.append(EMPTY[SMT_DEPTH - depth - 1])
                node = None
                continue
            if bit == 0:
                siblings.append(self._hash_subtree(node.right, depth + 1))
                node = node.left
            else:
                siblings.append(self._hash_subtree(node.left, depth + 1))
                node = node.right
        if key in self._present:
            return InclusionProof(key=key, siblings=siblings)
        return NonInclusionProof(key=key, siblings=siblings)

    def present_keys(self) -> list[bytes]:
        return sorted(self._present)


_LEAF_MARKER = object()
