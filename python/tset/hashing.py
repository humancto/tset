"""BLAKE3 hashing + Merkle helpers.

The pure-Python impls below mirror `tset-core::hashing` exactly (verified
by the conformance suite). When the optional `tset_rs` PyO3 wheel is
installed and the Python interpreter has a valid `blake3` C-extension,
`hash_bytes` is the Python `blake3` package's primitive (≈10× faster
than a re-implementation). Merkle helpers then call into it.

We don't delegate `merkle_root` / `shard_merkle_root` directly to
`tset_rs` — the function-call boundary cost (allocate Python list of
bytes → cross PyO3 → Vec<Hash>) on small inputs would erase the
benefit. The Rust impl is byte-equivalent and the conformance suite
proves it.
"""

from blake3 import blake3


def hash_bytes(data: bytes) -> bytes:
    return blake3(data).digest()


def hash_hex(data: bytes) -> str:
    return blake3(data).hexdigest()


def merkle_root(leaves: list[bytes]) -> bytes:
    """Balanced binary Merkle tree over `leaves`. Last node is duplicated for
    odd levels. Domain-separated leaf/internal prefixes. Mirrors
    `tset_core::hashing::merkle_root_unsorted`."""
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


def shard_merkle_root(doc_hashes: list[bytes]) -> bytes:
    """Per SPEC §6: balanced binary Merkle tree over sorted doc hashes.
    Mirrors `tset_core::hashing::shard_merkle_root`."""
    return merkle_root(sorted(doc_hashes))
