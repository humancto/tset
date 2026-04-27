"""BLAKE3 hashing + Merkle helpers.

When the optional `tset_rs` PyO3 wheel is installed, `merkle_root` and
`shard_merkle_root` delegate to the Rust core for byte-exact agreement
with `tset_core::hashing` (and a marginal speed bump on large inputs).

`hash_bytes` always uses the local `blake3` Python package — its FFI
overhead is high enough that delegating per-call would be slower than
the C extension for small inputs.

The pure-Python paths below remain as fallbacks for environments
without `tset_rs`. The conformance suite proves byte-equivalence.
"""

from __future__ import annotations

from blake3 import blake3


def _try_import_rust():
    try:
        import tset_rs

        return tset_rs
    except ImportError:
        return None


_RUST = _try_import_rust()


def hash_bytes(data: bytes) -> bytes:
    return blake3(data).digest()


def hash_hex(data: bytes) -> str:
    return blake3(data).hexdigest()


def merkle_root(leaves: list[bytes]) -> bytes:
    """Balanced binary Merkle tree over `leaves`. Last node is duplicated
    for odd levels. Domain-separated leaf/internal prefixes."""
    if _RUST is not None:
        return bytes(_RUST.merkle_root_unsorted_py(leaves))
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
    """Per SPEC §6: balanced binary Merkle tree over **sorted** doc hashes.
    Delegates to `tset_rs.shard_merkle_root_py` when available."""
    if _RUST is not None:
        return bytes(_RUST.shard_merkle_root_py(doc_hashes))
    return merkle_root(sorted(doc_hashes))
