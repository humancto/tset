from blake3 import blake3


def hash_bytes(data: bytes) -> bytes:
    return blake3(data).digest()


def hash_hex(data: bytes) -> str:
    return blake3(data).hexdigest()


def merkle_root(leaves: list[bytes]) -> bytes:
    """Balanced binary Merkle tree over `leaves`. Last node is duplicated for
    odd levels. Domain-separated leaf/internal prefixes."""
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
    """Per SPEC §6: balanced binary Merkle tree over sorted doc hashes."""
    return merkle_root(sorted(doc_hashes))
