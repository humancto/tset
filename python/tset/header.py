import struct
from dataclasses import dataclass

from tset.constants import (
    HEADER_SIZE,
    MAGIC_HEADER,
    VERSION_MAJOR,
    VERSION_MINOR,
    HASH_SIZE,
)


@dataclass
class Header:
    version_major: int
    version_minor: int
    flags: int
    manifest_offset: int
    manifest_size: int
    shard_merkle_root: bytes
    manifest_hash: bytes

    def encode(self) -> bytes:
        if len(self.shard_merkle_root) != HASH_SIZE:
            raise ValueError("shard_merkle_root must be 32 bytes")
        if len(self.manifest_hash) != HASH_SIZE:
            raise ValueError("manifest_hash must be 32 bytes")
        out = bytearray(HEADER_SIZE)
        out[0:4] = MAGIC_HEADER
        out[4] = self.version_major
        out[5] = self.version_minor
        struct.pack_into("<I", out, 8, self.flags)
        struct.pack_into("<Q", out, 16, self.manifest_offset)
        struct.pack_into("<Q", out, 24, self.manifest_size)
        out[32:64] = self.shard_merkle_root
        out[64:96] = self.manifest_hash
        return bytes(out)

    @classmethod
    def decode(cls, data: bytes) -> "Header":
        if len(data) < HEADER_SIZE:
            raise ValueError(f"header buffer too small: {len(data)} < {HEADER_SIZE}")
        if data[0:4] != MAGIC_HEADER:
            raise ValueError(f"bad header magic: {data[0:4]!r}")
        version_major = data[4]
        version_minor = data[5]
        if version_major > VERSION_MAJOR:
            raise ValueError(
                f"unsupported version {version_major}.{version_minor}; "
                f"this reader supports up to {VERSION_MAJOR}.{VERSION_MINOR}"
            )
        flags = struct.unpack_from("<I", data, 8)[0]
        manifest_offset = struct.unpack_from("<Q", data, 16)[0]
        manifest_size = struct.unpack_from("<Q", data, 24)[0]
        return cls(
            version_major=version_major,
            version_minor=version_minor,
            flags=flags,
            manifest_offset=manifest_offset,
            manifest_size=manifest_size,
            shard_merkle_root=bytes(data[32:64]),
            manifest_hash=bytes(data[64:96]),
        )
