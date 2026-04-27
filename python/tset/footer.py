import struct
from dataclasses import dataclass

from tset.constants import FOOTER_SIZE, MAGIC_FOOTER, TRUNCATED_HASH_SIZE


@dataclass
class Footer:
    manifest_size: int
    manifest_hash28: bytes

    def encode(self) -> bytes:
        if len(self.manifest_hash28) != TRUNCATED_HASH_SIZE:
            raise ValueError(f"manifest_hash28 must be {TRUNCATED_HASH_SIZE} bytes")
        out = bytearray(FOOTER_SIZE)
        struct.pack_into("<Q", out, 0, self.manifest_size)
        out[8:36] = self.manifest_hash28
        out[36:40] = MAGIC_FOOTER
        return bytes(out)

    @classmethod
    def decode(cls, data: bytes) -> "Footer":
        if len(data) != FOOTER_SIZE:
            raise ValueError(f"footer must be exactly {FOOTER_SIZE} bytes")
        if data[36:40] != MAGIC_FOOTER:
            raise ValueError(f"bad footer magic: {data[36:40]!r}")
        return cls(
            manifest_size=struct.unpack_from("<Q", data, 0)[0],
            manifest_hash28=bytes(data[8:36]),
        )
