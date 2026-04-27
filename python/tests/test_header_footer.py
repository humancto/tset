import pytest

from tset.constants import HEADER_SIZE, FOOTER_SIZE, MAGIC_HEADER, MAGIC_FOOTER, TRUNCATED_HASH_SIZE
from tset.header import Header
from tset.footer import Footer


def test_header_round_trip():
    h = Header(
        version_major=0,
        version_minor=1,
        flags=0,
        manifest_offset=12345,
        manifest_size=678,
        shard_merkle_root=b"\x01" * 32,
        manifest_hash=b"\x02" * 32,
    )
    enc = h.encode()
    assert len(enc) == HEADER_SIZE
    assert enc[:4] == MAGIC_HEADER
    h2 = Header.decode(enc)
    assert h == h2


def test_header_rejects_future_major():
    h = Header(0, 1, 0, 100, 200, b"\x00" * 32, b"\x00" * 32)
    bad = bytearray(h.encode())
    bad[4] = 99
    with pytest.raises(ValueError, match="unsupported version"):
        Header.decode(bytes(bad))


def test_header_rejects_bad_magic():
    h = Header(0, 1, 0, 100, 200, b"\x00" * 32, b"\x00" * 32)
    bad = bytearray(h.encode())
    bad[:4] = b"XXXX"
    with pytest.raises(ValueError, match="bad header magic"):
        Header.decode(bytes(bad))


def test_footer_round_trip():
    f = Footer(manifest_size=4242, manifest_hash28=b"\x07" * TRUNCATED_HASH_SIZE)
    enc = f.encode()
    assert len(enc) == FOOTER_SIZE
    assert enc[36:40] == MAGIC_FOOTER
    f2 = Footer.decode(enc)
    assert f == f2


def test_footer_rejects_bad_magic():
    f = Footer(manifest_size=1, manifest_hash28=b"\x00" * TRUNCATED_HASH_SIZE)
    bad = bytearray(f.encode())
    bad[36:40] = b"NOPE"
    with pytest.raises(ValueError, match="bad footer magic"):
        Footer.decode(bytes(bad))
