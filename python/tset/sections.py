"""On-disk binary sections — Python encoder parity with `tset_core::sections`.

Three section types: TSMT (Sparse Merkle Tree), TLOG (audit log),
TCOL (metadata columns). All three follow the same shape: 4-byte magic
+ 1-byte version + 3 reserved zeros + 8-byte payload-size header +
type-specific fixed fields + content_hash + payload.

Wire format is byte-identical to the Rust impl — verified by the
conformance suite (any v0.3.2 fixture written via either path is
byte-equivalent given the same deterministic inputs)."""

from __future__ import annotations

import json

from tset.constants import HASH_SIZE
from tset.hashing import hash_bytes


MAGIC_SMT = b"TSMT"
MAGIC_AUDIT_LOG = b"TLOG"
MAGIC_COLUMNS = b"TCOL"

TSMT_VERSION = 1
TLOG_VERSION = 1
TCOL_VERSION = 1

TSMT_HEADER_SIZE = 80
TLOG_HEADER_SIZE = 80
TCOL_HEADER_SIZE = 56


def _canonical_json(v) -> bytes:
    return json.dumps(v, sort_keys=True, separators=(",", ":")).encode("utf-8")


def encode_tsmt_section(present_keys: list[bytes], smt_root: bytes) -> bytes:
    if len(smt_root) != HASH_SIZE:
        raise ValueError(f"smt_root must be {HASH_SIZE} bytes")
    keys_sorted = sorted(present_keys)
    keys_bytes = b"".join(keys_sorted)
    content_hash = hash_bytes(keys_bytes)
    return b"".join([
        MAGIC_SMT,
        bytes([TSMT_VERSION]),
        b"\x00\x00\x00",  # reserved
        len(keys_sorted).to_bytes(8, "little"),
        smt_root,
        content_hash,
        keys_bytes,
    ])


def encode_tlog_section(audit_json: dict, log_root: bytes) -> bytes:
    if len(log_root) != HASH_SIZE:
        raise ValueError(f"log_root must be {HASH_SIZE} bytes")
    payload = _canonical_json(audit_json)
    content_hash = hash_bytes(payload)
    return b"".join([
        MAGIC_AUDIT_LOG,
        bytes([TLOG_VERSION]),
        b"\x00\x00\x00",
        len(payload).to_bytes(8, "little"),
        log_root,
        content_hash,
        payload,
    ])


def encode_tcol_section(columns_json: dict, row_count: int) -> bytes:
    payload = _canonical_json(columns_json)
    content_hash = hash_bytes(payload)
    return b"".join([
        MAGIC_COLUMNS,
        bytes([TCOL_VERSION]),
        b"\x00\x00\x00",
        len(payload).to_bytes(8, "little"),
        row_count.to_bytes(8, "little"),
        content_hash,
        payload,
    ])


def decode_tsmt_section(buf: bytes) -> dict:
    """Decode a TSMT section. Returns dict with keys: smt_version,
    num_present, smt_root, content_hash, present_keys."""
    if len(buf) < TSMT_HEADER_SIZE:
        raise ValueError("TSMT section truncated")
    if buf[:4] != MAGIC_SMT:
        raise ValueError(f"TSMT bad magic: {buf[:4]!r}")
    smt_version = buf[4]
    if smt_version != TSMT_VERSION:
        raise ValueError(f"TSMT unsupported smt_version: {smt_version}")
    num_present = int.from_bytes(buf[8:16], "little")
    smt_root = buf[16:48]
    content_hash = buf[48:80]
    keys_end = TSMT_HEADER_SIZE + num_present * HASH_SIZE
    if keys_end > len(buf):
        raise ValueError("TSMT keys exceed section")
    keys_bytes = buf[TSMT_HEADER_SIZE:keys_end]
    if hash_bytes(keys_bytes) != content_hash:
        raise ValueError("TSMT content_hash mismatch")
    keys = [keys_bytes[i : i + HASH_SIZE] for i in range(0, len(keys_bytes), HASH_SIZE)]
    if len(keys) > 1:
        for a, b in zip(keys, keys[1:]):
            if a >= b:
                raise ValueError("TSMT keys not strictly sorted")
    return {
        "smt_version": smt_version,
        "num_present": num_present,
        "smt_root": smt_root,
        "content_hash": content_hash,
        "present_keys": keys,
    }


def decode_tlog_section(buf: bytes) -> dict:
    if len(buf) < TLOG_HEADER_SIZE:
        raise ValueError("TLOG section truncated")
    if buf[:4] != MAGIC_AUDIT_LOG:
        raise ValueError(f"TLOG bad magic: {buf[:4]!r}")
    log_version = buf[4]
    if log_version != TLOG_VERSION:
        raise ValueError(f"TLOG unsupported log_version: {log_version}")
    payload_size = int.from_bytes(buf[8:16], "little")
    log_root = buf[16:48]
    content_hash = buf[48:80]
    payload_end = TLOG_HEADER_SIZE + payload_size
    if payload_end > len(buf):
        raise ValueError("TLOG payload exceeds section")
    payload = buf[TLOG_HEADER_SIZE:payload_end]
    if hash_bytes(payload) != content_hash:
        raise ValueError("TLOG content_hash mismatch")
    return {
        "log_version": log_version,
        "log_root": log_root,
        "content_hash": content_hash,
        "audit_json": json.loads(payload.decode("utf-8")),
    }


def decode_tcol_section(buf: bytes) -> dict:
    if len(buf) < TCOL_HEADER_SIZE:
        raise ValueError("TCOL section truncated")
    if buf[:4] != MAGIC_COLUMNS:
        raise ValueError(f"TCOL bad magic: {buf[:4]!r}")
    cols_version = buf[4]
    if cols_version != TCOL_VERSION:
        raise ValueError(f"TCOL unsupported cols_version: {cols_version}")
    payload_size = int.from_bytes(buf[8:16], "little")
    row_count = int.from_bytes(buf[16:24], "little")
    content_hash = buf[24:56]
    payload_end = TCOL_HEADER_SIZE + payload_size
    if payload_end > len(buf):
        raise ValueError("TCOL payload exceeds section")
    payload = buf[TCOL_HEADER_SIZE:payload_end]
    if hash_bytes(payload) != content_hash:
        raise ValueError("TCOL content_hash mismatch")
    return {
        "cols_version": cols_version,
        "row_count": row_count,
        "content_hash": content_hash,
        "columns_json": json.loads(payload.decode("utf-8")),
    }
