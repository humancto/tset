"""Append-only Merkle audit log of provenance events.

Per RFC §5.5, the log is a Certificate-Transparency-style append-only
structure. Each entry chains forward via `BLAKE3(prev_root || entry_bytes)`.
The latest `log_root` is committed in the manifest. v0.1 does not yet sign
entries (key management is open per RFC §10 items 17-18); the chained-hash
provides tamper-evidence under integrity assumptions for the manifest itself.
"""

from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field
from typing import Literal

from tset.hashing import hash_bytes


EventType = Literal["ingestion", "exclusion", "version_snapshot", "tokenizer_added"]


@dataclass
class AuditEvent:
    seq: int
    timestamp: float
    event_type: EventType
    payload: dict
    prev_root: str
    entry_hash: str
    chained_root: str
    # v0.4+: optional Ed25519 signature over entry_hash bytes (hex-encoded).
    signature: str | None = None

    def to_dict(self) -> dict:
        d = asdict(self)
        if d.get("signature") is None:
            d.pop("signature", None)
        return d


@dataclass
class AuditLog:
    entries: list[AuditEvent] = field(default_factory=list)
    log_root: str = ""
    # When signing, every appended entry is signed; verify() requires
    # a matching `writer_public_key` on the manifest's audit_log dict.
    writer_public_key: str | None = None

    def append(self, event_type: EventType, payload: dict) -> AuditEvent:
        seq = len(self.entries)
        prev_root = self.log_root
        # TSET_DETERMINISTIC_TIME (float seconds since epoch) makes audit
        # log timestamps reproducible — required for stable conformance
        # fixtures and any other test that hashes the manifest. Production
        # writes leave the env var unset and use wall time.
        import os as _os

        det = _os.environ.get("TSET_DETERMINISTIC_TIME")
        if det is not None:
            try:
                timestamp = float(det)
            except ValueError:
                timestamp = time.time()
        else:
            timestamp = time.time()
        entry_payload = {
            "seq": seq,
            "timestamp": timestamp,
            "event_type": event_type,
            "payload": payload,
        }
        entry_bytes = json.dumps(
            entry_payload, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        entry_hash = hash_bytes(entry_bytes)
        prev_root_bytes = bytes.fromhex(prev_root) if prev_root else b"\x00" * 32
        chained_root = hash_bytes(prev_root_bytes + entry_hash)
        ev = AuditEvent(
            seq=seq,
            timestamp=timestamp,
            event_type=event_type,
            payload=payload,
            prev_root=prev_root,
            entry_hash=entry_hash.hex(),
            chained_root=chained_root.hex(),
        )
        self.entries.append(ev)
        self.log_root = ev.chained_root
        return ev

    def verify(self) -> bool:
        prev_root = ""
        # Signature contract: if writer_public_key is set, every entry
        # MUST have a signature. If any entry has a signature but no
        # pubkey is published, reject.
        any_signed = any(ev.signature for ev in self.entries)
        if self.writer_public_key and not any_signed and self.entries:
            return False
        if not self.writer_public_key and any_signed:
            return False

        verifier = None
        if self.writer_public_key:
            try:
                import tset_rs  # type: ignore[import-not-found]

                verifier = tset_rs.verify_audit_signature
            except ImportError:
                # Pure-Python fallback would need PyNaCl. For now we
                # require tset_rs to verify signed logs.
                return False

        for i, ev in enumerate(self.entries):
            if ev.seq != i or ev.prev_root != prev_root:
                return False
            entry_payload = {
                "seq": ev.seq,
                "timestamp": ev.timestamp,
                "event_type": ev.event_type,
                "payload": ev.payload,
            }
            entry_bytes = json.dumps(
                entry_payload, sort_keys=True, separators=(",", ":")
            ).encode("utf-8")
            if hash_bytes(entry_bytes).hex() != ev.entry_hash:
                return False
            chained = hash_bytes(
                (bytes.fromhex(prev_root) if prev_root else b"\x00" * 32)
                + bytes.fromhex(ev.entry_hash)
            )
            if chained.hex() != ev.chained_root:
                return False
            if verifier is not None:
                if not ev.signature:
                    return False
                pk = bytes.fromhex(self.writer_public_key)
                sig = bytes.fromhex(ev.signature)
                if not verifier(pk, bytes.fromhex(ev.entry_hash), sig):
                    return False
            prev_root = ev.chained_root
        return prev_root == self.log_root

    def to_dict(self) -> dict:
        out: dict = {
            "entries": [e.to_dict() for e in self.entries],
            "log_root": self.log_root,
        }
        if self.writer_public_key:
            out["writer_public_key"] = self.writer_public_key
        return out

    @classmethod
    def from_dict(cls, data: dict) -> "AuditLog":
        log = cls()
        log.entries = [AuditEvent(**e) for e in data.get("entries", [])]
        log.log_root = data.get("log_root", "")
        log.writer_public_key = data.get("writer_public_key")
        return log
