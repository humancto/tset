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

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class AuditLog:
    entries: list[AuditEvent] = field(default_factory=list)
    log_root: str = ""

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
            prev_root = ev.chained_root
        return prev_root == self.log_root

    def to_dict(self) -> dict:
        return {
            "entries": [e.to_dict() for e in self.entries],
            "log_root": self.log_root,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "AuditLog":
        log = cls()
        log.entries = [AuditEvent(**e) for e in data.get("entries", [])]
        log.log_root = data.get("log_root", "")
        return log
