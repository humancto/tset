"""Cookbook: handle a GDPR-Article-17 deletion request end-to-end.

The full operational chain a compliance team runs when a user files
a deletion request against a TSET-published corpus:

  1. Receive request with a document hash (or compute it from the
     content the user submitted).
  2. Verify the document is currently committed in the corpus
     (otherwise the request is moot — produce a non-inclusion proof
     and reply).
  3. Add the hash to the dataset exclusion overlay with a signed
     audit-log entry.
  4. Re-publish the dataset_merkle_root.
  5. Hand the requester a non-inclusion proof against the new root,
     time-stamped, signed, with the audit entry referenced.

Run::

    pip install tset
    python -m examples.cookbook.deletion_end_to_end

This recipe walks every step. The only thing missing from a real
production run is signing the proof (the audit log is signed; the
proof itself isn't, by design — the verifier reproduces the proof
from the dataset bytes).
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO / "python"))


def main() -> int:
    from tset.dataset import Dataset, DatasetWriter
    from tset.tokenizers import ByteLevelTokenizer
    from tset.writer import Writer

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "ds"
        root.mkdir()
        shards_dir = root / "shards"
        shards_dir.mkdir()

        # ── 1. Build a corpus with three shards.
        target_doc = b"this is the document to be deleted, eventually"
        shards = []
        for i, payload in enumerate([
            b"alpha gamma delta epsilon",
            target_doc,
            b"zeta eta theta iota kappa",
        ]):
            shard_path = shards_dir / f"part-{i:04d}.tset"
            with Writer(str(shard_path)) as w:
                target_hash = w.add_document(payload, metadata={"row": i})
                if payload == target_doc:
                    deletion_target = target_hash
                w.add_tokenizer_view(ByteLevelTokenizer())
            shards.append(f"part-{i:04d}")

        dw = DatasetWriter(str(root))
        for name in shards:
            dw.register_shard(name)
        dw.close()

        ds_before = Dataset(str(root))
        root_before = ds_before.dataset_merkle_root()
        print(f"step 1  corpus published        root={root_before.hex()[:16]}…")

        # ── 2. The user submits a request. Verify the document IS in
        #     the corpus before bothering to delete it.
        proof = ds_before.prove_inclusion(deletion_target)
        # `prove_inclusion` returns (shard_path, InclusionProof). If
        # the document isn't found, it raises.
        print(f"step 2  inclusion confirmed     in {Path(proof[0]).name}")

        # ── 3. Add the hash to the exclusion overlay. open_existing()
        #     pulls forward the prior shards + audit log so the new
        #     close() produces a continuation snapshot, not a fresh
        #     start.
        dw2 = DatasetWriter(str(root), load_existing=True)
        dw2.add_exclusion(
            deletion_target,
            reason="GDPR Art. 17 erasure request 2026-04-29",
        )
        dw2.close()

        # ── 4. The new dataset_merkle_root differs — that's the entire
        #     point. Republish it (model card, transparency log, ledger).
        ds_after = Dataset(str(root))
        root_after = ds_after.dataset_merkle_root()
        assert root_before != root_after, "root must change after exclusion"
        print(f"step 4  new root published      root={root_after.hex()[:16]}…")

        # ── 5. Hand the requester a non-inclusion proof against the
        #     NEW root. The verifier checks the SMT non-inclusion proof
        #     for each shard PLUS the exclusion overlay membership.
        ni = ds_after.prove_non_inclusion(deletion_target)
        # prove_non_inclusion returns a dict; the exclusion overlay
        # membership flag is the strongest signal.
        assert ni["exclusion_overlay_includes"], (
            "non-inclusion proof must reference the exclusion overlay"
        )
        print(f"step 5  non-inclusion proof     overlay_includes=True")

        # ── 6. The audit log records who/when/why. Anyone with the
        #     dataset can reconstruct the full chain.
        import json
        manifest = json.loads(
            (root / "manifest.tset.json").read_text()
        )
        last = manifest["audit_log"]["entries"][-1]
        # The version_snapshot is appended after the exclusion event,
        # so the deletion entry is the second-to-last.
        deletion_entry = manifest["audit_log"]["entries"][-2]
        assert deletion_entry["event_type"] == "exclusion"
        assert deletion_entry["payload"]["doc_hash"] == deletion_target.hex()
        print(
            f"step 6  audit entry             "
            f"#{deletion_entry['seq']}  reason={deletion_entry['payload']['reason']!r}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
