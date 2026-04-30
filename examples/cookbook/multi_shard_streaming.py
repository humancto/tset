"""Cookbook: open a multi-shard TSET dataset and stream tokens, with
the dataset's exclusion overlay enforced across shards.

The single-shard ``Reader.stream_tokens`` is fine for one .tset file,
but production corpora are usually multi-shard datasets (each shard
~1-10 GB, dataset is hundreds to thousands of shards). This recipe
shows the canonical iteration pattern:

  1. Open the dataset, pick up its exclusion overlay
  2. For each shard, open a Reader and stream tokens
  3. Drop tokens whose doc_hash is in the overlay (the entire reason
     iteration is per-document, not per-batch)
  4. Yield (tokens, doc_hash, shard_idx) so the caller knows where
     each batch came from

Run::

    pip install tset
    python -m examples.cookbook.multi_shard_streaming
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path
from typing import Iterator

import numpy as np

_REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO / "python"))


def stream_dataset(
    dataset_root: str,
    tokenizer_id: str,
    batch_size: int = 1024,
) -> Iterator[tuple[np.ndarray, bytes, int]]:
    """Stream batches across every shard, honouring the exclusion overlay.

    Yields ``(tokens, doc_hash, shard_idx)``. Per-document iteration
    means tokens never cross document boundaries within a batch, so
    dropping an excluded document is a clean ``continue``.
    """
    from tset.dataset import Dataset
    from tset.reader import Reader

    ds = Dataset(dataset_root)
    excluded_hex = ds.exclusions()  # set[str] of excluded hex hashes
    for shard_idx, shard_path in enumerate(ds.shard_paths()):
        with Reader(str(shard_path)) as r:
            for tokens, doc_hash in r.stream_tokens(tokenizer_id, batch_size=batch_size):
                if doc_hash.hex() in excluded_hex:
                    continue
                yield tokens, doc_hash, shard_idx
    # Note: `Dataset.stream_tokens(tokenizer_id, batch_size)` does the
    # same thing as the loop above and is a one-liner. We expand it
    # here so you can see the per-shard structure for custom needs
    # (e.g. mixing in metadata predicates, adding rate limiting,
    # parallel reads with a pool).


def main() -> int:
    from tset.dataset import DatasetWriter
    from tset.tokenizers import ByteLevelTokenizer
    from tset.writer import Writer

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "ds"
        root.mkdir()
        (root / "shards").mkdir()

        # ── Build a 3-shard dataset. The middle shard contains a
        #    document we'll later exclude — we want to confirm the
        #    streaming consumer drops it cleanly.
        excluded_hash: bytes | None = None
        for i in range(3):
            shard_path = root / "shards" / f"part-{i:04d}.tset"
            with Writer(str(shard_path)) as w:
                for j in range(4):
                    payload = f"shard {i} doc {j} alpha beta gamma".encode()
                    h = w.add_document(payload, metadata={"shard": i, "row": j})
                    if i == 1 and j == 2:
                        excluded_hash = h
                w.add_tokenizer_view(ByteLevelTokenizer())

        dw = DatasetWriter(str(root))
        for i in range(3):
            dw.register_shard(f"part-{i:04d}")
        assert excluded_hash is not None
        dw.add_exclusion(excluded_hash, reason="demo: out-of-policy content")
        dw.close()

        # ── Stream every batch and tally per-shard / per-doc tokens.
        per_shard_docs: dict[int, set[str]] = {0: set(), 1: set(), 2: set()}
        per_shard_tokens: dict[int, int] = {0: 0, 1: 0, 2: 0}
        for tokens, doc_hash, shard_idx in stream_dataset(
            str(root), tokenizer_id="byte-level-v1", batch_size=64
        ):
            per_shard_docs[shard_idx].add(doc_hash.hex())
            per_shard_tokens[shard_idx] += int(tokens.size)

        excluded_hex = excluded_hash.hex()
        for i in range(3):
            print(
                f"shard {i}  docs={len(per_shard_docs[i])}  "
                f"tokens={per_shard_tokens[i]:,}"
            )
        print()
        print(f"excluded hash:    {excluded_hex[:16]}…")
        # The excluded doc must NOT appear in any shard's emitted
        # docs; that's the exclusion-overlay-enforces-across-shards
        # invariant.
        for i, docs in per_shard_docs.items():
            assert excluded_hex not in docs, (
                f"shard {i} leaked the excluded document"
            )
        # And the middle shard should be one document short.
        assert len(per_shard_docs[1]) == 3, (
            f"shard 1 should have 3 surviving docs, got {len(per_shard_docs[1])}"
        )
        print("verified          excluded document was dropped from streaming")
        print(
            f"verified          shard 1 has 3 surviving docs (was 4 before "
            f"exclusion)"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
