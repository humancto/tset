"""Cookbook: minimal "training loop" consuming TSET tokens.

The smallest realistic shape of a training data path:

  - Open a dataset, possibly multi-shard
  - Stream batches at a fixed token budget
  - Apply the exclusion overlay across shards
  - Pack tokens into fixed-length training rows (with the
    document-boundary signal preserved as a per-row mask)

This recipe avoids torch/jax to stay dependency-free. The output of
``iter_training_rows`` is a stream of NumPy arrays with the exact
shape a training step would consume — drop in a ``torch.from_numpy``
on the input array and you have the full thing.

Run::

    pip install tset
    python -m examples.cookbook.training_loop
"""

from __future__ import annotations

import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

import numpy as np

_REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO / "python"))


@dataclass
class TrainingRow:
    tokens: np.ndarray  # shape (seq_len,) dtype=uint32
    # Per-position mask: True at positions that crossed a document
    # boundary on the way into this row. Lets the loss mask out
    # cross-document attention, or the loop drop the row entirely.
    doc_boundary_mask: np.ndarray  # shape (seq_len,) dtype=bool


def iter_training_rows(
    dataset_root: str,
    tokenizer_id: str,
    seq_len: int = 256,
) -> Iterator[TrainingRow]:
    """Pack streamed tokens into fixed-length training rows.

    Per-document streaming guarantees we know where each document ends.
    We exploit that to record document boundaries in the mask.
    """
    from tset.dataset import Dataset

    ds = Dataset(dataset_root)
    buf = np.empty(0, dtype=np.uint32)
    boundaries: list[bool] = []  # parallel to buf

    for tokens, _doc_hash in ds.stream_tokens(tokenizer_id, batch_size=4096):
        # Append to the buffer; mark first element of each batch as a
        # potential boundary (it will only be a real boundary if the
        # PREVIOUS batch ended on a different doc, which we approximate
        # by marking the first element of every newly-streamed slab —
        # batch_size is per-doc so this is exact).
        new_mask = [False] * tokens.size
        if tokens.size > 0:
            new_mask[0] = True
        buf = np.concatenate([buf, tokens]) if buf.size else tokens
        boundaries.extend(new_mask)
        while buf.size >= seq_len:
            row_tokens = buf[:seq_len].copy()
            row_mask = np.array(boundaries[:seq_len], dtype=bool)
            buf = buf[seq_len:]
            boundaries = boundaries[seq_len:]
            yield TrainingRow(tokens=row_tokens, doc_boundary_mask=row_mask)
    # Tail: in production you'd usually drop the partial row. This
    # recipe drops it too — the loop ends after the last full seq_len
    # batch is yielded.


def main() -> int:
    from tset.dataset import DatasetWriter
    from tset.tokenizers import ByteLevelTokenizer
    from tset.writer import Writer

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "ds"
        root.mkdir()
        (root / "shards").mkdir()

        # Build a deterministic small dataset.
        for i in range(2):
            shard_path = root / "shards" / f"part-{i:04d}.tset"
            with Writer(str(shard_path)) as w:
                for j in range(8):
                    payload = f"shard {i} doc {j} ".encode() + b"alpha beta gamma delta " * 16
                    w.add_document(payload, metadata={"shard": i, "row": j})
                w.add_tokenizer_view(ByteLevelTokenizer())

        dw = DatasetWriter(str(root))
        dw.register_shard("part-0000")
        dw.register_shard("part-0001")
        dw.close()

        # Drive a fake training loop.
        SEQ_LEN = 128
        BATCH_SIZE = 4
        rows: list[TrainingRow] = []
        for row in iter_training_rows(str(root), "byte-level-v1", seq_len=SEQ_LEN):
            rows.append(row)
            if len(rows) >= BATCH_SIZE:
                # In a real loop: stack into a single (BATCH_SIZE, SEQ_LEN)
                # array and feed it to the model.
                inputs = np.stack([r.tokens for r in rows])  # (B, S)
                masks = np.stack([r.doc_boundary_mask for r in rows])  # (B, S)
                # Drop in torch.from_numpy(inputs).long() etc here.
                rows.clear()
                # First batch only — keep the recipe runtime tiny.
                print(f"batch shape       inputs={inputs.shape} dtype={inputs.dtype}")
                print(f"batch shape       masks={masks.shape}  dtype={masks.dtype}")
                print(f"first row tokens  {inputs[0, :16].tolist()}…")
                print(f"first row mask    {int(masks[0].sum())} doc boundaries in {SEQ_LEN} tokens")
                break

    print()
    print("Plug into PyTorch:")
    print("    import torch")
    print("    inputs = torch.from_numpy(inputs).long()")
    print("    masks  = torch.from_numpy(masks)")
    print("    logits = model(inputs)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
