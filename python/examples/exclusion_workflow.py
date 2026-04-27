"""End-to-end demonstration of User Story 2 / Benchmark E.

1. Build a multi-shard dataset including a `marker` document.
2. Receive an "exclusion request" naming the marker's hash.
3. Produce a dataset-level non-inclusion proof at the new snapshot.
4. Re-stream tokens and verify the marker's bytes do not appear.

This is *not* a model-level unlearning workflow — see RFC §5.7. It produces
the data-pipeline evidence artefact only.
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
import tempfile

from tset.dataset import Dataset, DatasetWriter
from tset.hashing import hash_bytes
from tset.tokenizers import ByteLevelTokenizer
from tset.writer import Writer


def build_demo_dataset(root: str, marker_text: bytes) -> bytes:
    if os.path.exists(root):
        shutil.rmtree(root)
    os.makedirs(root, exist_ok=True)
    dw = DatasetWriter(root)
    docs = [
        b"alpha document content.",
        b"beta document content.",
        marker_text,
        b"gamma document content.",
        b"delta document content.",
    ]
    for i, chunk in enumerate(_chunks(docs, 2)):
        with dw.shard_writer(f"part-{i:05d}") as sw:
            for d in chunk:
                sw.add_document(d, metadata={"doc_idx": i})
            sw.add_tokenizer_view(ByteLevelTokenizer())
        dw.register_shard(f"part-{i:05d}")
    dw.close()
    return hash_bytes(marker_text)


def _chunks(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--root", default=None)
    args = p.parse_args(argv)

    workdir = args.root or tempfile.mkdtemp(prefix="tset-demo-")
    marker = b"this is the document the user asked us to exclude."
    marker_hash = build_demo_dataset(workdir, marker)
    print(f"Built dataset at {workdir}; marker hash: {marker_hash.hex()}")

    ds = Dataset(workdir)
    pre_tokens = sum(int(b.size) for b, _ in ds.stream_tokens("byte-level-v1", 4096))
    print(f"Streamed {pre_tokens} tokens before exclusion")

    dw = DatasetWriter(workdir)
    for shard_path in ds.shard_paths():
        dw.register_shard(os.path.splitext(os.path.basename(shard_path))[0])
    dw.add_exclusion(marker_hash, reason="GDPR Article 17 request (demo)")
    dw.close()
    print("Recorded exclusion in dataset overlay")

    ds2 = Dataset(workdir)
    proof = ds2.prove_non_inclusion(marker_hash)
    print(f"Non-inclusion proof has {len(proof['shards'])} shard entries")
    print(f"Verifies: {ds2.verify_non_inclusion_proof(proof)}")

    streamed_bytes = bytearray()
    for batch, _ in ds2.stream_tokens("byte-level-v1", 4096):
        streamed_bytes.extend(batch.astype('uint8').tobytes())
    if marker in streamed_bytes:
        print("FAIL — marker still present after exclusion")
        return 1
    print("OK — marker absent from re-streamed corpus")
    return 0


if __name__ == "__main__":
    sys.exit(main())
