"""End-to-end recipe: OpenAI tiktoken → TSET.

Shows the full path for a GPT-style stack: a pretrained ``tiktoken``
encoding drives ``add_tokenizer_view``, the resulting shard verifies
reproducibility, and the tokens come back identical when read.

Run::

    pip install tiktoken
    python -m examples.recipes.tiktoken_recipe

In production with network access::

    import tiktoken
    enc = tiktoken.get_encoding("cl100k_base")  # or "o200k_base"
    wrapped = TiktokenTokenizer(enc, tokenizer_id="cl100k_base")
    # …rest is identical to the offline path below.

Two encodings are popular as of 2026:

  * ``cl100k_base`` — GPT-3.5 / GPT-4 era (100k vocab)
  * ``o200k_base``  — GPT-4o / o-series (200k vocab)

This recipe uses a tiny offline-constructed ``Encoding`` so it runs in
sandboxed CI environments without network. The integrity contract
(re-tokenization is byte-identical, reproducibility proof verifies)
is exactly the same — only the BPE table is smaller.

Custom tokenizer IDs are not in the global tokenizer registry, so the
``tset_rs`` Rust reader (which enforces the registry) refuses to stream
their views. We force the Python streaming path with
``TSET_PREFER_RUST=0`` here; the same workaround applies to any
tiktoken / SentencePiece / HF tokenizer.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path

os.environ.setdefault("TSET_PREFER_RUST", "0")

_REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO))
sys.path.insert(0, str(_REPO / "python"))


def main() -> int:
    try:
        import tiktoken
    except ImportError as e:
        sys.exit(f"this recipe needs `tiktoken`: pip install tiktoken ({e})")

    from tset.reader import Reader
    from tset.tiktoken_tokenizer import TiktokenTokenizer
    from tset.writer import Writer

    docs = [
        b"the quick brown fox jumps over the lazy dog",
        b"how vexingly quick daft zebras jump",
        b"sphinx of black quartz judge my vow",
        b"pack my box with five dozen liquor jugs",
        b"the five boxing wizards jump quickly",
    ]

    # Offline-constructed BPE table over the byte alphabet. tiktoken
    # accepts any deterministic mergeable_ranks dict; we use the 256
    # raw-byte tokens (ranks 0..255) so the encoding is fully defined
    # without a network download.
    mergeable_ranks = {bytes([i]): i for i in range(256)}
    enc = tiktoken.Encoding(
        name="byte-bpe-offline",
        pat_str=r"""[^\s]+|\s+""",
        mergeable_ranks=mergeable_ranks,
        special_tokens={},
    )
    wrapped = TiktokenTokenizer(enc, tokenizer_id="byte-bpe-offline")
    cfg = wrapped.config()
    print(
        f"wrapped tokenizer  id={cfg['id']}  vocab={cfg['vocab_size']:,}  "
        f"kind={cfg['kind']}  ranks_digest={cfg['tiktoken_ranks_digest'][:16]}…"
    )

    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "tiktoken-corpus.tset"
        with Writer(str(out)) as w:
            for d in docs:
                w.add_document(d, metadata={"len": len(d)})
            w.add_tokenizer_view(wrapped)
        print(f"wrote shard       {out.name}  ({out.stat().st_size:,} bytes)")

        r = Reader(str(out))
        for doc_hash, content in r.documents():
            expected_ids = list(wrapped.encode(content))
            recovered: list[int] = []
            for batch, dh in r.stream_tokens(cfg["id"], batch_size=2_000_000):
                if dh == doc_hash:
                    recovered.extend(int(x) for x in batch)
                    break
            if recovered != expected_ids:
                sys.exit(
                    f"mismatch on doc {doc_hash.hex()[:12]}: "
                    f"{len(expected_ids)} vs {len(recovered)} tokens"
                )
        print(f"verified          {len(docs)} documents tokenize identically on read")

        r.verify_tokenizer_view(cfg["id"], tokenizer=wrapped)
        print("verified          re-tokenization is byte-identical")

        view_cfg = r.manifest["tokenization_views"][cfg["id"]]
        print()
        print("manifest view config (the receipts pin):")
        print(json.dumps(view_cfg.get("tokenizer_config"), indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
