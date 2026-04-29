"""End-to-end recipe: BPE tokenizer (HuggingFace ``tokenizers``) → TSET.

Shows the full path real ML practitioners take with TSET: a pretrained
BPE tokenizer drives ``add_tokenizer_view``, the resulting shard
verifies reproducibility, and the tokens come back identical when read.

Two paths are demonstrated:

  1. Train a tiny BPE on a tiny corpus (fully offline, deterministic;
     this is the path the test exercises).
  2. Load a pretrained BPE from disk via ``tokenizers.Tokenizer.from_file``.
     Use this in production with the model your inference stack uses.

If you have ``Tokenizer.from_pretrained`` (HF Hub) available::

    from tokenizers import Tokenizer
    hf = Tokenizer.from_pretrained("Qwen/Qwen2.5-0.5B")
    wrapped = HfTokenizer(hf, tokenizer_id="qwen2-5-0-5b")
    # then identical to the offline path below

Custom tokenizer IDs are not in the global tokenizer registry, so the
``tset_rs`` Rust reader (which enforces the registry) refuses to stream
their views. We force the Python streaming path with
``TSET_PREFER_RUST=0`` in this recipe; the same workaround applies to
any HF / SentencePiece / user-defined tokenizer until the Rust side
gains the same registry-tolerant fallback the Python side has.

Run::

    pip install tokenizers
    python -m examples.recipes.hf_tokenizer_bpe
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path

# Custom tokenizer ID → Python streaming path (see module docstring).
os.environ.setdefault("TSET_PREFER_RUST", "0")


_REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO))
sys.path.insert(0, str(_REPO / "python"))


def main() -> int:
    # ── Imports gated on optional deps so the recipe's docstring is
    #    readable without anything installed.
    try:
        from tokenizers import Tokenizer
        from tokenizers.models import BPE
        from tokenizers.pre_tokenizers import Whitespace
        from tokenizers.trainers import BpeTrainer
    except ImportError as e:
        sys.exit(f"this recipe needs `tokenizers`: pip install tokenizers ({e})")

    from tset.hf_tokenizer import HfTokenizer
    from tset.reader import Reader
    from tset.writer import Writer

    # ── 1. Train a tiny BPE on a deterministic corpus.
    docs = [
        b"the quick brown fox jumps over the lazy dog",
        b"how vexingly quick daft zebras jump",
        b"sphinx of black quartz judge my vow",
        b"pack my box with five dozen liquor jugs",
        b"the five boxing wizards jump quickly",
    ]
    hf = Tokenizer(BPE(unk_token="[UNK]"))
    hf.pre_tokenizer = Whitespace()
    trainer = BpeTrainer(
        vocab_size=200,
        special_tokens=["[UNK]", "[BOS]", "[EOS]"],
        show_progress=False,
    )
    hf.train_from_iterator((d.decode("utf-8") for d in docs), trainer=trainer)

    # ── 2. Wrap as a TSET tokenizer view.
    wrapped = HfTokenizer(hf, tokenizer_id="bpe-pangrams-v1")
    cfg = wrapped.config()
    print(f"wrapped tokenizer  id={cfg['id']}  vocab={cfg['vocab_size']}  "
          f"kind={cfg['kind']}  state_digest={cfg['hf_state_digest'][:16]}…")

    # ── 3. Write a TSET shard with this view.
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "bpe-corpus.tset"
        with Writer(str(out)) as w:
            for d in docs:
                w.add_document(d, metadata={"len": len(d)})
            w.add_tokenizer_view(wrapped)
        print(f"wrote shard       {out.name}  ({out.stat().st_size:,} bytes)")

        # ── 4. Read tokens back; verify they match the tokenizer's
        #     own encode() call. This is the integrity contract.
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

        # ── 5. Reproducibility: ``verify_tokenizer_view`` re-encodes
        #     every doc via the wrapped tokenizer and asserts the
        #     bit-packed chunks on disk are byte-identical.
        r.verify_tokenizer_view(cfg["id"], tokenizer=wrapped)
        print("verified          re-tokenization is byte-identical")

        # ── 6. Persist the trained tokenizer alongside the shard.
        #     The HF state digest in the manifest pins which tokenizer
        #     produced the view; saving the JSON makes that pin
        #     externally re-loadable.
        tok_path = Path(tmp) / "bpe.tokenizer.json"
        tok_path.write_text(hf.to_str())
        # And reload to prove the round-trip works.
        reloaded = Tokenizer.from_file(str(tok_path))
        wrapped2 = HfTokenizer(reloaded, tokenizer_id=cfg["id"])
        cfg2 = wrapped2.config()
        if cfg["hf_state_digest"] != cfg2["hf_state_digest"]:
            sys.exit("hf_state_digest changed across save/load — broken")
        print(f"verified          tokenizer JSON round-trips with the same state digest")

        # Print the manifest's view config so users can see how their
        # tokenizer's identity is recorded.
        view_cfg = r.manifest["tokenization_views"][cfg["id"]]
        print()
        print("manifest view config (for the receipts pin):")
        print(json.dumps(view_cfg.get("tokenizer_config"), indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
