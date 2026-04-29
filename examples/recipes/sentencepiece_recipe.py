"""End-to-end recipe: SentencePiece → TSET.

The default tokenizer for the Llama / Mistral / Gemma family of models.
This recipe trains a tiny SentencePiece BPE on a deterministic in-memory
corpus, drives ``add_tokenizer_view``, then verifies that the tokens
come back byte-identical when the shard is read.

Run::

    pip install sentencepiece
    python -m examples.recipes.sentencepiece_recipe

For production, swap the trainer block for a load of an existing model::

    sp = sentencepiece.SentencePieceProcessor()
    sp.Load("/path/to/llama-tokenizer.spm")
    wrapped = SentencePieceTokenizer(sp, tokenizer_id="llama3-tokenizer-v1")
    # …rest is identical
"""

from __future__ import annotations

import io
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
        import sentencepiece as spm
    except ImportError as e:
        sys.exit(
            f"this recipe needs `sentencepiece`: pip install sentencepiece ({e})"
        )

    from tset.reader import Reader
    from tset.sentencepiece_tokenizer import SentencePieceTokenizer
    from tset.writer import Writer

    # ── 1. Train a tiny SentencePiece BPE in memory.
    docs = [
        "the quick brown fox jumps over the lazy dog",
        "how vexingly quick daft zebras jump",
        "sphinx of black quartz judge my vow",
        "pack my box with five dozen liquor jugs",
        "the five boxing wizards jump quickly",
    ]
    with tempfile.TemporaryDirectory() as tmp:
        corpus = Path(tmp) / "corpus.txt"
        corpus.write_text("\n".join(docs))
        model_prefix = Path(tmp) / "spm"
        spm.SentencePieceTrainer.Train(
            input=str(corpus),
            model_prefix=str(model_prefix),
            vocab_size=64,  # tiny but enough for the toy corpus
            model_type="bpe",
            character_coverage=1.0,
            bos_id=-1,
            eos_id=-1,
            pad_id=-1,
            unk_id=0,
        )
        sp = spm.SentencePieceProcessor()
        sp.Load(str(model_prefix.with_suffix(".model")))

        wrapped = SentencePieceTokenizer(sp, tokenizer_id="spm-pangrams-v1")
        cfg = wrapped.config()
        print(
            f"wrapped tokenizer  id={cfg['id']}  vocab={cfg['vocab_size']}  "
            f"kind={cfg['kind']}  proto_digest={cfg['spm_proto_digest'][:16]}…"
        )

        # ── 2. Write a TSET shard with this view.
        out = Path(tmp) / "spm-corpus.tset"
        with Writer(str(out)) as w:
            for d in docs:
                w.add_document(d.encode(), metadata={"len": len(d)})
            w.add_tokenizer_view(wrapped)
        print(f"wrote shard       {out.name}  ({out.stat().st_size:,} bytes)")

        # ── 3. Read tokens back; verify they match the tokenizer's
        #     own encode() call.
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

        # ── 4. Reproducibility: re-encode every doc via the wrapped
        #     tokenizer and assert the bit-packed chunks on disk are
        #     byte-identical.
        r.verify_tokenizer_view(cfg["id"], tokenizer=wrapped)
        print("verified          re-tokenization is byte-identical")

        # ── 5. Demonstrate that the proto digest pins the model
        #     identity: load a *different* SentencePiece model and
        #     confirm the digest changes.
        model2_prefix = Path(tmp) / "spm2"
        # train with a different vocab_size → different proto bytes
        spm.SentencePieceTrainer.Train(
            input=str(corpus),
            model_prefix=str(model2_prefix),
            vocab_size=48,
            model_type="bpe",
            character_coverage=1.0,
            bos_id=-1,
            eos_id=-1,
            pad_id=-1,
            unk_id=0,
        )
        sp2 = spm.SentencePieceProcessor()
        sp2.Load(str(model2_prefix.with_suffix(".model")))
        cfg2 = SentencePieceTokenizer(sp2, tokenizer_id=cfg["id"]).config()
        if cfg["spm_proto_digest"] == cfg2["spm_proto_digest"]:
            sys.exit("BUG: different SentencePiece models produced the same digest")
        print("verified          a different .spm model yields a different digest")

        # Print the manifest's view config so users can see how their
        # tokenizer's identity is recorded.
        view_cfg = r.manifest["tokenization_views"][cfg["id"]]
        print()
        print("manifest view config (the receipts pin):")
        print(json.dumps(view_cfg.get("tokenizer_config"), indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
