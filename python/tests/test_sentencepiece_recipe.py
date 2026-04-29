"""Smoke test for the SentencePiece recipe.

Skipped cleanly when ``sentencepiece`` isn't installed.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest


_REPO = Path(__file__).resolve().parents[2]


def test_sentencepiece_recipe_runs_end_to_end():
    pytest.importorskip("sentencepiece")
    env = os.environ.copy()
    env["TSET_PREFER_RUST"] = "0"
    result = subprocess.run(
        [sys.executable, "-m", "examples.recipes.sentencepiece_recipe"],
        cwd=_REPO,
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    assert result.returncode == 0, (
        f"recipe failed:\n--- stdout ---\n{result.stdout}\n"
        f"--- stderr ---\n{result.stderr}"
    )
    out = result.stdout
    assert "wrapped tokenizer" in out
    assert "wrote shard" in out
    assert "documents tokenize identically on read" in out
    assert "re-tokenization is byte-identical" in out
    assert "different .spm model yields a different digest" in out


def test_sentencepiece_adapter_round_trip(tmp_path):
    pytest.importorskip("sentencepiece")
    sys.path.insert(0, str(_REPO / "python"))
    os.environ["TSET_PREFER_RUST"] = "0"
    import sentencepiece as spm

    from tset.reader import Reader
    from tset.sentencepiece_tokenizer import SentencePieceTokenizer
    from tset.writer import Writer

    corpus = tmp_path / "corpus.txt"
    corpus.write_text(
        "the quick brown fox\nhow now brown cow\nthe rain in spain\n"
    )
    spm.SentencePieceTrainer.Train(
        input=str(corpus),
        model_prefix=str(tmp_path / "spm"),
        vocab_size=48,
        model_type="bpe",
        character_coverage=1.0,
        bos_id=-1,
        eos_id=-1,
        pad_id=-1,
        unk_id=0,
    )
    sp = spm.SentencePieceProcessor()
    sp.Load(str(tmp_path / "spm.model"))
    wrapped = SentencePieceTokenizer(sp, tokenizer_id="spm-test")
    cfg = wrapped.config()
    assert cfg["kind"] == "sentencepiece"
    assert len(cfg["spm_proto_digest"]) == 64

    out = tmp_path / "spm.tset"
    with Writer(str(out)) as w:
        w.add_document(b"the quick brown fox")
        w.add_document(b"how now brown cow")
        w.add_tokenizer_view(wrapped)

    r = Reader(str(out))
    r.verify_tokenizer_view("spm-test", tokenizer=wrapped)
    for doc_hash, content in r.documents():
        expected = list(wrapped.encode(content))
        recovered: list[int] = []
        for batch, dh in r.stream_tokens("spm-test", batch_size=2_000_000):
            if dh == doc_hash:
                recovered.extend(int(x) for x in batch)
                break
        assert recovered == expected
