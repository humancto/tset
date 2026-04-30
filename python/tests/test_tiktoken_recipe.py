"""Smoke test for the tiktoken recipe.

Skipped cleanly when ``tiktoken`` isn't installed. When it is, runs
the recipe end-to-end as a subprocess so any regression in the public
API path it exercises (Writer, Reader, TiktokenTokenizer,
verify_tokenizer_view) shows up in CI rather than at recipe time.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest


_REPO = Path(__file__).resolve().parents[2]


def test_tiktoken_recipe_runs_end_to_end():
    pytest.importorskip("tiktoken")
    env = os.environ.copy()
    env["TSET_PREFER_RUST"] = "0"
    result = subprocess.run(
        [sys.executable, "-m", "examples.recipes.tiktoken_recipe"],
        cwd=_REPO,
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
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
    assert "kind=tiktoken" in out


def test_tiktoken_adapter_round_trip(tmp_path):
    """Direct (non-subprocess) check that the adapter is correctly
    plumbed: encode → write → read → re-encode must be byte-identical."""
    pytest.importorskip("tiktoken")
    sys.path.insert(0, str(_REPO / "python"))
    os.environ["TSET_PREFER_RUST"] = "0"
    import tiktoken

    from tset.reader import Reader
    from tset.tiktoken_tokenizer import TiktokenTokenizer
    from tset.writer import Writer

    mergeable_ranks = {bytes([i]): i for i in range(256)}
    enc = tiktoken.Encoding(
        name="byte-bpe-test",
        pat_str=r"""[^\s]+|\s+""",
        mergeable_ranks=mergeable_ranks,
        special_tokens={},
    )
    wrapped = TiktokenTokenizer(enc, tokenizer_id="tiktoken-test")
    cfg = wrapped.config()
    assert cfg["kind"] == "tiktoken"
    # ranks_digest pins the BPE table — must be deterministic.
    assert len(cfg["tiktoken_ranks_digest"]) == 64

    out = tmp_path / "tt.tset"
    with Writer(str(out)) as w:
        w.add_document(b"hello world")
        w.add_document(b"goodbye world")
        w.add_tokenizer_view(wrapped)

    r = Reader(str(out))
    r.verify_tokenizer_view("tiktoken-test", tokenizer=wrapped)
    for doc_hash, content in r.documents():
        expected = list(wrapped.encode(content))
        recovered: list[int] = []
        for batch, dh in r.stream_tokens("tiktoken-test", batch_size=2_000_000):
            if dh == doc_hash:
                recovered.extend(int(x) for x in batch)
                break
        assert recovered == expected
