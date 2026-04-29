"""Smoke test for the HF BPE tokenizer recipe.

Skipped cleanly when ``tokenizers`` isn't installed. When it is, runs
the recipe end-to-end as a subprocess so any regression in the public
API path it exercises (Writer, Reader, HfTokenizer, verify_tokenizer_view)
shows up in CI rather than at recipe time.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest


_REPO = Path(__file__).resolve().parents[2]


def test_hf_bpe_recipe_runs_end_to_end():
    pytest.importorskip("tokenizers")
    env = os.environ.copy()
    env["TSET_PREFER_RUST"] = "0"
    result = subprocess.run(
        [sys.executable, "-m", "examples.recipes.hf_tokenizer_bpe"],
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
    # Each verified milestone the recipe prints
    assert "wrapped tokenizer" in out
    assert "wrote shard" in out
    assert "documents tokenize identically on read" in out
    assert "re-tokenization is byte-identical" in out
    assert "tokenizer JSON round-trips" in out
