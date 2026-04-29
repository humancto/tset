"""Smoke tests for the five cookbook recipes.

Each runs the recipe as a subprocess and checks for the milestone
strings the recipe prints. The recipes are deliberately deterministic
so a regression in any public API path they exercise (Writer, Reader,
Dataset, DatasetWriter, metadata columns, exclusion overlay,
stream_tokens) shows up here rather than at recipe time.

The Parquet recipe is `pytest.importorskip`'d on pyarrow so the suite
still passes on a minimal install.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest


_REPO = Path(__file__).resolve().parents[2]


def _run(module: str, timeout: int = 60) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env["TSET_PREFER_RUST"] = "0"
    return subprocess.run(
        [sys.executable, "-m", module],
        cwd=_REPO,
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=timeout,
    )


def test_parquet_to_tset_recipe():
    pytest.importorskip("pyarrow")
    r = _run("examples.cookbook.parquet_to_tset")
    assert r.returncode == 0, r.stderr
    assert "wrote shard" in r.stdout
    assert "metadata columns" in r.stdout
    assert "first row lang" in r.stdout


def test_deletion_end_to_end_recipe():
    r = _run("examples.cookbook.deletion_end_to_end")
    assert r.returncode == 0, r.stderr
    out = r.stdout
    assert "step 1" in out and "corpus published" in out
    assert "step 2" in out and "inclusion confirmed" in out
    assert "step 4" in out and "new root published" in out
    assert "step 5" in out and "overlay_includes=True" in out
    assert "step 6" in out and "reason='GDPR Art. 17" in out


def test_verify_offline_recipe():
    r = _run("examples.cookbook.verify_offline")
    assert r.returncode == 0, r.stderr
    out = r.stdout
    assert "shard_merkle_root" in out
    assert "smt_root" in out
    assert "manifest_hash" in out
    assert "verifies=True" in out
    assert "All receipts verified offline" in out


def test_multi_shard_streaming_recipe():
    r = _run("examples.cookbook.multi_shard_streaming")
    assert r.returncode == 0, r.stderr
    out = r.stdout
    assert "shard 0" in out and "shard 1" in out and "shard 2" in out
    assert "verified" in out
    assert "excluded document was dropped" in out


def test_training_loop_recipe():
    r = _run("examples.cookbook.training_loop")
    assert r.returncode == 0, r.stderr
    out = r.stdout
    assert "batch shape" in out
    assert "inputs=(4, 128)" in out
    assert "masks=(4, 128)" in out
    assert "Plug into PyTorch" in out
