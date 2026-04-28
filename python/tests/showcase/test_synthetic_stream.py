"""Synthetic stream smoke tests.

The synthetic generator backs the scaling story in ``SCALING.md``. We
test it offline (no network needed): generate a small corpus, convert,
verify ratios fall in the band the doc claims.
"""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture(scope="module")
def syn_corpus(tmp_path_factory):
    from examples.datasets.synthetic_stream.generate import generate

    work = tmp_path_factory.mktemp("syn")
    jsonl = work / "syn-1m.jsonl"
    info = generate(1_000_000, jsonl, seed=0xC0FFEE)
    return {"work": work, "jsonl": jsonl, "info": info}


def test_generator_is_deterministic(tmp_path):
    """Same seed must produce byte-identical output."""
    from examples.datasets.synthetic_stream.generate import generate

    a = tmp_path / "a.jsonl"
    b = tmp_path / "b.jsonl"
    info_a = generate(200_000, a, seed=12345)
    info_b = generate(200_000, b, seed=12345)
    assert info_a["blake2b"] == info_b["blake2b"]
    assert a.read_bytes() == b.read_bytes()


def test_generator_respects_target_size(syn_corpus):
    """Output size lands within ±5% of target."""
    target = 1_000_000
    actual = syn_corpus["jsonl"].stat().st_size
    assert 0.95 * target <= actual <= 1.05 * target


def test_lean_tset_is_under_2x_jsonl_at_1mb(syn_corpus):
    """SCALING.md claims ~1.57× JSONL on synthetic at 1 MB. Assert
    a generous band so the test isn't fragile to writer changes."""
    from examples.datasets.synthetic_stream.scale_bench import _convert

    tset_path = syn_corpus["work"] / "syn-1m.tset"
    info = _convert(syn_corpus["jsonl"], tset_path, two_views=False, binary_sections=False)
    ratio = info["size"] / syn_corpus["jsonl"].stat().st_size
    assert 1.2 < ratio < 2.0, (
        f"synthetic 1MB TSET/JSONL ratio {ratio:.2f} outside band [1.2, 2.0]"
    )
