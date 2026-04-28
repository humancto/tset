"""Click 8.1.7 source code → TSET smoke tests.

A condensed test for the second-dataset case: large docs, code corpus,
TSET ends up smaller than JSONL. Skips on network unavailability.
"""

from __future__ import annotations

import urllib.error

import pytest

from tset.reader import Reader
from tset.tokenizers import ByteLevelTokenizer


@pytest.fixture(scope="module")
def click_corpus(tmp_path_factory):
    from examples.datasets.click_source import convert as conv
    from examples.datasets.click_source import download as dl

    try:
        dl.fetch_corpus()
    except (urllib.error.URLError, OSError) as e:
        pytest.skip(f"network unavailable: {e}")

    work = tmp_path_factory.mktemp("click_source")
    jsonl = work / "corpus.jsonl"
    tset = work / "corpus.tset"
    n = conv.write_jsonl(jsonl)
    conv.convert_to_tset(jsonl, tset)
    return {"jsonl": jsonl, "tset": tset, "n": n}


def test_at_least_50_python_files(click_corpus):
    """Click 8.1.7 has 71 .py files; assert a generous lower bound."""
    assert click_corpus["n"] >= 50


def test_tset_smaller_than_jsonl_on_code(click_corpus):
    """The "large docs ⇒ TSET smaller than JSONL" claim from SCALING.md."""
    tset_size = click_corpus["tset"].stat().st_size
    jsonl_size = click_corpus["jsonl"].stat().st_size
    ratio = tset_size / jsonl_size
    assert ratio < 1.0, (
        f"on Click source TSET should be < JSONL, got ratio={ratio:.2f}"
    )


def test_byte_level_view_round_trips(click_corpus):
    """A handful of files: tokens via the TSET view match direct
    tokenize(). Same shape assertion as the Shakespeare suite."""
    r = Reader(str(click_corpus["tset"]))
    tok = ByteLevelTokenizer()
    for doc_hash, content in list(r.documents())[:10]:
        expected = list(tok.encode(content))
        actual: list[int] = []
        for batch, dh in r.stream_tokens("byte-level-v1", batch_size=2_000_000):
            if dh == doc_hash:
                actual.extend(int(x) for x in batch)
                break
        assert actual == expected, f"mismatch on {doc_hash.hex()[:12]}"


def test_inclusion_and_non_inclusion_proofs(click_corpus):
    r = Reader(str(click_corpus["tset"]))
    h, _ = next(iter(r.documents()))
    assert r.prove_inclusion(h).verify(r.smt_root())
    assert r.prove_non_inclusion(b"\xff" * 32).verify(r.smt_root())
