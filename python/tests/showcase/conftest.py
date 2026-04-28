"""Pytest plumbing for the showcase suite.

These tests exercise the example pipelines in ``examples/datasets/`` end
to end against real public corpora. Each module-level fixture either
returns a populated cache + converted shard, or skips the entire module
if the source cannot be reached (sandbox without network, etc.).

Crucially we do NOT skip on integrity failures — those make tests fail.
The skip is reserved for "infrastructure not available", not "the
format is broken".
"""

from __future__ import annotations

import os
import sys
import urllib.error

import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO = os.path.normpath(os.path.join(_HERE, "..", "..", ".."))
if _REPO not in sys.path:
    sys.path.insert(0, _REPO)
_PYTHON = os.path.join(_REPO, "python")
if _PYTHON not in sys.path:
    sys.path.insert(0, _PYTHON)


def _try_or_skip_network(callable_):
    """Run ``callable_`` and skip the test only on transport-level failures.

    A 404/403 from the upstream (HTTPError) is a real regression — the
    dataset URL or its ETag has moved — so we let it propagate as a
    test failure. We only skip when the network itself is unreachable:
    DNS lookup failure, connection refused/reset, timeout, sandbox host
    block, etc. ``urllib.error.HTTPError`` inherits from URLError and
    must be checked first because of that.

    Hash mismatches in ``cache.fetch`` raise ValueError and propagate
    unconditionally — those are real integrity failures.
    """
    try:
        return callable_()
    except urllib.error.HTTPError:
        # Real HTTP-status failure: 404/403/5xx. Don't mask it.
        raise
    except urllib.error.URLError as e:
        pytest.skip(f"network unavailable for showcase fetch: {e}")
    except OSError as e:
        if "Network is unreachable" in str(e) or "host" in str(e).lower():
            pytest.skip(f"network unavailable: {e}")
        raise


@pytest.fixture(scope="session")
def shakespeare_corpus(tmp_path_factory):
    """Returns the converted TinyShakespeare TSET path + JSONL path.

    Module-scoped so we only run convert.py once per pytest session.
    """
    from examples.datasets.tinyshakespeare import convert as conv
    from examples.datasets.tinyshakespeare import download as dl

    src = _try_or_skip_network(dl.fetch_corpus)

    work = tmp_path_factory.mktemp("shakespeare")
    jsonl = work / "corpus.jsonl"
    tset = work / "corpus.tset"

    text = src.read_text(encoding="utf-8")
    paragraphs = conv.split_paragraphs(text)
    n = conv.write_jsonl(paragraphs, jsonl)
    assert n > 0
    conv.convert_to_tset(jsonl, tset)
    return {
        "src": src,
        "jsonl": jsonl,
        "tset": tset,
        "paragraphs": paragraphs,
        "n_paragraphs": len(paragraphs),
    }
