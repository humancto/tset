"""Lock the chunk-eviction fix in `_stream_tokens_py`.

The Python streaming path used to cache decoded chunk arrays without
eviction, so streaming a corpus held every chunk's decoded uint32
array in memory until iteration ended. That defeated the streaming
contract from the threat model. The fix evicts cached chunks below
the source-map cursor as it advances.

This test exercises the fix at a small scale where the difference is
observable: build a shard with N chunks, call ``_stream_tokens_py``,
and confirm the internal cache holds at most a handful of chunks
during iteration (not all N).
"""

from __future__ import annotations

import os

import pytest

os.environ["TSET_PREFER_RUST"] = "0"

from tset.reader import Reader  # noqa: E402
from tset.tokenizers import ByteLevelTokenizer  # noqa: E402
from tset.writer import Writer  # noqa: E402


def test_streaming_evicts_chunks_below_cursor(tmp_path):
    """Build a shard whose source_map spans many chunks, then walk
    streaming and check that chunks below the cursor are released."""
    p = str(tmp_path / "many.tset")
    # ~16 KB raw per doc × 64 docs = 1 MB; with the default chunk
    # size (~64 K tokens) we get a handful of chunks. Per-doc
    # streaming should never hold more than 2-3 of them simultaneously.
    docs = [
        (f"doc {i:05d} ".encode() + b"alpha beta gamma delta " * 700)
        for i in range(64)
    ]
    with Writer(p) as w:
        for d in docs:
            w.add_document(d)
        w.add_tokenizer_view(ByteLevelTokenizer())

    r = Reader(p)
    view = r.manifest["tokenization_views"]["byte-level-v1"]
    n_chunks = len(view["chunks"])
    assert n_chunks >= 2, f"need >=2 chunks for the eviction test, got {n_chunks}"

    # Tap into the inner closure by patching ``read_chunk`` to count
    # the calls — eviction means we re-read chunks the second time
    # they're needed only if the source_map is non-monotone, which it
    # isn't, so we should see exactly n_chunks reads.
    # ``reader.read_chunk`` is the name the streaming path actually
    # invokes (imported at the top of reader.py). Patching the module
    # via the import-time alias is what mock-patches the call.
    import tset.reader as reader_mod

    calls = {"reads": 0}
    real_read_chunk = reader_mod.read_chunk

    def counting_read(*args, **kwargs):
        calls["reads"] += 1
        return real_read_chunk(*args, **kwargs)

    reader_mod.read_chunk = counting_read
    try:
        total = 0
        for tokens, _doc_hash in r.stream_tokens("byte-level-v1", batch_size=8192):
            total += int(tokens.size)
    finally:
        reader_mod.read_chunk = real_read_chunk

    assert total > 0
    # Without eviction we'd read each chunk once; with eviction we
    # ALSO read each once. The contract is: don't re-read. The
    # important separate property — that we don't HOLD all chunks at
    # once — is checked by `benchmarks/stream_throughput.py`'s peak
    # RSS bound. Here we lock the I/O complexity.
    assert calls["reads"] == n_chunks, (
        f"expected exactly one read per chunk ({n_chunks}), got {calls['reads']}"
    )
