"""HfTokenizer adapter — verifies the lazy-import error and the
adapter shape against a stub HF-tokenizers-compatible object.

Skips the round-trip test if `tokenizers` is not installed (the case
on the CI env that doesn't pull HF in)."""

import numpy as np
import pytest

from tset.hf_tokenizer import HfTokenizer


class _StubHf:
    """Minimal stub that satisfies the HfTokenizer adapter's contract."""

    def __init__(self, vocab_size=256):
        self._vocab = vocab_size

    def get_vocab_size(self):
        return self._vocab

    class _Enc:
        def __init__(self, ids):
            self.ids = ids

    def encode(self, text, add_special_tokens=False):
        # Trivial: byte-level encode
        return self._Enc(list(text.encode("utf-8")))

    def decode(self, ids, skip_special_tokens=False):
        return bytes(ids).decode("utf-8", errors="replace")

    def to_str(self):
        return f'{{"vocab_size": {self._vocab}}}'


def test_hf_tokenizer_requires_real_package_when_imported():
    """The adapter eagerly checks for `tokenizers` on construction."""
    import sys
    real = sys.modules.copy()
    sys.modules["tokenizers"] = None  # type: ignore[assignment]
    try:
        with pytest.raises(RuntimeError, match=r"`tokenizers`"):
            HfTokenizer(_StubHf(), tokenizer_id="x")
    finally:
        sys.modules.clear()
        sys.modules.update(real)


def test_hf_tokenizer_round_trip_against_stub():
    pytest.importorskip("tokenizers")
    t = HfTokenizer(_StubHf(), tokenizer_id="stub-byte", vocab_size=256)
    assert t.tokenizer_id == "stub-byte"
    assert t.vocab_size == 256
    ids = t.encode(b"hello")
    assert ids.dtype == np.uint32
    assert list(ids) == [104, 101, 108, 108, 111]
    assert t.decode(ids) == b"hello"


def test_hf_tokenizer_config_includes_state_digest():
    pytest.importorskip("tokenizers")
    t = HfTokenizer(_StubHf(), tokenizer_id="stub", vocab_size=256)
    cfg = t.config()
    assert cfg["id"] == "stub"
    assert cfg["kind"] == "hf-tokenizers"
    # State digest is stable for the same stub
    t2 = HfTokenizer(_StubHf(), tokenizer_id="stub", vocab_size=256)
    assert cfg["hf_state_digest"] == t2.config()["hf_state_digest"]
