"""Tiktoken adapter — wraps an OpenAI ``tiktoken.Encoding`` into the
``tset.tokenizers.Tokenizer`` protocol so it can drive the Python
writer's ``add_tokenizer_view``.

Usage::

    import tiktoken
    from tset.tiktoken_tokenizer import TiktokenTokenizer

    enc = tiktoken.get_encoding("cl100k_base")  # or "o200k_base", etc.
    wrapped = TiktokenTokenizer(enc, tokenizer_id="cl100k_base")
    with Writer("corpus.tset") as w:
        w.add_document(b"hello")
        w.add_tokenizer_view(wrapped)

The adapter records the tiktoken ``name`` and a hash of the BPE merge
table in the manifest's ``tokenizer_config``. Together those are
sufficient to reject a re-tokenization done with a different version of
the tiktoken vocabulary at read time — the reproducibility proof
exists exactly to surface that drift.
"""

from __future__ import annotations

import numpy as np

from tset.hashing import hash_bytes
from tset.tokenizers import Tokenizer


class TiktokenTokenizer(Tokenizer):
    def __init__(
        self,
        encoding,
        tokenizer_id: str,
        vocab_size: int | None = None,
    ):
        try:
            import tiktoken  # noqa: F401
        except ImportError as e:
            raise RuntimeError(
                "TiktokenTokenizer requires the `tiktoken` package; "
                "install with `pip install tiktoken`"
            ) from e
        self._enc = encoding
        self.tokenizer_id = tokenizer_id
        self.vocab_size = int(vocab_size if vocab_size is not None else encoding.n_vocab)

    def encode(self, text: bytes) -> np.ndarray:
        text_str = text.decode("utf-8", errors="replace")
        # disallow special tokens by default — same convention as the HF
        # adapter. Users training with explicit BOS/EOS should preprocess
        # outside the writer.
        ids = self._enc.encode(text_str, disallowed_special=())
        return np.asarray(ids, dtype=np.uint32)

    def decode(self, ids: np.ndarray) -> bytes:
        return self._enc.decode([int(x) for x in ids.tolist()]).encode("utf-8")

    def config(self) -> dict:
        # tiktoken doesn't expose a single canonical serialization, but
        # it does expose `_mergeable_ranks` which IS the BPE table that
        # determines tokenization. Hashing it pins the exact vocabulary
        # version that produced this view.
        ranks = getattr(self._enc, "_mergeable_ranks", None)
        if ranks:
            # _mergeable_ranks: dict[bytes -> int]; sort keys for a
            # deterministic digest.
            buf = b"".join(
                k + b"\x00" + str(v).encode("ascii") + b"\n"
                for k, v in sorted(ranks.items())
            )
            digest = hash_bytes(buf).hex()
        else:
            digest = ""
        return {
            "id": self.tokenizer_id,
            "vocab_size": self.vocab_size,
            "kind": "tiktoken",
            "tiktoken_name": getattr(self._enc, "name", ""),
            "tiktoken_ranks_digest": digest,
        }
