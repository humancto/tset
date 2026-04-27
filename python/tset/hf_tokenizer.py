"""HuggingFace `tokenizers` adapter — wraps a pretrained `Tokenizer`
into the `tset.tokenizers.Tokenizer` protocol so it can drive the Python
writer's `add_tokenizer_view`.

Lazy import of the `tokenizers` package — clear runtime error with an
install hint if missing.

Why a wrapper rather than first-class registry support: HF tokenizers
ship enormous vocabularies (Llama 3 = 128k, Qwen = 152k) and per-call
state. Marshalling them across the PyO3 boundary into the Rust core
isn't viable today; the wrapper lets users tokenize via Python on the
Python writer path while the Rust writer handles the byte-level and
whitespace cases natively.
"""

from __future__ import annotations

import json

import numpy as np

from tset.hashing import hash_bytes
from tset.tokenizers import Tokenizer


class HfTokenizer(Tokenizer):
    """Adapter around a `tokenizers.Tokenizer` instance.

    Construction:
        from tokenizers import Tokenizer as HfBase
        hf = HfBase.from_pretrained("Qwen/Qwen2.5-0.5B")
        wrapped = HfTokenizer(hf, tokenizer_id="qwen2-5-0-5b")
        with Writer("corpus.tset") as w:
            w.add_document(b"hello")
            w.add_tokenizer_view(wrapped)
    """

    def __init__(
        self,
        hf_tokenizer,
        tokenizer_id: str,
        vocab_size: int | None = None,
    ):
        try:
            from tokenizers import Tokenizer as _HfBase  # noqa: F401
        except ImportError as e:
            raise RuntimeError(
                "HfTokenizer requires the `tokenizers` package; "
                "install with `pip install tokenizers`"
            ) from e
        self._hf = hf_tokenizer
        self.tokenizer_id = tokenizer_id
        # Trust the HF object for vocab_size unless overridden
        self.vocab_size = vocab_size or int(hf_tokenizer.get_vocab_size())

    def encode(self, text: bytes) -> np.ndarray:
        text_str = text.decode("utf-8", errors="replace")
        ids = self._hf.encode(text_str, add_special_tokens=False).ids
        return np.asarray(ids, dtype=np.uint32)

    def decode(self, ids: np.ndarray) -> bytes:
        ids_list = [int(x) for x in ids.tolist()]
        return self._hf.decode(ids_list, skip_special_tokens=False).encode("utf-8")

    def config(self) -> dict:
        # Include the HF tokenizer's serialized state hash so the
        # reproducibility proof actually identifies *this* tokenizer
        # configuration, not just the registered ID.
        try:
            serialized = self._hf.to_str()
        except Exception:
            serialized = ""
        cfg_digest = hash_bytes(serialized.encode("utf-8")).hex() if serialized else ""
        return {
            "id": self.tokenizer_id,
            "vocab_size": self.vocab_size,
            "kind": "hf-tokenizers",
            "hf_state_digest": cfg_digest,
        }
