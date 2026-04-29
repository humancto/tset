"""SentencePiece adapter — wraps a ``sentencepiece.SentencePieceProcessor``
into the ``tset.tokenizers.Tokenizer`` protocol.

Usage::

    import sentencepiece as spm
    sp = spm.SentencePieceProcessor()
    sp.Load("model.spm")  # or sp.LoadFromSerializedProto(bytes_)
    from tset.sentencepiece_tokenizer import SentencePieceTokenizer
    wrapped = SentencePieceTokenizer(sp, tokenizer_id="llama-tokenizer-v1")
    with Writer("corpus.tset") as w:
        w.add_document(b"hello")
        w.add_tokenizer_view(wrapped)

The adapter records a hash of the serialized SentencePiece model in the
manifest's ``tokenizer_config``. That digest binds the view to exactly
the model bytes used at write time; loading a different .spm file
produces a different digest and the reproducibility proof rejects.
"""

from __future__ import annotations

import numpy as np

from tset.hashing import hash_bytes
from tset.tokenizers import Tokenizer


class SentencePieceTokenizer(Tokenizer):
    def __init__(
        self,
        sp_processor,
        tokenizer_id: str,
        vocab_size: int | None = None,
    ):
        try:
            import sentencepiece  # noqa: F401
        except ImportError as e:
            raise RuntimeError(
                "SentencePieceTokenizer requires the `sentencepiece` package; "
                "install with `pip install sentencepiece`"
            ) from e
        self._sp = sp_processor
        self.tokenizer_id = tokenizer_id
        self.vocab_size = int(
            vocab_size if vocab_size is not None else sp_processor.GetPieceSize()
        )

    def encode(self, text: bytes) -> np.ndarray:
        text_str = text.decode("utf-8", errors="replace")
        ids = self._sp.EncodeAsIds(text_str)
        return np.asarray(ids, dtype=np.uint32)

    def decode(self, ids: np.ndarray) -> bytes:
        return self._sp.DecodeIds([int(x) for x in ids.tolist()]).encode("utf-8")

    def config(self) -> dict:
        # SentencePiece exposes the trained model as a serialized proto.
        # Hashing it pins the exact piece-table that produced this view —
        # version drift across HF model card releases is the most common
        # reproducibility failure for SentencePiece-based stacks.
        try:
            proto = self._sp.serialized_model_proto()
            digest = hash_bytes(proto).hex()
        except Exception:
            digest = ""
        return {
            "id": self.tokenizer_id,
            "vocab_size": self.vocab_size,
            "kind": "sentencepiece",
            "spm_proto_digest": digest,
        }
