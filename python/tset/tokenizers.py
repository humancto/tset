import json
import re
from abc import ABC, abstractmethod

import numpy as np

from tset.hashing import hash_bytes


class Tokenizer(ABC):
    tokenizer_id: str
    vocab_size: int

    @abstractmethod
    def encode(self, text: bytes) -> np.ndarray:
        ...

    @abstractmethod
    def decode(self, ids: np.ndarray) -> bytes:
        ...

    @abstractmethod
    def config(self) -> dict:
        ...

    def config_hash(self) -> bytes:
        canonical = json.dumps(self.config(), sort_keys=True, separators=(",", ":"))
        return hash_bytes(canonical.encode("utf-8"))

    @classmethod
    def from_config(cls, cfg: dict) -> "Tokenizer":
        return cls()


class ByteLevelTokenizer(Tokenizer):
    tokenizer_id = "byte-level-v1"
    vocab_size = 256

    def encode(self, text: bytes) -> np.ndarray:
        return np.frombuffer(text, dtype=np.uint8).astype(np.uint32, copy=True)

    def decode(self, ids: np.ndarray) -> bytes:
        if (ids >= 256).any():
            raise ValueError("byte-level tokenizer received ID >= 256")
        return ids.astype(np.uint8).tobytes()

    def config(self) -> dict:
        return {"id": self.tokenizer_id, "vocab_size": self.vocab_size, "kind": "byte"}


_WHITESPACE_RE = re.compile(rb"(\s+|\S+)")


class WhitespaceTokenizer(Tokenizer):
    """Deterministic whitespace tokenizer with a fixed-size hashed vocabulary.
    Maps each token to ID = (BLAKE3(token) mod (vocab_size - 1)) + 1.
    ID 0 is reserved for byte-fallback markers (unused in v0.1)."""

    tokenizer_id = "whitespace-hashed-v1"

    def __init__(self, vocab_size: int = 65536):
        if vocab_size < 2:
            raise ValueError("vocab_size must be >= 2")
        self.vocab_size = vocab_size

    def encode(self, text: bytes) -> np.ndarray:
        tokens = _WHITESPACE_RE.findall(text)
        ids = np.empty(len(tokens), dtype=np.uint32)
        modulus = self.vocab_size - 1
        for i, tok in enumerate(tokens):
            digest = hash_bytes(tok)
            ids[i] = (int.from_bytes(digest[:8], "little") % modulus) + 1
        return ids

    def decode(self, ids: np.ndarray) -> bytes:
        raise NotImplementedError(
            "whitespace-hashed tokenizer is one-way; decode not supported"
        )

    def config(self) -> dict:
        return {
            "id": self.tokenizer_id,
            "vocab_size": self.vocab_size,
            "kind": "whitespace-hashed",
        }

    @classmethod
    def from_config(cls, cfg: dict) -> "WhitespaceTokenizer":
        return cls(vocab_size=int(cfg["vocab_size"]))


_REGISTRY: dict[str, type[Tokenizer]] = {
    ByteLevelTokenizer.tokenizer_id: ByteLevelTokenizer,
    WhitespaceTokenizer.tokenizer_id: WhitespaceTokenizer,
}


def get_tokenizer(tokenizer_id: str, **kwargs) -> Tokenizer:
    return get_tokenizer_class(tokenizer_id)(**kwargs)


def get_tokenizer_class(tokenizer_id: str) -> type[Tokenizer]:
    if tokenizer_id not in _REGISTRY:
        raise KeyError(f"unknown tokenizer_id: {tokenizer_id!r}")
    return _REGISTRY[tokenizer_id]


def register_tokenizer(cls: type[Tokenizer]) -> type[Tokenizer]:
    _REGISTRY[cls.tokenizer_id] = cls
    return cls


def reproducibility_test_vector(
    tokenizer: Tokenizer,
    documents: dict[bytes, bytes],
    sample_size: int = 4,
) -> dict:
    """Build a v0.1 reproducibility proof: pick up to `sample_size` documents
    deterministically (sorted by hash), tokenize them, hash the concatenated
    token bytes."""
    if not documents:
        return {"doc_hashes": [], "expected_token_arrays_hash": ""}
    sorted_hashes = sorted(documents.keys())
    sampled = sorted_hashes[:sample_size]
    pieces: list[bytes] = []
    for h in sampled:
        ids = tokenizer.encode(documents[h])
        pieces.append(ids.astype(np.uint32).tobytes())
    digest = hash_bytes(b"".join(pieces))
    return {
        "doc_hashes": [h.hex() for h in sampled],
        "expected_token_arrays_hash": digest.hex(),
    }


def verify_reproducibility(
    tokenizer: Tokenizer,
    test_vector: dict,
    documents: dict[bytes, bytes],
) -> None:
    """Raises if the tokenizer disagrees with the recorded test vector."""
    expected = test_vector.get("expected_token_arrays_hash", "")
    if not expected:
        return
    pieces: list[bytes] = []
    for hex_h in test_vector["doc_hashes"]:
        h = bytes.fromhex(hex_h)
        if h not in documents:
            raise ValueError(
                f"reproducibility test vector references missing document {hex_h}"
            )
        ids = tokenizer.encode(documents[h])
        pieces.append(ids.astype(np.uint32).tobytes())
    actual = hash_bytes(b"".join(pieces)).hex()
    if actual != expected:
        raise ValueError(
            f"tokenizer reproducibility check failed for {tokenizer.tokenizer_id}: "
            f"expected {expected}, got {actual}"
        )
