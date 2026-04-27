"""Drop-in Writer that delegates to the Rust implementation via tset_rs.

Same public surface as `tset.Writer` (so existing call sites swap by
import) — but `add_tokenizer_view` accepts a Tokenizer instance the way
the Python writer does, and translates it to the (id, vocab_size) pair
the Rust binding takes.

Use when you want the Rust writer's throughput without rewriting calls:

    from tset.rust_writer import RustWriter as Writer
"""

from __future__ import annotations

from typing import Any

from tset.tokenizers import Tokenizer


class RustWriter:
    def __init__(self, path: str, shard_id: str | None = None) -> None:
        try:
            import tset_rs  # type: ignore[import-not-found]
        except ImportError as e:
            raise RuntimeError(
                "RustWriter requires the optional `tset_rs` PyO3 wheel. "
                "Build it with `cd crates/tset-py && maturin build --release`."
            ) from e
        self._inner = tset_rs.Writer(path, shard_id) if shard_id else tset_rs.Writer(path)
        self.path = path

    def __enter__(self) -> "RustWriter":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is None:
            self.close()

    def add_document(
        self,
        content: bytes | str,
        metadata: dict[str, Any] | None = None,
    ) -> bytes:
        if isinstance(content, str):
            content = content.encode("utf-8")
        if metadata is None:
            return self._inner.add_document(content)
        return self._inner.add_document(content, metadata)

    def add_tokenizer_view(self, tokenizer: Tokenizer) -> None:
        # Python tokenizer object → (id, vocab_size). Only the two built-in
        # tokenizers are wired through tset-py for now; custom Python
        # tokenizers can't be passed across the FFI boundary.
        self._inner.add_tokenizer_view(tokenizer.tokenizer_id, int(tokenizer.vocab_size))

    def add_subset(self, name: str, predicate: str, default_weight: float) -> None:
        self._inner.add_subset(name, predicate, float(default_weight))

    def close(self) -> None:
        self._inner.close()
