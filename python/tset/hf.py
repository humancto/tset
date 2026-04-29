"""HuggingFace ``datasets`` integration for TSET.

Three entry points:

- :func:`from_tset` — open a TSET shard as a ``datasets.Dataset``
- :func:`from_dataset` — open a multi-shard TSET dataset as a single
  ``datasets.Dataset`` (exclusion overlay applied automatically)
- :func:`to_tset` — write a ``datasets.Dataset`` into a TSET shard

All functions lazy-import ``datasets`` so this module loads without it.
Install with ``pip install tset[hf]`` or ``pip install datasets``.

Example::

    from tset.hf import from_tset
    ds = from_tset("corpus.tset", with_tokens=True)
    ds = ds.filter(lambda r: r["lang"] == "en")
    ds.train_test_split(test_size=0.1)
    # …continue with the standard datasets API
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, Iterator

__all__ = ["from_tset", "from_dataset", "to_tset"]


# ── Internal helpers ─────────────────────────────────────────────────


def _require_datasets():
    try:
        import datasets as _ds  # type: ignore[import-not-found]

        return _ds
    except ImportError as e:
        raise RuntimeError(
            "tset.hf requires the `datasets` library; install with "
            "`pip install tset[hf]` or `pip install datasets`."
        ) from e


# Reserved keys we put on every record. Metadata columns that collide
# with a reserved name (or with another already-claimed output name)
# get an iterative ``meta_`` prefix — see ``_build_column_name_map``.
_RESERVED = ("text", "doc_hash", "tokens")


def _per_doc_token_iter(reader, view: str) -> Iterator[tuple[bytes, list[int]]]:
    """Yield ``(doc_hash, tokens)`` per document, lazily.

    ``Reader.stream_tokens`` yields ``(batch, doc_hash)`` where
    consecutive batches with the same ``doc_hash`` belong to the same
    document. This wrapper accumulates a per-document list and emits it
    on the boundary, then drops it. Memory is O(largest single doc),
    not O(corpus) — the eager-dict version that used to live here
    materialized the entire shard in memory before the first record
    yielded, defeating ``streaming=True``.
    """
    current_hash: bytes | None = None
    current_tokens: list[int] = []
    for batch, dh in reader.stream_tokens(view, batch_size=4096):
        if current_hash is not None and dh != current_hash:
            yield current_hash, current_tokens
            current_tokens = []
        current_hash = dh
        current_tokens.extend(int(x) for x in batch)
    if current_hash is not None:
        yield current_hash, current_tokens


def _build_column_name_map(
    col_names: list[str], with_tokens: bool
) -> dict[str, str]:
    """Map each input metadata column to a non-colliding output name.

    Reserved keys (``text``, ``doc_hash``, optionally ``tokens``) are
    avoided by iterative ``meta_`` prefixing: ``text`` → ``meta_text``;
    if ``meta_text`` is itself a metadata column it becomes
    ``meta_meta_text``, and so on. Processing is in input order; first
    claim wins. The mapping is computed once and reused for every row,
    so column names stay stable across the dataset.
    """
    used: set[str] = {"text", "doc_hash"}
    if with_tokens:
        used.add("tokens")
    mapping: dict[str, str] = {}
    for name in col_names:
        candidate = name
        while candidate in used:
            candidate = f"meta_{candidate}"
        mapping[name] = candidate
        used.add(candidate)
    return mapping


def _row_records(
    reader,
    *,
    view: str | None,
    with_tokens: bool,
    with_metadata: bool,
) -> Iterator[dict[str, Any]]:
    """Yield one dict per document, in writer-insertion order.

    Streams tokens per-document when ``with_tokens=True`` so callers
    using ``streaming=True`` actually get O(1) memory regardless of
    corpus size.
    """
    chosen_view = view
    token_iter: Iterator[tuple[bytes, list[int]]] | None = None
    if with_tokens:
        if chosen_view is None:
            ids = reader.tokenizer_ids()
            if not ids:
                raise ValueError(
                    "with_tokens=True but the shard has no tokenizer views"
                )
            chosen_view = ids[0]
        token_iter = _per_doc_token_iter(reader, chosen_view)

    cols = reader.metadata_columns() if with_metadata else None
    col_names = cols.names() if cols is not None else []
    name_map = _build_column_name_map(col_names, with_tokens=with_tokens)
    col_data = {name: cols.column(name) for name in col_names} if cols else {}

    for i, (doc_hash, content) in enumerate(reader.documents()):
        rec: dict[str, Any] = {
            "text": content.decode("utf-8", errors="replace"),
            "doc_hash": doc_hash.hex(),
        }
        if token_iter is not None:
            try:
                tok_hash, tok_list = next(token_iter)
            except StopIteration:
                tok_hash, tok_list = doc_hash, []
            # The two iterators walk in writer-insertion order; a
            # mismatch here would mean the shard is internally
            # inconsistent. Surface it loudly rather than silently
            # mis-pair docs and tokens.
            if tok_hash != doc_hash:
                raise RuntimeError(
                    f"internal: token iterator out of sync with document "
                    f"order at index {i} (doc {doc_hash.hex()[:12]} != "
                    f"tokens for {tok_hash.hex()[:12]})"
                )
            rec["tokens"] = tok_list
        for name in col_names:
            value = col_data[name][i] if i < len(col_data[name]) else None
            rec[name_map[name]] = value
        yield rec


# ── Public API ───────────────────────────────────────────────────────


def from_tset(
    path: str | Path,
    *,
    view: str | None = None,
    with_tokens: bool = False,
    with_metadata: bool = True,
    streaming: bool = False,
):
    """Open a TSET shard as a HuggingFace dataset.

    Args:
        path: shard path.
        view: tokenizer view to expose as a ``tokens`` column. If
            ``None`` and ``with_tokens=True``, the first registered view
            is used.
        with_tokens: include bit-packed token IDs as a ``tokens`` column.
        with_metadata: pass through metadata columns recorded in TCOL /
            inline manifest. Names that collide with reserved keys
            (``text``, ``doc_hash``, ``tokens``) are prefixed ``meta_``.
        streaming: return a ``datasets.IterableDataset`` that lazily
            reads on iteration. Use for corpora that don't fit in
            memory.

    Returns:
        ``datasets.Dataset`` (default) or ``datasets.IterableDataset``
        when ``streaming=True``.
    """
    _ds = _require_datasets()
    from tset.reader import Reader

    path_str = str(path)

    def gen():
        with Reader(path_str) as r:
            yield from _row_records(
                r,
                view=view,
                with_tokens=with_tokens,
                with_metadata=with_metadata,
            )

    if streaming:
        return _ds.IterableDataset.from_generator(gen)
    return _ds.Dataset.from_generator(gen)


def from_dataset(
    path: str | Path,
    *,
    view: str | None = None,
    with_tokens: bool = False,
    with_metadata: bool = True,
    streaming: bool = False,
):
    """Open a multi-shard TSET dataset as a single HuggingFace dataset.

    Reads every shard registered in ``<path>/dataset.json`` and applies
    the exclusion overlay (``<path>/exclusions.json``) automatically:
    documents whose ``doc_hash`` appears in the overlay are filtered
    out before they reach the HF dataset.

    Returns ``datasets.Dataset`` or ``datasets.IterableDataset`` per
    ``streaming``.
    """
    _ds = _require_datasets()
    from tset.dataset import Dataset

    path_str = str(path)

    def gen():
        ds = Dataset(path_str)
        excluded = ds.exclusions()
        for r in ds.shards():
            try:
                for rec in _row_records(
                    r,
                    view=view,
                    with_tokens=with_tokens,
                    with_metadata=with_metadata,
                ):
                    if rec["doc_hash"] in excluded:
                        continue
                    yield rec
            finally:
                r.close()

    if streaming:
        return _ds.IterableDataset.from_generator(gen)
    return _ds.Dataset.from_generator(gen)


def to_tset(
    hf_ds,
    path: str | Path,
    *,
    content_field: str = "text",
    metadata_fields: Iterable[str] | None = None,
    tokenizer=None,
    binary_sections: bool = False,
) -> dict:
    """Write a HuggingFace dataset into a TSET shard.

    This is the inverse of :func:`from_tset` and the most common
    on-ramp: take any HuggingFace dataset (loaded via
    ``datasets.load_dataset``) and ship it as a single TSET shard with
    a Merkle root, signed audit log, and proofs.

    Args:
        hf_ds: any iterable of dict records — ``datasets.Dataset``,
            ``IterableDataset``, ``DatasetDict[split]``, or a list of
            dicts.
        path: output ``.tset`` path.
        content_field: which field carries the document body.
        metadata_fields: which fields to preserve as TSET metadata
            columns. ``None`` keeps none. ``"*"`` keeps every field
            other than ``content_field``.
        tokenizer: a :class:`tset.tokenizers.Tokenizer` instance. If
            omitted, :class:`tset.tokenizers.ByteLevelTokenizer` is used.
        binary_sections: emit v0.3.2 on-disk TSMT / TLOG / TCOL sections
            in addition to the inline JSON forms. See
            ``examples/datasets/SCALING.md`` for the size implications.

    Returns:
        ``{"path", "documents", "tokenizer_id"}``.

    Raises:
        TypeError: if a record's ``content_field`` is not ``str`` or
            ``bytes``.
        KeyError: if ``content_field`` is missing from a record.
    """
    from tset.writer import Writer

    if tokenizer is None:
        from tset.tokenizers import ByteLevelTokenizer

        tokenizer = ByteLevelTokenizer()

    keep_all_metadata = metadata_fields == "*"
    fields = (
        [] if metadata_fields is None or keep_all_metadata else list(metadata_fields)
    )

    n = 0
    with Writer(str(path)) as w:
        if binary_sections:
            w.enable_binary_sections()
        for record in hf_ds:
            if content_field not in record:
                raise KeyError(
                    f"content_field {content_field!r} not in record (got "
                    f"{sorted(record.keys())})"
                )
            content = record[content_field]
            if not isinstance(content, (str, bytes)):
                raise TypeError(
                    f"content_field {content_field!r} must be str or bytes, "
                    f"got {type(content).__name__}"
                )
            if keep_all_metadata:
                md = {k: v for k, v in record.items() if k != content_field}
            else:
                md = {k: record.get(k) for k in fields if k in record}
            w.add_document(content, metadata=md)
            n += 1
        w.add_tokenizer_view(tokenizer)

    return {
        "path": str(path),
        "documents": n,
        "tokenizer_id": tokenizer.tokenizer_id,
    }
