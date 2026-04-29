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


# Reserved keys we put on every record. If a metadata column collides
# we prefix it with ``meta_`` instead of clobbering the receipt.
_RESERVED = ("text", "doc_hash", "tokens")


def _read_tokens_per_doc(reader, view: str) -> dict[bytes, list[int]]:
    """Build a ``doc_hash → token list`` map by streaming the view once.

    The reader yields per-document batches with the doc_hash attached, so
    we collect by hash rather than by sequential index — that way the
    map remains correct even if the doc order changes between
    ``Reader.documents()`` and the token stream.
    """
    out: dict[bytes, list[int]] = {}
    for batch, dh in reader.stream_tokens(view, batch_size=1_000_000):
        out.setdefault(dh, []).extend(int(x) for x in batch)
    return out


def _row_records(
    reader,
    *,
    view: str | None,
    with_tokens: bool,
    with_metadata: bool,
) -> Iterator[dict[str, Any]]:
    """Yield one dict per document, in writer-insertion order."""
    tokens_lookup: dict[bytes, list[int]] = {}
    chosen_view = view
    if with_tokens:
        if chosen_view is None:
            ids = reader.tokenizer_ids()
            if not ids:
                raise ValueError(
                    "with_tokens=True but the shard has no tokenizer views"
                )
            chosen_view = ids[0]
        tokens_lookup = _read_tokens_per_doc(reader, chosen_view)

    cols = reader.metadata_columns() if with_metadata else None
    col_names = cols.names() if cols is not None else []
    # Snapshot the column data once; columns are list-backed so this is cheap.
    col_data = {name: cols.column(name) for name in col_names} if cols else {}

    for i, (doc_hash, content) in enumerate(reader.documents()):
        rec: dict[str, Any] = {
            "text": content.decode("utf-8", errors="replace"),
            "doc_hash": doc_hash.hex(),
        }
        if with_tokens:
            rec["tokens"] = tokens_lookup.get(doc_hash, [])
        for name in col_names:
            value = col_data[name][i] if i < len(col_data[name]) else None
            if name in _RESERVED:
                rec[f"meta_{name}"] = value
            else:
                rec[name] = value
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
