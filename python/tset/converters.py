"""Format converters: JSONL → TSET, Parquet → TSET (optional pyarrow).

MDS and WebDataset converters are stubbed for v0.1 — they require additional
dependencies (`mosaicml-streaming` / `webdataset`) that we don't take on
unconditionally. The interface they will land on is documented here so the
v0.2 work is just plumbing.
"""

from __future__ import annotations

import json
from typing import Iterable, Iterator

from tset.tokenizers import Tokenizer
from tset.writer import Writer


def jsonl_records(path: str, content_field: str = "text") -> Iterator[dict]:
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def jsonl_to_tset(
    jsonl_path: str,
    tset_path: str,
    tokenizer: Tokenizer,
    content_field: str = "text",
    metadata_fields: Iterable[str] | None = None,
) -> dict:
    metadata_fields = list(metadata_fields or [])
    n = 0
    with Writer(tset_path) as w:
        for rec in jsonl_records(jsonl_path):
            content = rec[content_field]
            md = {k: rec.get(k) for k in metadata_fields if k in rec}
            w.add_document(content, metadata=md)
            n += 1
        w.add_tokenizer_view(tokenizer)
    return {"input": jsonl_path, "output": tset_path, "documents": n}


def parquet_to_tset(
    parquet_path: str,
    tset_path: str,
    tokenizer: Tokenizer,
    content_column: str = "text",
    metadata_columns: Iterable[str] | None = None,
) -> dict:
    try:
        import pyarrow.parquet as pq
    except ImportError as e:
        raise RuntimeError(
            "parquet_to_tset requires pyarrow; install with `pip install pyarrow`"
        ) from e
    table = pq.read_table(parquet_path)
    metadata_columns = list(metadata_columns or [])
    n = 0
    with Writer(tset_path) as w:
        text_col = table[content_column].to_pylist()
        meta_lookup = {c: table[c].to_pylist() for c in metadata_columns if c in table.column_names}
        for i, content in enumerate(text_col):
            md = {c: meta_lookup[c][i] for c in meta_lookup}
            w.add_document(content, metadata=md)
            n += 1
        w.add_tokenizer_view(tokenizer)
    return {"input": parquet_path, "output": tset_path, "documents": n}
