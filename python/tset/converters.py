"""Format converters: JSONL → TSET, Parquet → TSET (optional pyarrow),
WebDataset (.tar) → TSET (stdlib tarfile), MDS → TSET
(via `mosaicml-streaming` if installed)."""

from __future__ import annotations

import json
import os
import tarfile
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


def webdataset_to_tset(
    tar_path: str,
    tset_path: str,
    tokenizer: Tokenizer,
    content_extension: str = "txt",
    metadata_extensions: Iterable[str] | None = None,
) -> dict:
    """WebDataset .tar shard → TSET.

    WebDataset groups files by basename (stem). For each sample we treat
    the file with extension `content_extension` as the document body, and
    files matching `metadata_extensions` (default: any `.json` / `.cls`)
    as metadata fields keyed by their extension.

    No external dep — uses stdlib `tarfile`. Iterates the tar in order
    and emits one TSET document per stem that has a content file.
    """
    if metadata_extensions is None:
        metadata_extensions = ["json", "cls"]
    metadata_extensions = list(metadata_extensions)

    samples: dict[str, dict] = {}
    n = 0
    with Writer(tset_path) as w:
        with tarfile.open(tar_path, mode="r") as tf:
            for member in tf:
                if not member.isfile():
                    continue
                stem, _, ext = member.name.partition(".")
                f = tf.extractfile(member)
                if f is None:
                    continue
                data = f.read()
                samples.setdefault(stem, {})[ext] = data

        for stem, parts in samples.items():
            content = parts.get(content_extension)
            if content is None:
                continue
            md: dict = {"_stem": stem}
            for ext in metadata_extensions:
                if ext in parts:
                    raw = parts[ext]
                    if ext == "json":
                        try:
                            md.update(json.loads(raw.decode("utf-8")))
                        except (UnicodeDecodeError, json.JSONDecodeError):
                            md[f"_{ext}_raw_b64_len"] = len(raw)
                    else:
                        try:
                            md[ext] = raw.decode("utf-8")
                        except UnicodeDecodeError:
                            md[f"{ext}_bytes"] = len(raw)
            w.add_document(content, metadata=md)
            n += 1
        w.add_tokenizer_view(tokenizer)
    return {"input": tar_path, "output": tset_path, "documents": n}


def mds_to_tset(
    mds_dir: str,
    tset_path: str,
    tokenizer: Tokenizer,
    content_column: str = "text",
    metadata_columns: Iterable[str] | None = None,
) -> dict:
    """MosaicML Streaming Dataset (MDS) → TSET.

    Requires `mosaicml-streaming` to be installed (the MDS binary format
    spec is non-trivial; reusing the official reader keeps us honest).
    Raises RuntimeError with an install hint if missing.

    Iterates the MDS dataset in shard order, treating `content_column`
    as the document body and `metadata_columns` as per-doc metadata.
    """
    try:
        from streaming import StreamingDataset  # type: ignore[import-not-found]
    except ImportError as e:
        raise RuntimeError(
            "mds_to_tset requires mosaicml-streaming; install with "
            "`pip install mosaicml-streaming`"
        ) from e
    metadata_columns = list(metadata_columns or [])
    n = 0
    ds = StreamingDataset(local=mds_dir, remote=None, shuffle=False)  # type: ignore[call-arg]
    with Writer(tset_path) as w:
        for sample in ds:  # type: ignore[assignment]
            content = sample[content_column]
            if isinstance(content, str):
                content = content.encode("utf-8")
            md = {c: sample.get(c) for c in metadata_columns if c in sample}
            w.add_document(content, metadata=md)
            n += 1
        w.add_tokenizer_view(tokenizer)
    return {"input": mds_dir, "output": tset_path, "documents": n}


def hf_dataset_view(tset_path: str, tokenizer_id: str = "byte-level-v1"):
    """Yield each document as a {`text`, `tokens`, `doc_hash`} dict, suitable
    for `datasets.Dataset.from_generator`. Lazy import so `datasets` is
    only required if the user actually calls this function."""
    from tset.reader import Reader  # local import to avoid circular

    def gen():
        with Reader(tset_path) as r:
            for doc_hash, content in r.documents():
                yield {
                    "text": content.decode("utf-8", errors="replace"),
                    "doc_hash": doc_hash.hex(),
                }

    return gen


def to_huggingface_dataset(tset_path: str, tokenizer_id: str = "byte-level-v1"):
    """Materialize a TSET shard as a HuggingFace `datasets.Dataset`.

    Lazy-imports `datasets`; raises a clear error if missing. Returns a
    `Dataset` with columns `text` (str) and `doc_hash` (str hex).
    """
    try:
        from datasets import Dataset as HFDataset  # type: ignore[import-not-found]
    except ImportError as e:
        raise RuntimeError(
            "to_huggingface_dataset requires `datasets`; install with "
            "`pip install datasets`"
        ) from e
    gen = hf_dataset_view(tset_path, tokenizer_id)
    return HFDataset.from_generator(gen)
