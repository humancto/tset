"""Cookbook: a directory of Parquet files → one TSET shard, in five lines.

The most common ingest path: someone hands you a Parquet directory
(typical of Hugging Face datasets, Spark exports, dbt models) with a
``text`` column and a few metadata columns. You want a content-addressed
TSET shard with a tokenizer view bound to it.

Run::

    pip install tset pyarrow
    python -m examples.cookbook.parquet_to_tset

The five-line pattern is the body of ``ingest_parquet_dir`` below.
The rest of the file is a self-contained demo that builds a Parquet
directory and round-trips it.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO / "python"))


def ingest_parquet_dir(parquet_dir: str, out_path: str, text_field: str = "text") -> None:
    """Ingest every Parquet file in ``parquet_dir`` into a single TSET shard.

    The five-line core (drop the imports + signature and you have it):
        ds = pyarrow.dataset.dataset(parquet_dir, format="parquet")
        with Writer(out_path) as w:
            for batch in ds.to_batches():
                for row in batch.to_pylist():
                    w.add_document(row[text_field].encode("utf-8"), metadata=row)
            w.add_tokenizer_view(ByteLevelTokenizer())
    """
    import pyarrow.dataset as ds_mod

    from tset.tokenizers import ByteLevelTokenizer
    from tset.writer import Writer

    ds = ds_mod.dataset(parquet_dir, format="parquet")
    with Writer(out_path) as w:
        for batch in ds.to_batches():
            for row in batch.to_pylist():
                meta = {k: v for k, v in row.items() if k != text_field}
                w.add_document(row[text_field].encode("utf-8"), metadata=meta)
        w.add_tokenizer_view(ByteLevelTokenizer())


def main() -> int:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as e:
        sys.exit(f"this recipe needs `pyarrow`: pip install pyarrow ({e})")

    from tset.reader import Reader

    with tempfile.TemporaryDirectory() as tmp:
        # ── Build a tiny Parquet directory.
        pq_dir = Path(tmp) / "pq"
        pq_dir.mkdir()
        for i, (chunk, lang) in enumerate([("alpha beta", "en"), ("hola mundo", "es"), ("bonjour", "fr")]):
            tbl = pa.table({
                "text": [chunk, f"{chunk} {chunk}"],
                "lang": [lang, lang],
                "row_id": [i * 2, i * 2 + 1],
            })
            pq.write_table(tbl, pq_dir / f"part-{i:04d}.parquet")

        out = Path(tmp) / "corpus.tset"
        ingest_parquet_dir(str(pq_dir), str(out))
        print(f"wrote shard       {out.name}  ({out.stat().st_size:,} bytes)")

        r = Reader(str(out))
        docs = list(r.documents())
        print(f"document_count    {len(docs)}")
        print(f"shard_merkle_root {r.header.shard_merkle_root.hex()[:16]}…")
        print(f"smt_root          {r.smt_root().hex()[:16]}…")

        # The metadata columns should mirror the Parquet schema.
        cols = r.metadata_columns().to_dict()
        print(f"metadata columns  {sorted(cols.get('columns', {}).keys())}")
        # Per-row metadata is preserved (column-major: cols['columns']['lang']
        # is the list of values across rows).
        first_lang = cols["columns"]["lang"][0]
        print(f"first row lang    {first_lang!r}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
