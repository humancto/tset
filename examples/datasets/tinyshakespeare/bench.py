"""Storage and read-throughput benchmark for TinyShakespeare.

Same corpus, expressed as:

- raw text    (the original 1 MB file)
- JSONL       (one paragraph per record)
- JSONL+zstd  (the standard "compressed JSONL" baseline)
- Parquet     (if pyarrow is installed; skipped otherwise)
- TSET        (1 view · binary sections off)
- TSET+views  (2 views · binary sections on, the "full" config)

For each format we measure on-disk size and a fair read throughput:
fully decode every document and tally a checksum so the OS page cache
can't trick us into reporting a no-op.

Outputs JSON to ``out/bench.json`` and a Markdown table to stdout.
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
import time
from pathlib import Path

from examples.datasets._lib import format_bytes, format_duration, measure
from examples.datasets.tinyshakespeare.convert import JSONL, OUT_DIR, TSET, split_paragraphs
from examples.datasets.tinyshakespeare.download import fetch_corpus

RESULTS_JSON = OUT_DIR / "bench.json"


def _checksum_text_iter(it) -> str:
    h = hashlib.blake2b(digest_size=16)
    for s in it:
        if isinstance(s, str):
            s = s.encode("utf-8")
        h.update(s)
    return h.hexdigest()


def _bench_raw(path: Path) -> dict:
    size = path.stat().st_size
    with measure("raw read") as m:
        data = path.read_text(encoding="utf-8")
        cksum = _checksum_text_iter([data])
    return {"size": size, "read_seconds": m.seconds, "checksum": cksum}


def _bench_jsonl(path: Path) -> dict:
    size = path.stat().st_size
    with measure("jsonl read") as m:
        n = 0
        h = hashlib.blake2b(digest_size=16)
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                rec = json.loads(line)
                h.update(rec["text"].encode("utf-8"))
                n += 1
    return {
        "size": size,
        "read_seconds": m.seconds,
        "documents": n,
        "checksum": h.hexdigest(),
    }


def _bench_jsonl_zstd(jsonl_path: Path) -> dict:
    try:
        import zstandard as zstd  # type: ignore
    except ImportError:
        return {"skipped": "zstandard not installed"}

    zpath = OUT_DIR / "corpus.jsonl.zst"
    raw = jsonl_path.read_bytes()
    with measure("zstd compress") as m_w:
        zpath.write_bytes(zstd.ZstdCompressor(level=3).compress(raw))
    size = zpath.stat().st_size
    with measure("jsonl+zstd read") as m_r:
        decompressed = zstd.ZstdDecompressor().decompress(zpath.read_bytes())
        h = hashlib.blake2b(digest_size=16)
        for line in decompressed.splitlines():
            if not line:
                continue
            rec = json.loads(line)
            h.update(rec["text"].encode("utf-8"))
    return {
        "size": size,
        "write_seconds": m_w.seconds,
        "read_seconds": m_r.seconds,
        "checksum": h.hexdigest(),
    }


def _bench_parquet(jsonl_path: Path) -> dict:
    try:
        import pyarrow as pa  # type: ignore
        import pyarrow.parquet as pq  # type: ignore
    except ImportError:
        return {"skipped": "pyarrow not installed"}

    ppath = OUT_DIR / "corpus.parquet"
    rows = [json.loads(l) for l in jsonl_path.read_text(encoding="utf-8").splitlines() if l]
    table = pa.Table.from_pylist(rows)
    with measure("parquet write") as m_w:
        pq.write_table(table, str(ppath), compression="zstd")
    size = ppath.stat().st_size
    with measure("parquet read") as m_r:
        t = pq.read_table(str(ppath), columns=["text"])
        h = hashlib.blake2b(digest_size=16)
        for s in t["text"].to_pylist():
            h.update(s.encode("utf-8"))
    return {
        "size": size,
        "write_seconds": m_w.seconds,
        "read_seconds": m_r.seconds,
        "checksum": h.hexdigest(),
    }


def _bench_tset_full(tset_path: Path) -> dict:
    from tset.reader import Reader

    size = tset_path.stat().st_size
    r = Reader(str(tset_path))
    with measure("tset doc-store read") as m_doc:
        h = hashlib.blake2b(digest_size=16)
        n = 0
        for _doc_hash, content in r.documents():
            h.update(content)
            n += 1
    with measure("tset stream tokens (byte-level)") as m_tok:
        total_tokens = 0
        for tokens, _ in r.stream_tokens("byte-level-v1", batch_size=4096):
            total_tokens += int(tokens.size)
    return {
        "size": size,
        "documents": n,
        "doc_read_seconds": m_doc.seconds,
        "doc_checksum": h.hexdigest(),
        "token_stream_seconds": m_tok.seconds,
        "tokens_streamed": total_tokens,
    }


def _bench_tset_minimal(jsonl_path: Path) -> dict:
    """Re-write a TSET shard with a single tokenizer view and no
    binary sections — the "minimal" config — for a fair size comparison
    against JSONL+zstd / Parquet.
    """
    from tset.converters import jsonl_to_tset
    from tset.tokenizers import ByteLevelTokenizer

    minimal = OUT_DIR / "corpus.minimal.tset"
    if minimal.exists():
        minimal.unlink()
    with measure("tset write (1 view)") as m_w:
        jsonl_to_tset(
            str(jsonl_path),
            str(minimal),
            ByteLevelTokenizer(),
            content_field="text",
        )
    size = minimal.stat().st_size
    return {"size": size, "write_seconds": m_w.seconds}


def _bench_tset_with_sections(jsonl_path: Path) -> dict:
    """Build a separate shard with v0.3.2 binary sections enabled and
    benchmark THAT, so the "+ sections" row reflects reality.

    This is what Codex flagged on the original bench.py: the row was
    labelled "2 views + sections" but read from the no-sections shard
    produced by ``tinyshakespeare/convert.py``. We now write a dedicated
    shard with ``enable_binary_sections()`` for this row.
    """
    import json

    from tset.reader import Reader
    from tset.tokenizers import ByteLevelTokenizer, WhitespaceTokenizer
    from tset.writer import Writer

    sections_path = OUT_DIR / "corpus.with_sections.tset"
    if sections_path.exists():
        sections_path.unlink()

    with measure("tset write (2 views + sections)") as m_w:
        with Writer(str(sections_path)) as w:
            w.enable_binary_sections()
            with jsonl_path.open("r", encoding="utf-8") as f:
                for line in f:
                    rec = json.loads(line)
                    w.add_document(rec["text"], metadata={"id": rec.get("id")})
            w.add_tokenizer_view(ByteLevelTokenizer())
            w.add_tokenizer_view(WhitespaceTokenizer(vocab_size=4096))

    size = sections_path.stat().st_size
    r = Reader(str(sections_path))
    with measure("tset doc-store read (with sections)") as m_doc:
        n = 0
        h = hashlib.blake2b(digest_size=16)
        for _doc_hash, content in r.documents():
            h.update(content)
            n += 1
    with measure("tset stream tokens (with sections)") as m_tok:
        total_tokens = 0
        for tokens, _ in r.stream_tokens("byte-level-v1", batch_size=4096):
            total_tokens += int(tokens.size)
    return {
        "size": size,
        "documents": n,
        "write_seconds": m_w.seconds,
        "doc_read_seconds": m_doc.seconds,
        "doc_checksum": h.hexdigest(),
        "token_stream_seconds": m_tok.seconds,
        "tokens_streamed": total_tokens,
    }


def main() -> int:
    if not TSET.exists():
        print(f"error: {TSET} not found — run convert.py first", file=sys.stderr)
        return 2

    raw = fetch_corpus()
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    rows = {
        "raw_text": _bench_raw(raw),
        "jsonl": _bench_jsonl(JSONL),
        "jsonl_zstd": _bench_jsonl_zstd(JSONL),
        "parquet": _bench_parquet(JSONL),
        "tset_minimal_1view": _bench_tset_minimal(JSONL),
        # The shard at TSET (from convert.py) is "2 views, no sections" —
        # convert.py deliberately leaves sections off because they bloat
        # in v0.3.2. We benchmark it as its own row, then build a
        # separate shard with sections actually enabled for the
        # "with sections" row so labels and data agree.
        "tset_2views_no_sections": _bench_tset_full(TSET),
        "tset_2views_with_sections": _bench_tset_with_sections(JSONL),
    }
    payload = {
        "dataset": "tinyshakespeare",
        "documents": rows["jsonl"].get("documents", 0),
        "raw_size": rows["raw_text"]["size"],
        "rows": rows,
    }
    RESULTS_JSON.write_text(json.dumps(payload, indent=2, sort_keys=True))

    # ── Markdown table for the README/docs ────────────────────────
    print()
    print(
        f"| Format | On-disk size | Size vs JSONL | Read time | Notes |"
    )
    print(
        f"|---|---:|---:|---:|---|"
    )
    base = rows["jsonl"]["size"]
    for label, key, note in [
        ("Raw text",                       "raw_text",                  "no record structure"),
        ("JSONL",                          "jsonl",                     "baseline"),
        ("JSONL + zstd",                   "jsonl_zstd",                "compressed text"),
        ("Parquet (zstd)",                 "parquet",                   "columnar"),
        ("TSET · 1 view",                  "tset_minimal_1view",        "no binary sections"),
        ("TSET · 2 views, no sections",    "tset_2views_no_sections",   "lean prod config"),
        ("TSET · 2 views + sections",      "tset_2views_with_sections", "v0.4 sections-only (no inline duplication)"),
    ]:
        r = rows[key]
        if r.get("skipped"):
            print(f"| {label} | _{r['skipped']}_ |  |  |  |")
            continue
        size = r["size"]
        ratio = size / base
        rt = r.get("read_seconds") or r.get("doc_read_seconds")
        rt_s = format_duration(rt) if rt else "—"
        print(
            f"| {label} "
            f"| {format_bytes(size)} "
            f"| {ratio:.2f}× "
            f"| {rt_s} "
            f"| {note} |"
        )
    print()
    print(f"results written to {RESULTS_JSON.relative_to(Path.cwd())}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
