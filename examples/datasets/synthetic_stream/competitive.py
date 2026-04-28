"""Cross-format competitive benchmark.

Same synthetic corpus written as: raw JSONL, JSONL+zstd, Parquet+zstd,
WebDataset (.tar), MDS (mosaicml-streaming), and TSET (3 configs).
For each format we record on-disk size, write time, and read time.

Pyarrow / mosaicml-streaming / webdataset are imported lazily; absent
deps are surfaced in the output as ``"skipped"``.
"""

from __future__ import annotations

import io
import json
import os
import sys
import tarfile
import time
from pathlib import Path

from examples.datasets._lib import format_bytes, format_duration, measure
from examples.datasets.synthetic_stream.generate import generate

OUT = Path(__file__).resolve().parent / "out"
TARGET = int(os.environ.get("TSET_COMP_BYTES", "10_000_000"))


# ── helpers ───────────────────────────────────────────────────────


def _read_text_field(jsonl: Path) -> int:
    """Just read the text field of every record. Used as the `read all
    documents` benchmark for JSON-shaped formats."""
    h = 0
    with jsonl.open("r", encoding="utf-8") as f:
        for line in f:
            h ^= hash(json.loads(line)["text"])
    return h


# ── format adapters ───────────────────────────────────────────────


def write_jsonl_zstd(jsonl: Path, dest: Path) -> dict:
    try:
        import zstandard as zstd  # type: ignore
    except ImportError:
        return {"skipped": "zstandard"}
    with measure("zstd write") as m:
        dest.write_bytes(zstd.ZstdCompressor(level=3).compress(jsonl.read_bytes()))
    return {"size": dest.stat().st_size, "write_seconds": m.seconds}


def read_jsonl_zstd(dest: Path) -> dict:
    try:
        import zstandard as zstd  # type: ignore
    except ImportError:
        return {"skipped": "zstandard"}
    with measure("zstd read") as m:
        text = zstd.ZstdDecompressor().decompress(dest.read_bytes()).decode("utf-8")
        n = sum(1 for line in text.splitlines() if line)
    return {"read_seconds": m.seconds, "documents": n}


def write_parquet(jsonl: Path, dest: Path) -> dict:
    try:
        import pyarrow as pa  # type: ignore
        import pyarrow.parquet as pq  # type: ignore
    except ImportError:
        return {"skipped": "pyarrow"}
    rows = [json.loads(l) for l in jsonl.read_text(encoding="utf-8").splitlines() if l]
    table = pa.Table.from_pylist(rows)
    with measure("parquet write") as m:
        pq.write_table(table, str(dest), compression="zstd")
    return {"size": dest.stat().st_size, "write_seconds": m.seconds}


def read_parquet(dest: Path) -> dict:
    try:
        import pyarrow.parquet as pq  # type: ignore
    except ImportError:
        return {"skipped": "pyarrow"}
    with measure("parquet read") as m:
        t = pq.read_table(str(dest), columns=["text"])
        n = len(t)
    return {"read_seconds": m.seconds, "documents": n}


def write_webdataset(jsonl: Path, dest: Path) -> dict:
    """WebDataset is a tar of grouped files. Each record becomes
    ``<id>.txt`` + ``<id>.json``."""
    if dest.exists():
        dest.unlink()
    n = 0
    with measure("webdataset write") as m, tarfile.open(dest, "w") as tar:
        for line in jsonl.read_text(encoding="utf-8").splitlines():
            if not line:
                continue
            rec = json.loads(line)
            stem = rec.get("id") or f"doc-{n:08d}"
            tdata = rec["text"].encode("utf-8")
            mdata = json.dumps(
                {k: v for k, v in rec.items() if k != "text"},
                sort_keys=True,
            ).encode("utf-8")
            for ext, data in (("txt", tdata), ("json", mdata)):
                ti = tarfile.TarInfo(name=f"{stem}.{ext}")
                ti.size = len(data)
                tar.addfile(ti, io.BytesIO(data))
            n += 1
    return {"size": dest.stat().st_size, "write_seconds": m.seconds, "documents": n}


def read_webdataset(dest: Path) -> dict:
    n = 0
    with measure("webdataset read") as m, tarfile.open(dest, "r") as tar:
        for member in tar:
            if member.name.endswith(".txt"):
                f = tar.extractfile(member)
                if f is not None:
                    f.read()
                    n += 1
    return {"read_seconds": m.seconds, "documents": n}


def write_mds(jsonl: Path, dest_dir: Path) -> dict:
    try:
        from streaming.base.format.mds.encodings import is_mds_encoding  # noqa: F401
        from streaming import MDSWriter  # type: ignore
    except ImportError:
        return {"skipped": "mosaicml-streaming"}
    if dest_dir.exists():
        import shutil

        shutil.rmtree(dest_dir)
    dest_dir.mkdir(parents=True)
    rows = [json.loads(l) for l in jsonl.read_text(encoding="utf-8").splitlines() if l]
    columns = {"id": "str", "text": "str", "lang": "str", "section": "str"}
    with measure("mds write") as m, MDSWriter(out=str(dest_dir), columns=columns) as w:
        for r in rows:
            w.write(
                {
                    "id": r.get("id", ""),
                    "text": r["text"],
                    "lang": r.get("lang", ""),
                    "section": r.get("section", ""),
                }
            )
    total = sum(p.stat().st_size for p in dest_dir.iterdir() if p.is_file())
    return {"size": total, "write_seconds": m.seconds, "documents": len(rows)}


def write_tset(jsonl: Path, dest: Path, *, two_views: bool, binary_sections: bool) -> dict:
    from tset.tokenizers import ByteLevelTokenizer, WhitespaceTokenizer
    from tset.writer import Writer

    if dest.exists():
        dest.unlink()
    n = 0
    with measure("tset write") as m:
        with Writer(str(dest)) as w:
            if binary_sections:
                w.enable_binary_sections()
            with jsonl.open("r", encoding="utf-8") as f:
                for line in f:
                    rec = json.loads(line)
                    w.add_document(rec["text"], metadata={"lang": rec.get("lang"), "section": rec.get("section")})
                    n += 1
            w.add_tokenizer_view(ByteLevelTokenizer())
            if two_views:
                w.add_tokenizer_view(WhitespaceTokenizer(vocab_size=4096))
    return {"size": dest.stat().st_size, "write_seconds": m.seconds, "documents": n}


def read_tset(dest: Path) -> dict:
    from tset.reader import Reader

    n = 0
    with measure("tset read") as m:
        r = Reader(str(dest))
        for _h, _content in r.documents():
            n += 1
    return {"read_seconds": m.seconds, "documents": n}


# ── orchestration ─────────────────────────────────────────────────


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    jsonl = OUT / f"comp-{TARGET}.jsonl"
    if not jsonl.exists() or jsonl.stat().st_size < TARGET * 0.95:
        print(f"generating {format_bytes(TARGET)} JSONL …")
        info = generate(TARGET, jsonl)
        print(f"  {info['documents']:,} docs · {format_bytes(jsonl.stat().st_size)}")
    else:
        print(f"reusing {jsonl.name} ({format_bytes(jsonl.stat().st_size)})")

    base = jsonl.stat().st_size
    rows: dict[str, dict] = {}

    rows["jsonl"] = {"size": base, **{"read": _read_via("jsonl", jsonl)}}

    zpath = OUT / f"comp-{TARGET}.jsonl.zst"
    rows["jsonl_zstd"] = {**write_jsonl_zstd(jsonl, zpath), **{"read": read_jsonl_zstd(zpath) if zpath.exists() else {}}}

    ppath = OUT / f"comp-{TARGET}.parquet"
    rows["parquet_zstd"] = {**write_parquet(jsonl, ppath), **{"read": read_parquet(ppath) if ppath.exists() else {}}}

    tpath = OUT / f"comp-{TARGET}.tar"
    rows["webdataset"] = {**write_webdataset(jsonl, tpath), **{"read": read_webdataset(tpath)}}

    mpath = OUT / f"comp-{TARGET}.mds"
    rows["mds"] = write_mds(jsonl, mpath)

    for label, opts in (
        ("tset_1v_json",      dict(two_views=False, binary_sections=False)),
        ("tset_2v_json",      dict(two_views=True,  binary_sections=False)),
        ("tset_2v_sections",  dict(two_views=True,  binary_sections=True)),
    ):
        dest = OUT / f"comp-{TARGET}.{label}.tset"
        rows[label] = {**write_tset(jsonl, dest, **opts), **{"read": read_tset(dest)}}

    payload = {"target_bytes": TARGET, "jsonl_size": base, "rows": rows}
    out_json = OUT / "competitive.json"
    out_json.write_text(json.dumps(payload, indent=2))

    # ── summary table ────────────────────────────────────────────
    print()
    print("| Format | On-disk size | Size vs JSONL | Write | Read | Notes |")
    print("|---|---:|---:|---:|---:|---|")
    print(f"| JSONL                 | {format_bytes(base):>10} | 1.00× | — | {format_duration(rows['jsonl']['read'].get('read_seconds', 0)):>8} | baseline |")
    for label, note in [
        ("jsonl_zstd",     "zstd-compressed text"),
        ("parquet_zstd",   "columnar"),
        ("webdataset",     "tar of grouped files"),
        ("mds",            "MosaicML streaming format"),
        ("tset_1v_json",   "TSET · 1 view, no sections"),
        ("tset_2v_json",   "TSET · 2 views, no sections"),
        ("tset_2v_sections","TSET · 2 views + v0.3.2 sections"),
    ]:
        r = rows.get(label) or {}
        if r.get("skipped"):
            print(f"| {label:<22}| _{r['skipped']} not installed_ |  |  |  |  |")
            continue
        size = r.get("size")
        if size is None:
            continue
        ratio = size / base
        wt = r.get("write_seconds")
        rt = (r.get("read") or {}).get("read_seconds")
        print(
            f"| {label:<22}| {format_bytes(size):>10} | {ratio:.2f}× "
            f"| {format_duration(wt) if wt else '—':>8} "
            f"| {format_duration(rt) if rt else '—':>8} | {note} |"
        )
    print()
    print(f"results written to {out_json}")
    return 0


def _read_via(_label: str, jsonl: Path) -> dict:
    with measure("read") as m:
        n = 0
        with jsonl.open("r", encoding="utf-8") as f:
            for line in f:
                json.loads(line)
                n += 1
    return {"read_seconds": m.seconds, "documents": n}


if __name__ == "__main__":
    sys.exit(main())
