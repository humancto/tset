"""Format-agnostic benchmark runner.

Run a single benchmark::

    python -m benchmarks.harness --benchmark storage --corpus-size-mb 10

Run all benchmarks at the default size::

    python -m benchmarks.harness --benchmark all
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Callable

# Make `tset` and `benchmarks` importable when the harness is run as a script
_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO = os.path.dirname(os.path.dirname(_HERE))
sys.path.insert(0, _REPO)
sys.path.insert(0, os.path.join(_REPO, "python"))

from benchmarks.baselines.jsonl_baseline import write_jsonl_baseline
from benchmarks.baselines.raw_bin import write_raw_bin
from benchmarks.harness.corpus import (
    CorpusRecord,
    generate,
    total_text_bytes,
    write_jsonl,
)

from tset.dataset import Dataset, DatasetWriter
from tset.hashing import hash_bytes
from tset.reader import Reader
from tset.tokenizers import ByteLevelTokenizer, WhitespaceTokenizer
from tset.writer import Writer, append_tokenizer_view


RESULTS_DIR = os.path.join(_REPO, "benchmarks", "results")
CORPUS_DIR = os.path.join(_REPO, "benchmarks", "corpus")
os.makedirs(RESULTS_DIR, exist_ok=True)
os.makedirs(CORPUS_DIR, exist_ok=True)


def _write_tset(records: list[CorpusRecord], path: str, tokenizer=None) -> dict:
    tokenizer = tokenizer or ByteLevelTokenizer()
    t0 = time.perf_counter()
    with Writer(path) as w:
        for r in records:
            w.add_document(
                r.text.encode("utf-8"),
                metadata={
                    "source_url": r.source_url,
                    "source_type": r.source_type,
                    "lang": r.lang,
                    "quality_score": r.quality_score,
                },
            )
        w.add_subset("high_quality", "quality_score >= 0.5", 0.7)
        w.add_subset("low_quality", "quality_score < 0.5", 0.3)
        w.add_tokenizer_view(tokenizer)
    t1 = time.perf_counter()
    return {
        "format": "tset",
        "path": path,
        "size_bytes": os.path.getsize(path),
        "documents": len(records),
        "write_seconds": round(t1 - t0, 4),
    }


def benchmark_storage(records: list[CorpusRecord]) -> dict:
    text_bytes = total_text_bytes(records)
    raw_path = os.path.join(CORPUS_DIR, "raw.bin")
    jsonl_path = os.path.join(CORPUS_DIR, "raw.jsonl")
    tset_path = os.path.join(CORPUS_DIR, "raw.tset")

    raw = write_raw_bin(records, raw_path)
    jsonl = write_jsonl_baseline(records, jsonl_path)
    tset = _write_tset(records, tset_path)

    return {
        "benchmark": "A_storage",
        "source_text_bytes": text_bytes,
        "documents": len(records),
        "results": {
            "raw_bin": {**raw, "bytes_per_source_byte": raw["size_bytes"] / text_bytes},
            "jsonl":   {**jsonl, "bytes_per_source_byte": jsonl["size_bytes"] / text_bytes},
            "tset":    {**tset, "bytes_per_source_byte": tset["size_bytes"] / text_bytes},
        },
        "tset_overhead_vs_raw_bin": round(
            (tset["size_bytes"] - raw["size_bytes"]) / raw["size_bytes"], 4
        ),
    }


def benchmark_tokenizer_swap(records: list[CorpusRecord]) -> dict:
    """Benchmark C: time to add a second tokenization view to an existing
    .tset shard, vs full re-export from JSONL."""
    text_bytes = total_text_bytes(records)
    jsonl_path = os.path.join(CORPUS_DIR, "swap.jsonl")
    tset_path = os.path.join(CORPUS_DIR, "swap.tset")

    write_jsonl(records, jsonl_path)
    base = _write_tset(records, tset_path, tokenizer=ByteLevelTokenizer())

    t0 = time.perf_counter()
    append_tokenizer_view(tset_path, WhitespaceTokenizer(vocab_size=4096))
    swap_seconds = time.perf_counter() - t0

    # Full re-export baseline: rebuild .tset from JSONL with the new tokenizer
    re_export_path = os.path.join(CORPUS_DIR, "swap_reexport.tset")
    t0 = time.perf_counter()
    with Writer(re_export_path) as w:
        with open(jsonl_path, "r", encoding="utf-8") as f:
            for line in f:
                rec = json.loads(line)
                w.add_document(
                    rec["text"].encode("utf-8"),
                    metadata={
                        "source_url": rec.get("source_url"),
                        "lang": rec.get("lang"),
                        "quality_score": rec.get("quality_score"),
                    },
                )
        w.add_tokenizer_view(WhitespaceTokenizer(vocab_size=4096))
    re_export_seconds = time.perf_counter() - t0

    speedup = re_export_seconds / swap_seconds if swap_seconds > 0 else float("inf")
    return {
        "benchmark": "C_tokenizer_swap",
        "source_text_bytes": text_bytes,
        "documents": len(records),
        "tset_swap_seconds": round(swap_seconds, 4),
        "full_re_export_seconds": round(re_export_seconds, 4),
        "speedup_x": round(speedup, 2),
        "v02_target_x": 10,
        "v1_target_x": 20,
    }


def benchmark_streaming(records: list[CorpusRecord]) -> dict:
    """Benchmark B (lite): tokens/sec for a single-process consumer.

    Times three readers back-to-back on the same shard so the deltas are
    apples-to-apples:
      - Python reader (TSET_PREFER_RUST=0)
      - Rust-backed reader via the Python adapter (TSET_PREFER_RUST=1, default)
      - np.fromfile of a raw uint32 .bin (the unrealistic upper bound)
    Real Benchmark B is multi-node S3 — that's a v0.3+ harness item.
    """
    tset_path = os.path.join(CORPUS_DIR, "stream.tset")
    raw_path = os.path.join(CORPUS_DIR, "stream.bin")
    _write_tset(records, tset_path)
    write_raw_bin(records, raw_path)

    import numpy as np

    def _time_reader(use_rust: bool) -> tuple[float, int]:
        prev = os.environ.get("TSET_PREFER_RUST")
        os.environ["TSET_PREFER_RUST"] = "1" if use_rust else "0"
        try:
            t0 = time.perf_counter()
            total = 0
            with Reader(tset_path) as r:
                for batch, _ in r.stream_tokens("byte-level-v1", batch_size=4096):
                    total += int(batch.size)
            return time.perf_counter() - t0, total
        finally:
            if prev is None:
                os.environ.pop("TSET_PREFER_RUST", None)
            else:
                os.environ["TSET_PREFER_RUST"] = prev

    py_seconds, py_total = _time_reader(use_rust=False)
    py_tps = py_total / py_seconds if py_seconds else 0

    rust_available = False
    rust_tps: float | None = None
    rust_seconds: float | None = None
    try:
        import tset_rs  # noqa: F401

        rust_available = True
        rust_seconds, rust_total = _time_reader(use_rust=True)
        rust_tps = rust_total / rust_seconds if rust_seconds else 0
    except ImportError:
        pass

    t0 = time.perf_counter()
    arr = np.fromfile(raw_path, dtype=np.uint32)
    n = int(arr.size)
    bins_seconds = time.perf_counter() - t0
    raw_tps = n / bins_seconds if bins_seconds else 0

    out: dict[str, object] = {
        "benchmark": "B_streaming_lite",
        "py_reader_tokens_per_sec": int(py_tps),
        "py_reader_seconds": round(py_seconds, 4),
        "raw_bin_tokens_per_sec": int(raw_tps),
        "py_pct_of_raw": round(py_tps / raw_tps, 4) if raw_tps else None,
        "v02_target_pct": 0.7,
        "v1_target_pct": 0.85,
        "note": "single-process; full Benchmark B requires multi-node S3",
    }
    if rust_available and rust_tps is not None:
        out["rust_reader_tokens_per_sec"] = int(rust_tps)
        out["rust_reader_seconds"] = round(rust_seconds or 0.0, 4)
        out["rust_pct_of_raw"] = round(rust_tps / raw_tps, 4) if raw_tps else None
        out["rust_speedup_over_py"] = (
            round(rust_tps / py_tps, 2) if py_tps else None
        )
    else:
        out["rust_reader_tokens_per_sec"] = None
        out["note"] += " — tset_rs not installed; rust path skipped"
    return out


def benchmark_compliance(records: list[CorpusRecord]) -> dict:
    """Benchmark D: three queries against the manifest + columns."""
    tset_path = os.path.join(CORPUS_DIR, "compliance.tset")
    _write_tset(records, tset_path)

    out: dict = {"benchmark": "D_compliance", "queries": {}}
    with Reader(tset_path) as r:
        cols = r.metadata_columns()

        t0 = time.perf_counter()
        nyt_rows = cols.filter_sql_like("source_url LIKE '%example.com%'")
        out["queries"]["url_pattern"] = {
            "matches": len(nyt_rows),
            "seconds": round(time.perf_counter() - t0, 4),
        }

        t0 = time.perf_counter()
        low_rows = cols.filter_sql_like("quality_score < 0.3")
        out["queries"]["quality_filter"] = {
            "matches": len(low_rows),
            "seconds": round(time.perf_counter() - t0, 4),
        }

        t0 = time.perf_counter()
        ai_act = _eu_ai_act_summary(r)
        out["queries"]["eu_ai_act_summary"] = {
            "fields": list(ai_act.keys()),
            "seconds": round(time.perf_counter() - t0, 4),
        }
        out["eu_ai_act_template"] = ai_act
    return out


def _eu_ai_act_summary(reader: Reader) -> dict:
    cols = reader.metadata_columns()
    types = {}
    if "source_type" in cols.names():
        for v in cols.column("source_type"):
            if v is None:
                continue
            types[v] = types.get(v, 0) + 1
    langs = {}
    if "lang" in cols.names():
        for v in cols.column("lang"):
            if v is None:
                continue
            langs[v] = langs.get(v, 0) + 1
    return {
        "shard_id": reader.manifest["shard_id"],
        "snapshot_root": reader.manifest.get("smt_root", ""),
        "document_count": cols.row_count,
        "tokenization_views": reader.tokenizer_ids(),
        "source_type_distribution": types,
        "language_distribution": langs,
        "total_tokens_per_view": {
            v: reader.view_total_tokens(v) for v in reader.tokenizer_ids()
        },
        "_disclaimer": (
            "TSET produces these technical fields automatically. Whether the"
            " published summary satisfies any specific regulatory obligation"
            " is for legal counsel to determine — see RFC §5.7."
        ),
    }


def benchmark_exclusion(records: list[CorpusRecord]) -> dict:
    """Benchmark E: end-to-end non-inclusion proof workflow."""
    root = os.path.join(CORPUS_DIR, "excl-dataset")
    if os.path.exists(root):
        import shutil

        shutil.rmtree(root)
    marker_text = b"unique marker doc abc123 xyz789 to be excluded"
    docs = [r.text.encode("utf-8") for r in records]
    docs.insert(len(docs) // 2, marker_text)

    t0 = time.perf_counter()
    dw = DatasetWriter(root)
    chunk_size = max(1, len(docs) // 4)
    chunks = [docs[i : i + chunk_size] for i in range(0, len(docs), chunk_size)]
    for i, chunk in enumerate(chunks):
        with dw.shard_writer(f"part-{i:05d}") as sw:
            for d in chunk:
                sw.add_document(d)
            sw.add_tokenizer_view(ByteLevelTokenizer())
        dw.register_shard(f"part-{i:05d}")
    dw.close()
    build_seconds = time.perf_counter() - t0

    marker_hash = hash_bytes(marker_text)

    t0 = time.perf_counter()
    dw = DatasetWriter(root)
    ds = Dataset(root)
    for shard_path in ds.shard_paths():
        dw.register_shard(os.path.splitext(os.path.basename(shard_path))[0])
    dw.add_exclusion(marker_hash, "demo")
    dw.close()
    record_seconds = time.perf_counter() - t0

    t0 = time.perf_counter()
    ds2 = Dataset(root)
    proof = ds2.prove_non_inclusion(marker_hash)
    ok = ds2.verify_non_inclusion_proof(proof)
    proof_seconds = time.perf_counter() - t0

    t0 = time.perf_counter()
    streamed = bytearray()
    for batch, _ in ds2.stream_tokens("byte-level-v1", 4096):
        streamed.extend(batch.astype("uint8").tobytes())
    rest_seconds = time.perf_counter() - t0
    marker_absent = marker_text not in streamed

    return {
        "benchmark": "E_exclusion",
        "shards": len(chunks),
        "documents": len(docs),
        "build_seconds": round(build_seconds, 4),
        "record_exclusion_seconds": round(record_seconds, 4),
        "proof_seconds": round(proof_seconds, 4),
        "regenerate_stream_seconds": round(rest_seconds, 4),
        "non_inclusion_proof_verifies": ok,
        "marker_absent_after_exclusion": marker_absent,
        "v02_target_seconds": 600,
        "total_end_to_end_seconds": round(
            build_seconds + record_seconds + proof_seconds + rest_seconds, 4
        ),
    }


_BENCHMARKS: dict[str, Callable[[list[CorpusRecord]], dict]] = {
    "storage": benchmark_storage,
    "tokenizer_swap": benchmark_tokenizer_swap,
    "streaming": benchmark_streaming,
    "compliance": benchmark_compliance,
    "exclusion": benchmark_exclusion,
}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--benchmark",
        choices=list(_BENCHMARKS.keys()) + ["all"],
        default="all",
    )
    p.add_argument("--corpus-size-mb", type=float, default=2.0)
    p.add_argument("--seed", type=int, default=0)
    p.add_argument("--quick", action="store_true", help="alias for --corpus-size-mb 0.5")
    args = p.parse_args(argv)

    if args.quick:
        args.corpus_size_mb = 0.5

    target_bytes = int(args.corpus_size_mb * 1024 * 1024)
    print(f"Generating {args.corpus_size_mb} MB corpus (seed={args.seed})...")
    records = generate(target_bytes, seed=args.seed)
    print(f"  {len(records)} documents, {total_text_bytes(records)} text bytes")

    bench_names = list(_BENCHMARKS.keys()) if args.benchmark == "all" else [args.benchmark]
    out: dict = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "corpus_size_mb": args.corpus_size_mb,
        "documents": len(records),
        "results": {},
    }
    for name in bench_names:
        print(f"Running benchmark {name}...")
        out["results"][name] = _BENCHMARKS[name](records)
        print(f"  {json.dumps(out['results'][name], indent=2, default=str)}")

    out_path = os.path.join(
        RESULTS_DIR,
        f"benchmark-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}.json",
    )
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, sort_keys=True, default=str)
    print(f"Wrote {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
