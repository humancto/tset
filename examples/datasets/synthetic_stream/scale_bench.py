"""Run the storage + size-breakdown comparison at multiple scales.

Generates synthetic JSONL corpora at 1 MB, 10 MB, 100 MB (and optionally
1 GB if ``TSET_SYN_LARGE=1``), converts each to TSET in three configs:

- ``json-only``       no binary sections, 1 view (lean prod)
- ``json-only-2v``    no binary sections, 2 views
- ``v0.3.2-sections`` 2 views + binary sections (v0.4 forward-compat)

For each (size × config) we record the on-disk size and the per-region
breakdown via ``profile_size.profile``. The output JSON drives the
SCALING.md story.

Usage:

    python -m examples.datasets.synthetic_stream.scale_bench

This is intentionally not part of the standard ``make showcase`` target
because the 100 MB run takes ~2 minutes.
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

from examples.datasets._lib import format_bytes, format_duration, measure
from examples.datasets._lib.profile_size import profile
from examples.datasets.synthetic_stream.generate import generate

OUT = Path(__file__).resolve().parent / "out"
SCALES = [1_000_000, 10_000_000, 100_000_000]
if os.environ.get("TSET_SYN_LARGE") == "1":
    SCALES.append(1_000_000_000)


def _convert(jsonl: Path, dest: Path, *, two_views: bool, binary_sections: bool) -> dict:
    from tset.tokenizers import ByteLevelTokenizer, WhitespaceTokenizer
    from tset.writer import Writer

    if dest.exists():
        dest.unlink()
    n = 0
    t0 = time.perf_counter()
    with Writer(str(dest)) as w:
        if binary_sections:
            w.enable_binary_sections()
        with jsonl.open("r", encoding="utf-8") as f:
            for line in f:
                rec = json.loads(line)
                w.add_document(
                    rec["text"],
                    metadata={"lang": rec.get("lang"), "section": rec.get("section")},
                )
                n += 1
        w.add_tokenizer_view(ByteLevelTokenizer())
        if two_views:
            w.add_tokenizer_view(WhitespaceTokenizer(vocab_size=4096))
    return {
        "path": str(dest),
        "size": dest.stat().st_size,
        "documents_written": n,
        "write_seconds": time.perf_counter() - t0,
    }


def _zstd_size(jsonl: Path) -> int | None:
    try:
        import zstandard as zstd  # type: ignore

        return len(zstd.ZstdCompressor(level=3).compress(jsonl.read_bytes()))
    except ImportError:
        return None


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    results: list[dict] = []
    for target in SCALES:
        print(f"\n=== {format_bytes(target)} ===")
        jsonl = OUT / f"syn-{target}.jsonl"
        if not jsonl.exists() or jsonl.stat().st_size < target * 0.95:
            print(f"  generating {format_bytes(target)} JSONL …")
            with measure("gen") as m:
                info = generate(target, jsonl)
            print(
                f"    {info['documents']:,} docs · {format_bytes(jsonl.stat().st_size)} "
                f"in {format_duration(m.seconds)}"
            )
        else:
            print(f"  reusing {jsonl.name} ({format_bytes(jsonl.stat().st_size)})")

        zstd_size = _zstd_size(jsonl)

        configs = [
            ("json-only-1v", dict(two_views=False, binary_sections=False)),
            ("json-only-2v", dict(two_views=True, binary_sections=False)),
            ("v0.3.2-sections", dict(two_views=True, binary_sections=True)),
        ]
        per_target: dict[str, object] = {
            "target_bytes": target,
            "jsonl_size": jsonl.stat().st_size,
            "jsonl_zstd_size": zstd_size,
            "documents": _count_lines(jsonl),
            "configs": {},
        }
        for label, opts in configs:
            tset = OUT / f"syn-{target}.{label}.tset"
            with measure(label) as m:
                info = _convert(jsonl, tset, **opts)
            prof = profile(tset)
            per_target["configs"][label] = {
                "size": tset.stat().st_size,
                "write_seconds": m.seconds,
                "size_vs_jsonl": tset.stat().st_size / jsonl.stat().st_size,
                "size_vs_jsonl_zstd": (tset.stat().st_size / zstd_size) if zstd_size else None,
                "breakdown": [
                    {"region": region, "bytes": sz}
                    for region, sz in prof["rows"]
                ],
            }
            print(
                f"  {label:<22}  {format_bytes(tset.stat().st_size):>10}  "
                f"({tset.stat().st_size / jsonl.stat().st_size:.2f}× JSONL)  "
                f"in {format_duration(m.seconds)}"
            )
        results.append(per_target)

    out_json = OUT / "scale_bench.json"
    out_json.write_text(json.dumps(results, indent=2))
    print(f"\nresults written to {out_json}")
    return 0


def _count_lines(path: Path) -> int:
    n = 0
    with path.open("rb") as f:
        for _ in f:
            n += 1
    return n


if __name__ == "__main__":
    sys.exit(main())
