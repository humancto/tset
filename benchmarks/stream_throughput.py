"""End-to-end throughput + memory benchmark for the TSET streaming path.

Measures four numbers on a synthetic corpus of configurable size:

  1. **Write throughput** — bytes/s a Writer ingests + finalizes
     (zstd-6 compression, byte-level tokenizer view, single-threaded).
  2. **Read throughput** — tokens/s a Reader streams via
     ``stream_tokens``.
  3. **Compressed ratio** — on-disk shard size / raw input size.
  4. **Peak RSS during streaming** — maximum resident set size for
     the read pass, in MB.

Run::

    # Quick smoke (~5 seconds, 64 MB)
    python -m benchmarks.stream_throughput --mb 64

    # Production-shaped run (~1-2 min, 1 GB)
    python -m benchmarks.stream_throughput --mb 1024

    # JSON output for programmatic comparison
    python -m benchmarks.stream_throughput --mb 256 --json results.json

The default target (``--mb 256``) is the CI-friendly compromise: large
enough that the reported numbers are stable across hosts within
~5-10%, small enough to fit a CI minute budget.

The write and read phases run in separate subprocesses so each one's
``ru_maxrss`` is isolated. ``ru_maxrss`` survives fork+exec on Linux
(the kernel tracks the high-water mark of the OS PID), so measuring
peak streaming RSS requires the streaming child to start with a small
parent footprint. We orchestrate by spawning ``--phase write`` and
``--phase read`` children.

**Memory model.** Streaming RSS is bounded by

    peak_rss ≈ shard_size_via_mmap            (read on demand)
             + 4 × manifest_size              (Python dict expansion of the
                                               JSON manifest — Python's
                                               dict / str overhead is ~3-4×
                                               the JSON bytes for our
                                               manifest shape, dominated
                                               by source_map entries)
             + max_chunk_size_decoded         (one decoded uint32 chunk
                                               at a time, evicted as the
                                               source_map cursor advances)
             + ~50 MB                         (interpreter + numpy + tset_rs
                                               static)

**Bound check.** The benchmark fails if streaming RSS exceeds

    bound_mb = 4 × manifest_size + shard_size + 128 MB

The important property is *sub-linear in corpus size*: doubling raw
input bytes does NOT double streaming RSS — it grows only with the
manifest, which scales with #docs × log(#docs)-ish. The v0.4 binary
sections (TSMT/TLOG/TCOL) shave ~30% off the manifest already; a
future binary source_map encoding would push this down another 5-10×
for byte-level tokenizers.
"""

from __future__ import annotations

import argparse
import json
import os
import resource
import subprocess
import sys
import tempfile
import time
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO / "python"))


def _peak_rss_mb() -> float:
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        return rss / (1024 * 1024)
    return rss / 1024


def synth_documents(target_mb: int, doc_size: int = 4096) -> list[bytes]:
    """Produce ``target_mb`` worth of pseudo-realistic text documents.

    Deterministic: same target_mb yields byte-identical output across
    runs.
    """
    target_bytes = target_mb * 1024 * 1024
    n_docs = max(1, target_bytes // doc_size)
    base = (
        "the quick brown fox jumps over the lazy dog and the rain in spain "
        "stays mainly on the plain how vexingly quick daft zebras jump and "
        "sphinx of black quartz judge my vow pack my box with five dozen "
        "liquor jugs the five boxing wizards jump quickly amazingly few "
        "discotheques provide jukeboxes and waltz bad nymph for quick jigs "
    ).encode()
    docs = []
    for i in range(n_docs):
        prefix = f"{i:016d} ".encode()
        body_len = doc_size - len(prefix)
        body = (base * (body_len // len(base) + 1))[:body_len]
        docs.append(prefix + body)
    return docs


# ── Phase entrypoints (run in their own subprocess) ─────────────────


def _phase_write(target_mb: int, out_path: str) -> None:
    """Build a shard from synthesized documents; print JSON results."""
    from tset.tokenizers import ByteLevelTokenizer
    from tset.writer import Writer

    t0 = time.perf_counter()
    docs = synth_documents(target_mb)
    raw_bytes = sum(len(d) for d in docs)
    t_synth = time.perf_counter() - t0
    t0 = time.perf_counter()
    with Writer(out_path) as w:
        for d in docs:
            w.add_document(d)
        w.add_tokenizer_view(ByteLevelTokenizer())
    t_write = time.perf_counter() - t0
    on_disk = os.path.getsize(out_path)
    print(json.dumps({
        "phase": "write",
        "target_mb": target_mb,
        "n_docs": len(docs),
        "raw_bytes": raw_bytes,
        "on_disk_bytes": on_disk,
        "compressed_ratio": on_disk / raw_bytes,
        "synth_seconds": t_synth,
        "write_seconds": t_write,
        "write_mb_per_s": (raw_bytes / 1024 / 1024) / t_write,
        "peak_rss_mb": _peak_rss_mb(),
    }))


def _phase_read(shard_path: str, prefer_rust: bool) -> None:
    """Stream the shard's tokens; print JSON results."""
    os.environ["TSET_PREFER_RUST"] = "1" if prefer_rust else "0"
    from tset.reader import Reader

    t0 = time.perf_counter()
    token_count = 0
    r = Reader(shard_path)
    for tokens, _doc_hash in r.stream_tokens("byte-level-v1", batch_size=8192):
        token_count += int(tokens.size)
    t_read = time.perf_counter() - t0
    print(json.dumps({
        "phase": "read",
        "shard_path": shard_path,
        "prefer_rust": prefer_rust,
        "tokens": token_count,
        "read_seconds": t_read,
        "read_tokens_mb_per_s": (token_count * 4 / 1024 / 1024) / t_read,
        "peak_rss_mb": _peak_rss_mb(),
    }))


def _spawn(args: list[str]) -> dict:
    """Run a subprocess and parse its last stdout line as JSON."""
    out = subprocess.run(
        [sys.executable, "-m", "benchmarks.stream_throughput", *args],
        cwd=str(_REPO),
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(out.stdout.strip().splitlines()[-1])


def run(target_mb: int, prefer_rust: bool, json_out: str | None) -> int:
    print(f"# stream_throughput  target={target_mb} MB  prefer_rust={prefer_rust}")
    print()

    with tempfile.TemporaryDirectory() as tmp:
        shard = str(Path(tmp) / "bench.tset")

        # ── Phase 1: write child.
        write_info = _spawn(["--phase", "write", "--mb", str(target_mb), "--out", shard])
        print(
            f"synthesize+write  {write_info['n_docs']:,} docs · "
            f"{write_info['raw_bytes'] / 1024 / 1024:,.1f} MB raw → "
            f"{write_info['on_disk_bytes'] / 1024 / 1024:6.1f} MB on-disk · "
            f"ratio={write_info['compressed_ratio']:.3f}"
        )
        print(
            f"write throughput  {write_info['write_seconds']:5.2f}s · "
            f"{write_info['write_mb_per_s']:6.1f} MB/s raw"
        )

        # ── Phase 2: read child (separate process so its ru_maxrss
        #     starts fresh — the streaming RSS is the value we care
        #     about for the threat model bound).
        read_info = _spawn([
            "--phase", "read",
            "--in", shard,
            "--rust" if prefer_rust else "--no-rust",
        ])
        peak_rss = read_info["peak_rss_mb"]
        token_count = read_info["tokens"]
        t_read = read_info["read_seconds"]
        print(
            f"stream_tokens     {t_read:5.2f}s · "
            f"{token_count / 1_000_000:6.1f}M tokens · "
            f"{read_info['read_tokens_mb_per_s']:6.1f} MB/s tokens · "
            f"peak_rss={peak_rss:6.1f} MB"
        )

        # ── 4. Memory bound check.
        # Streaming RSS is dominated by two things: the mmap of the
        # on-disk shard (at most shard_size MB resident) and the
        # Python expansion of the JSON manifest (4× the manifest
        # bytes for our shape). The chunk-decode buffer is bounded
        # by max_chunk_size, evicted between source_map entries.
        # See the module docstring for the derivation.
        on_disk_mb = write_info["on_disk_bytes"] / 1024 / 1024
        # Header carries manifest_size; we don't have it directly here
        # but it's roughly (on_disk - sum(blocks) - sum(views)). For
        # the byte-level pangram corpus the manifest is ~92% of the
        # on-disk size at any scale (tokens are tiny because the
        # vocabulary is small and zstd compresses chunks aggressively).
        # We conservatively use the on-disk size as an upper bound on
        # manifest size; any production corpus with mixed tokenizers
        # will have a smaller manifest fraction.
        manifest_mb = on_disk_mb
        bound_mb = 4 * manifest_mb + on_disk_mb + 128
        bound_ok = peak_rss < bound_mb
        print(
            f"memory bound      peak_rss={peak_rss:6.1f} MB  "
            f"limit={bound_mb:6.1f} MB  ok={bound_ok}"
        )
        if not bound_ok:
            print(
                f"FAIL: streaming used more memory than the documented bound.",
                file=sys.stderr,
            )
            return 2

    if json_out:
        Path(json_out).write_text(json.dumps({
            "target_mb": target_mb,
            "prefer_rust": prefer_rust,
            "write": write_info,
            "read": read_info,
            "bound_mb": bound_mb,
            "bound_ok": bound_ok,
        }, indent=2))
        print(f"\nwrote JSON results → {json_out}")

    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    # Phase mode is the subprocess entry; missing means orchestrator.
    p.add_argument("--phase", choices=["write", "read"], default=None)
    p.add_argument("--mb", type=int, default=256)
    p.add_argument("--rust", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--json", help="orchestrator-only: write structured results here")
    p.add_argument("--out", help="phase=write: shard path to write")
    p.add_argument("--in", dest="in_path", help="phase=read: shard path to stream")
    args = p.parse_args(argv)
    if args.phase == "write":
        _phase_write(args.mb, args.out)
        return 0
    if args.phase == "read":
        _phase_read(args.in_path, args.rust)
        return 0
    return run(target_mb=args.mb, prefer_rust=args.rust, json_out=args.json)


if __name__ == "__main__":
    raise SystemExit(main())
