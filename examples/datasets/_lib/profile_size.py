"""Profile the byte breakdown of a TSET shard.

Reports the size contribution of each region:

- header              (fixed)
- doc store           (zstd-compressed text blocks)
- tokenizer views     (bit-packed token IDs + per-chunk content_hash)
- TSMT/TLOG/TCOL      (binary sections, if v0.3.2 sections enabled)
- manifest            (canonical JSON tail)
- footer              (fixed)

Useful for answering "why is TSET 5x JSONL?" — we add up each region
explicitly so the answer is provable from the file, not asserted.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from tset.constants import HEADER_SIZE
from tset.reader import Reader


FOOTER_SIZE = 64  # mirrors header per SPEC


def profile(path: Path) -> dict[str, Any]:
    total = path.stat().st_size
    r = Reader(str(path))
    m = r.manifest

    # ── Doc store ────────────────────────────────────────────────
    blocks = m.get("document_store", {}).get("blocks", []) or []
    doc_store_bytes = sum(b.get("compressed_size", 0) for b in blocks)

    # ── Tokenizer views (dict keyed by tokenizer_id) ─────────────
    view_bytes: dict[str, int] = {}
    chunk_hash_bytes: dict[str, int] = {}
    for vid, v in (m.get("tokenization_views") or {}).items():
        # view_size is recorded by the writer as the total on-disk
        # extent of the view (chunk headers + bit-packed tokens).
        # Fall back to summing chunks if absent.
        if "view_size" in v:
            view_bytes[vid] = v["view_size"]
        else:
            view_bytes[vid] = sum(c.get("compressed_size", 0) for c in v.get("chunks", []))
        # 32 bytes of content_hash per chunk live inside the manifest,
        # but the chunks-on-disk also carry hash material in their
        # framing. We surface chunk count here for the report.
        chunk_hash_bytes[vid] = 32 * len(v.get("chunks", []))

    # ── Optional binary sections (v0.3.2) ────────────────────────
    # The writer registers each section with its own manifest key. v0.4
    # is expected to consolidate these under "binary_sections".
    section_bytes: dict[str, int] = {}
    for key, label in (
        ("smt_section", "TSMT"),
        ("audit_log_section", "TLOG"),
        ("metadata_columns_section", "TCOL"),
    ):
        info = m.get(key)
        if info and "size" in info:
            section_bytes[label] = info["size"]
    # Also support the v0.4-shaped registry if it ever appears.
    bs = m.get("binary_sections") or {}
    for name in ("tsmt", "tlog", "tcol"):
        info = bs.get(name)
        if info and "length" in info:
            section_bytes[name.upper()] = info["length"]

    # ── Manifest = whatever's left between the last on-disk region
    #     and the footer. We compute it as a residual so it's exact. ─
    accounted = HEADER_SIZE + doc_store_bytes + sum(view_bytes.values()) + sum(section_bytes.values()) + FOOTER_SIZE
    manifest_bytes = max(0, total - accounted)

    rows: list[tuple[str, int]] = [
        ("header (fixed)", HEADER_SIZE),
        ("doc store (zstd-compressed text)", doc_store_bytes),
    ]
    for vid, sz in view_bytes.items():
        rows.append((f"view: {vid}", sz))
    for name, sz in section_bytes.items():
        rows.append((f"section: {name}", sz))
    rows.append(("manifest (canonical JSON tail)", manifest_bytes))
    rows.append(("footer (fixed)", FOOTER_SIZE))

    return {
        "path": str(path),
        "total": total,
        "rows": rows,
        "view_chunk_counts": {
            vid: len((m.get("tokenization_views") or {}).get(vid, {}).get("chunks", []))
            for vid in view_bytes
        },
        "doc_store_blocks": len(blocks),
        "doc_store_uncompressed": sum(b.get("uncompressed_size", 0) for b in blocks),
    }


def render_markdown(prof: dict[str, Any]) -> str:
    total = prof["total"]
    out = [
        "| Region | Bytes | % of file |",
        "|---|---:|---:|",
    ]
    for label, sz in prof["rows"]:
        pct = (sz / total * 100.0) if total else 0.0
        out.append(f"| {label} | {sz:,} | {pct:5.1f}% |")
    out.append(f"| **total** | **{total:,}** | **100.0%** |")
    return "\n".join(out)
