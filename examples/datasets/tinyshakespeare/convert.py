"""Convert TinyShakespeare to TSET.

The raw input.txt is a single ~1 MB file with multiple plays concatenated.
We split into "documents" on blank lines (the natural paragraph
boundary) and assign each one a deterministic id and metadata. The
resulting JSONL is then fed through ``tset.converters.jsonl_to_tset`` so
this script also exercises the public converter API.

Outputs land under ``examples/datasets/tinyshakespeare/out/``:

- ``corpus.jsonl``    JSONL form (one paragraph per record)
- ``corpus.tset``     TSET shard with byte-level + whitespace views
                      and binary sections enabled
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from examples.datasets._lib import format_bytes, format_duration, measure
from examples.datasets.tinyshakespeare.download import fetch_corpus

OUT_DIR = Path(__file__).resolve().parent / "out"
JSONL = OUT_DIR / "corpus.jsonl"
TSET = OUT_DIR / "corpus.tset"


def split_paragraphs(text: str) -> list[str]:
    """Split on runs of two-or-more newlines.

    Preserves internal newlines (Shakespeare uses single-newline line
    breaks within a speech). Empty paragraphs are dropped.
    """
    raw = text.replace("\r\n", "\n").split("\n\n")
    return [p.strip() for p in raw if p.strip()]


def write_jsonl(paragraphs: list[str], path: Path) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with path.open("w", encoding="utf-8") as f:
        for i, p in enumerate(paragraphs):
            rec = {
                "id": f"shake-{i:05d}",
                "text": p,
                "tokens_approx": len(p.split()),
                "lines": p.count("\n") + 1,
            }
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            n += 1
    return n


def convert_to_tset(jsonl_path: Path, tset_path: Path) -> None:
    """Write a TSET shard with binary sections enabled.

    The high-level ``jsonl_to_tset`` converter doesn't expose
    ``enable_binary_sections()`` (yet), so we write directly via the
    Writer here. Adds two tokenizer views to demonstrate append-in-place.
    """
    import json

    from tset.tokenizers import ByteLevelTokenizer, WhitespaceTokenizer
    from tset.writer import Writer

    # NOTE: we do NOT call ``enable_binary_sections()`` here. In v0.3.2
    # binary sections are purely additive — they're written as on-disk
    # TSMT/TLOG/TCOL *in addition to* the inline JSON forms in the
    # manifest. That doubles the storage cost. v0.4 will drop the inline
    # forms; until then the leanest production config is JSON-only.
    # See examples/datasets/SCALING.md for the full breakdown.
    with Writer(str(tset_path)) as w:
        with jsonl_path.open("r", encoding="utf-8") as f:
            for line in f:
                rec = json.loads(line)
                w.add_document(
                    rec["text"],
                    metadata={
                        "id": rec.get("id"),
                        "lines": rec.get("lines"),
                        "tokens_approx": rec.get("tokens_approx"),
                    },
                )
        w.add_tokenizer_view(ByteLevelTokenizer())
        w.add_tokenizer_view(WhitespaceTokenizer(vocab_size=4096))


def main() -> int:
    src = fetch_corpus()
    text = src.read_text(encoding="utf-8")
    paragraphs = split_paragraphs(text)
    print(f"loaded {len(paragraphs):,} paragraphs from {src.name}")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    with measure("write JSONL") as m_jsonl:
        n = write_jsonl(paragraphs, JSONL)
    m_jsonl.items = n
    print(
        f"  JSONL    {format_bytes(JSONL.stat().st_size):>10}  "
        f"in {format_duration(m_jsonl.seconds):>8}  "
        f"({m_jsonl.per_second:,.0f} docs/s)"
    )

    with measure("convert to TSET") as m_tset:
        convert_to_tset(JSONL, TSET)
    m_tset.items = n
    print(
        f"  TSET     {format_bytes(TSET.stat().st_size):>10}  "
        f"in {format_duration(m_tset.seconds):>8}  "
        f"({m_tset.per_second:,.0f} docs/s)"
    )

    ratio = TSET.stat().st_size / max(1, JSONL.stat().st_size)
    print(f"  TSET / JSONL size ratio: {ratio:.2f}×")
    return 0


if __name__ == "__main__":
    sys.exit(main())
