"""Convert the Click 8.1.7 Python source files into a TSET shard.

Each .py file becomes one TSET document; the file path is stored as
metadata. Demonstrates that TSET handles a non-natural-language corpus
(very different vocabulary distribution from prose) without changes.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from examples.datasets._lib import format_bytes, format_duration, measure
from examples.datasets.click_source.download import iter_python_files

OUT_DIR = Path(__file__).resolve().parent / "out"
JSONL = OUT_DIR / "corpus.jsonl"
TSET = OUT_DIR / "corpus.tset"


def write_jsonl(path: Path) -> int:
    files = iter_python_files()
    path.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with path.open("w", encoding="utf-8") as f:
        for name, content in files:
            try:
                text = content.decode("utf-8")
            except UnicodeDecodeError:
                continue
            rec = {
                "id": name,
                "text": text,
                "lines": text.count("\n") + 1,
                "size": len(content),
            }
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            n += 1
    return n


def convert_to_tset(jsonl_path: Path, tset_path: Path) -> None:
    from tset.tokenizers import ByteLevelTokenizer
    from tset.writer import Writer

    if tset_path.exists():
        tset_path.unlink()
    with Writer(str(tset_path)) as w:
        with jsonl_path.open("r", encoding="utf-8") as f:
            for line in f:
                rec = json.loads(line)
                w.add_document(
                    rec["text"],
                    metadata={"path": rec["id"], "lines": rec["lines"]},
                )
        w.add_tokenizer_view(ByteLevelTokenizer())


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with measure("write JSONL") as m_j:
        n = write_jsonl(JSONL)
    print(f"  {n} python files written to JSONL "
          f"({format_bytes(JSONL.stat().st_size)} in {format_duration(m_j.seconds)})")

    with measure("convert to TSET") as m_t:
        convert_to_tset(JSONL, TSET)
    size_t = TSET.stat().st_size
    size_j = JSONL.stat().st_size
    print(f"  TSET written     ({format_bytes(size_t)} in {format_duration(m_t.seconds)})")
    print(f"  TSET / JSONL ratio: {size_t / size_j:.2f}×")
    return 0


if __name__ == "__main__":
    sys.exit(main())
