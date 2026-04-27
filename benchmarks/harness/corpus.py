"""Synthetic corpus generator.

Per RFC §7 the production benchmark corpus is a 10 GB subset of RedPajama-V2.
For local CI we generate a deterministic synthetic corpus that mirrors the
same shape (web/code/books/academic mix) at a configurable size.
"""

from __future__ import annotations

import json
import os
import random
from dataclasses import dataclass

_SOURCES = [
    ("web",      "https://example.com/", 0.40),
    ("code",     "https://github.com/",  0.20),
    ("book",     "https://library.org/", 0.20),
    ("academic", "https://arxiv.org/",   0.20),
]
_LANGS = ["en", "en", "en", "en", "fr", "de", "es", "ja"]


@dataclass
class CorpusRecord:
    text: str
    source_url: str
    source_type: str
    lang: str
    quality_score: float


def _make_text(rng: random.Random, target_bytes: int, source_type: str) -> str:
    if source_type == "code":
        snippets = [
            "def f(x):\n    return x + 1\n",
            "for i in range(10):\n    print(i)\n",
            "class A:\n    def __init__(self): self.v = 0\n",
            "import numpy as np\n",
        ]
    elif source_type == "academic":
        snippets = [
            "We propose a novel approach to ",
            "The dataset consists of ",
            "Our results demonstrate that ",
            "In conclusion, the proposed method ",
        ]
    elif source_type == "book":
        snippets = [
            "Once upon a time, in a far away land, ",
            "The day began like any other, ",
            "She looked out across the ",
            "He could not believe what he saw.\n",
        ]
    else:  # web
        snippets = [
            "Click here to learn more about ",
            "Posted by user123 on ",
            "Top 10 reasons why ",
            "BREAKING NEWS: ",
        ]
    out = []
    used = 0
    while used < target_bytes:
        s = rng.choice(snippets) + rng.choice(_LANGS) + " " + str(rng.randint(0, 999)) + "\n"
        out.append(s)
        used += len(s)
    return "".join(out)[:target_bytes]


def generate(target_bytes: int, seed: int = 0) -> list[CorpusRecord]:
    rng = random.Random(seed)
    records: list[CorpusRecord] = []
    bytes_so_far = 0
    doc_idx = 0
    while bytes_so_far < target_bytes:
        weights = [w for _, _, w in _SOURCES]
        st_index = rng.choices(range(len(_SOURCES)), weights=weights, k=1)[0]
        st_name, st_prefix, _ = _SOURCES[st_index]
        size = rng.randint(256, 4096)
        text = _make_text(rng, size, st_name)
        rec = CorpusRecord(
            text=text,
            source_url=f"{st_prefix}{doc_idx}",
            source_type=st_name,
            lang=rng.choice(_LANGS),
            quality_score=round(rng.random(), 3),
        )
        records.append(rec)
        bytes_so_far += len(text)
        doc_idx += 1
    return records


def write_jsonl(records: list[CorpusRecord], path: str) -> int:
    os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
    bytes_written = 0
    with open(path, "w", encoding="utf-8") as f:
        for r in records:
            line = json.dumps(
                {
                    "text": r.text,
                    "source_url": r.source_url,
                    "source_type": r.source_type,
                    "lang": r.lang,
                    "quality_score": r.quality_score,
                }
            )
            f.write(line + "\n")
            bytes_written += len(line) + 1
    return bytes_written


def total_text_bytes(records: list[CorpusRecord]) -> int:
    return sum(len(r.text.encode("utf-8")) for r in records)
