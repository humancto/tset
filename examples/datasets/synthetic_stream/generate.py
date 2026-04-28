"""Deterministic synthetic corpus generator.

Used for scaling experiments. Produces N documents whose total size is
~target_bytes, with realistic-ish structure (Zipf-like word frequency,
punctuation, line breaks). Same seed → byte-identical output.
"""

from __future__ import annotations

import hashlib
import json
import os
import random
from pathlib import Path

DEFAULT_SEED = 0xC0FFEE


# A small fixed vocabulary. Chosen to be representative of English-ish
# text, not to fool a real tokenizer benchmark.
_VOCAB = (
    "the of and to in a is that for it with as on at by an be this from "
    "which or but not are have was were has had they we you all about "
    "model training data tokens shard binary format proof verify hash "
    "record document audit log signed root tree leaf chunk view stream "
    "prove inclusion exclusion encoding decoding stable canonical "
    "frequency vocabulary deterministic reproducible byte-identical"
).split()


def _zipfian_sample(rng: random.Random, vocab: list[str], n: int) -> list[str]:
    weights = [1.0 / (i + 1) for i in range(len(vocab))]
    total = sum(weights)
    weights = [w / total for w in weights]
    cumulative: list[float] = []
    s = 0.0
    for w in weights:
        s += w
        cumulative.append(s)
    out: list[str] = []
    for _ in range(n):
        r = rng.random()
        # binary search would be cheaper; n is small enough for linear here
        for i, c in enumerate(cumulative):
            if r <= c:
                out.append(vocab[i])
                break
    return out


def make_document(rng: random.Random, target_words: int) -> str:
    n_paragraphs = max(1, rng.randint(1, 6))
    paragraphs: list[str] = []
    remaining = target_words
    for i in range(n_paragraphs):
        share = remaining // (n_paragraphs - i) if i < n_paragraphs - 1 else remaining
        words = _zipfian_sample(rng, _VOCAB, share)
        sentence_breaks = sorted(rng.sample(range(share), k=min(share // 8, share - 1)) if share > 1 else [])
        out: list[str] = []
        for j, w in enumerate(words):
            out.append(w)
            if j in sentence_breaks:
                out.append(rng.choice([". ", "; ", ", "]))
            else:
                out.append(" ")
        if out and out[-1] == " ":
            out[-1] = "."
        paragraphs.append("".join(out))
        remaining -= share
    return "\n".join(paragraphs)


def generate(target_bytes: int, out_path: Path, seed: int = DEFAULT_SEED) -> dict:
    """Write a JSONL corpus of approximately ``target_bytes`` to ``out_path``."""
    rng = random.Random(seed)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    bytes_written = 0
    n = 0
    h = hashlib.blake2b(digest_size=16)
    with out_path.open("w", encoding="utf-8") as f:
        while bytes_written < target_bytes:
            words = rng.randint(40, 320)  # avg ~180 words per doc
            text = make_document(rng, words)
            rec = {
                "id": f"syn-{n:08d}",
                "text": text,
                "lang": rng.choice(["en", "en", "en", "en", "fr"]),
                "section": rng.choice(["a", "b", "c"]),
            }
            line = json.dumps(rec, ensure_ascii=False) + "\n"
            line_bytes = line.encode("utf-8")
            f.write(line)
            bytes_written += len(line_bytes)
            h.update(line_bytes)
            n += 1
    return {
        "path": str(out_path),
        "documents": n,
        "bytes": out_path.stat().st_size,
        "seed": seed,
        "blake2b": h.hexdigest(),
    }


def main() -> int:
    target = int(os.environ.get("TSET_SYN_BYTES", "1000000"))
    out = Path(__file__).resolve().parent / "out" / f"syn-{target}.jsonl"
    if out.exists():
        out.unlink()
    info = generate(target, out)
    print(json.dumps(info, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
