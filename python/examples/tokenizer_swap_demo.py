"""Append a second tokenization view to an existing .tset shard.

Demonstrates User Story 1 / Benchmark C: switching tokenizers without
re-reading source documents.
"""

from __future__ import annotations

import argparse
import sys
import time

from tset.reader import Reader
from tset.tokenizers import ByteLevelTokenizer, WhitespaceTokenizer
from tset.writer import append_tokenizer_view


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--shard", required=True, help="path to existing .tset")
    p.add_argument(
        "--add-tokenizer",
        choices=["byte-level", "whitespace"],
        required=True,
    )
    p.add_argument("--vocab-size", type=int, default=65536)
    args = p.parse_args(argv)

    tok = (
        ByteLevelTokenizer()
        if args.add_tokenizer == "byte-level"
        else WhitespaceTokenizer(vocab_size=args.vocab_size)
    )

    t0 = time.perf_counter()
    append_tokenizer_view(args.shard, tok)
    t1 = time.perf_counter()

    with Reader(args.shard) as r:
        ids = r.tokenizer_ids()
        totals = {v: r.view_total_tokens(v) for v in ids}
    print(
        {
            "shard": args.shard,
            "elapsed_sec": round(t1 - t0, 4),
            "views_after": ids,
            "total_tokens_per_view": totals,
        }
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
