"""Convert a JSONL corpus into a single-shard .tset file.

Usage::

    python python/examples/jsonl_to_tset.py \\
        --input corpus.jsonl --output corpus.tset --tokenizer byte-level
"""

from __future__ import annotations

import argparse
import sys

from tset.converters import jsonl_to_tset
from tset.tokenizers import ByteLevelTokenizer, WhitespaceTokenizer


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True, help="path to input JSONL")
    p.add_argument("--output", required=True, help="path to output .tset")
    p.add_argument(
        "--tokenizer",
        choices=["byte-level", "whitespace"],
        default="byte-level",
    )
    p.add_argument("--vocab-size", type=int, default=65536)
    p.add_argument("--content-field", default="text")
    p.add_argument(
        "--metadata-field",
        action="append",
        default=[],
        help="JSONL field to lift into per-document metadata (repeat as needed)",
    )
    args = p.parse_args(argv)

    tok = (
        ByteLevelTokenizer()
        if args.tokenizer == "byte-level"
        else WhitespaceTokenizer(vocab_size=args.vocab_size)
    )
    result = jsonl_to_tset(
        args.input,
        args.output,
        tokenizer=tok,
        content_field=args.content_field,
        metadata_fields=args.metadata_field,
    )
    print(result)
    return 0


if __name__ == "__main__":
    sys.exit(main())
