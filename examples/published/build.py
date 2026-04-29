"""Build the published TSET corpus.

Takes a deterministic slice of TinyShakespeare and ships it as a v0.3.2
TSET shard with a published Merkle root. Anyone can re-run this script
and confirm the same root — that's the whole point.

Determinism knobs (set automatically by this script):

  TSET_DETERMINISTIC_CREATED_AT   manifest.created_at
  TSET_DETERMINISTIC_SNAPSHOT_ID  snapshot id in audit log
  TSET_DETERMINISTIC_TIME         audit-log timestamps

Output:

  examples/published/corpus.tset            committed binary
  examples/published/PUBLISHED-ROOT.txt     human-readable receipt
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

# Determinism: keep these stable so the published root is reproducible.
# The values themselves are arbitrary; what matters is that anyone
# re-running the build with the same env produces the same bytes.
DETERMINISTIC_ENV = {
    "TSET_DETERMINISTIC_CREATED_AT": "2026-01-01T00:00:00+00:00",
    "TSET_DETERMINISTIC_SNAPSHOT_ID": "tset-published-shakespeare-v0001",
    "TSET_DETERMINISTIC_TIME": "1735689600.0",  # 2025-01-01 UTC
}

# How many paragraphs to keep from TinyShakespeare. Small enough that
# the .tset binary stays comfortably commitable; large enough that
# inclusion / non-inclusion proofs have meaningful tree depth (the SMT
# is fixed-depth 256, but the doc count makes the audit log nontrivial).
N_PARAGRAPHS = 200

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "corpus.tset"
RECEIPT = ROOT / "PUBLISHED-ROOT.txt"


def _ensure_deterministic_env() -> None:
    for k, v in DETERMINISTIC_ENV.items():
        os.environ[k] = v


def _bootstrap_path() -> None:
    repo_root = ROOT.parent.parent
    sys.path.insert(0, str(repo_root))
    sys.path.insert(0, str(repo_root / "python"))


def main() -> int:
    _ensure_deterministic_env()
    _bootstrap_path()

    from examples.datasets._lib.cache import fetch
    from examples.datasets.tinyshakespeare.convert import split_paragraphs
    from examples.datasets.tinyshakespeare.download import SHA256, URL
    from tset.reader import Reader
    from tset.tokenizers import ByteLevelTokenizer
    from tset.writer import Writer

    print("Fetching TinyShakespeare …")
    src = fetch(URL, SHA256)
    paragraphs = split_paragraphs(src.read_text(encoding="utf-8"))
    selected = paragraphs[:N_PARAGRAPHS]
    print(f"  using first {len(selected)} of {len(paragraphs):,} paragraphs")

    if OUT.exists():
        OUT.unlink()
    print(f"Writing {OUT} …")
    with Writer(str(OUT)) as w:
        for i, p in enumerate(selected):
            w.add_document(p.encode("utf-8"), metadata={"para_id": i})
        w.add_tokenizer_view(ByteLevelTokenizer())

    size = OUT.stat().st_size
    r = Reader(str(OUT))
    smt_root_hex = r.smt_root().hex()
    shard_merkle_root_hex = r.header.shard_merkle_root.hex()
    manifest_hash_hex = r.header.manifest_hash.hex()
    doc_count = len(list(r.doc_order_hex()))
    audit_entries = len(r.audit_log().entries)
    first_doc_hash_hex = next(iter(r.doc_order_hex()))

    receipt = (
        "TSET published-corpus receipt\n"
        "=============================\n"
        "\n"
        f"Built from        : github.com/karpathy/char-rnn (TinyShakespeare)\n"
        f"Source SHA-256    : {SHA256}\n"
        f"Paragraphs taken  : first {N_PARAGRAPHS} (of {len(paragraphs):,})\n"
        f"Tokenizer view    : byte-level-v1\n"
        f"\n"
        f"--- on-disk artefact ---\n"
        f"Path              : examples/published/corpus.tset\n"
        f"Size              : {size:,} bytes\n"
        f"\n"
        f"--- receipts ---\n"
        f"shard_merkle_root : {shard_merkle_root_hex}\n"
        f"smt_root          : {smt_root_hex}\n"
        f"manifest_hash     : {manifest_hash_hex}\n"
        f"document_count    : {doc_count}\n"
        f"audit_log_entries : {audit_entries}\n"
        f"first_doc_hash    : {first_doc_hash_hex}\n"
        f"\n"
        f"--- determinism ---\n"
        f"To reproduce, set:\n"
    )
    for k, v in DETERMINISTIC_ENV.items():
        receipt += f"  {k}={v}\n"
    receipt += (
        "\n"
        "Then run: python -m examples.published.build\n"
        "\n"
        "Re-running on the same Python + env yields a byte-identical\n"
        "corpus.tset and the same receipts above.\n"
    )
    RECEIPT.write_text(receipt)
    print(f"\nWrote {RECEIPT}")
    print("\n" + receipt)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
