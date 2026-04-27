"""TSET — open standard for LLM training data.

The public API is assembled progressively as modules land. Top-level imports
that have not yet shipped are guarded so partial trees still import cleanly.
"""

from tset.constants import VERSION_MAJOR, VERSION_MINOR, MAGIC_HEADER, MAGIC_FOOTER

__all__ = ["VERSION_MAJOR", "VERSION_MINOR", "MAGIC_HEADER", "MAGIC_FOOTER"]
__version__ = "0.1.0"


def _maybe(name: str, attrs: list[str]) -> None:
    try:
        mod = __import__(f"tset.{name}", fromlist=attrs)
    except ImportError:
        return
    for a in attrs:
        if hasattr(mod, a):
            globals()[a] = getattr(mod, a)
            __all__.append(a)


_maybe("hashing", ["hash_bytes", "hash_hex", "shard_merkle_root"])
_maybe(
    "tokenizers",
    [
        "Tokenizer",
        "ByteLevelTokenizer",
        "WhitespaceTokenizer",
        "get_tokenizer",
        "register_tokenizer",
        "verify_reproducibility",
    ],
)
_maybe("writer", ["Writer"])
_maybe("reader", ["Reader"])
_maybe("smt", ["SparseMerkleTree", "InclusionProof", "NonInclusionProof", "EMPTY_ROOT"])
_maybe("audit_log", ["AuditLog", "AuditEvent"])
_maybe("dataset", ["Dataset", "DatasetWriter"])
_maybe("mixture", ["WeightedSampler", "Subset"])
_maybe("dataloader", ["DataLoader"])
_maybe("columns", ["MetadataColumns"])
