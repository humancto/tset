"""TSET — open standard for LLM training data.

The public API is assembled progressively as modules land. Top-level imports
that have not yet shipped are guarded so partial trees still import cleanly.
"""

from tset.constants import VERSION_MAJOR, VERSION_MINOR, MAGIC_HEADER, MAGIC_FOOTER

__all__ = ["VERSION_MAJOR", "VERSION_MINOR", "MAGIC_HEADER", "MAGIC_FOOTER"]

# Sourced from the installed package metadata so a single source of truth
# (pyproject.toml) drives both the wheel name and __version__.
try:
    from importlib.metadata import version as _pkg_version

    __version__ = _pkg_version("tset")
except Exception:  # noqa: BLE001 — uninstalled tree, fall back to wire spec
    __version__ = f"{VERSION_MAJOR}.{VERSION_MINOR}.0+local"


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
