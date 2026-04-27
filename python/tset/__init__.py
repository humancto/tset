"""TSET — open standard for LLM training data."""

from tset.constants import VERSION_MAJOR, VERSION_MINOR, MAGIC_HEADER, MAGIC_FOOTER
from tset.writer import Writer
from tset.reader import Reader
from tset.tokenizers import (
    ByteLevelTokenizer,
    WhitespaceTokenizer,
    Tokenizer,
    get_tokenizer,
)
from tset.dataset import Dataset, DatasetWriter
from tset.mixture import WeightedSampler, Subset
from tset.smt import SparseMerkleTree, InclusionProof, NonInclusionProof
from tset.audit_log import AuditLog, AuditEvent
from tset.dataloader import DataLoader

__all__ = [
    "VERSION_MAJOR",
    "VERSION_MINOR",
    "MAGIC_HEADER",
    "MAGIC_FOOTER",
    "Writer",
    "Reader",
    "Tokenizer",
    "ByteLevelTokenizer",
    "WhitespaceTokenizer",
    "get_tokenizer",
    "Dataset",
    "DatasetWriter",
    "WeightedSampler",
    "Subset",
    "SparseMerkleTree",
    "InclusionProof",
    "NonInclusionProof",
    "AuditLog",
    "AuditEvent",
    "DataLoader",
]

__version__ = "0.1.0"
