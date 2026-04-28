"""Shared utilities for the TSET dataset showcase.

This package is intentionally small: a content-addressed download cache,
a license registry, and a tiny metrics layer. Each dataset under
``examples/datasets/<name>/`` builds on it.

Nothing here is part of the TSET public API. Treat it as test/example
infrastructure.
"""

from examples.datasets._lib.cache import Cache, cache_root, fetch
from examples.datasets._lib.licenses import LICENSES, License, license_for
from examples.datasets._lib.metrics import (
    format_bytes,
    format_duration,
    measure,
    Measurement,
)

__all__ = [
    "Cache",
    "cache_root",
    "fetch",
    "LICENSES",
    "License",
    "license_for",
    "format_bytes",
    "format_duration",
    "measure",
    "Measurement",
]
