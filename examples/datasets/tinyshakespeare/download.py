"""Fetch the TinyShakespeare corpus into the content-addressed cache.

Source: https://github.com/karpathy/char-rnn (public domain).
Idempotent: re-running is a no-op when the cache hit succeeds.
"""

from __future__ import annotations

from pathlib import Path

from examples.datasets._lib import fetch, license_for

URL = (
    "https://raw.githubusercontent.com/karpathy/char-rnn/master/"
    "data/tinyshakespeare/input.txt"
)
SHA256 = "86c4e6aa9db7c042ec79f339dcb96d42b0075e16b8fc2e86bf0ca57e2dc565ed"
DATASET = "tinyshakespeare"


def fetch_corpus() -> Path:
    license_for(DATASET)  # registry gate (raises if unregistered)
    return fetch(URL, SHA256)


def main() -> int:
    path = fetch_corpus()
    size = path.stat().st_size
    print(f"tinyshakespeare cached at {path}")
    print(f"  size       {size:,} bytes")
    print(f"  sha256     {SHA256}")
    print(f"  license    {license_for(DATASET).spdx}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
