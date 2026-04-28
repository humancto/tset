"""Fetch the Click 8.1.7 source tarball into the cache and unpack the
``.py`` files. Source: https://github.com/pallets/click (BSD-3-Clause).
"""

from __future__ import annotations

import io
import tarfile
from pathlib import Path

from examples.datasets._lib import fetch, license_for

URL = "https://github.com/pallets/click/archive/refs/tags/8.1.7.tar.gz"
SHA256 = "89251974dba8552b4e22990ca34adfb93a47ba7deb27fe7358a6661a09ca8793"
DATASET = "click_source"


def fetch_corpus() -> Path:
    license_for(DATASET)
    return fetch(URL, SHA256)


def iter_python_files() -> list[tuple[str, bytes]]:
    """Return ``(name, contents)`` for every .py file in the tarball."""
    tar_path = fetch_corpus()
    out: list[tuple[str, bytes]] = []
    with tarfile.open(tar_path, "r:gz") as tar:
        for member in tar:
            if not member.isfile() or not member.name.endswith(".py"):
                continue
            f = tar.extractfile(member)
            if f is None:
                continue
            out.append((member.name, f.read()))
    return out


def main() -> int:
    path = fetch_corpus()
    files = iter_python_files()
    total_bytes = sum(len(b) for _, b in files)
    print(f"click 8.1.7 cached at {path}")
    print(f"  python files     {len(files)}")
    print(f"  total size       {total_bytes:,} bytes")
    print(f"  license          {license_for(DATASET).spdx}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
