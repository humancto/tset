"""Content-addressed download cache.

Files live under ``examples/datasets/.cache/by-sha256/<hex>``. Calling
``fetch(url, expected_sha256)`` returns the cached path; if the file is
absent it is downloaded atomically (``<file>.partial`` then renamed) and
verified before being made visible.

No external dependencies. ``urllib.request`` only.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import tempfile
import urllib.request
from dataclasses import dataclass
from pathlib import Path


def cache_root() -> Path:
    """Resolve the on-disk cache root.

    Honors ``TSET_SHOWCASE_CACHE`` if set, otherwise falls back to
    ``examples/datasets/.cache`` relative to the repo root.
    """
    env = os.environ.get("TSET_SHOWCASE_CACHE")
    if env:
        return Path(env).expanduser()
    return Path(__file__).resolve().parents[1] / ".cache"


@dataclass(frozen=True)
class Cache:
    root: Path

    def path_for(self, sha256_hex: str) -> Path:
        return self.root / "by-sha256" / sha256_hex

    def has(self, sha256_hex: str) -> bool:
        return self.path_for(sha256_hex).is_file()

    def store(self, src_path: Path, sha256_hex: str) -> Path:
        dest = self.path_for(sha256_hex)
        dest.parent.mkdir(parents=True, exist_ok=True)
        if dest.exists():
            return dest
        # rename is atomic on the same filesystem
        tmp = dest.with_suffix(".partial")
        shutil.move(str(src_path), str(tmp))
        tmp.rename(dest)
        return dest


def _hash_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(64 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def fetch(
    url: str,
    expected_sha256: str,
    *,
    cache: Cache | None = None,
    timeout: float = 30.0,
) -> Path:
    """Return a local path to ``url`` whose contents hash to
    ``expected_sha256``.

    Behavior:

    - If the cache already holds a file with that SHA, return it.
    - Otherwise download to a temp file, hash it, and only insert into
      the cache if the hash matches. This makes the cache strictly
      content-addressed: no poisoned entries.
    """
    cache = cache or Cache(cache_root())
    expected = expected_sha256.lower()
    if cache.has(expected):
        return cache.path_for(expected)

    cache.root.mkdir(parents=True, exist_ok=True)
    fd, tmp_str = tempfile.mkstemp(prefix="tset-fetch-", dir=cache.root)
    os.close(fd)
    tmp = Path(tmp_str)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "tset-showcase/0.1"})
        with urllib.request.urlopen(req, timeout=timeout) as resp, tmp.open("wb") as out:
            shutil.copyfileobj(resp, out)
        actual = _hash_file(tmp)
        if actual != expected:
            raise ValueError(
                f"sha256 mismatch for {url}: expected {expected}, got {actual}"
            )
        return cache.store(tmp, expected)
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass
