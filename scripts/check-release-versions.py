#!/usr/bin/env python3
"""Pre-flight version-sync check for cutting a release.

Reads the four version-bearing files and asserts they all agree.
Mirrors the `sanity` job in `.github/workflows/release.yml` but runs
locally so the maintainer catches drift BEFORE pushing a `v*` tag.

Usage::

    python scripts/check-release-versions.py            # report current versions
    python scripts/check-release-versions.py 0.3.2      # also check tag matches

Exits non-zero on any mismatch.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def workspace_version() -> str | None:
    """Cargo.toml [workspace.package].version"""
    text = (ROOT / "Cargo.toml").read_text()
    in_section = False
    for line in text.splitlines():
        if line.strip().startswith("[workspace.package]"):
            in_section = True
            continue
        if in_section and line.strip().startswith("["):
            in_section = False
        if in_section:
            m = re.match(r'\s*version\s*=\s*"([^"]+)"', line)
            if m:
                return m.group(1)
    return None


def pyproject_version() -> str | None:
    """python/pyproject.toml [project].version"""
    text = (ROOT / "python" / "pyproject.toml").read_text()
    in_project = False
    for line in text.splitlines():
        s = line.strip()
        if s == "[project]":
            in_project = True
            continue
        if in_project and s.startswith("[") and s != "[project]":
            in_project = False
        if in_project:
            m = re.match(r'\s*version\s*=\s*"([^"]+)"', line)
            if m:
                return m.group(1)
    return None


def path_dep_version(manifest: Path) -> str | None:
    """Pin on `tset-core = { path = ..., version = "..." }`."""
    text = manifest.read_text()
    m = re.search(r'tset-core\s*=\s*\{[^}]*version\s*=\s*"([^"]+)"', text)
    return m.group(1) if m else None


def main(argv: list[str]) -> int:
    versions = {
        "Cargo.toml [workspace.package]": workspace_version(),
        "python/pyproject.toml [project]": pyproject_version(),
        "crates/tset-cli/Cargo.toml [tset-core dep]": path_dep_version(
            ROOT / "crates" / "tset-cli" / "Cargo.toml"
        ),
        "crates/tset-py/Cargo.toml [tset-core dep]": path_dep_version(
            ROOT / "crates" / "tset-py" / "Cargo.toml"
        ),
    }

    print("Versions in release-gated files:")
    pad = max(len(k) for k in versions)
    for k, v in versions.items():
        print(f"  {k:<{pad}}  {v or '(MISSING)'}")

    distinct = {v for v in versions.values() if v}
    if len(distinct) != 1:
        print("\nFAIL: versions disagree")
        return 1
    canonical = distinct.pop()

    if len(argv) >= 2:
        wanted = argv[1].lstrip("v")
        if wanted != canonical:
            print(f"\nFAIL: tag {argv[1]} != canonical version {canonical}")
            return 1
        print(f"\nAll four files agree: {canonical}  (matches tag {argv[1]})")
    else:
        print(f"\nAll four files agree: {canonical}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
