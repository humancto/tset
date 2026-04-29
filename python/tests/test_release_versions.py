"""Catch release-version drift on every PR.

The release workflow (`.github/workflows/release.yml`) cross-checks
the four version-bearing files against the tag at release time. By
running the same check on every CI run we catch drift earlier — a
maintainer can't accidentally bump one Cargo.toml and forget the
others, only to discover it when they push the tag.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def test_release_versions_in_sync():
    """``scripts/check-release-versions.py`` exits zero."""
    repo = Path(__file__).resolve().parents[2]
    script = repo / "scripts" / "check-release-versions.py"
    assert script.is_file(), f"missing {script}"
    result = subprocess.run(
        [sys.executable, str(script)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, (
        f"release-version drift detected:\n"
        f"--- stdout ---\n{result.stdout}\n"
        f"--- stderr ---\n{result.stderr}"
    )
