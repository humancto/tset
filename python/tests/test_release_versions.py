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


REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "check-release-versions.py"


def _run(*args, cwd=None):
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        capture_output=True,
        text=True,
        check=False,
        cwd=str(cwd) if cwd else None,
    )


def test_release_versions_in_sync():
    """``scripts/check-release-versions.py`` exits zero on the real tree."""
    assert SCRIPT.is_file()
    result = _run()
    assert result.returncode == 0, (
        f"release-version drift detected:\n"
        f"--- stdout ---\n{result.stdout}\n"
        f"--- stderr ---\n{result.stderr}"
    )


def test_unparseable_file_fails_loudly(tmp_path):
    """Regression for Codex P1 on PR #10.

    Pre-fix the script silently dropped ``None`` values from the
    distinct-set check, so a file we couldn't parse a version out of
    would still pass the gate as long as the remaining files agreed.
    The fix treats any missing parse as a hard failure.

    Build a fake repo that mimics the real layout but has an
    unparseable Cargo.toml (no [workspace.package] block at all).
    Run the script with `cwd=fake_repo` would not work because the
    script resolves ROOT relative to itself; instead we run a
    micro-repo through a temp copy of the script's logic.
    """
    # Easiest end-to-end check: import the script as a module and
    # exercise its pure functions without spawning a subprocess.
    import importlib.util

    spec = importlib.util.spec_from_file_location("crv", SCRIPT)
    assert spec is not None and spec.loader is not None
    crv = importlib.util.module_from_spec(spec)
    # The module reads ROOT at import time; monkey-patch via runtime.
    spec.loader.exec_module(crv)

    # Build a fake tree with one unparseable Cargo.toml + a valid
    # python/pyproject.toml + valid path-dep manifests.
    (tmp_path / "Cargo.toml").write_text(
        '# no [workspace.package] block here\nname = "x"\n'
    )
    (tmp_path / "python").mkdir()
    (tmp_path / "python" / "pyproject.toml").write_text(
        '[project]\nname = "tset"\nversion = "0.3.2"\n'
    )
    (tmp_path / "crates" / "tset-cli").mkdir(parents=True)
    (tmp_path / "crates" / "tset-cli" / "Cargo.toml").write_text(
        '[package]\nname = "tset-cli"\n'
        '[dependencies]\n'
        'tset-core = { path = "../tset-core", version = "0.3.2" }\n'
    )
    (tmp_path / "crates" / "tset-py").mkdir(parents=True)
    (tmp_path / "crates" / "tset-py" / "Cargo.toml").write_text(
        '[package]\nname = "tset-py"\n'
        '[dependencies]\n'
        'tset-core = { path = "../tset-core", version = "0.3.2" }\n'
    )

    crv.ROOT = tmp_path  # type: ignore[attr-defined]

    rc = crv.main([str(SCRIPT)])
    assert rc == 1, (
        "expected non-zero exit when one file has no parseable version; "
        "Codex P1 regression"
    )


def test_tag_mismatch_fails(tmp_path, monkeypatch):
    """Sanity: if the tag arg doesn't match the parsed canonical version,
    exit non-zero. This branch existed pre-fix but we add a test for it
    so a future refactor can't silently regress."""
    import importlib.util

    spec = importlib.util.spec_from_file_location("crv", SCRIPT)
    crv = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(crv)

    # Real tree, fake tag
    rc = crv.main([str(SCRIPT), "0.99.0"])
    assert rc == 1
