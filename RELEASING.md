# Releasing TSET

This is the maintainer recipe for cutting a release. The release workflow
([`.github/workflows/release.yml`](.github/workflows/release.yml)) does
everything once a `v*` tag is pushed; this doc covers the prep, the tag
push, and the post-release smoke test.

## What gets released

| Artifact | Where it lands | Trigger |
|---|---|---|
| `tset-core` crate | <https://crates.io/crates/tset-core> | tag push |
| `tset-cli` crate | <https://crates.io/crates/tset-cli> | tag push (after tset-core indexes) |
| `tset-py` crate | <https://crates.io/crates/tset-py> | tag push (after tset-core indexes) |
| `tset` Python wheel + sdist | <https://pypi.org/project/tset/> | tag push (after crates publish) |
| GitHub Pages refresh | <https://humancto.github.io/tset/> | merge to `main` (independent) |

## One-time setup

Already configured (do not re-do):

1. **GitHub Actions secret `CARGO_REGISTRY_TOKEN`** — token from
   <https://crates.io/settings/tokens> with the `publish-update` scope
   for the three crate names.
2. **PyPI Trusted Publishing** — go to
   <https://pypi.org/manage/project/tset/settings/publishing/> and
   register this repo's `release.yml` workflow as a trusted publisher.
   No API token needed; OIDC handles it.
3. **GitHub environment `pypi`** — repo Settings → Environments → `pypi`
   exists. Optionally require a maintainer reviewer before any deploy.

## Cutting a release

### 1. Pick the version

Crate version tracks the binary format version: a v0.3.x crate
implements the v0.3.x wire format. Patch component is reserved for
implementation fixes that do **not** change the wire format.

| Change | Bump |
|---|---|
| Wire format change (new section, new manifest field) | minor (0.3.2 → 0.4.0) |
| Implementation fix, same wire format | patch (0.3.2 → 0.3.3) |
| Breaking API change in `tset-core` Rust API but no wire change | patch + note in CHANGELOG |

### 2. Update versions in lockstep

The workspace version + the Python package version must match:

- `Cargo.toml` → `[workspace.package].version`
- `python/pyproject.toml` → `[project].version`
- `crates/tset-cli/Cargo.toml` → `tset-core = { path = ..., version = "<X>" }`
- `crates/tset-py/Cargo.toml`  → `tset-core = { path = ..., version = "<X>" }`

Run the pre-flight:

```bash
python scripts/check-release-versions.py 0.3.3
```

Output on a clean tree:

```
Versions in release-gated files:
  Cargo.toml [workspace.package]              0.3.3
  python/pyproject.toml [project]             0.3.3
  crates/tset-cli/Cargo.toml [tset-core dep]  0.3.3
  crates/tset-py/Cargo.toml [tset-core dep]   0.3.3

All four files agree: 0.3.3  (matches tag 0.3.3)
```

The same check runs in `python/tests/test_release_versions.py`, so CI
catches drift on every PR — not just at tag-push time. The release
workflow's `sanity` job re-runs it once more before any publish.

### 3. Update the changelog

Append a section to [`CHANGELOG.md`](CHANGELOG.md). Conventional shape:

```markdown
## v0.3.3 — 2026-04-30

### Added
…

### Fixed
…

### Wire format
- (none) | (changes)
```

### 4. Tag and push

From the merge commit on `main`:

```bash
git tag -s v0.3.3 -m "v0.3.3"
git push origin v0.3.3
```

The release workflow runs automatically. Watch
<https://github.com/humancto/tset/actions> — six jobs:

1. `sanity` — version match check.
2. `crates-core` — publishes `tset-core`.
3. `crates-downstream` (matrix) — waits for `tset-core` to index,
   then publishes `tset-cli` and `tset-py` in parallel.
4. `python-wheels` (matrix) — cibuildwheel across linux/macos/windows
   x x86_64/arm64.
5. `python-sdist` — pure source distribution.
6. `python-publish` — uploads wheels + sdist to PyPI via Trusted
   Publishing.

Total time: ~25 minutes on green.

### 5. Smoke test

```bash
# Crates
cargo install tset-cli
tset --version    # should print v0.3.3

# Python
python -m venv /tmp/tset-smoke && source /tmp/tset-smoke/bin/activate
pip install tset==0.3.3
python -c "import tset, tset_rs; print(tset.__version__)"
```

Both should report the new version. The Python smoke also confirms the
Rust extension wheel was actually included in the install.

## Recovering from a partial release

crates.io versions are immutable; re-publishing a published version
fails. The workflow has a `skip_crates` input that lets you re-run
**only the Python pipeline** after the crates have already gone out.

### Scenario A — crates published, Python wheels failed

Most common case: e.g. a flaky cibuildwheel runner, a PyPI Trusted
Publishing config typo. Re-run with crates skipped:

```bash
gh workflow run release.yml -f version=0.3.3 -f skip_crates=true
```

The `crates-core` and `crates-downstream` jobs are gated by
`if: inputs.skip_crates != true` and will be **skipped** (not failed).
`python-publish` accepts a skipped `crates-downstream` as long as
`python-wheels` and `python-sdist` succeed, so it still runs.

### Scenario B — `tset-core` published but `tset-cli` / `tset-py` failed

Rare (the waiter handles the typical race). If it does happen:

1. Manually publish the missing crate from a maintainer machine:
   ```bash
   CARGO_REGISTRY_TOKEN=... cargo publish \
     --manifest-path crates/tset-cli/Cargo.toml
   ```
2. Then run Scenario A to ship the Python artefacts.

### Scenario C — wrong version tagged

[Yank the bad
versions](https://doc.rust-lang.org/cargo/reference/publishing.html#cargo-yank)
on crates.io and on PyPI, bump to the next patch, and re-tag.
