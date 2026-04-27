# Contributing to TSET

Thanks for your interest in TSET. This document is short on purpose.

## Status

TSET is at RFC v0.4 with a v0.1 reference implementation. The format is **not
frozen**. Expect breaking binary-layout changes until v1.0. Pin a specific
release if you intend to ship a reader.

## Reporting issues

Please use GitHub Issues. For format/spec ambiguities, attach:

- a minimal `.tset` file or the bytes that triggered the issue,
- the exact reader implementation + version,
- the section of [`SPEC.md`](SPEC.md) you believe is being violated.

## Pull requests

1. Open an issue first for anything beyond a one-line fix or doc tweak.
2. Run tests locally: `pytest python/tests/`.
3. Run benchmarks if you touched the writer/reader hot path:
   `python -m benchmarks.harness --quick`.
4. Match the existing code style: no comments unless the *why* is non-obvious;
   prefer plain functions over classes; avoid adding dependencies.

## Spec changes

Any change that affects the binary layout, manifest schema, or conformance
obligations requires:

- A short RFC entry under `spec/` referencing the GitHub issue.
- A bumped version in `SPEC.md` §8 (minor for additive, major for breaking).
- Test coverage demonstrating the change.

See [`governance/RFC_PROCESS.md`](governance/RFC_PROCESS.md) for the full
process.

## License

By contributing you agree that your contributions are MIT-licensed.
