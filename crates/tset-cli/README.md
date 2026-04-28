# tset-cli

[![Crates.io](https://img.shields.io/crates/v/tset-cli.svg)](https://crates.io/crates/tset-cli)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/humancto/tset/blob/main/LICENSE)

The `tset` command-line tool — inspect, verify, and convert TSET shards.

## Install

```bash
cargo install tset-cli
```

This puts the `tset` binary on your `$PATH`.

## Usage

```bash
# Convert a JSONL corpus to TSET (with on-disk binary sections)
tset convert jsonl input.jsonl out.tset --binary-sections

# Inspect a shard (header, manifest, signing pubkey, sections present)
tset inspect out.tset

# Full integrity check: manifest hash, footer, Merkle root,
# audit-log signature, per-view byte-identical re-tokenization
tset verify out.tset
```

`tset --help` lists every subcommand.

## What it can do today

| Subcommand | Status |
|---|---|
| `inspect` | Header + manifest + section summary |
| `verify` | Full integrity check + per-view reproducibility |
| `convert jsonl` | JSONL → TSET (with optional `--binary-sections`) |

Roadmap: `tset diff`, `tset stats`, `tset add-exclusion`, `tset convert hf`.

## Resources

- **Homepage:** <https://humancto.github.io/tset/>
- **Showcase + measured numbers:** <https://humancto.github.io/tset/showcase/>
- **Spec:** [SPEC.md](https://github.com/humancto/tset/blob/main/SPEC.md)

## License

MIT.
