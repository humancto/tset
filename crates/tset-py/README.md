# tset-py

[![Crates.io](https://img.shields.io/crates/v/tset-py.svg)](https://crates.io/crates/tset-py)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/humancto/tset/blob/main/LICENSE)

PyO3 bindings for [`tset-core`](https://crates.io/crates/tset-core) — the
native engine behind the [`tset`](https://pypi.org/project/tset/) Python
wheel.

You almost certainly do not want to use this crate directly. End users:

```bash
pip install tset
```

That ships a prebuilt wheel of `tset-py` (exposed as the Python module
`tset_rs`) plus the pure-Python reference implementation under `tset`.
The Python package auto-delegates hot paths to `tset_rs` when present.

## Direct use (from another Rust crate)

```toml
[dependencies]
tset-py = "0.3"
```

The `rlib` form is exported, but the only thing you'll find here is
PyO3-flavored type wrappers around `tset-core`. For native Rust use,
go straight to [`tset-core`](https://crates.io/crates/tset-core).

## License

MIT.
