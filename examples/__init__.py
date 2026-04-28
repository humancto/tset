"""Make the in-tree ``tset`` package importable when running examples
via ``python -m examples.datasets.<name>.<script>`` from the repo root.

The Python package ships at ``<repo>/python/tset`` and is not pip-installed
in this environment; the test suite uses the same trick from
``python/tests/conftest.py``.
"""

import os
import sys as _sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_PYTHON_DIR = os.path.normpath(os.path.join(_HERE, os.pardir, "python"))
if os.path.isdir(_PYTHON_DIR) and _PYTHON_DIR not in _sys.path:
    _sys.path.insert(0, _PYTHON_DIR)
