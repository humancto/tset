import os
import sys

# Make `tset` importable when running `pytest` from repo root or python/.
_HERE = os.path.dirname(os.path.abspath(__file__))
_PYTHON_DIR = os.path.dirname(_HERE)
if _PYTHON_DIR not in sys.path:
    sys.path.insert(0, _PYTHON_DIR)
