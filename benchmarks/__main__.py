"""Re-export so `python -m benchmarks.harness ...` works."""
from benchmarks.harness.runner import main

if __name__ == "__main__":
    raise SystemExit(main())
