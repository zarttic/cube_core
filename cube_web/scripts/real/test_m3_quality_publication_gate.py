"""Compatibility entrypoint for the M3 real-quality gate."""

from pathlib import Path
from runpy import run_path

if __name__ == "__main__":
    run_path(str(Path(__file__).parents[1] / "run_m3_quality_publication_gate.py"), run_name="__main__")
