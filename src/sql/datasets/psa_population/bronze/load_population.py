"""Compatibility entry point; requires the documented batch parameters."""
import runpy
from pathlib import Path

if __name__ == "__main__":
    runpy.run_path(str(Path(__file__).resolve().parents[5] / 'src/sql/datasets/psa_population/bronze/load_psa_population.py'), run_name="__main__")
