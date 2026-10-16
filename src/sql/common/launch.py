"""Serverless Spark Python bootstrap: establish __file__ through runpy explicitly."""

import argparse
import runpy
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--entry-point", required=True)
    args, remaining = parser.parse_known_args()
    root = Path(args.repo_root).resolve()
    path = (root / args.entry_point).resolve()
    if not path.is_relative_to(root) or path.suffix != ".py" or not path.is_file():
        raise ValueError(
            "Entry point must be an existing Python file inside the bundle"
        )
    sys.path.insert(0, str(root / "src/sql/common"))
    sys.path.insert(0, str(path.parent))
    sys.argv = [str(path), *remaining]
    runpy.run_path(str(path), run_name="__main__")


if __name__ == "__main__":
    main()
