"""Land only the seven profiled BLGF workbooks from a pinned HF revision."""

import re
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "common"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bronze"))
from load_blgf_ldrrmf_annual_lgu import EXPECTED_ROWS, read_lgu_file
from runtime import (
    batch_is_complete,
    batch_path,
    completed_batch,
    parser,
    publish_batch,
)


def main():
    from huggingface_hub import snapshot_download

    arguments = parser(__doc__)
    arguments.add_argument("--hf-repo", required=True)
    arguments.add_argument("--hf-revision", required=True)
    arguments.add_argument("--hf-folder", default="data/ldrrmf")
    args = arguments.parse_args()
    if not re.fullmatch(r"[0-9a-f]{40}", args.hf_revision):
        raise ValueError("hf-revision must be a pinned full commit hash")
    if Path(args.hf_folder).is_absolute() or ".." in Path(args.hf_folder).parts:
        raise ValueError("hf-folder must stay within the source repository")
    destination = batch_path(args, "blgf_ldrrmf_annual_lgu")
    if batch_is_complete(destination):
        completed_batch(destination)
        return
    names = [f"FY{year}-LDRRMF-by-LGU.xlsx" for year in EXPECTED_ROWS]
    with tempfile.TemporaryDirectory() as temp:
        snapshot_download(
            repo_id=args.hf_repo,
            repo_type="dataset",
            revision=args.hf_revision,
            allow_patterns=[f"{args.hf_folder}/{name}" for name in names],
            local_dir=temp,
        )
        files = []
        for year, name in zip(EXPECTED_ROWS, names):
            file = Path(temp) / args.hf_folder / name
            count = len(read_lgu_file(file))
            if count != EXPECTED_ROWS[year]:
                raise ValueError(
                    f"FY{year}: row count differs from profile; no batch published"
                )
            files.append((file, count))
        publish_batch(destination, files)


if __name__ == "__main__":
    main()
