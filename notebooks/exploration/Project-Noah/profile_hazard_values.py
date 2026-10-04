import json
import subprocess
from pathlib import Path

import pandas as pd

DATASET_DIR = Path("data/raw/noah/datasets")
PROFILE_DIR = Path("data/profiling/noah")


def get_hazard_values(
    file_path: Path,
    field_name: str,
) -> dict:
    """Read hazard-class values without loading geometry."""

    layer_name = file_path.stem

    result = subprocess.run(
        [
            "ogrinfo",
            "-json",
            "-geom=no",
            str(file_path),
            layer_name,
        ],
        capture_output=True,
        text=True,
        check=True,
    )

    data = json.loads(result.stdout)

    features = data.get("features", [])

    values = []

    for feature in features:
        properties = feature.get("properties", {})
        values.append(properties.get(field_name))

    counts = pd.Series(values).value_counts(
        dropna=False
    ).to_dict()

    return counts


def profile_flood() -> list[dict]:
    """Profile Var values in all flood files."""

    directory = (
        DATASET_DIR
        / "NOAH-Flood-Hazard"
        / "25-year"
    )

    records = []

    for file_path in sorted(directory.glob("*.shp")):

        print(f"Flood: {file_path.name}")

        counts = get_hazard_values(
            file_path,
            "Var",
        )

        records.append(
            {
                "hazard_type": "flood_25yr",
                "file_name": file_path.name,
                "field_name": "Var",
                "value_counts": json.dumps(
                    counts,
                    default=str,
                ),
            }
        )

    return records


def profile_landslide() -> list[dict]:
    """Profile LH values in all landslide files."""

    directory = (
        DATASET_DIR
        / "NOAH-Landslide-Hazard"
        / "LandslideHazards"
    )

    records = []

    for file_path in sorted(directory.glob("*.shp")):

        print(f"Landslide: {file_path.name}")

        counts = get_hazard_values(
            file_path,
            "LH",
        )

        records.append(
            {
                "hazard_type": "landslide",
                "file_name": file_path.name,
                "field_name": "LH",
                "value_counts": json.dumps(
                    counts,
                    default=str,
                ),
            }
        )

    return records


def profile_storm_surge() -> list[dict]:
    """Profile HAZ values in all storm surge files."""

    directory = (
        DATASET_DIR
        / "NOAH-Storm-Surge"
    )

    records = []

    for advisory_dir in sorted(
        directory.glob("SSA*")
    ):

        advisory_level = advisory_dir.name

        for file_path in sorted(
            advisory_dir.glob("*.shp")
        ):

            print(
                f"Storm Surge: "
                f"{advisory_level} - "
                f"{file_path.name}"
            )

            counts = get_hazard_values(
                file_path,
                "HAZ",
            )

            records.append(
                {
                    "hazard_type": "storm_surge",
                    "advisory_level": advisory_level,
                    "file_name": file_path.name,
                    "field_name": "HAZ",
                    "value_counts": json.dumps(
                        counts,
                        default=str,
                    ),
                }
            )

    return records


def save_profile(
    records: list[dict],
    output_name: str,
) -> None:
    """Save profiling results."""

    PROFILE_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path = (
        PROFILE_DIR
        / output_name
    )

    df = pd.DataFrame(records)

    df.to_csv(
        output_path,
        index=False,
    )

    print(
        f"\nProfile saved: "
        f"{output_path}"
    )


def main() -> None:
    """Profile hazard-class values."""

    print("=" * 80)
    print("NOAH HAZARD VALUE PROFILING")
    print("=" * 80)

    flood_records = profile_flood()

    save_profile(
        flood_records,
        "flood_25year_hazard_values.csv",
    )

    landslide_records = profile_landslide()

    save_profile(
        landslide_records,
        "landslide_hazard_values.csv",
    )

    storm_surge_records = profile_storm_surge()

    save_profile(
        storm_surge_records,
        "storm_surge_hazard_values.csv",
    )

    all_records = (
        flood_records
        + landslide_records
        + storm_surge_records
    )

    save_profile(
        all_records,
        "noah_hazard_values.csv",
    )

    print("\n" + "=" * 80)
    print("HAZARD VALUE PROFILING COMPLETE")
    print("=" * 80)

    print(
        f"Total files profiled: "
        f"{len(all_records)}"
    )


if __name__ == "__main__":
    main()