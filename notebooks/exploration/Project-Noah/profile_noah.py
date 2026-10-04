import json
import subprocess
from pathlib import Path

import pandas as pd

DATASET_DIR = Path("data/raw/noah/datasets")
PROFILE_DIR = Path("data/profiling/noah")


def run_ogrinfo(file_path: Path) -> str:
    """Run ogrinfo summary output for one Shapefile."""

    layer_name = file_path.stem

    result = subprocess.run(
        [
            "ogrinfo",
            "-so",
            str(file_path),
            layer_name,
        ],
        capture_output=True,
        text=True,
        check=True,
    )

    return result.stdout


def parse_ogrinfo(
    output: str,
) -> dict:
    """Extract basic metadata from ogrinfo output."""

    result = {
        "geometry_type": None,
        "feature_count": None,
        "extent": None,
        "crs": None,
        "fields": [],
    }

    lines = output.splitlines()

    for line in lines:

        line = line.strip()

        if line.startswith("Geometry:"):
            result["geometry_type"] = (
                line.replace(
                    "Geometry:",
                    "",
                ).strip()
            )

        elif line.startswith("Feature Count:"):
            result["feature_count"] = int(
                line.replace(
                    "Feature Count:",
                    "",
                ).strip()
            )

        elif line.startswith("Extent:"):
            result["extent"] = (
                line.replace(
                    "Extent:",
                    "",
                ).strip()
            )

        elif line.startswith("Layer SRS WKT:"):
            result["crs"] = "EPSG:4326"

        elif ":" in line and not line.startswith(
            (
                "INFO:",
                "Metadata:",
                "Data axis",
                "Layer name",
            )
        ):
            field_name = line.split(":")[0].strip()

            if field_name and field_name not in {
                "Geometry",
                "Extent",
                "Feature Count",
            }:
                result["fields"].append(
                    field_name
                )

    return result


def profile_file(
    file_path: Path,
    hazard_type: str,
    advisory_level: str | None = None,
) -> dict:
    """Profile one Shapefile using GDAL."""

    print(
        f"Profiling {hazard_type}: "
        f"{file_path.name}"
    )

    ogr_output = run_ogrinfo(file_path)

    metadata = parse_ogrinfo(
        ogr_output
    )

    return {
        "hazard_type": hazard_type,
        "advisory_level": advisory_level,
        "file_name": file_path.name,
        "file_path": str(file_path),
        "file_size_mb": round(
            file_path.stat().st_size
            / (1024 * 1024),
            2,
        ),
        "feature_count": metadata[
            "feature_count"
        ],
        "geometry_type": metadata[
            "geometry_type"
        ],
        "extent": metadata["extent"],
        "crs": metadata["crs"],
        "fields": json.dumps(
            metadata["fields"]
        ),
    }


def profile_flood() -> list[dict]:
    """Profile all 25-year flood Shapefiles."""

    directory = (
        DATASET_DIR
        / "NOAH-Flood-Hazard"
        / "25-year"
    )

    records = []

    for file_path in sorted(
        directory.glob("*.shp")
    ):
        records.append(
            profile_file(
                file_path,
                hazard_type="flood_25yr",
            )
        )

    return records


def profile_landslide() -> list[dict]:
    """Profile all Landslide Shapefiles."""

    directory = (
        DATASET_DIR
        / "NOAH-Landslide-Hazard"
        / "LandslideHazards"
    )

    records = []

    for file_path in sorted(
        directory.glob("*.shp")
    ):
        records.append(
            profile_file(
                file_path,
                hazard_type="landslide",
            )
        )

    return records


def profile_storm_surge() -> list[dict]:
    """Profile all Storm Surge Shapefiles."""

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

            records.append(
                profile_file(
                    file_path,
                    hazard_type="storm_surge",
                    advisory_level=advisory_level,
                )
            )

    return records


def save_profile(
    records: list[dict],
    output_name: str,
) -> None:
    """Save profiling results to CSV."""

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


def print_summary(
    records: list[dict],
    name: str,
) -> None:
    """Print summary information."""

    df = pd.DataFrame(records)

    print("\n" + "=" * 80)
    print(
        f"{name.upper()} PROFILE SUMMARY"
    )
    print("=" * 80)

    if df.empty:
        print("No files found.")
        return

    print(
        f"Files profiled: {len(df)}"
    )

    print(
        f"Total features: "
        f"{df['feature_count'].sum():,}"
    )

    print("\nGeometry types:")

    print(
        df["geometry_type"]
        .value_counts()
        .to_string()
    )

    print("\nCRS:")

    print(
        df["crs"]
        .value_counts()
        .to_string()
    )

    print("\nField structures:")

    print(
        df["fields"]
        .value_counts()
        .to_string()
    )

    print("\nLargest files:")

    print(
        df[
            [
                "file_name",
                "file_size_mb",
                "feature_count",
            ]
        ]
        .sort_values(
            "file_size_mb",
            ascending=False,
        )
        .head(10)
        .to_string(index=False)
    )


def main() -> None:
    """Profile all NOAH hazard datasets."""

    print("=" * 80)
    print("NOAH DATA PROFILING")
    print("=" * 80)

    flood_records = profile_flood()

    save_profile(
        flood_records,
        "flood_25year_profile.csv",
    )

    print_summary(
        flood_records,
        "flood_25year",
    )

    landslide_records = profile_landslide()

    save_profile(
        landslide_records,
        "landslide_profile.csv",
    )

    print_summary(
        landslide_records,
        "landslide",
    )

    storm_surge_records = profile_storm_surge()

    save_profile(
        storm_surge_records,
        "storm_surge_profile.csv",
    )

    print_summary(
        storm_surge_records,
        "storm_surge",
    )

    all_records = (
        flood_records
        + landslide_records
        + storm_surge_records
    )

    save_profile(
        all_records,
        "noah_all_hazards_profile.csv",
    )

    print("\n" + "=" * 80)
    print(
        "ALL NOAH PROFILING COMPLETE"
    )
    print("=" * 80)

    print(
        f"Total Shapefiles profiled: "
        f"{len(all_records)}"
    )


if __name__ == "__main__":
    main()