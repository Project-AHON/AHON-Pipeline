import subprocess
from datetime import datetime, timezone
from pathlib import Path


def run_ogr2ogr(command: list[str]) -> None:
    """Run an ogr2ogr command."""
    subprocess.run(command, check=True)


def ensure_output_directory(output_path: Path) -> None:
    """Create the output directory if it does not exist."""
    output_path.parent.mkdir(parents=True, exist_ok=True)


def get_ingest_timestamp() -> str:
    """Return the UTC ingestion timestamp."""
    return datetime.now(timezone.utc).isoformat()


TARGET_CRS = "EPSG:4326"
GEOMETRY_TYPE = "MULTIPOLYGON25D"

DATASET_DIR = Path("data/raw/noah/datasets")
BRONZE_DIR = Path("data/bronze/noah")

STORM_SURGE_DIR = DATASET_DIR / "NOAH-Storm-Surge"
STORM_SURGE_OUTPUT = BRONZE_DIR / "storm_surge" / "storm_surge.gpkg"
STORM_SURGE_LAYER = "bronze_noah_storm_surge"


def ingest_storm_surge() -> None:
    """Ingest Storm Surge Shapefiles into Bronze.

    Source field:
        HAZ

    Advisory scenarios:
        SSA1, SSA2, SSA3, SSA4

    HAZ and advisory_level are kept separate because they
    represent different concepts.
    """
    print("\n" + "=" * 80)
    print("INGESTING STORM SURGE DATA")
    print("=" * 80)

    shapefiles = []

    for advisory_dir in sorted(STORM_SURGE_DIR.glob("SSA*")):
        advisory_level = advisory_dir.name

        for shapefile in sorted(advisory_dir.glob("*.shp")):
            shapefiles.append((advisory_level, shapefile))

    if not shapefiles:
        raise FileNotFoundError(
            f"No Storm Surge Shapefiles found in {STORM_SURGE_DIR}"
        )

    ensure_output_directory(STORM_SURGE_OUTPUT)

    first_file = True
    ingest_timestamp = get_ingest_timestamp()

    for advisory_level, shapefile in shapefiles:
        print(f"Storm Surge: {advisory_level} - {shapefile.name}")

        mode_args = [] if first_file else ["-update", "-append"]

        command = [
            "ogr2ogr",
            "-f", "GPKG",
            *mode_args,
            "-nln", STORM_SURGE_LAYER,
            "-nlt", GEOMETRY_TYPE,
            str(STORM_SURGE_OUTPUT),
            str(shapefile),
            "-t_srs", TARGET_CRS,
            "-sql",
            (
                "SELECT "
                "HAZ AS haz, "
                f"'{advisory_level}' AS advisory_level, "
                f"'{shapefile.name}' AS source_file, "
                "'STORM_SURGE' AS source_dataset, "
                "'HAZ' AS source_field, "
                f"'{TARGET_CRS}' AS source_crs, "
                f"'{ingest_timestamp}' AS ingest_timestamp "
                "FROM "
                f'"{shapefile.stem}"'
            ),
        ]

        run_ogr2ogr(command)
        first_file = False

    print(f"\nStorm Surge Bronze complete: {STORM_SURGE_OUTPUT}")


def main() -> None:
    ingest_storm_surge()


if __name__ == "__main__":
    main()
