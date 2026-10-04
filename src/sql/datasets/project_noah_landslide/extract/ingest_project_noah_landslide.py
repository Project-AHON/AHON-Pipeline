from pathlib import Path
from datetime import datetime, timezone
import subprocess


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

LANDSLIDE_DIR = DATASET_DIR / "NOAH-Landslide-Hazard" / "LandslideHazards"
LANDSLIDE_OUTPUT = BRONZE_DIR / "landslide" / "landslide.gpkg"
LANDSLIDE_LAYER = "bronze_noah_landslide"


def ingest_landslide() -> None:
    """Ingest Landslide Shapefiles into Bronze.

    Actual source field:
        LH

    Documentation refers to:
        HAZ

    The actual source field LH is preserved.

    Hazard values:
        1 = Low
        2 = Medium
        3 = High
    """
    print("\n" + "=" * 80)
    print("INGESTING LANDSLIDE DATA")
    print("=" * 80)

    shapefiles = sorted(LANDSLIDE_DIR.glob("*.shp"))

    if not shapefiles:
        raise FileNotFoundError(
            f"No Landslide Shapefiles found in {LANDSLIDE_DIR}"
        )

    ensure_output_directory(LANDSLIDE_OUTPUT)

    first_file = True
    ingest_timestamp = get_ingest_timestamp()

    for shapefile in shapefiles:
        print(f"Landslide: {shapefile.name}")

        mode_args = [] if first_file else ["-update", "-append"]

        command = [
            "ogr2ogr",
            "-f", "GPKG",
            *mode_args,
            "-nln", LANDSLIDE_LAYER,
            "-nlt", GEOMETRY_TYPE,
            str(LANDSLIDE_OUTPUT),
            str(shapefile),
            "-t_srs", TARGET_CRS,
            "-sql",
            (
                "SELECT "
                "LH AS lh, "
                f"'{shapefile.name}' AS source_file, "
                "'LANDSLIDE' AS source_dataset, "
                "'NONE' AS scenario, "
                "'LH' AS source_field, "
                f"'{TARGET_CRS}' AS source_crs, "
                f"'{ingest_timestamp}' AS ingest_timestamp "
                "FROM "
                f'"{shapefile.stem}"'
            ),
        ]

        run_ogr2ogr(command)
        first_file = False

    print(f"\nLandslide Bronze complete: {LANDSLIDE_OUTPUT}")


def main() -> None:
    ingest_landslide()


if __name__ == "__main__":
    main()
