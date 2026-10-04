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

FLOOD_DIR = DATASET_DIR / "NOAH-Flood-Hazard" / "25-year"
FLOOD_OUTPUT = BRONZE_DIR / "flood_25yr" / "flood_25yr.gpkg"
FLOOD_LAYER = "bronze_noah_flood"


def ingest_flood() -> None:
    """Ingest 25-year Flood Shapefiles into Bronze.

    Source field:
        Var

    Bronze field:
        var

    Hazard values:
        1 = Low
        2 = Medium
        3 = High
    """
    print("\n" + "=" * 80)
    print("INGESTING FLOOD DATA")
    print("=" * 80)

    shapefiles = sorted(FLOOD_DIR.glob("*.shp"))

    if not shapefiles:
        raise FileNotFoundError(
            f"No Flood Shapefiles found in {FLOOD_DIR}"
        )

    ensure_output_directory(FLOOD_OUTPUT)

    first_file = True
    ingest_timestamp = get_ingest_timestamp()

    for shapefile in shapefiles:
        print(f"Flood: {shapefile.name}")

        mode_args = [] if first_file else ["-update", "-append"]

        command = [
            "ogr2ogr",
            "-f", "GPKG",
            *mode_args,
            "-nln", FLOOD_LAYER,
            "-nlt", GEOMETRY_TYPE,
            str(FLOOD_OUTPUT),
            str(shapefile),
            "-t_srs", TARGET_CRS,
            "-sql",
            (
                "SELECT "
                "Var AS var, "
                f"'{shapefile.name}' AS source_file, "
                "'FLOOD' AS source_dataset, "
                "'25YEAR' AS scenario, "
                "'Var' AS source_field, "
                f"'{TARGET_CRS}' AS source_crs, "
                f"'{ingest_timestamp}' AS ingest_timestamp "
                "FROM "
                f'"{shapefile.stem}"'
            ),
        ]

        run_ogr2ogr(command)
        first_file = False

    print(f"\nFlood Bronze complete: {FLOOD_OUTPUT}")


def main() -> None:
    ingest_flood()


if __name__ == "__main__":
    main()
