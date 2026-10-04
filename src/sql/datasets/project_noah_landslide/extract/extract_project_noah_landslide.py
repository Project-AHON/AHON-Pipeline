"""
AHON - Project NOAH Data Extraction

Standalone extraction script for one NOAH hazard dataset.

This stage only extracts source Shapefile components from the original ZIP
archives. It does not create GeoPackages, Parquet, or upload to Databricks.
"""

import io
import zipfile
from pathlib import Path

ARCHIVE_DIR = Path("data/raw/noah/archives")
DATASET_DIR = Path("data/raw/noah/datasets")

ALLOWED_EXTENSIONS = {
    ".shp", ".shx", ".dbf", ".prj", ".cpg",
    ".qix", ".sbn", ".sbx", ".qpj",
}

COPY_CHUNK_SIZE = 1024 * 1024


def extract_nested_shapefile(
    outer_zip: zipfile.ZipFile,
    nested_zip_name: str,
    output_dir: Path,
) -> bool:
    output_dir.mkdir(parents=True, exist_ok=True)

    nested_name = Path(nested_zip_name).stem
    expected_shp = output_dir / f"{nested_name}.shp"

    if expected_shp.exists():
        print(f"    SKIP: {expected_shp.name} already exists.")
        return False

    nested_zip_data = outer_zip.read(nested_zip_name)
    extracted = False

    with zipfile.ZipFile(io.BytesIO(nested_zip_data)) as nested_zip:
        for file_name in nested_zip.namelist():
            file_path = Path(file_name)
            extension = file_path.suffix.lower()

            if extension not in ALLOWED_EXTENSIONS:
                continue

            output_path = output_dir / file_path.name

            if output_path.exists():
                print(f"      SKIP COMPONENT: {output_path.name}")
                continue

            print(f"      Extracting: {file_path.name}")

            with (
                nested_zip.open(file_name) as source,
                open(output_path, "wb") as destination,
            ):
                while True:
                    chunk = source.read(COPY_CHUNK_SIZE)
                    if not chunk:
                        break
                    destination.write(chunk)

            extracted = True

    return extracted

def extract_landslide() -> None:
    """Extract NOAH Landslide Hazard province ZIPs."""

    output_dir = (
        DATASET_DIR / "NOAH-Landslide-Hazard" / "LandslideHazards"
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    archive_files = sorted(ARCHIVE_DIR.glob("Landslide_*.zip"))

    if not archive_files:
        raise FileNotFoundError(
            f"No Landslide_*.zip files found in {ARCHIVE_DIR}"
        )

    extracted = 0
    skipped = 0

    for archive_path in archive_files:
        print()
        print(f"Processing landslide archive: {archive_path.name}")

        with zipfile.ZipFile(archive_path, "r") as outer_zip:
            province_archives = [
                name for name in outer_zip.namelist()
                if "LandslideHazards/" in name
                and name.lower().endswith(".zip")
            ]

            print(f"Found {len(province_archives)} province archives.")

            for province_zip in province_archives:
                print(f"  Dataset: {Path(province_zip).stem}")

                was_extracted = extract_nested_shapefile(
                    outer_zip, province_zip, output_dir
                )

                if was_extracted:
                    extracted += 1
                else:
                    skipped += 1

    print()
    print("Landslide extraction complete.")
    print(f"New datasets extracted: {extracted}")
    print(f"Existing datasets skipped: {skipped}")
    print(f"Output directory: {output_dir}")


if __name__ == "__main__":
    extract_landslide()
