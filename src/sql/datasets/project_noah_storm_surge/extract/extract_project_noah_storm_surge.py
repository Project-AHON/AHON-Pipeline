"""
AHON - Project NOAH Data Extraction

Standalone extraction script for one NOAH hazard dataset.

This stage only extracts source Shapefile components from the original ZIP
archives. It does not create GeoPackages, Parquet, or upload to Databricks.
"""

from pathlib import Path
import io
import zipfile


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

def extract_storm_surge() -> None:
    """Extract NOAH Storm Surge SSA1-SSA4 province ZIPs."""

    archive_path = ARCHIVE_DIR / "Storm-Surge_01.zip"
    output_base = DATASET_DIR / "NOAH-Storm-Surge"

    if not archive_path.exists():
        raise FileNotFoundError(
            f"Storm Surge archive not found: {archive_path}"
        )

    extracted = 0
    skipped = 0

    with zipfile.ZipFile(archive_path, "r") as outer_zip:
        province_archives = [
            name for name in outer_zip.namelist()
            if "StormSurgeAdvisory" in name
            and name.lower().endswith(".zip")
        ]

        print()
        print(
            f"Storm Surge province archives found: "
            f"{len(province_archives)}"
        )

        for province_zip in province_archives:
            path_parts = Path(province_zip).parts

            advisory_folder = next(
                (
                    part for part in path_parts
                    if part.startswith("StormSurgeAdvisory")
                ),
                None,
            )

            if advisory_folder is None:
                print(
                    f"    WARNING: Could not determine advisory "
                    f"level for {province_zip}"
                )
                continue

            advisory_level = advisory_folder.replace(
                "StormSurgeAdvisory", ""
            )

            output_dir = output_base / f"SSA{advisory_level}"
            output_dir.mkdir(parents=True, exist_ok=True)

            print()
            print(
                f"  Dataset: SSA{advisory_level} / "
                f"{Path(province_zip).stem}"
            )

            was_extracted = extract_nested_shapefile(
                outer_zip, province_zip, output_dir
            )

            if was_extracted:
                extracted += 1
            else:
                skipped += 1

    print()
    print("Storm Surge extraction complete.")
    print(f"New datasets extracted: {extracted}")
    print(f"Existing datasets skipped: {skipped}")
    print(f"Output directory: {output_base}")


if __name__ == "__main__":
    extract_storm_surge()
