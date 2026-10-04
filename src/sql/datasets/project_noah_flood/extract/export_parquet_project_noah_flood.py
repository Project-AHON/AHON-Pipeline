"""Export Project NOAH Flood 25-Year Bronze GeoPackage to Parquet."""

from pathlib import Path
import gc
import subprocess

import pyarrow.parquet as pq
import pyogrio

PROJECT_ROOT = Path(__file__).resolve().parents[3]
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze" / "noah"
EXPORT_DIR = PROJECT_ROOT / "data" / "export" / "noah"
PARQUET_DIR = Path.home() / "ahon-parquet"

INPUT_PATH = BRONZE_DIR / "flood_25yr" / "flood_25yr.gpkg"
LAYER = "bronze_noah_flood"
TEMP_2D_PATH = EXPORT_DIR / "_temp" / "flood_25yr_2d.gpkg"
OUTPUT_PATH = PARQUET_DIR / "flood_25yr.parquet"
TARGET_CRS = "EPSG:4326"
ARROW_BATCH_SIZE = 25


def run_ogr2ogr(command: list[str]) -> None:
    print("\nRunning ogr2ogr:")
    print(" ".join(command))
    subprocess.run(command, check=True)


def get_source_info() -> dict:
    if not INPUT_PATH.exists():
        raise FileNotFoundError(
            f"Bronze GeoPackage not found: {INPUT_PATH}\n"
            "Run ingest_noah.py first."
        )

    info = pyogrio.read_info(INPUT_PATH, layer=LAYER)
    print(f"Bronze input: {INPUT_PATH}")
    print(f"Layer: {LAYER}")
    print(f"Features: {info['features']:,}")
    print(f"Geometry: {info['geometry_type']}")
    print(f"CRS: {info['crs']}")

    if str(info["crs"]) != TARGET_CRS:
        raise RuntimeError(f"Expected CRS {TARGET_CRS}, found {info['crs']}")

    return info


def create_2d_geopackage() -> None:
    if TEMP_2D_PATH.exists():
        print(f"Reusing existing 2D GeoPackage: {TEMP_2D_PATH}")
        return

    TEMP_2D_PATH.parent.mkdir(parents=True, exist_ok=True)
    run_ogr2ogr([
        "ogr2ogr", "-f", "GPKG",
        str(TEMP_2D_PATH), str(INPUT_PATH), LAYER,
        "-dim", "XY",
        "-t_srs", TARGET_CRS,
        "-nln", LAYER,
        "-overwrite",
    ])


def validate_2d(expected_features: int) -> None:
    info = pyogrio.read_info(TEMP_2D_PATH, layer=LAYER)
    print(f"2D features: {info['features']:,}")
    print(f"2D geometry: {info['geometry_type']}")
    print(f"2D CRS: {info['crs']}")

    if info["features"] != expected_features:
        raise RuntimeError("2D GeoPackage feature count mismatch")
    if str(info["crs"]) != TARGET_CRS:
        raise RuntimeError("2D GeoPackage CRS mismatch")


def export_parquet(expected_features: int) -> None:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    if OUTPUT_PATH.exists():
        OUTPUT_PATH.unlink()

    writer = None
    rows_written = 0

    with pyogrio.open_arrow(
        TEMP_2D_PATH,
        layer=LAYER,
        batch_size=ARROW_BATCH_SIZE,
        use_pyarrow=True,
    ) as (metadata, reader):
        print(f"Arrow geometry type: {metadata['geometry_type']}")

        while True:
            try:
                batch = reader.read_next_batch()
            except StopIteration:
                break

            if batch.num_rows == 0:
                del batch
                continue

            if writer is None:
                writer = pq.ParquetWriter(
                    OUTPUT_PATH,
                    batch.schema,
                    compression="snappy",
                )

            writer.write_batch(batch)
            rows_written += batch.num_rows
            print(f"Written: {rows_written:,}/{expected_features:,}")
            del batch
            gc.collect()

    if writer is not None:
        writer.close()

    actual_rows = pq.ParquetFile(OUTPUT_PATH).metadata.num_rows
    if actual_rows != expected_features:
        raise RuntimeError(
            f"Parquet row-count mismatch: expected {expected_features:,}, found {actual_rows:,}"
        )

    print("\nFLOOD PARQUET EXPORT: PASS")
    print(f"Rows: {actual_rows:,}")
    print(f"Output: {OUTPUT_PATH}")


def main() -> None:
    source_info = get_source_info()
    expected_features = source_info["features"]
    create_2d_geopackage()
    validate_2d(expected_features)
    export_parquet(expected_features)


if __name__ == "__main__":
    main()
