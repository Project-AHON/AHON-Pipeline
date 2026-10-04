"""Export Project NOAH Landslide Bronze GeoPackage to chunked Parquet."""

from pathlib import Path
import gc
import subprocess

import pyarrow.parquet as pq
import pyogrio

PROJECT_ROOT = Path(__file__).resolve().parents[3]
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze" / "noah"
EXPORT_DIR = PROJECT_ROOT / "data" / "export" / "noah"
PARQUET_DIR = Path.home() / "ahon-parquet"

INPUT_PATH = BRONZE_DIR / "landslide" / "landslide.gpkg"
LAYER = "bronze_noah_landslide"
TEMP_2D_PATH = EXPORT_DIR / "_temp" / "landslide_2d.gpkg"
OUTPUT_DIR = PARQUET_DIR / "landslide_chunks"
TARGET_CRS = "EPSG:4326"
ARROW_BATCH_SIZE = 25
CHUNK_SIZE = 100


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


def write_chunk(batches: list, chunk_number: int) -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / f"landslide_part_{chunk_number:03d}.parquet"

    writer = None
    rows = 0

    try:
        for batch in batches:
            if batch.num_rows == 0:
                continue

            if writer is None:
                writer = pq.ParquetWriter(
                    output_path,
                    batch.schema,
                    compression="snappy",
                )

            writer.write_batch(batch)
            rows += batch.num_rows
    finally:
        if writer is not None:
            writer.close()

    actual_rows = pq.ParquetFile(output_path).metadata.num_rows
    if actual_rows != rows:
        raise RuntimeError(
            f"Chunk {chunk_number} row-count mismatch: expected {rows}, found {actual_rows}"
        )

    print(f"Chunk {chunk_number:03d}: {actual_rows:,} rows -> {output_path}")
    return actual_rows


def export_landslide(expected_features: int) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for old_chunk in OUTPUT_DIR.glob("landslide_part_*.parquet"):
        old_chunk.unlink()

    rows_processed = 0
    chunk_number = 0
    chunk_batches = []
    chunk_rows = 0

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

            chunk_batches.append(batch)
            chunk_rows += batch.num_rows
            rows_processed += batch.num_rows

            if chunk_rows >= CHUNK_SIZE:
                chunk_number += 1
                write_chunk(chunk_batches, chunk_number)
                chunk_batches = []
                chunk_rows = 0
                gc.collect()

    if chunk_batches:
        chunk_number += 1
        write_chunk(chunk_batches, chunk_number)
        chunk_batches = []
        gc.collect()

    chunk_files = sorted(OUTPUT_DIR.glob("landslide_part_*.parquet"))
    total_rows = sum(pq.ParquetFile(path).metadata.num_rows for path in chunk_files)

    if total_rows != expected_features:
        raise RuntimeError(
            f"Landslide row-count mismatch: expected {expected_features:,}, found {total_rows:,}"
        )

    print("\nLANDSLIDE PARQUET EXPORT: PASS")
    print(f"Chunks: {len(chunk_files)}")
    print(f"Total rows: {total_rows:,}")
    print(f"Location: {OUTPUT_DIR}")


def main() -> None:
    source_info = get_source_info()
    expected_features = source_info["features"]
    create_2d_geopackage()
    validate_2d(expected_features)
    export_landslide(expected_features)


if __name__ == "__main__":
    main()
