from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import rasterio


# ============================================================
# CONFIGURATION
# ============================================================

CHIRPS_BASE_URL = (
    "https://data.chc.ucsb.edu/products/CHIRPS/v3.0/"
    "monthly/global/tifs"
)

# Databricks Volume
OUTPUT_DIR = Path(
    "/Volumes/ahon/reference/source/chc_chirps"
)

# AHON historical coverage
CHIRPS_START_YEAR = 2018
CHIRPS_START_MONTH = 1

# Expected CHIRPS raster structure
EXPECTED_WIDTH = 7200
EXPECTED_HEIGHT = 2400
EXPECTED_BANDS = 1
EXPECTED_DTYPE = "float32"
EXPECTED_CRS = "EPSG:4326"


# ============================================================
# 1. BUILD CHIRPS URL
# ============================================================

def build_chirps_url(year: int, month: int) -> str:
    """
    Build the URL for a CHIRPS v3 monthly GeoTIFF.
    """

    filename = (
        f"chirps-v3.0.{year}.{month:02d}.tif"
    )

    return f"{CHIRPS_BASE_URL}/{filename}"


# ============================================================
# 2. FIND LATEST AVAILABLE CHIRPS MONTH
# ============================================================

def get_latest_available_chirps_month():
    """
    Find the latest available CHIRPS monthly GeoTIFF.

    Starts from the current UTC year/month and checks
    backwards until an available dataset is found.

    Returns
    -------
    tuple
        (year, month)
    """

    current_date = datetime.now(timezone.utc)

    year = current_date.year
    month = current_date.month

    while year >= CHIRPS_START_YEAR:

        filename = (
            f"chirps-v3.0.{year}.{month:02d}.tif"
        )

        source_url = build_chirps_url(
            year,
            month,
        )

        print(
            f"Checking CHIRPS availability: "
            f"{year}-{month:02d}"
        )

        request = Request(
            source_url,
            method="HEAD",
            headers={
                "User-Agent": "AHON-Pipeline/1.0"
            },
        )

        try:
            with urlopen(
                request,
                timeout=30,
            ) as response:

                if response.status == 200:
                    print(
                        f"Latest available CHIRPS month: "
                        f"{year}-{month:02d}"
                    )

                    return year, month

        except HTTPError as error:

            if error.code != 404:
                raise

        except URLError as error:
            print(
                f"Connection error while checking "
                f"{filename}: {error}"
            )
            raise

        # Move backwards one month
        if month == 1:
            year -= 1
            month = 12
        else:
            month -= 1

    raise RuntimeError(
        "Could not find an available CHIRPS "
        "monthly dataset."
    )


# ============================================================
# 3. FIND EXISTING FILES IN VOLUME
# ============================================================

def get_existing_chirps_months(
    output_dir: Path,
):
    """
    Find CHIRPS year/month files that already exist
    in the Databricks Volume.

    Returns
    -------
    set
        Set of (year, month) tuples.
    """

    output_dir = Path(output_dir)

    existing_months = set()

    if not output_dir.exists():
        return existing_months

    for file in output_dir.glob(
        "chirps-v3.0.*.tif"
    ):

        parts = file.stem.split(".")

        # Expected:
        # chirps-v3.0.2026.08
        if len(parts) != 4:
            continue

        try:
            year = int(parts[2])
            month = int(parts[3])

        except ValueError:
            continue

        if 1 <= month <= 12:
            existing_months.add(
                (year, month)
            )

    return existing_months


# ============================================================
# 4. GENERATE MONTH RANGE
# ============================================================

def get_month_range(
    start_year: int,
    start_month: int,
    end_year: int,
    end_month: int,
):
    """
    Generate every year/month combination
    between start and end dates.
    """

    current_year = start_year
    current_month = start_month

    while True:

        yield current_year, current_month

        if (
            current_year == end_year
            and current_month == end_month
        ):
            break

        if current_month == 12:
            current_year += 1
            current_month = 1

        else:
            current_month += 1


# ============================================================
# 5. DOWNLOAD ONE CHIRPS MONTH
# ============================================================

def download_chirps_month(
    year: int,
    month: int,
    output_dir: Path,
):
    """
    Download one CHIRPS monthly GeoTIFF.

    Returns
    -------
    Path
        Path to the downloaded file.
    """

    filename = (
        f"chirps-v3.0.{year}.{month:02d}.tif"
    )

    source_url = build_chirps_url(
        year,
        month,
    )

    output_path = (
        Path(output_dir) / filename
    )

    request = Request(
        source_url,
        headers={
            "User-Agent": "AHON-Pipeline/1.0"
        },
    )

    print(
        f"Downloading: {filename}"
    )

    print(
        f"Source: {source_url}"
    )

    try:
        with (
            urlopen(
                request,
                timeout=120,
            ) as response,
            output_path.open(
                "wb"
            ) as file,
        ):
            file.write(
                response.read()
            )

    except HTTPError as error:

        # Remove incomplete file
        output_path.unlink(
            missing_ok=True
        )

        if error.code == 404:
            print(
                f"NOT AVAILABLE: {filename}"
            )

        else:
            print(
                f"HTTP error while downloading "
                f"{filename}: {error}"
            )

        raise

    except URLError as error:

        # Remove incomplete file
        output_path.unlink(
            missing_ok=True
        )

        print(
            f"Connection error while downloading "
            f"{filename}: {error}"
        )

        raise

    return output_path


# ============================================================
# 6. VALIDATE CHIRPS TIFF
# ============================================================

def validate_chirps_raster(
    file_path: Path,
):
    """
    Validate a downloaded CHIRPS GeoTIFF.

    Checks:
    - CRS
    - width
    - height
    - number of bands
    - data type
    """

    print(
        f"Validating: {file_path.name}"
    )

    try:

        with rasterio.open(file_path) as src:

            # CRS exists
            if src.crs is None:
                raise ValueError(
                    "Missing CRS"
                )

            # Raster dimensions
            if (
                src.width != EXPECTED_WIDTH
                or src.height != EXPECTED_HEIGHT
            ):
                raise ValueError(
                    "Unexpected raster size: "
                    f"{src.width} x {src.height}"
                )

            # Number of bands
            if src.count != EXPECTED_BANDS:
                raise ValueError(
                    "Unexpected number of bands: "
                    f"{src.count}"
                )

            # Data type
            if src.dtypes[0] != EXPECTED_DTYPE:
                raise ValueError(
                    "Unexpected data type: "
                    f"{src.dtypes[0]}"
                )

            # CRS
            if str(src.crs) != EXPECTED_CRS:
                raise ValueError(
                    "Unexpected CRS: "
                    f"{src.crs}"
                )

    except Exception:

        # Delete invalid file
        file_path.unlink(
            missing_ok=True
        )

        raise

    print(
        f"Validation passed: {file_path.name}"
    )


# ============================================================
# 7. INCREMENTAL INGESTION PIPELINE
# ============================================================

def ingest_chirps_incremental(
    output_dir: Path = OUTPUT_DIR,
):
    """
    Incrementally ingest available CHIRPS monthly files.

    The pipeline:

    1. Finds the latest available CHIRPS month.
    2. Finds existing files in the Volume.
    3. Determines which months are missing.
    4. Downloads missing months.
    5. Validates each downloaded TIFF.
    6. Saves validated files directly to the Volume.

    Existing files are skipped.
    """

    output_dir = Path(output_dir)

    
    # Prepare Volume directory
    

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    print(
        "\n============================================"
    )
    print(
        "AHON CHIRPS INCREMENTAL INGESTION"
    )
    print(
        "============================================"
    )

    print(
        f"Output directory: {output_dir}"
    )

    
    # Find latest available month
    

    latest_year, latest_month = (
        get_latest_available_chirps_month()
    )

    
    # Find existing files
    

    existing_months = (
        get_existing_chirps_months(
            output_dir
        )
    )

    print(
        f"\nExisting CHIRPS files: "
        f"{len(existing_months)}"
    )

    print(
        f"Latest available month: "
        f"{latest_year}-{latest_month:02d}"
    )

    
    # Determine missing months
    

    missing_months = []

    for year, month in get_month_range(
        start_year=CHIRPS_START_YEAR,
        start_month=CHIRPS_START_MONTH,
        end_year=latest_year,
        end_month=latest_month,
    ):

        if (year, month) not in existing_months:

            missing_months.append(
                (year, month)
            )

    print(
        f"Missing CHIRPS files: "
        f"{len(missing_months)}"
    )

    
    # Nothing to ingest
    

    if not missing_months:

        print(
            "\nNo new CHIRPS files to ingest."
        )

        print(
            "Pipeline completed successfully."
        )

        return

    
    # Download and validate missing months
    

    successful_ingestions = 0

    for year, month in missing_months:

        filename = (
            f"chirps-v3.0."
            f"{year}.{month:02d}.tif"
        )

        print(
            f"\n--------------------------------------------"
        )

        print(
            f"Processing: {year}-{month:02d}"
        )

        try:

            # Download
            file_path = download_chirps_month(
                year=year,
                month=month,
                output_dir=output_dir,
            )

            # Validate
            validate_chirps_raster(
                file_path
            )

            successful_ingestions += 1

            print(
                f"SUCCESS: {filename}"
            )

        except Exception as error:

            print(
                f"FAILED: {filename}"
            )

            print(
                f"Error: {error}"
            )

            # Continue with the next month
            continue
        
    # Final summary

    print(
        "INGESTION SUMMARY"
    )

    print(
        f"Latest available: "
        f"{latest_year}-{latest_month:02d}"
    )

    print(
        f"Existing before run: "
        f"{len(existing_months)}"
    )

    print(
        f"Missing before run: "
        f"{len(missing_months)}"
    )

    print(
        f"Successfully ingested: "
        f"{successful_ingestions}"
    )



# PIPELINE ENTRY POINT

if __name__ == "__main__":

    ingest_chirps_incremental()