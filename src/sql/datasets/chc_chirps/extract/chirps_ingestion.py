from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import pandas as pd
import rasterio


# ============================================================
# CONFIGURATION
# ============================================================

CHIRPS_BASE_URL = (
    "https://data.chc.ucsb.edu/products/CHIRPS/v3.0/"
    "monthly/global/tifs"
)

CHIRPS_START_YEAR = 1981

EXPECTED_WIDTH = 7200
EXPECTED_HEIGHT = 2400
EXPECTED_BANDS = 1
EXPECTED_DTYPE = "float32"
EXPECTED_CRS = "EPSG:4326"


# ============================================================
# LATEST AVAILABLE MONTH
# ============================================================

def get_latest_available_chirps_month():
    """
    Find the latest available CHIRPS v3 monthly GeoTIFF.

    The function starts from the current UTC year/month and
    checks backwards until it finds an available dataset.

    Returns
    -------
    tuple
        (year, month)

    Example
    -------
    (2026, 7)
    """

    current_date = datetime.now(timezone.utc)

    year = current_date.year
    month = current_date.month

    while year >= CHIRPS_START_YEAR:

        filename = (
            f"chirps-v3.0.{year}.{month:02d}.tif"
        )

        source_url = (
            f"{CHIRPS_BASE_URL}/{filename}"
        )

        print(
            f"Checking CHIRPS availability: "
            f"{year}-{month:02d}"
        )

        request = Request(
            source_url,
            method="HEAD"
        )

        try:

            with urlopen(
                request,
                timeout=30
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

        except URLError:

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
# INGEST ONE MONTH
# ============================================================

def ingest_chirps_month(
    year,
    month,
    output_dir
):
    """
    Download and validate one monthly CHIRPS v3 GeoTIFF.

    Existing files are skipped.

    Parameters
    ----------
    year : int
        CHIRPS year, e.g. 2018.

    month : int
        CHIRPS month, from 1 to 12.

    output_dir : Path
        Directory where the downloaded file will be stored.

    Returns
    -------
    dict
        Metadata describing the ingestion result.
    """

    # --------------------------------------------------------
    # Validate month
    # --------------------------------------------------------

    if month < 1 or month > 12:

        raise ValueError(
            f"Month must be between 1 and 12. "
            f"Received: {month}"
        )

    # --------------------------------------------------------
    # Build filename and URL
    # --------------------------------------------------------

    filename = (
        f"chirps-v3.0.{year}.{month:02d}.tif"
    )

    source_url = (
        f"{CHIRPS_BASE_URL}/{filename}"
    )

    # --------------------------------------------------------
    # Prepare output directory
    # --------------------------------------------------------

    output_dir = Path(output_dir)

    output_dir.mkdir(
        parents=True,
        exist_ok=True
    )

    output_file = (
        output_dir / filename
    )

    # --------------------------------------------------------
    # Timestamp
    # --------------------------------------------------------

    ingestion_timestamp = (
        datetime.now(timezone.utc).isoformat()
    )

    # --------------------------------------------------------
    # Idempotency check
    # --------------------------------------------------------

    if output_file.exists():

        file_size = (
            output_file.stat().st_size
        )

        print(
            f"SKIP: {filename} already exists."
        )

        return {
            "year": year,
            "month": month,
            "filename": filename,
            "status": "skipped",
            "source_url": source_url,
            "path": output_file,
            "file_size": file_size,
            "ingestion_timestamp": ingestion_timestamp,
        }

    # --------------------------------------------------------
    # Download
    # --------------------------------------------------------

    print(
        f"Downloading: {filename}"
    )

    print(
        f"Source: {source_url}"
    )

    try:

        with urlopen(
            source_url,
            timeout=120
        ) as response:

            with open(
                output_file,
                "wb"
            ) as file:

                file.write(
                    response.read()
                )

    except HTTPError as error:

        # Remove incomplete file if one was created
        output_file.unlink(
            missing_ok=True
        )

        if error.code == 404:

            print(
                f"NOT AVAILABLE: {filename}"
            )

            return {
                "year": year,
                "month": month,
                "filename": filename,
                "status": "not_available",
                "source_url": source_url,
                "path": output_file,
                "file_size": None,
                "ingestion_timestamp": ingestion_timestamp,
            }

        raise

    except URLError:

        # Remove incomplete file
        output_file.unlink(
            missing_ok=True
        )

        raise

    # --------------------------------------------------------
    # Validate downloaded GeoTIFF
    # --------------------------------------------------------

    try:

        with rasterio.open(
            output_file
        ) as src:

            # CRS
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

        # Remove invalid/incomplete file
        output_file.unlink(
            missing_ok=True
        )

        raise

    # --------------------------------------------------------
    # File metadata
    # --------------------------------------------------------

    file_size = (
        output_file.stat().st_size
    )

    print(
        f"SUCCESS: {filename}"
    )

    print(
        f"Size: {file_size:,} bytes"
    )

    # --------------------------------------------------------
    # Return ingestion metadata
    # --------------------------------------------------------

    return {
        "year": year,
        "month": month,
        "filename": filename,
        "status": "downloaded",
        "source_url": source_url,
        "path": output_file,
        "file_size": file_size,
        "ingestion_timestamp": ingestion_timestamp,
    }


# ============================================================
# INGEST DATE RANGE
# ============================================================

def ingest_chirps_range(
    start_year,
    start_month,
    end_year,
    end_month,
    output_dir,
):
    """
    Ingest CHIRPS v3 monthly GeoTIFFs within
    a specified date range.

    Existing files are skipped.

    Parameters
    ----------
    start_year : int
        Starting year.

    start_month : int
        Starting month.

    end_year : int
        Ending year.

    end_month : int
        Ending month.

    output_dir : Path
        Directory for downloaded files.

    Returns
    -------
    list
        List of ingestion result dictionaries.
    """

    # --------------------------------------------------------
    # Validate start month
    # --------------------------------------------------------

    if start_month < 1 or start_month > 12:

        raise ValueError(
            "start_month must be between 1 and 12."
        )

    # --------------------------------------------------------
    # Validate end month
    # --------------------------------------------------------

    if end_month < 1 or end_month > 12:

        raise ValueError(
            "end_month must be between 1 and 12."
        )

    # --------------------------------------------------------
    # Convert dates to comparable indexes
    # --------------------------------------------------------

    start_index = (
        start_year * 12
        + start_month
    )

    end_index = (
        end_year * 12
        + end_month
    )

    if start_index > end_index:

        raise ValueError(
            "Start date must be before "
            "or equal to end date."
        )

    # --------------------------------------------------------
    # Initialize current date
    # --------------------------------------------------------

    current_year = start_year
    current_month = start_month

    results = []

    # --------------------------------------------------------
    # Process each month
    # --------------------------------------------------------

    while True:

        print(
            f"\nProcessing "
            f"{current_year}-{current_month:02d}"
        )

        result = ingest_chirps_month(
            year=current_year,
            month=current_month,
            output_dir=output_dir,
        )

        results.append(
            result
        )

        # ----------------------------------------------------
        # Stop at requested end date
        # ----------------------------------------------------

        if (
            current_year == end_year
            and current_month == end_month
        ):

            break

        # ----------------------------------------------------
        # Move to next month
        # ----------------------------------------------------

        if current_month == 12:

            current_year += 1
            current_month = 1

        else:

            current_month += 1

    return results


# ============================================================
# CREATE INGESTION MANIFEST
# ============================================================

def create_ingestion_manifest(
    results
):
    """
    Convert ingestion results into a pandas DataFrame.

    The manifest provides an audit-friendly summary
    of the ingestion execution.

    Parameters
    ----------
    results : list
        Results returned by the ingestion functions.

    Returns
    -------
    pandas.DataFrame
        Ingestion manifest.
    """

    manifest = pd.DataFrame(
        results
    )

    if manifest.empty:

        return manifest

    # --------------------------------------------------------
    # Convert Path objects to strings
    # --------------------------------------------------------

    if "path" in manifest.columns:

        manifest["path"] = (
            manifest["path"].astype(str)
        )

    # --------------------------------------------------------
    # Sort chronologically
    # --------------------------------------------------------

    manifest = (
        manifest
        .sort_values(
            ["year", "month"]
        )
        .reset_index(
            drop=True
        )
    )

    return manifest