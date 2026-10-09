# Databricks notebook source
"""Extract PSA 2024 census population data from the PXWeb API.

The raw CSV is saved to a Unity Catalog volume, but only after the response
passes basic validation, so a bad response never replaces a good extract.
"""

from __future__ import annotations

import csv
import io
import sys
from pathlib import Path
from typing import Any

import requests

# COMMAND ----------

# PXWeb API endpoint for the 2024 census table:
# Total Population, Urban Population, and Percent Urban by geographic location
API_URL = "https://openstat.psa.gov.ph/PXWeb/api/v1/en/DB/1A/PO_2024/0241A6DPUP1.px"

# Destination: the source volume, one folder per dataset (naming standard)
VOLUME_ROOT = Path("/Volumes/ahon/reference/source")
DATASET_NAME = "psa_population"
TARGET_PATH = VOLUME_ROOT / DATASET_NAME / "2024_population_urban.csv"

REQUEST_TIMEOUT_SECONDS = 60

# Columns the CSV header must contain before the file is saved
REQUIRED_COLUMNS = (
    "Geographic Location",
    "Total Population",
    "Urban Population",
    "Percent Urban",
)

# POST query: select all geographic locations and all parameters, return as CSV
QUERY_BODY: dict[str, Any] = {
    "query": [
        {
            "code": "Geographic Location",
            "selection": {"filter": "all", "values": ["*"]},
        },
        {
            "code": "Parameter",
            "selection": {"filter": "all", "values": ["*"]},
        },
    ],
    "response": {"format": "csv"},
}

# COMMAND ----------


class ExtractValidationError(ValueError):
    """Raised when the API response is not a usable CSV extract."""


def report(message: str) -> None:
    """Write a line to stdout, which Databricks shows as cell output."""
    sys.stdout.write(f"{message}\n")


def fetch_csv() -> requests.Response:
    """POST the PXWeb query and return the HTTP-validated response."""
    resp = requests.post(
        API_URL,
        json=QUERY_BODY,
        headers={"Accept": "text/csv"},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    resp.raise_for_status()
    return resp


def to_utf8(content: bytes) -> bytes:
    """Return the content as UTF-8 bytes, converting from Latin-1 if needed.

    PXWeb may return Latin-1 instead of UTF-8 (for example, accented letters in
    place names), and the bronze loader reads UTF-8 only. Latin-1 maps every
    byte to a character, so the conversion never fails; validate_csv still
    checks the structure afterwards.
    """
    try:
        content.decode("utf-8-sig")
    except UnicodeDecodeError:
        return content.decode("latin-1").encode("utf-8")
    return content


def validate_csv(content: bytes) -> tuple[list[str], int]:
    """Check that the response is a non-empty CSV with the expected structure.

    Raises ExtractValidationError, before anything is written, if the response
    is not valid UTF-8, is empty, lacks a required column, has a malformed row,
    or has no data rows. Returns the header columns and the number of data rows.
    """
    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        msg = "Response is not valid UTF-8 text"
        raise ExtractValidationError(msg) from exc
    if not text.strip():
        msg = "Response is empty"
        raise ExtractValidationError(msg)

    reader = csv.DictReader(io.StringIO(text))
    fieldnames = list(reader.fieldnames or [])
    missing = [c for c in REQUIRED_COLUMNS if c not in fieldnames]
    if missing:
        msg = f"Response is missing expected columns: {missing}"
        raise ExtractValidationError(msg)

    row_count = 0
    for row_count, row in enumerate(reader, start=1):
        # DictReader marks short rows with None values, long rows with a None key
        if None in row or None in row.values():
            msg = f"Malformed CSV row {row_count}"
            raise ExtractValidationError(msg)
    if row_count == 0:
        msg = "Response has a header but no data rows"
        raise ExtractValidationError(msg)
    return fieldnames, row_count


def save_csv(content: bytes, target: Path) -> None:
    """Write the raw CSV bytes to the target path, creating folders as needed."""
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(content)


def main() -> None:
    """Download the dataset, validate it, save it to the volume, and summarize."""
    resp = fetch_csv()
    content = to_utf8(resp.content)
    # Validate first: on failure this raises and the existing file is untouched
    fieldnames, row_count = validate_csv(content)
    save_csv(content, TARGET_PATH)

    report(f"Saved {len(content):,} bytes to {TARGET_PATH}")
    if content != resp.content:
        report("Converted the response from Latin-1 to UTF-8")
    report(f"Data rows: {row_count:,}")
    report(f"Header: {fieldnames}")


# COMMAND ----------

if __name__ == "__main__":
    main()