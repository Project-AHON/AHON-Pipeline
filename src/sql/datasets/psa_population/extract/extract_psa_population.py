"""PSA OpenSTAT extract; source contract from PR #59, not a barangay source."""

import csv
import io
import sys
import tempfile
from pathlib import Path
from typing import Any

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "common"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bronze"))
from load_psa_population import source_parser
from runtime import (
    batch_is_complete,
    batch_path,
    completed_batch,
    publish_batch,
)

REQUIRED_COLUMNS = (
    "Geographic Location",
    "Total Population",
    "Urban Population",
    "Percent Urban",
)
QUERY_BODY: dict[str, Any] = {
    "query": [
        {
            "code": "Geographic Location",
            "selection": {"filter": "all", "values": ["*"]},
        },
        {"code": "Parameter", "selection": {"filter": "all", "values": ["*"]}},
    ],
    "response": {"format": "csv"},
}


class ExtractValidationError(ValueError):
    """Raised when the API response is not a usable CSV extract."""


def report(message: str) -> None:
    """Write a line to stdout, which Databricks shows as cell output."""
    sys.stdout.write(f"{message}\n")


def fetch_csv(api_url, timeout) -> requests.Response:
    """POST the PXWeb query and return the HTTP-validated response."""
    resp = requests.post(
        api_url,
        json=QUERY_BODY,
        headers={"Accept": "text/csv"},
        timeout=timeout,
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


def main():
    p = source_parser()
    p.add_argument("--api-url", required=True)
    p.add_argument("--request-timeout", required=True, type=float)
    args = p.parse_args()
    destination = batch_path(args, "psa_population")
    if batch_is_complete(destination):
        completed_batch(destination)
        return
    content = to_utf8(fetch_csv(args.api_url, args.request_timeout).content)
    _, rows = validate_csv(content)
    # Fixed census profile from the current PR dictionary; not the stale PR body.
    if rows != sum(args.expected_profile):
        raise ValueError(
            f"PSA profile expected {sum(args.expected_profile)} rows, found {rows}; batch not published"
        )
    with tempfile.TemporaryDirectory() as temp:
        if Path(args.source_file).name != args.source_file:
            raise ValueError("source-file must be a safe single filename")
        file = Path(temp) / args.source_file
        file.write_bytes(content)
        publish_batch(destination, [(file, rows)])


if __name__ == "__main__":
    main()
