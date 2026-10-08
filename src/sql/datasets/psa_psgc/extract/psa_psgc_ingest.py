import json
import time
from pathlib import Path

import requests

# ============================================================
# Configuration
# ============================================================

API_BASE_URL = "https://classification.psa.gov.ph/psgc"

RAW_PATH = "/Volumes/ahon/reference/source/psa_psgc"

DEFAULT_PERIODS = [
    "Q2_2024",
    "April_2024",
    "Q4_2023",
    "Q2_2021",
]


# ============================================================
# API Extraction
# ============================================================

def extract_psgc_records(api_token, periods):
    """
    Extract PSGC records from the PSA PSGC API.

    Each record is tagged with the API period/version it came from
    so records from different PSGC snapshots can be distinguished.
    """

    if not api_token:
        raise ValueError(
            "PSGC API token is required. "
            "Please provide the token before running the ingestion."
        )

    if not periods:
        raise ValueError("At least one PSGC API period must be provided.")

    extracted_records = {}

    for period in periods:
        print(f"Extracting PSGC data for {period}...")

        url = f"{API_BASE_URL}/{period}/all"
        period_records = []

        while url:
            response = requests.get(
                url,
                params={"token": api_token},
                timeout=60,
            )

            response.raise_for_status()

            data = response.json()

            results = data.get("results", [])

            for record in results:
                # Preserve the API period so the source snapshot
                # is not lost during ingestion.
                record["_api_period"] = period
                period_records.append(record)

            # The API provides the next page through the `next` field.
            url = data.get("next")

            if url:
                time.sleep(0.2)

        if not period_records:
            raise RuntimeError(
                f"No PSGC records were returned for API period: {period}"
            )

        extracted_records[period] = period_records

        print(
            f"Extracted {len(period_records):,} records "
            f"for {period}."
        )

    return extracted_records


# ============================================================
# Raw Source Storage
# ============================================================

def save_raw_records(extracted_records, raw_path):
    """
    Save extracted PSGC records as raw JSON files.

    One file is created for each PSGC API period.
    """

    raw_directory = Path(raw_path)
    raw_directory.mkdir(parents=True, exist_ok=True)

    for period, records in extracted_records.items():
        output_file = raw_directory / f"{period}.json"

        with output_file.open("w", encoding="utf-8") as file:
            json.dump(
                records,
                file,
                ensure_ascii=False,
                indent=2,
            )

        print(
            f"Saved {len(records):,} records to "
            f"{output_file}"
        )


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":
    # In Databricks, use a secret/widget for the API token.
    # Do not hardcode the token in this file.

    try:
        api_token = dbutils.widgets.get("psgc_api_token").strip()
    except Exception as exc:
        raise RuntimeError(
            "The Databricks widget 'psgc_api_token' was not found. "
            "Create the widget and provide the PSGC API token."
        ) from exc

    try:
        periods_input = dbutils.widgets.get("psgc_periods")
    except Exception:  # noqa: BLE001
        periods_input = ",".join(DEFAULT_PERIODS)

    periods = [
        period.strip()
        for period in periods_input.split(",")
        if period.strip()
    ]

    records = extract_psgc_records(
        api_token=api_token,
        periods=periods,
    )

    save_raw_records(
        extracted_records=records,
        raw_path=RAW_PATH,
    )

    print("PSGC extraction completed successfully.")