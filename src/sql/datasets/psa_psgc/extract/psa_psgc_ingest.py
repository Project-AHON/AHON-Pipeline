
import os
import json
import requests
from datetime import datetime, timezone


# ============================================================
# 1. CONFIGURATION
# ============================================================

SECRET_SCOPE = "psgc-secrets"
SECRET_KEY = "psgc_api_token"

API_BASE_URL = "https://classification.psa.gov.ph/psgc"

API_PERIODS = [
    "Q2_2024",
    "April_2024",
    "Q4_2023",
    "Q2_2021",
]

RAW_PATH = "/Volumes/ahon_dev/reference/source/psa_psgc"

REQUEST_TIMEOUT = 120
PAGE_SIZE = 1000


# ============================================================
# 2. LOAD API TOKEN FROM DATABRICKS SECRETS
# ============================================================

psgc_api_token = dbutils.secrets.get(
    scope=SECRET_SCOPE,
    key=SECRET_KEY,
)

if not psgc_api_token:
    raise ValueError("PSGC API token is empty.")

print("PSGC API token loaded successfully.")


# ============================================================
# 3. PREPARE RAW STORAGE
# ============================================================

os.makedirs(RAW_PATH, exist_ok=True)

print(f"Raw data destination: {RAW_PATH}")


# ============================================================
# 4. FETCH ALL PAGES FOR ONE PSGC SNAPSHOT
# ============================================================

def fetch_psgc_snapshot(period):
    """
    Fetch every page returned by the PSGC API.

    Returns:
        all_records: all PSGC records collected
        metadata: snapshot ingestion metadata
    """

    api_url = f"{API_BASE_URL}/{period}/all"

    params = {
        "token": psgc_api_token,
        "limit": PAGE_SIZE,
    }

    all_records = []
    page_number = 0
    expected_count = None

    with requests.Session() as session:
        session.headers.update({
            "Accept": "application/json",
        })

        while api_url:

            page_number += 1

            print(
                f"Fetching {period} — page {page_number}"
            )

            response = session.get(
                api_url,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )

            response.raise_for_status()

            payload = response.json()

            if not isinstance(payload, dict):
                raise ValueError(
                    f"Unexpected response format for {period}, "
                    f"page {page_number}."
                )

            if "results" not in payload:
                raise ValueError(
                    f"Missing 'results' in {period}, "
                    f"page {page_number}. "
                    f"Available keys: {list(payload.keys())}"
                )

            results = payload["results"]

            if not isinstance(results, list):
                raise ValueError(
                    f"'results' is not a list for {period}, "
                    f"page {page_number}."
                )

            # Capture the API's reported total record count.
            if payload.get("count") is not None:
                page_count = int(payload["count"])

                if (
                    expected_count is not None
                    and expected_count != page_count
                ):
                    raise ValueError(
                        f"Inconsistent API count for {period}: "
                        f"{expected_count} versus {page_count}."
                    )

                expected_count = page_count

            all_records.extend(results)

            print(
                f"  Records on page: {len(results):,} | "
                f"Total collected: {len(all_records):,}"
            )

            # Follow the next URL returned by the API.
            api_url = payload.get("next")

            # The next URL may already contain its query parameters.
            # Do not append the original parameters again.
            params = None

            if not results and api_url:
                raise RuntimeError(
                    f"An empty page was returned while a next page "
                    f"still exists for {period}."
                )

    if not all_records:
        raise RuntimeError(
            f"No records were returned for {period}."
        )

    # Do not save a potentially incomplete snapshot.
    if (
        expected_count is not None
        and len(all_records) != expected_count
    ):
        raise RuntimeError(
            f"Incomplete snapshot for {period}. "
            f"API reports {expected_count:,} records, but "
            f"{len(all_records):,} were fetched. "
            "The raw file was not overwritten."
        )

    metadata = {
        "period": period,
        "records_fetched": len(all_records),
        "api_reported_count": expected_count,
        "pages_fetched": page_number,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }

    return all_records, metadata


# ============================================================
# 5. INGEST AND SAVE EACH COMPLETE SNAPSHOT
# ============================================================

ingestion_results = []

for period in API_PERIODS:

    print("\n" + "=" * 60)
    print(f"Ingesting PSGC snapshot: {period}")
    print("=" * 60)

    records, metadata = fetch_psgc_snapshot(period)

    # Preserve the API response envelope where possible.
    # The records are complete even if the API originally
    # returned multiple pages.
    complete_payload = {
        "count": metadata["api_reported_count"]
            if metadata["api_reported_count"] is not None
            else len(records),
        "next": None,
        "previous": None,
        "results": records,
        "_ingestion_metadata": metadata,
    }

    raw_file = os.path.join(
        RAW_PATH,
        f"{period}.json",
    )

    # Write to a temporary file first so an interrupted write
    # does not replace the previous snapshot with partial JSON.
    temporary_file = raw_file + ".tmp"

    try:
        with open(
            temporary_file,
            "w",
            encoding="utf-8",
        ) as file:
            json.dump(
                complete_payload,
                file,
                ensure_ascii=False,
            )

        os.replace(temporary_file, raw_file)

    finally:
        if os.path.exists(temporary_file):
            os.remove(temporary_file)

    file_size = os.path.getsize(raw_file)

    print(f"Snapshot: {period}")
    print(f"Pages fetched: {metadata['pages_fetched']:,}")
    print(f"Records fetched: {metadata['records_fetched']:,}")
    print(f"File size: {file_size:,} bytes")
    print(f"Saved to: {raw_file}")

    ingestion_results.append({
        **metadata,
        "file_size_bytes": file_size,
        "file_path": raw_file,
    })


# ============================================================
# 6. INGESTION SUMMARY
# ============================================================

print("\n" + "=" * 60)
print("PSGC INGESTION SUMMARY")
print("=" * 60)

for result in ingestion_results:
    print(
        f"{result['period']}: "
        f"{result['records_fetched']:,} records, "
        f"{result['pages_fetched']:,} pages"
    )

print("\nAll PSGC snapshots were fetched and saved successfully.")
print(f"Raw snapshots location: {RAW_PATH}")