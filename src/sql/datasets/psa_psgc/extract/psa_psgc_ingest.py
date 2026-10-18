"""Stream the four documented PSA PSGC snapshots into a completed raw batch."""

import json
import sys
import tempfile
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "common"))
from runtime import (
    batch_is_complete,
    batch_path,
    completed_batch,
    parser,
    publish_batch,
    row_counts,
)

def extract_period(token, period, target, session, api_base_url, expected_rows, timeout):
    origin = urlparse(api_base_url)
    if origin.scheme != "https" or not origin.hostname or origin.username or origin.password or origin.query or origin.fragment:
        raise ValueError("Expected an approved HTTPS PSA API URL without credentials")
    url = f"{api_base_url.rstrip('/')}/{period}/all"
    rows = 0
    seen = set()
    with target.open("w", encoding="utf-8") as stream:
        while url:
            if (
                urlparse(url).netloc != origin.netloc
                or urlparse(url).scheme != "https"
            ):
                raise ValueError("Unexpected pagination origin")
            if url in seen:
                raise ValueError("Pagination cycle")
            seen.add(url)
            response = session.get(url, params={"token": token}, timeout=timeout, allow_redirects=False)
            if response.status_code != 200:
                raise RuntimeError(
                    f"PSGC request failed with HTTP {response.status_code}"
                )
            payload = response.json()
            if not isinstance(payload.get("results"), list) or not payload["results"]:
                raise ValueError("Missing or empty PSGC results")
            for record in payload["results"]:
                if (
                    not isinstance(record, dict)
                    or not {"code", "area_name", "geographic_level"} <= record.keys()
                ):
                    raise ValueError("PSGC source structure changed")
                record["_api_period"] = period
                stream.write(json.dumps(record, ensure_ascii=False) + "\n")
                rows += 1
            next_page = payload.get("next")
            url = urljoin(url, next_page) if next_page else None
    if rows != expected_rows:
        raise ValueError(
            f"{period}: expected {expected_rows}, found {rows}; investigate snapshot change"
        )
    return rows


def main():
    from pyspark.dbutils import DBUtils
    from pyspark.sql import SparkSession

    arguments = parser(__doc__)
    arguments.add_argument("--secret-scope", required=True)
    arguments.add_argument("--secret-key", required=True)
    arguments.add_argument("--api-base-url", required=True)
    arguments.add_argument("--expected-rows", required=True, type=row_counts)
    arguments.add_argument("--request-timeout", required=True, type=float)
    args = arguments.parse_args()
    if not args.secret_scope or not args.secret_key:
        raise ValueError("PSA extraction requires an existing approved token secret scope and key")
    if args.request_timeout <= 0:
        raise ValueError("PSGC request timeout must be positive")
    destination = batch_path(args, "psa_psgc")
    if batch_is_complete(destination):
        completed_batch(destination)
        return
    token = DBUtils(SparkSession.builder.getOrCreate()).secrets.get(
        args.secret_scope, args.secret_key
    ).strip()
    if not token:
        raise ValueError("PSGC API secret is empty")
    try:
        with tempfile.TemporaryDirectory() as temp, requests.Session() as session:
            files = []
            for period, expected in args.expected_rows.items():
                file = Path(temp) / f"{period}.jsonl"
                files.append((file, extract_period(token, period, file, session, args.api_base_url, expected, args.request_timeout)))
            publish_batch(destination, files)
    except requests.RequestException:
        raise RuntimeError("PSGC network request failed; batch not published") from None


if __name__ == "__main__":
    main()
