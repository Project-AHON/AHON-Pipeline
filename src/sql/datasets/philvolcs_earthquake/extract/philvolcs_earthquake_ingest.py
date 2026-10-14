"""Extract explicit PHIVOLCS months; never publish a partially fetched range."""

import sys
import tempfile
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "common"))
from runtime import (
    batch_is_complete,
    batch_path,
    completed_batch,
    parser,
    publish_batch,
)

MONTH_NAMES = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
]
SOURCE_COLUMNS = [
    "Date-Time",
    "Latitude",
    "Longitude",
    "Depth",
    "Magnitude",
    "Location",
    "Month",
    "Year",
]


def months_between(start, end, source_timezone):
    first, last = [
        datetime.strptime(value, "%Y-%m").replace(tzinfo=timezone.utc)
        for value in (start, end)
    ]
    if first > last:
        raise ValueError("start-month must not exceed end-month")
    now = datetime.now(ZoneInfo(source_timezone))
    if (last.year, last.month) > (now.year, now.month):
        raise ValueError("Future source months cannot be requested")
    while first <= last:
        yield first.year, first.month
        first = first.replace(
            year=first.year + (first.month == 12), month=first.month % 12 + 1
        )


def fetch_month(session, year, month, archive_url_template, latest_url, timeout, source_timezone):
    name = MONTH_NAMES[month - 1]
    url = archive_url_template.format(year=year, month=month, month_name=name)
    response = session.get(url, timeout=timeout)
    now = datetime.now(ZoneInfo(source_timezone))
    if response.status_code == 404 and (year, month) == (now.year, now.month):
        response = session.get(latest_url, timeout=timeout)
    response.raise_for_status()
    # Source parser from main: skip first presentation row, require six columns.
    # Keep banner/invalid records for Silver to flag, and preserve numeric text.
    tables = pd.read_html(
        StringIO(response.text),
        skiprows=1,
        keep_default_na=False,
        converters={i: str for i in range(6)},
    )
    matching = [table for table in tables if table.shape[1] == 6 and not table.empty]
    if len(matching) != 1:
        raise ValueError(
            f"{year}-{month:02}: expected exactly one six-column event table"
        )
    frame = matching[0]
    frame.columns = SOURCE_COLUMNS[:6]
    frame["Month"] = name
    frame["Year"] = year
    return frame


def main():
    arguments = parser(__doc__)
    arguments.add_argument("--start-month", required=True)
    arguments.add_argument("--end-month", required=True)
    arguments.add_argument("--archive-url-template", required=True)
    arguments.add_argument("--latest-url", required=True)
    arguments.add_argument("--request-timeout", required=True, type=float)
    arguments.add_argument("--source-timezone", required=True)
    args = arguments.parse_args()
    if args.start_month == 'auto' or args.end_month == 'auto':
        today = datetime.now(ZoneInfo(args.source_timezone))
        previous = (today.replace(day=1) - timedelta(days=1)).strftime('%Y-%m')
        args.start_month = previous if args.start_month == 'auto' else args.start_month
        args.end_month = previous if args.end_month == 'auto' else args.end_month
    destination = batch_path(args, "philvolcs_earthquake_data")
    if batch_is_complete(destination):
        completed_batch(destination)
        return
    with tempfile.TemporaryDirectory() as temp, requests.Session() as session:
        files = []
        for year, month in months_between(args.start_month, args.end_month, args.source_timezone):
            frame = fetch_month(session, year, month, args.archive_url_template, args.latest_url, args.request_timeout, args.source_timezone)
            file = Path(temp) / f"phivolcs_earthquake_{year}_{month:02}.csv"
            frame.to_csv(file, index=False, encoding="utf-8-sig")
            files.append((file, len(frame)))
        publish_batch(destination, files)


if __name__ == "__main__":
    main()
