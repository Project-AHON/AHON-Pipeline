"""Earthquake-only landing contracts; no Spark dependency."""

import csv
import hashlib
import json
import math
import re
import shutil
from collections import Counter
from datetime import datetime
from pathlib import Path

SOURCE_COLUMNS = [
    "Date-Time", "Latitude", "Longitude", "Depth", "Magnitude", "Location",
    "Month", "Year",
]
LINEAGE = ["_source_name", "_source_ref", "_ingested_at", "_batch_id", "_row_hash"]
LOCALITY_PATTERN = r"(?i)^[0-9]+ km [NSEW0-9°º .]+ of ([^()]+) \(([^()]+)\)$"


def normalize(value):
    """Case and whitespace only: keep accents, City of, Santo and other tokens."""
    return " ".join((value or "").strip().lower().split())


def locality(description):
    match = re.fullmatch(LOCALITY_PATTERN, (description or "").strip())
    return tuple(normalize(v) for v in match.groups()) if match else ("", "")


def finite(value, minimum, maximum=None):
    try:
        number = float(value)
    except (ValueError, TypeError):
        return False
    return math.isfinite(number) and number >= minimum and (
        maximum is None or number <= maximum
    )


def source_time(value):
    try:
        return datetime.strptime((value or "").strip().upper(), "%d %B %Y - %I:%M %p")
    except (ValueError, TypeError):
        return None


def digest(path):
    value = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def validate_landing(path, expected_digest, expected_periods):
    """Stream immutable CSV before inference. Keep bad event observations auditable."""
    if not re.fullmatch(r"[0-9a-f]{64}", expected_digest):
        raise ValueError("Expected a lowercase SHA-256 landing digest")
    if digest(path) != expected_digest:
        raise ValueError("Landing digest differs; no target written")
    counts = Counter()
    rows = blank_required = invalid_events = 0
    with Path(path).open(encoding="utf-8-sig", newline="") as stream:
        reader = csv.reader(stream, strict=True)
        if next(reader, None) != SOURCE_COLUMNS:
            raise ValueError("Landing header must contain exactly eight source columns")
        for row in reader:
            if len(row) != 8:
                raise ValueError("Malformed CSV row; no target written")
            rows += 1
            blank_required += any(not row[i].strip() for i in [0, 1, 2, 3, 4, 6, 7])
            timestamp = source_time(row[0])
            invalid_events += not (
                timestamp and finite(row[1], -90, 90) and finite(row[2], -180, 180)
                and finite(row[3], 0) and finite(row[4], 0)
            )
            try:
                month = datetime.strptime(row[6].strip(), "%B").month
                if not re.fullmatch(r"[0-9]{4}", row[7].strip()):
                    raise ValueError("Invalid year")
                counts[f"{int(row[7]):04d}-{month:02d}"] += 1
            except ValueError:
                counts["invalid_period"] += 1
    if rows == 0:
        raise ValueError("Empty landing; records expected")
    return {
        "raw_rows": rows, "blank_required": blank_required,
        "invalid_events": invalid_events, "period_counts": dict(counts),
        "requested_missing_periods": sorted(set(expected_periods) - counts.keys()),
        "file_sha256": expected_digest,
    }


def land_source(source, destination, expected_digest, expected_periods):
    """Validate all CSV structure before publishing an immutable copy; never replace."""
    source = Path(source)
    destination = Path(destination)
    result = validate_landing(source, expected_digest, expected_periods)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        if digest(destination) != expected_digest:
            raise ValueError("Immutable landing already exists with different bytes")
        return result
    temporary = destination.with_suffix(destination.suffix + ".partial")
    shutil.copyfile(source, temporary)
    if digest(temporary) != expected_digest:
        raise ValueError("Source changed while landing; no complete copy published")
    # rename on the Windows local test host refuses an existing destination.
    # Jobs must have max_concurrent_runs=1; never run two writers for this dataset.
    temporary.rename(destination)
    return result


def record_landing_contract(path, landing, hash_schema):
    """Keep durable file/hash evidence beside raw data, independent of job log retention."""
    manifest = Path(str(path) + ".manifest.json")
    value = {"file_sha256": landing["file_sha256"], "raw_rows": landing["raw_rows"],
             "source_columns": SOURCE_COLUMNS, "hash_schema": hash_schema,
             "hash_contract": "legacy-inferred-concat-ws-double-pipe-sha256",
             "period_counts": landing["period_counts"]}
    if manifest.exists():
        if json.loads(manifest.read_text(encoding="utf-8")) != value:
            raise ValueError("Existing landing/hash manifest differs; reviewed migration required")
        return
    # Exclusive creation; interrupted JSON is never accepted as a complete contract.
    with manifest.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, sort_keys=True)


def check_schema(actual, expected):
    if actual != expected:
        raise ValueError("Schema differs: use a fresh dev target or reviewed migration")


def stop_on_failures(results):
    failures = [r["check_name"] for r in results if r["action"] == "stop"
                and r["status"] != "PASS"]
    if failures:
        raise ValueError("Stop rules failed: " + ", ".join(failures))


def candidates(locality_name, province_name, reference):
    """Fixture oracle for exact candidate reporting, never geographic assignment."""
    if not locality_name or not province_name:
        return {"candidate_codes": [], "candidate_status": "unparsed"}
    codes = sorted(r["psgc_code"] for r in reference
                   if r["geographic_level"] in {"CITY", "MUN"}
                   and normalize(r["area_name"]) == locality_name
                   and normalize(r.get("province_name")) == province_name)
    return {"candidate_codes": codes, "candidate_status": (
        "single_candidate" if len(codes) == 1 else "ambiguous" if codes else "unmatched"
    )}


def report(value):
    print(json.dumps(value, default=str, sort_keys=True))
