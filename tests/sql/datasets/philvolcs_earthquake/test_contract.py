"""Small fixtures for landing integrity, candidate semantics and stop gates."""

import csv
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "src/sql/datasets/philvolcs_earthquake"))
from earthquake_contract import (
    SOURCE_COLUMNS,
    candidates,
    check_schema,
    digest,
    finite,
    land_source,
    locality,
    normalize,
    record_landing_contract,
    source_time,
    stop_on_failures,
    validate_landing,
)


def csv_file(tmp_path, rows, header=SOURCE_COLUMNS):
    file = tmp_path / "source.csv"
    with file.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(header)
        writer.writerows(rows)
    return file


NORMAL = ["31 January 2019 - 02:55 pm", "10.1", "122.5", "12", "3.2",
          "025 km S 58° W of Sipalay (Negros Occidental)", "January", "2019"]


def test_invalid_observations_are_retained_and_periods_reported(tmp_path):
    bad = ["banner", "NaN", "Infinity", "bad", "", "", "January", "2019"]
    file = csv_file(tmp_path, [NORMAL, NORMAL, bad])
    result = validate_landing(file, digest(file), ["2019-01", "2019-02"])
    assert result["raw_rows"] == 3
    assert result["invalid_events"] == 1
    assert result["requested_missing_periods"] == ["2019-02"]
    assert result["period_counts"] == {"2019-01": 3}


@pytest.mark.parametrize("rows,header", [([], SOURCE_COLUMNS), ([NORMAL[:-1]], SOURCE_COLUMNS),
                                       ([NORMAL], SOURCE_COLUMNS[::-1])])
def test_structure_stops_before_publication(tmp_path, rows, header):
    file = csv_file(tmp_path, rows, header)
    target = tmp_path / "immutable.csv"
    with pytest.raises(ValueError):
        land_source(file, target, digest(file), ["2019-01"])
    assert not target.exists()


def test_immutable_landing_and_second_unchanged_run(tmp_path):
    file = csv_file(tmp_path, [NORMAL])
    target = tmp_path / "immutable.csv"
    first = land_source(file, target, digest(file), ["2019-01"])
    stat = target.stat().st_mtime_ns
    second = land_source(file, target, digest(file), ["2019-01"])
    assert first == second and target.stat().st_mtime_ns == stat
    changed = NORMAL.copy()
    changed[4] = "4.0"
    csv_file(tmp_path, [changed])
    with pytest.raises(ValueError, match="different bytes"):
        land_source(file, target, digest(file), ["2019-01"])


def test_landing_integrity_blocks_changed_file(tmp_path):
    file = csv_file(tmp_path, [NORMAL])
    expected = digest(file)
    file.write_text("modified", encoding="utf-8")
    with pytest.raises(ValueError, match="digest"):
        validate_landing(file, expected, ["2019-01"])


def test_landing_manifest_pins_hash_schema_and_never_rewrites(tmp_path):
    file = csv_file(tmp_path, [NORMAL])
    landing = validate_landing(file, digest(file), ["2019-01"])
    schema = dict.fromkeys(SOURCE_COLUMNS, "string")
    record_landing_contract(file, landing, schema)
    manifest = Path(str(file) + ".manifest.json")
    first = manifest.stat().st_mtime_ns
    record_landing_contract(file, landing, schema)
    assert manifest.stat().st_mtime_ns == first
    with pytest.raises(ValueError, match="migration"):
        record_landing_contract(file, landing, {**schema, "Latitude": "double"})


@pytest.mark.parametrize("value", [None, "", "bad", "NaN", "Infinity", "-Infinity", "-1"])
def test_finite_nonnegative(value):
    assert not finite(value, 0)


def test_physical_bounds_are_not_profile_maxima():
    assert finite("900", 0) and finite("8.0", 0)
    assert finite("90", -90, 90) and not finite("91", -90, 90)
    assert finite("-180", -180, 180) and not finite("181", -180, 180)


def test_source_time_case_and_invalid_time():
    assert source_time(NORMAL[0]).hour == 14
    assert source_time("31 February 2026 - 12:00 AM") is None
    assert source_time("") is None


def test_parser_conservatively_uses_only_simple_supported_description():
    assert locality(NORMAL[5]) == ("sipalay", "negros occidental")
    assert locality("025 km N 20° E of City of Santo Niño (Samar)") == (
        "city of santo niño", "samar"
    )
    assert normalize(" Santo  Niño ") == "santo niño"


@pytest.mark.parametrize("description", [None, "", "offshore", "Sipalay", "of Sipalay (Negros)",
    "011 km S 45° E of Balut Island (Municipality Of Sarangani) (Davao Occidental)"])
def test_empty_malformed_nested_descriptions_are_unparsed(description):
    assert locality(description) == ("", "")


def test_same_name_province_disambiguation_and_reference_level():
    ref = [
        {"psgc_code": "0100101000", "area_name": "San Jose", "province_name": "Alpha", "geographic_level": "MUN"},
        {"psgc_code": "0200101000", "area_name": "San Jose", "province_name": "Beta", "geographic_level": "MUN"},
        {"psgc_code": "0100101001", "area_name": "San Jose", "province_name": "Alpha", "geographic_level": "BGY"},
    ]
    assert candidates("san jose", "alpha", ref) == {
        "candidate_codes": ["0100101000"], "candidate_status": "single_candidate"
    }
    assert candidates("san jose", "beta", ref)["candidate_codes"] == ["0200101000"]
    assert candidates("san jose", "gamma", ref)["candidate_status"] == "unmatched"
    ref.append({**ref[0], "psgc_code": "0100102000"})
    assert candidates("san jose", "alpha", ref)["candidate_status"] == "ambiguous"


def test_independent_city_missing_province_needs_evidence():
    ref = [{"psgc_code": "0100101000", "area_name": "City of Example",
            "province_name": None, "geographic_level": "CITY"}]
    assert candidates("city of example", "alpha", ref)["candidate_status"] == "unmatched"


def test_stop_gate_including_blocked_prerequisite():
    stop_on_failures([{"check_name": "bad_event", "action": "flag", "status": "WARN"}])
    for status in ["FAIL", "NOT_RUN", "BLOCKED"]:
        with pytest.raises(ValueError, match="missing_lineage"):
            stop_on_failures([{"check_name": "missing_lineage", "action": "stop", "status": status}])


def test_snapshot_and_nullable_schema_changes_are_not_automatic():
    with pytest.raises(ValueError, match="migration"):
        check_schema({"event_time": "timestamp"}, {"event_time": "timestamp_ntz"})
    with pytest.raises(ValueError, match="migration"):
        check_schema({"_api_period": "Q2_2026"}, {"_api_period": "Q2_2024"})
