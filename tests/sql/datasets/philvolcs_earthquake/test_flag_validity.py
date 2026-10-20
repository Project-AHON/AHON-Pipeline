"""has_dq_issues must equal the observation's validity, for valid and invalid records."""

import ast
import sys
from pathlib import Path

import great_expectations as gx
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "src/sql/datasets/philvolcs_earthquake"))
from earthquake_contract import INVALID_OBSERVATION_SQL, flag_matches_validity

# One valid observation; each invalid one breaks a single field
VALID = {"event_time": "2019-01-31 14:55:00", "latitude": 10.1, "longitude": 122.5,
         "depth_km": 12.0, "magnitude": 3.2, "source_year": 2019, "source_month": 1}
INVALID = {
    "missing_time": {**VALID, "event_time": None},
    "latitude_out_of_range": {**VALID, "latitude": 95.0},
    "negative_depth": {**VALID, "depth_km": -1.0},
    "nan_magnitude": {**VALID, "magnitude": float("nan")},
    "infinite_longitude": {**VALID, "longitude": float("inf")},
    "wrong_source_month": {**VALID, "source_month": 2},
}


def failed_rows(frame):
    """Rows where flag_matches_validity is not True, counted by GX as the gate does."""
    frame = frame.assign(flag_matches_validity=flag_matches_validity(frame["has_dq_issues"], frame["invalid"]))
    source = gx.get_context(mode="ephemeral").data_sources.add_pandas(name="earthquake_flags")
    batch = (source.add_dataframe_asset(name="silver").add_batch_definition_whole_dataframe("all")
             .get_batch(batch_parameters={"dataframe": frame}))
    result = batch.validate(gx.expectations.ExpectColumnValuesToBeInSet(
        column="flag_matches_validity", value_set=[True]))
    return result.result["unexpected_count"]


def test_correct_flags_pass_for_valid_and_invalid_records():
    frame = pd.DataFrame({"has_dq_issues": [False, True], "invalid": [False, True]})
    assert failed_rows(frame) == 0


@pytest.mark.parametrize("flag,invalid", [(True, False), (False, True)])
def test_wrong_flag_fails_for_valid_and_invalid_records(flag, invalid):
    frame = pd.DataFrame({"has_dq_issues": [flag], "invalid": [invalid]})
    assert failed_rows(frame) == 1


def test_valid_record_flagged_by_mistake_is_caught():
    # The earlier rule (has_dq_issues OR NOT invalid) passed this row
    flag, invalid = True, False
    assert (flag or not invalid) is True
    assert flag_matches_validity(flag, invalid) is False


@pytest.mark.parametrize("file", ["src/sql/datasets/philvolcs_earthquake/observation_pipeline.py",
                                  "src/sql/monitoring/philvolcs_earthquake/silver_dq_gate.py"])
def test_build_and_gate_share_one_validity_expression(file):
    path = ROOT / file
    imported = {alias.name for node in ast.walk(ast.parse(path.read_text(encoding="utf-8")))
                if isinstance(node, ast.ImportFrom) and node.module == "earthquake_contract"
                for alias in node.names}
    assert "INVALID_OBSERVATION_SQL" in imported
    assert "event_time IS NULL OR latitude IS NULL" not in path.read_text(encoding="utf-8")


def test_validity_expression_on_spark_records():
    pyspark = pytest.importorskip("pyspark")  # runs where Spark is installed; CI has no Spark
    from pyspark.sql import functions as F

    try:
        spark = pyspark.sql.SparkSession.builder.master("local[1]").getOrCreate()
    except Exception as error:  # e.g. Databricks Connect's pyspark allows only remote sessions
        pytest.skip(f"no local Spark session: {error}")
    records = {"valid": VALID, **INVALID}
    frame = spark.createDataFrame(
        [(name, *row.values()) for name, row in records.items()],
        "name string, event_time string, latitude double, longitude double, depth_km double, "
        "magnitude double, source_year int, source_month int",
    ).withColumn("event_time", F.to_timestamp("event_time"))
    invalid = {row["name"]: row["invalid"]
               for row in frame.select("name", F.expr(INVALID_OBSERVATION_SQL).alias("invalid")).collect()}
    assert invalid == {"valid": False, **{name: True for name in INVALID}}
