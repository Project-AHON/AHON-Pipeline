"""Exercise the installed GX engine, not a mocked success flag."""

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src/sql/common"))
import quality


@pytest.mark.parametrize("dataset", ["psa_psgc", "psa_population", "cmci", "blgf_ldrrmf", "earthquake", "osm"])
def test_all_datasets_use_real_gx_expectations(dataset):
    status, evidence = quality.validate_metric("fixture_cardinality", dataset, 0)
    result = json.loads(evidence)
    assert status == "PASS" and result["success"] is True
    assert result["meta"]["engine"] == "great_expectations"
    assert result["expectation_config"]["type"] == "expect_column_values_to_be_between"
    assert result["expectation_config"]["meta"]["table_name"] == dataset


@pytest.mark.parametrize("severity,status", [("ERROR", "BLOCKED"), ("WARN", "WARN")])
def test_gx_failure_keeps_severity_and_evidence(severity, status):
    disposition, evidence = quality.validate_metric("fixture_invalid", "fixture", 3, severity)
    assert disposition == status
    assert json.loads(evidence)["success"] is False


@pytest.mark.parametrize("count", [-1, 0.5, None, True, "0"])
def test_invalid_rule_metrics_cannot_be_reported_as_pass(count):
    with pytest.raises(ValueError, match="nonnegative integer"):
        quality.validate_metric("fixture_invalid", "fixture", count)


def test_gx_unavailability_cannot_fall_back_to_a_success(monkeypatch):
    def unavailable():
        raise RuntimeError("GX unavailable")
    monkeypatch.setattr(quality, "_metric_batch", unavailable)
    with pytest.raises(RuntimeError, match="GX unavailable"):
        quality.validate_metric("fixture", "fixture", 0)


def test_blocking_gx_evidence_is_written_before_exception():
    class Spark:
        def __init__(self):
            self.statements = []
            self.row = None

        def sql(self, statement):
            self.statements.append(statement)

        def table(self, table):
            return type("Table", (), {"columns": ["validation_result"]})()

        def createDataFrame(self, rows, schema):
            self.row = rows[0]
            return type("Frame", (), {"createOrReplaceTempView": lambda self, name: None})()

    spark = Spark()
    with pytest.raises(quality.QualityError):
        quality.check(spark, "fixture_catalog", "fixture_run", "duplicate_keys", "fixture", 1)
    assert spark.statements[-1].lstrip().startswith("MERGE INTO")
    assert spark.row[5] == "BLOCKED"
    assert json.loads(spark.row[7])["success"] is False
