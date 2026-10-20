"""Regression coverage for persisted gate metrics and configured census years."""
import ast
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import great_expectations as gx
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src/sql/common"))
import dq_gate


@pytest.mark.parametrize("kind,kwargs,details,count,success,expected", [
    ("expect_table_row_count_to_be_between", {"min_value": 1}, {}, 0, False, 1),
    ("expect_table_row_count_to_be_between", {"max_value": 5}, {}, 8, False, 3),
    ("expect_table_row_count_to_equal", {"value": 10}, {}, 7, False, 3),
    ("expect_table_row_count_to_be_between", {"min_value": 1}, {}, 1, True, 0),
    ("expect_column_values_to_not_be_null", {}, {"unexpected_count": 3}, 10, False, 3),
    ("expect_column_values_to_not_be_null", {}, {"unexpected_count": None}, 10, False, 1),
    ("expect_table_columns_to_match_set", {}, {}, 10, False, 1),
])
def test_failure_metric_cannot_report_zero_for_a_failed_contract(kind, kwargs, details, count, success, expected):
    assert dq_gate.failure_metric(kind, kwargs, details, count, success) == expected


@pytest.mark.parametrize("filename", ["gold_dq_gate.py", "openstat_silver_dq_gate.py", "openstat_gold_dq_gate.py"])
def test_selected_census_year_reaches_each_population_gate(filename):
    tree = ast.parse((ROOT / "src/sql/monitoring/psa_population" / filename).read_text())
    selected = next(node for node in tree.body if isinstance(node, ast.Assign)
                    and any(isinstance(t, ast.Name) and t.id == "CENSUS_YEAR" for t in node.targets))
    namespace = {"dbutils": SimpleNamespace(widgets=SimpleNamespace(get=lambda key: "2020"))}
    exec(compile(ast.Module(body=[selected], type_ignores=[]), filename, "exec"), namespace)
    assert namespace["CENSUS_YEAR"] == 2020


def test_real_gx_gate_persists_failure_evidence_before_raising(monkeypatch):
    context = gx.get_context(mode="ephemeral")
    source = context.data_sources.add_pandas(name="gate_regression")
    definition = source.add_dataframe_asset(name="empty").add_batch_definition_whole_dataframe("all")
    batch = definition.get_batch(batch_parameters={"dataframe": pd.DataFrame({"id": pd.Series(dtype="int64")})})
    monkeypatch.setattr(dq_gate, "_get_data_source", lambda: source)
    monkeypatch.setattr(dq_gate, "_get_batch", lambda *_: batch)
    monkeypatch.setattr(dq_gate, "ensure_monitoring", lambda *_: None)
    rows, statements = [], []

    def frame(data, schema):
        rows.extend(data)
        return SimpleNamespace(createOrReplaceTempView=lambda _: None)

    spark = SimpleNamespace(createDataFrame=frame, sql=statements.append)
    rule = dq_gate.rule(gx.expectations.ExpectTableRowCountToBeBetween(min_value=1), "nonempty", "volume", "stop")
    with pytest.raises(RuntimeError, match="nonempty"):
        dq_gate.run_dq_gate(spark, SimpleNamespace(count=lambda: 0, columns=[]), [rule],
                            source_name="fixture", layer="silver", table_name="fixture",
                            key_columns=["id"], results_table="fixture.monitoring.dq_result", run_id="same_batch")
    assert rows[0][:6] == ("same_batch", "fixture_silver_nonempty", "silver.fixture", 1, "ERROR", "BLOCKED")
    assert json.loads(rows[0][7])["success"] is False
    assert "MERGE INTO" in statements[-1]
