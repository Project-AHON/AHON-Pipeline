"""Execution failures retain their cause; a successful retry clears their marker."""

import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src/sql/common"))
import quality
import run_python


@pytest.fixture
def runner(monkeypatch):
    spark = SimpleNamespace(sql=lambda statement: None)
    sql = ModuleType("pyspark.sql")
    sql.SparkSession = SimpleNamespace(builder=SimpleNamespace(getOrCreate=lambda: spark))
    monkeypatch.setitem(sys.modules, "pyspark", ModuleType("pyspark"))
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql)
    monkeypatch.setattr(sys, "path", sys.path.copy())
    monkeypatch.setattr(sys, "argv", ["run_python.py", "--catalog", "fixture", "--source-volume", "source",
                                    "--batch-id", "delivery", "--source-period", "fixture_period",
                                    "--file", "src/sql/gold/dim_lgu.py"])
    monkeypatch.setattr(quality, "ensure_monitoring", lambda *args: None)
    recorded = []

    def check(*args):
        recorded.append(args[3:])
        if args[-1]:
            raise quality.QualityError("persisted check failed")

    monkeypatch.setattr(quality, "check", check)
    return recorded


def test_runtime_error_remains_visible_and_successful_retry_resets_marker(monkeypatch, runner):
    arguments = sys.argv.copy()

    def broken(*args, **kwargs):
        raise OSError("fixture write failed")

    monkeypatch.setattr(run_python.runpy, "run_path", broken)
    with pytest.raises(OSError, match="fixture write failed"):
        run_python.main()
    monkeypatch.setattr(sys, "argv", arguments)
    monkeypatch.setattr(run_python.runpy, "run_path", lambda *args, **kwargs: None)
    run_python.main()
    assert [row[-1] for row in runner] == [1, 0]
    assert runner[0][:-1] == runner[1][:-1]


def test_named_quality_failure_is_preserved_without_a_generic_replacement(monkeypatch, runner):
    def blocked(*args, **kwargs):
        raise quality.QualityError("named_source_rule")

    monkeypatch.setattr(run_python.runpy, "run_path", blocked)
    with pytest.raises(quality.QualityError, match="named_source_rule"):
        run_python.main()
    assert runner == []
