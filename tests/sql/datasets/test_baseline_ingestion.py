"""Failure and retry tests; fixtures are synthetic, never replacement source data."""

import argparse
import importlib.util
import shutil
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "src/sql/common"))
from runtime import batch_path, completed_batch, identifier, publish_batch


def load_module(name, relative):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def extracts():
    return {
        "psgc": load_module(
            "psgc_extract_test", "src/sql/datasets/psa_psgc/extract/psa_psgc_ingest.py"
        ),
        "population": load_module(
            "population_extract_test",
            "src/sql/datasets/psa_population/extract/extract_psa_population.py",
        ),
        "earthquake": load_module(
            "earthquake_extract_test",
            "src/sql/datasets/philvolcs_earthquake/extract/philvolcs_earthquake_ingest.py",
        ),
    }


def test_batch_retry_and_immutability(tmp_path):
    raw = tmp_path / "input.csv"
    raw.write_text("a,b\n1,2\n")
    batch = tmp_path / "batch"
    first = publish_batch(batch, [(raw, 1)])
    assert publish_batch(batch, [(raw, 1)]) == first
    raw.write_text("a,b\n3,4\n")
    with pytest.raises(ValueError, match="cannot be changed"):
        publish_batch(batch, [(raw, 1)])
    assert (batch / "input.csv").read_text() == "a,b\n1,2\n"


def test_partial_copy_never_publishes_and_retry_repairs(tmp_path, monkeypatch):
    a, b = tmp_path / "a.csv", tmp_path / "b.csv"
    a.write_text("a")
    b.write_text("b")
    original = shutil.copyfile

    def failing_copy(source, target):
        if Path(source).name == "b.csv":
            raise OSError("Synthetic interrupted transfer")
        return original(source, target)

    batch = tmp_path / "batch"
    monkeypatch.setattr(shutil, "copyfile", failing_copy)
    with pytest.raises(OSError):
        publish_batch(batch, [(a, 1), (b, 1)])
    assert not (batch / "_SUCCESS.json").exists()
    with pytest.raises(FileNotFoundError):
        completed_batch(batch)
    monkeypatch.setattr(shutil, "copyfile", original)
    publish_batch(batch, [(a, 1), (b, 1)])
    assert len(completed_batch(batch)["files"]) == 2


def test_checksum_tampering_is_a_critical_failure(tmp_path):
    raw = tmp_path / "input.csv"
    raw.write_text("original")
    batch = tmp_path / "batch"
    publish_batch(batch, [(raw, 1)])
    (batch / "input.csv").write_text("tampered")
    with pytest.raises(ValueError, match="integrity"):
        completed_batch(batch)


@pytest.mark.parametrize("value", ["../other", "x; DROP TABLE x", "AHON", "x.y"])
def test_configuration_rejects_identifiers_and_batch_traversal(value):
    with pytest.raises(argparse.ArgumentTypeError):
        identifier(value)
    if value != "AHON":
        with pytest.raises(ValueError):
            batch_path(
                argparse.Namespace(
                    catalog="test", source_volume="source", batch_id=value
                ),
                "psa_psgc",
            )


@pytest.mark.parametrize(
    "csv",
    [
        b"",
        b"wrong,columns\n1,2\n",
        b"Geographic Location,Total Population,Urban Population,Percent Urban\n",
        b"Geographic Location,Total Population,Urban Population,Percent Urban\nX,1,2\n",
    ],
)
def test_population_rejects_unusable_extract(extracts, csv):
    with pytest.raises(ValueError):
        extracts["population"].validate_csv(csv)


def test_psgc_rejects_cross_origin_pagination(extracts, tmp_path):
    module = extracts["psgc"]
    response = Mock(status_code=200)
    response.json.return_value = {
        "results": [{"code": "1", "area_name": "Synthetic", "geographic_level": "Reg"}],
        "next": "https://untrusted.invalid/page",
    }
    session = Mock()
    session.get.return_value = response
    with pytest.raises(ValueError, match="origin"):
        module.extract_period(
            "synthetic-secret", "fixture_period", tmp_path / "source.jsonl", session,
            "https://approved.invalid/psgc", 1, 10
        )
    assert session.get.call_count == 1


def test_psgc_http_failure_never_exposes_token(extracts, tmp_path):
    session = Mock()
    session.get.return_value = Mock(status_code=403)
    with pytest.raises(RuntimeError) as error:
        extracts["psgc"].extract_period(
            "synthetic-secret", "fixture_period", tmp_path / "source.jsonl", session,
            "https://approved.invalid/psgc", 1, 10
        )
    assert "synthetic-secret" not in str(error.value)


def test_earthquake_month_range_and_structure(extracts):
    module = extracts["earthquake"]
    assert list(module.months_between("2024-12", "2025-01", "UTC")) == [(2024, 12), (2025, 1)]
    with pytest.raises(ValueError):
        list(module.months_between("2025-02", "2025-01", "UTC"))
    response = Mock(
        text="<table><tr><td>Unexpected source</td></tr></table>", status_code=200
    )
    session = Mock()
    session.get.return_value = response
    with pytest.raises(ValueError):
        module.fetch_month(session, 2024, 1, "https://approved.invalid/{year}/{month_name}", "https://approved.invalid/latest", 10, "UTC")


def test_bundle_tasks_use_existing_files_and_have_no_cycles():
    import yaml
    for resource in (ROOT / "resources").glob("*.job.yml"):
        config = yaml.safe_load(resource.read_text())
        for job in config["resources"]["jobs"].values():
            tasks = job["tasks"]
            seen = set()
            for task in tasks:
                assert all(dep["task_key"] in seen for dep in task.get("depends_on", []))
                seen.add(task["task_key"])
                if "spark_python_task" in task:
                    file = task["spark_python_task"]["python_file"]
                    assert (ROOT / "resources" / file).resolve().is_file()
                    params = task["spark_python_task"]["parameters"]
                    if "--file" in params:
                        assert (ROOT / params[params.index("--file") + 1]).is_file()


def test_interrupted_completion_marker_can_be_repaired(tmp_path):
    raw = tmp_path / "input.csv"
    raw.write_text("source")
    batch = tmp_path / "batch"
    batch.mkdir()
    (batch / "_SUCCESS.json").write_text('{"files":')
    publish_batch(batch, [(raw, 1)])
    assert completed_batch(batch)["files"][0]["rows"] == 1


def test_wired_sql_is_valid_and_each_section_is_one_statement():
    import sqlglot
    import sqlparse
    import yaml
    for resource in (ROOT / "resources").glob("*.job.yml"):
        config = yaml.safe_load(resource.read_text())
        for job in config["resources"]["jobs"].values():
            for task in job["tasks"]:
                params = task.get("spark_python_task", {}).get("parameters", [])
                if "--file" in params:
                    path = ROOT / params[params.index("--file") + 1]
                    if path.suffix == ".sql":
                        for section in sqlparse.split(path.read_text(encoding="utf-8")):
                            assert len(sqlglot.parse(section, read="databricks")) == 1
