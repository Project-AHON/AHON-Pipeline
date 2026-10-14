"""Verify the deployed DAG and runtime asset contracts, beyond bundle schema validation."""

import ast
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]


def jobs():
    files = list((ROOT / "resources/jobs").glob("*.yml"))
    assert len(files) == 3
    for file in files:
        for name, job in yaml.safe_load(file.read_text())["resources"]["jobs"].items():
            yield file, name, job


def test_every_task_has_packaged_executable_assets_and_bounded_retries():
    count = 0
    for file, _, job in jobs():
        assert job["max_concurrent_runs"] == "${var.max_concurrent_runs}"
        assert "schedule" not in job and "continuous" not in job
        envs = {e["environment_key"] for e in job["environments"]}
        seen = set()
        for task in job["tasks"]:
            count += 1
            assert task["run_if"] == "ALL_SUCCESS"
            assert task["max_retries"] in (0, "${var.task_retries}")
            assert task["environment_key"] in envs
            assert all(dep["task_key"] in seen for dep in task.get("depends_on", []))
            seen.add(task["task_key"])
            entry = task["spark_python_task"]
            assert (file.parent / entry["python_file"]).resolve().is_file()
            args = entry["parameters"]
            path = ROOT / args[args.index("--entry-point") + 1]
            assert path.is_file()
            ast.parse(path.read_text(encoding="utf-8"))
            if "--file" in args:
                assert (ROOT / args[args.index("--file") + 1]).is_file()
    assert count == 32


def test_independent_sources_are_not_blocked_by_cmci_or_earthquake():
    graph = next(job for _, name, job in jobs() if name == "ahon_pipeline")
    dependencies = {t["task_key"]: {d["task_key"] for d in t.get("depends_on", [])} for t in graph["tasks"]}
    for name in ["psgc_extract", "population_extract", "ldrrmf_extract", "earthquake_extract", "osm_extract"]:
        assert dependencies[name] == {"setup"}
    assert dependencies["ldrrmf_gold"] == {"ldrrmf_silver", "dim_lgu"}
    assert dependencies["barangay_population_gold"] == {"barangay_population_silver", "dim_lgu"}
    assert dependencies["earthquake_silver"] == {"earthquake_bronze", "psgc_silver"}
    assert not any("earthquake_gold" == task for task in dependencies)


def test_approved_mapping_override_is_synced_instead_of_missing_at_runtime():
    file = ROOT / "src/sql/datasets/blgf_ldrrmf_annual_lgu/silver/blgf_ldrrmf_lgu_map_overrides.csv"
    assert file.is_file()
    config = yaml.safe_load((ROOT / "databricks.yml").read_text())
    assert "src/**" in config["sync"]["include"]
    assert not any("csv" in exclusion for exclusion in config["sync"].get("exclude", []))


def test_operational_settings_have_no_committed_defaults():
    config = yaml.safe_load((ROOT / "databricks.yml").read_text())
    assert all("default" not in value for value in config["variables"].values())
    for _, _, job in jobs():
        assert job["timeout_seconds"] == "${var.job_timeout_seconds}"
        for environment in job["environments"]:
            assert environment["spec"]["environment_version"] == "${var.environment_version}"
        for task in job["tasks"]:
            assert task["timeout_seconds"] == "${var.task_timeout_seconds}"
            params = task["spark_python_task"]["parameters"]
            assert not any(value.startswith("https://") or value.startswith("/Volumes/") for value in params)
            for flag in ["--source-period", "--census-year", "--start-year", "--end-year", "--source-file", "--expected-rows", "--expected-profile"]:
                if flag in params:
                    assert params[params.index(flag) + 1].startswith("${var.")


def test_cmci_definition_asset_is_explicit_and_packaged():
    config = yaml.safe_load((ROOT / "databricks.yml").read_text())
    assert "resources/cmci_indicators.csv" in config["sync"]["include"]
    assert (ROOT / "resources/cmci_indicators.csv").is_file()
    pipeline = next(job for _, name, job in jobs() if name == "ahon_pipeline")
    for task in pipeline["tasks"]:
        if task["task_key"].startswith("cmci_"):
            assert "--indicator-file" in task["spark_python_task"]["parameters"]
