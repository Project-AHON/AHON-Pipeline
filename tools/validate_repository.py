"""Offline bundle asset, dependency, schema and Markdown-link checks."""

import json
import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def validate():
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
    assert set(bundle["targets"]) == {"dev", "prod"}
    jobs = {}
    for path in (ROOT / "resources").glob("*.job.yml"):
        jobs.update(yaml.safe_load(path.read_text())["resources"]["jobs"])
    dependencies = set((ROOT / "requirements.txt").read_text().splitlines())
    for key, job in jobs.items():
        tasks = {t["task_key"]: t for t in job["tasks"]}
        assert len(tasks) == len(job["tasks"]), key
        for environment in job.get("environments", []):
            assert set(environment["spec"]["dependencies"]) == dependencies
        for task in tasks.values():
            assert all(d["task_key"] in tasks for d in task.get("depends_on", []))
            script = task.get("spark_python_task")
            if script:
                assert (ROOT / script["python_file"].removeprefix("../")).is_file()
                parameters = script["parameters"]
                for flag in ["--entry-point", "--file"]:
                    if flag in parameters:
                        asset = parameters[parameters.index(flag) + 1]
                        if asset.startswith("{{"):
                            parameter = re.fullmatch(
                                r"{{job.parameters.(\w+)}}", asset
                            ).group(1)
                            asset = next(
                                p["default"]
                                for p in job.get("parameters", [])
                                if p["name"] == parameter
                            )
                        assert (ROOT / asset).is_file()
            child = task.get("run_job_task")
            if child:
                assert (
                    re.fullmatch(
                        r"\$\{resources.jobs.(\w+).id\}", child["job_id"]
                    ).group(1)
                    in jobs
                )

        def visit(name, trail, tasks=tasks, key=key):
            assert name not in trail, f"Cycle in {key}: {trail}"
            for dep in tasks[name].get("depends_on", []):
                visit(dep["task_key"], trail | {name})

        for name in tasks:
            visit(name, set())
    json.loads((ROOT / "resources/contracts/source_contracts.json").read_text())
    broken = []
    for path in [ROOT / "README.md", *(ROOT / "docs").rglob("*.md")]:
        for target in re.findall(r"\]\(([^\s)]+)", path.read_text(encoding="utf-8")):
            if target.startswith(("http:", "https:", "#", "mailto:")):
                continue
            target = target.split("#")[0]
            if target and not (path.parent / target).exists():
                broken.append(f"{path.relative_to(ROOT)}: {target}")
    assert not broken, "\n".join(broken)
    print(
        f"PASS: {len(jobs)} job resources, packaged entry points, dependency DAGs and documentation links"
    )


if __name__ == "__main__":
    validate()
