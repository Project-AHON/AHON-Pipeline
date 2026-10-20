"""Bind dashboard fields to maintained serving contracts, not personal catalogs."""
import json
import re
from pathlib import Path

import sqlglot
from sqlglot import exp

ROOT = Path(__file__).resolve().parents[2]


def test_dashboard_queries_resolve_to_maintained_component_columns():
    export = next((ROOT / "resources/dashboard").glob("*.lvdash.json"))
    dashboard = json.loads(export.read_text())
    sql_contracts = {
        "population_components": "src/sql/platinum/population_components.sql",
        "earthquake_observation_summary": "src/sql/platinum/earthquake_components.sql",
    }
    columns = {}
    for table, path in sql_contracts.items():
        tree = sqlglot.parse_one((ROOT / path).read_text(), read="databricks")
        columns[table] = set(tree.find(exp.Select).named_selects)
    funding = sqlglot.parse((ROOT / "src/sql/gold/build_fact_ldrrmf.sql").read_text(), read="databricks")
    candidate = next(tree for tree in funding if isinstance(tree, exp.Create))
    columns["funding_components"] = set(candidate.find(exp.Select).named_selects) | {
        "reference_area_name", "geographic_level", "interpretation"}
    dictionary = (ROOT / "docs/data/data-dictionary/cmci_pillar_coverage.md").read_text()
    columns["cmci_pillar_coverage"] = set(re.findall(r"^\| `([^`]+)` \|", dictionary, re.M))
    datasets = {}
    for dataset in dashboard["datasets"]:
        query = sqlglot.parse_one("".join(dataset["queryLines"]), read="databricks")
        tables = list(query.find_all(exp.Table))
        assert len(tables) == 1 and not tables[0].db and not tables[0].catalog
        table = tables[0].name
        assert table in columns
        assert set(query.named_selects) <= columns[table]
        assert not list(query.find_all(exp.AggFunc)), "Do not combine unlike scores or reference periods"
        datasets[dataset["name"]] = set(query.named_selects)
    assert len(datasets) == 4
    for page in dashboard["pages"]:
        for entry in page["layout"]:
            widget = entry["widget"]
            if "spec" not in widget:
                continue
            spec = widget["spec"]
            assert spec["version"] == 2
            for query in widget["queries"]:
                fields = query["query"]["fields"]
                names = {field["name"] for field in fields}
                assert names <= datasets[query["query"]["datasetName"]]
                encoded = spec["encodings"].get("columns", spec["encodings"].get("fields", []))
                assert {field["fieldName"] for field in encoded} <= names
                if spec["widgetType"] == "table":
                    assert query["query"]["disaggregated"] is True
