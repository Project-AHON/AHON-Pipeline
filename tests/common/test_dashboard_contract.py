"""Guard the reviewed PR105 presentation and maintained dataset contracts."""
import copy
import hashlib
import json
from pathlib import Path

import sqlglot
from sqlglot import exp

ROOT = Path(__file__).resolve().parents[2]
EXPORT = ROOT / "resources/dashboard/Project AHON LGU Command Center for DRRM Plans.lvdash.json"
CONTRACT = ROOT / "tests/fixtures/dashboard_original_contract.json"


def test_reviewed_dashboard_structure_and_output_bindings_are_preserved():
    dashboard = json.loads(EXPORT.read_text(encoding="utf-8"))
    contract = json.loads(CONTRACT.read_text(encoding="utf-8"))
    presentation = copy.deepcopy(dashboard)
    for page in presentation["pages"]:
        for entry in page["layout"]:
            widget = entry["widget"]
            # Titles and explanatory text may correct misleading terminology.
            widget.pop("multilineTextboxSpec", None)
            if "spec" in widget:
                widget["spec"].pop("frame", None)
    presentation = {key: presentation[key] for key in ["pages", "relationshipGraphs", "uiSettings"]}
    digest = hashlib.sha256(json.dumps(presentation, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    assert digest == contract["structure_sha256"], "Reviewed pages/widgets/layout/filter bindings changed"
    actual = {}
    for dataset in dashboard["datasets"]:
        query = sqlglot.parse_one(dataset["config"]["source"], read="databricks")
        assert isinstance(query, exp.Select)
        actual[dataset["name"]] = query.named_selects
        for table in query.find_all(exp.Table):
            if table.catalog:
                assert table.catalog == "ahon_dev"
                assert (table.db, table.name) in {
                    ("gold", "dim_lgu"), ("gold", "fact_population"),
                    ("gold", "fact_ldrrmf"), ("gold", "fact_cmci_indicator"),
                    ("gold", "dim_cmci_indicator"),
                    ("silver", "philvolcs_earthquake_observation_clean"),
                }
    assert actual == contract["dataset_columns"], "Existing widget output-column contracts changed"


def test_unavailable_geometry_and_methods_are_not_fabricated():
    dashboard = json.loads(EXPORT.read_text(encoding="utf-8"))
    datasets = {d["name"]: d["config"] for d in dashboard["datasets"]}
    city = sqlglot.parse_one(datasets["lgu_city_map"]["source"], read="databricks")
    withheld = {"geometry_wkt", "quake_count", "max_magnitude", "cmci_total_score", "cmci_overall_score"}
    for field in city.expressions:
        if field.alias_or_name in withheld:
            assert isinstance(field.this, exp.Cast) and isinstance(field.this.this, exp.Null)
    earthquake = datasets["earthquake_events"]["source"]
    assert "WHERE NOT has_dq_issues" in earthquake
    assert "CAST(NULL AS STRING) AS region_code" in earthquake
    for name in ["lgu_funding", "lgu_funding_yoy", "lgu_city_map"]:
        sql = datasets[name]["source"].upper()
        assert "REGEXP_REPLACE" not in sql and "LPAD(" not in sql
    measures = {m["name"]: m for m in datasets["lgu_city_map"]["measures"]}
    assert measures["cmci_resiliency"]["expr"] == "AVG(cmci_resiliency)"


def test_baseline_comparison_and_selection_counters_use_existing_columns():
    dashboard = json.loads(EXPORT.read_text(encoding="utf-8"))
    datasets = {d["name"]: d["config"] for d in dashboard["datasets"]}
    baseline = sqlglot.parse_one(datasets["lgu_baseline_long"]["source"], read="databricks")
    baseline_ctes = {cte.alias: cte.this for cte in baseline.args["with_"].expressions}
    city = sqlglot.parse_one(datasets["lgu_city_map"]["source"], read="databricks")
    city = sqlglot.parse_one(city.sql(dialect="databricks"), read="databricks")
    # SQL serialization lifts nested CTEs into the outer WITH clause.
    city_ctes = city.args.pop("with_")
    assert baseline_ctes["base"] == city
    for cte in city_ctes.expressions:
        assert baseline_ctes[cte.alias] == cte.this
    funding = datasets["lgu_funding"]
    source = sqlglot.parse_one(funding["source"], read="databricks")
    assert "utilization_tier" in source.named_selects
    measures = {m["name"]: m for m in funding["measures"]}
    assert "MAX(utilization_tier)" in measures["utilization_tier_selection_status"]["expr"]
    for name, config in datasets.items():
        columns = set(sqlglot.parse_one(config["source"], read="databricks").named_selects)
        measure_names = {measure["name"] for measure in config.get("measures", [])}
        for field in config.get("dimensions", []) + config.get("measures", []):
            expression = sqlglot.parse_one(field["expr"], read="databricks")
            references = {column.name for column in expression.find_all(exp.Column)}
            # Derived measures may refer to other declared measures (e.g. YoY).
            available = columns | (measure_names - {field["name"]})
            assert references <= available, (name, field["name"], references - available)
