"""Targeted fiscal SQL and invalid-to-valid CMCI correction checks on prior fixtures."""

import base64
import hashlib
import json
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src/sql/common"))
sys.path.insert(0, str(ROOT / "src/sql/datasets/cmci"))
sys.path.insert(0, str(Path(__file__).parent))
from quality import check
from runtime import batch_path, parser, publish_batch
from verify_delta import invoke, module


def verify(spark, args):
    if "integration" not in args.catalog:
        raise ValueError("Use prior isolated synthetic fixtures")
    spark.sql(f"USE CATALOG {args.catalog}")
    assert spark.table("bronze.blgf_ldrrmf_annual_lgu").count() == 2
    # Complete population Gold after the prior suite's valid correction, using current code.
    correction = SimpleNamespace(**vars(args))
    prior_corrections = (
        spark.table("bronze.psa_population")
        .where("_batch_id LIKE '%_correction'")
        .select("_batch_id")
        .distinct()
        .collect()
    )
    assert len(prior_corrections) == 1, (
        "Expected exactly one prior synthetic correction batch"
    )
    correction.batch_id = prior_corrections[0]._batch_id
    correction.source_period = "Q2_2024"
    population = module(
        "population_correction_verify",
        "src/sql/datasets/psa_population/silver/clean_psa_population.py",
    )
    population_gold = module(
        "population_correction_gold_verify", "src/sql/gold/build_fact_population.py"
    )
    for _ in range(2):
        population.transform(spark, correction)
        population_gold.build(spark, correction)
    assert spark.table("gold.fact_population").count() == 1744
    invoke(
        "src/sql/common/run_python.py",
        args,
        [
            "--file",
            "src/sql/datasets/blgf_ldrrmf_annual_lgu/silver/build_blgf_ldrrmf_lgu_map.py",
        ],
    )
    for _ in range(2):
        invoke(
            "src/sql/common/run_sql.py",
            args,
            [
                "--file",
                "src/sql/datasets/blgf_ldrrmf_annual_lgu/silver/clean_blgf_ldrrmf_annual_lgu.sql",
            ],
        )
        invoke(
            "src/sql/common/run_sql.py",
            args,
            ["--file", "src/sql/gold/build_fact_ldrrmf.sql"],
        )
    assert spark.table("gold.fact_ldrrmf").count() == 2
    assert (
        spark.table("gold.fact_ldrrmf")
        .where("total_appropriation = 0 AND utilization_rate_total IS NULL")
        .count()
        == 1
    )
    assert (
        spark.table("gold.fact_ldrrmf")
        .where("utilization_rate_total = 2 AND has_dq_issues")
        .count()
        == 1
    )
    cmci = module("cmci_correction_verify", "src/sql/datasets/cmci/pipeline.py")
    args.source_period = "Q2_2024"
    with tempfile.TemporaryDirectory() as temp:
        mapping = Path(temp) / "mapping.csv"
        mapping.write_text(
            "psgc_code,cmci_name,source_period,reviewed,reviewer,evidence\n0100101000,Fixture locality,Q2_2024,true,synthetic-test,fixture\n"
        )
        args.mapping_file = str(mapping)
        mappings = cmci.reviewed_mapping(mapping, args.source_period)
        code = list(cmci.APPROVED_INDICATORS.values())[2]
        for state in ["invalid", "recovered"]:
            scope = SimpleNamespace(**vars(args))
            scope.batch_id += "_" + state
            html = "<title>Data Portal Results</title>"
            for index, label in enumerate(cmci.APPROVED_INDICATORS):
                value = (
                    "-"
                    if index == 0
                    else "NaN"
                    if index == 1 or (index == 2 and state == "invalid")
                    else "1"
                )
                html += f"<h2>{label}</h2><table><tr><th>Province / LGU</th><th>2024</th></tr><tr><td>Fixture locality</td><td>{value}</td></tr></table>"
            file = Path(temp) / "correction.json"
            file.write_text(
                json.dumps(
                    {
                        "html": html,
                        "response_bytes_base64": base64.b64encode(
                            html.encode()
                        ).decode(),
                        "response_sha256": hashlib.sha256(html.encode()).hexdigest(),
                        "mappings": mappings,
                        "years": ["2024"],
                        "source_period": args.source_period,
                    }
                )
            )
            publish_batch(batch_path(scope, "cmci_data_portal"), [(file, 35)])
            for _ in range(2):
                cmci.bronze(spark, scope)
                cmci.silver(spark, scope)
                cmci.gold(spark, scope)
                cmci.platinum(spark, scope)
            row = (
                spark.table("gold.fact_cmci_indicator")
                .where(f"indicator_code = '{code}'")
                .first()
            )
            assert (
                row.raw_score is None and row.has_dq_issues
                if state == "invalid"
                else row.raw_score == 1 and not row.has_dq_issues
            )
            assert spark.table("gold.fact_cmci_indicator").count() == 34
    invoke("src/sql/platinum/build_evidence.py", args)
    assert (
        spark.table("platinum.lgu_evidence_status")
        .where("risk_level IS NOT NULL OR risk_preparedness_gap IS NOT NULL")
        .count()
        == 0
    )
    check(spark, args.catalog, args.batch_id, "component_integration", "fixture", 0)
    print(
        json.dumps(
            {
                "status": "PASS",
                "suite": "components",
                "checks": [
                    "actual fiscal mapper/SQL replay",
                    "zero denominator",
                    "overspending",
                    "invalid correction withheld",
                    "valid correction recovery",
                    "withheld risk and gap",
                ],
            }
        )
    )


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    verify(SparkSession.builder.getOrCreate(), parser(__doc__).parse_args())
