"""Real Delta replay, correction, quarantine, mapping and release-gate tests on isolated dev data."""

import csv
import importlib.util
import json
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src/sql/common"))
from quality import check, upsert
from runtime import parser, publish_batch


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, ROOT / path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


def invoke(path, args, extra=()):
    import runpy

    previous = sys.argv
    sys.argv = [
        path,
        "--catalog",
        args.catalog,
        "--source-volume",
        args.source_volume,
        "--batch-id",
        args.batch_id,
        *extra,
    ]
    try:
        runpy.run_path(str(ROOT / path), run_name="__main__")
    finally:
        sys.argv = previous


def verify(spark, args):
    if "integration" not in args.catalog or args.catalog == "ahon":
        raise ValueError(
            "Synthetic verification requires a dedicated integration catalog"
        )
    spark.sql(
        f"CREATE CATALOG IF NOT EXISTS {args.catalog} COMMENT 'Isolated synthetic AHON verification'"
    )
    spark.sql(f"USE CATALOG {args.catalog}")
    for schema in ["reference", "bronze", "silver", "gold", "platinum", "monitoring"]:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS reference.{args.source_volume}")
    # A fixture LGU is deliberately synthetic, with a competing locality in another province.
    psgc = spark.createDataFrame(
        [
            (
                "0100000000",
                "Region 0",
                "REG",
                "0100000000",
                None,
                None,
                None,
                "Q2_2024",
            ),
            (
                "0100100000",
                "Province 0_0",
                "PROV",
                "0100000000",
                "0100100000",
                None,
                "Province 0_0",
                "Q2_2024",
            ),
            (
                "0100101000",
                "Town 0_0_0",
                "MUN",
                "0100000000",
                "0100100000",
                "0100101000",
                "Province 0_0",
                "Q2_2024",
            ),
            (
                "0100201000",
                "Town 0_0_0",
                "MUN",
                "0100000000",
                "0100200000",
                "0100201000",
                "Other province",
                "Q2_2024",
            ),
        ],
        "psgc_code string, area_name string, geographic_level string, region_psgc string, province_psgc string, citymun_psgc string, province_name string, _api_period string",
    )
    upsert(
        spark,
        psgc,
        f"{args.catalog}.silver.psa_psgc_clean",
        ["psgc_code", "_api_period"],
    )
    dim = psgc.selectExpr(
        "psgc_code",
        "area_name",
        "geographic_level",
        "region_psgc AS region_code",
        "province_psgc AS province_code",
        "citymun_psgc AS municipality_code",
        "cast(NULL AS STRING) AS barangay_code",
        "'Q2_2024' AS source_period",
    )
    upsert(spark, dim, f"{args.catalog}.gold.dim_lgu", ["psgc_code"])
    from runtime import batch_path

    with tempfile.TemporaryDirectory() as temp:
        file = Path(temp) / "2024_population_urban.csv"
        rows = [["PHILIPPINES 1/", "100", "40", "40"]]
        for region in range(18):
            rows.append([f"..Region {region}", "100", "40", "40"])
            for province in range(7 if region < 10 else 6):
                rows.append([f"....Province {region}_{province}", "100", "40", "40"])
                # 118 provinces, first 73 have one additional locality.
                province_index = (
                    sum(7 if r < 10 else 6 for r in range(region)) + province
                )
                for town in range(14 if province_index < 73 else 13):
                    rows.append(
                        [f"......Town {region}_{province}_{town}", "100", "40", "40"]
                    )
        assert len(rows) == 1744
        rows[-1][1] = "1.5"  # fractional persons must be quarantined
        with file.open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "Geographic Location",
                    "Total Population",
                    "Urban Population",
                    "Percent Urban",
                ]
            )
            writer.writerows(rows)
        publish_batch(batch_path(args, "psa_population"), [(file, len(rows))])
        invoke("src/sql/datasets/psa_population/bronze/load_psa_population.py", args)
        pop = module(
            "population_verify",
            "src/sql/datasets/psa_population/silver/clean_psa_population.py",
        )
        pop.transform(spark, args)
        before = (
            spark.table("silver.psa_population_clean")
            .select("_observation_id", "_batch_id", "_processed_at")
            .collect()
        )
        invoke("src/sql/datasets/psa_population/bronze/load_psa_population.py", args)
        pop.transform(spark, args)
        after = (
            spark.table("silver.psa_population_clean")
            .select("_observation_id", "_batch_id", "_processed_at")
            .collect()
        )
        assert set(map(tuple, before)) == set(map(tuple, after))
        assert spark.table("bronze.psa_population").count() == 1744
        assert spark.table("silver.psa_population_clean").count() == 1743
        assert spark.table("silver.psa_population_reject").count() == 1
        args.source_period = "Q2_2024"
        population_gold = module(
            "population_gold_verify", "src/sql/gold/build_fact_population.py"
        )
        population_gold.build(spark, args)
        assert (
            spark.table("gold.fact_population")
            .where("psgc_code = '0100101000'")
            .count()
            == 1
        )
        assert spark.table("gold.fact_population").count() == 1743
        # Correction creates a new immutable source version; accepted business keys update.
        corrected = SimpleNamespace(**vars(args))
        corrected.batch_id = args.batch_id + "_correction"
        rows[-1][1] = "100"
        with file.open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "Geographic Location",
                    "Total Population",
                    "Urban Population",
                    "Percent Urban",
                ]
            )
            writer.writerows(rows)
        publish_batch(batch_path(corrected, "psa_population"), [(file, len(rows))])
        invoke(
            "src/sql/datasets/psa_population/bronze/load_psa_population.py", corrected
        )
        pop.transform(spark, corrected)
        assert spark.table("bronze.psa_population").count() == 3488
        assert spark.table("silver.psa_population_clean").count() == 1744
        assert spark.table("silver.psa_population_reject").count() == 1
        # Identical earthquake rows retain separate source ordinals; event identity withheld.
        sys.path.insert(0, str(ROOT / "src/sql/datasets/philvolcs_earthquake"))
        eq = module(
            "eq_verify", "src/sql/datasets/philvolcs_earthquake/observation_pipeline.py"
        )
        eq_file = Path(temp) / "phivolcs_earthquake_2024_01.csv"
        normal = [
            "01 January 2024 - 01:00 AM",
            "10",
            "120",
            "5",
            "3",
            "025 km N 20° E of Town 0_0_0 (Province 0_0)",
            "January",
            "2024",
        ]
        with eq_file.open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(eq.SOURCE_COLUMNS)
            writer.writerows([normal, normal, [*normal[:1], "NaN", *normal[2:]]])
        publish_batch(batch_path(args, "philvolcs_earthquake_data"), [(eq_file, 3)])
        eq.bronze(spark, args)
        eq.silver(spark, args)
        eq.bronze(spark, args)
        eq.silver(spark, args)
        assert spark.table("bronze.philvolcs_earthquake_observation").count() == 3
        assert spark.table("silver.philvolcs_earthquake_observation_clean").count() == 3
        assert spark.table("silver.philvolcs_earthquake_reject").count() == 1
        assert (
            spark.table("silver.philvolcs_earthquake_observation_clean")
            .where("psgc_code IS NOT NULL")
            .count()
            == 0
        )
        # Actual CMCI pipeline: 35 source indicators with one missing and one invalid.
        sys.path.insert(0, str(ROOT / "src/sql/datasets/cmci"))
        cmci = module("cmci_verify", "src/sql/datasets/cmci/pipeline.py")
        map_file = Path(temp) / "mapping.csv"
        map_file.write_text(
            "psgc_code,cmci_name,source_period,reviewed,reviewer,evidence\n0100101000,Fixture locality,Q2_2024,true,synthetic-test,fixture\n",
            encoding="utf-8",
        )
        args.mapping_file = str(map_file)
        mappings = cmci.reviewed_mapping(map_file, args.source_period)
        html = "<html><title>Data Portal Results</title>"
        for index, label in enumerate(cmci.APPROVED_INDICATORS):
            value = "-" if index == 0 else "NaN" if index == 1 else "1"
            html += f"<h2>{label}</h2><table><tr><th>Province / LGU</th><th>2024</th></tr><tr><td>Fixture locality</td><td>{value}</td></tr></table>"
        cmci_file = Path(temp) / "batch_00000.json"
        cmci_file.write_text(
            json.dumps(
                {
                    "html": html,
                    "mappings": mappings,
                    "years": ["2024"],
                    "source_period": args.source_period,
                    "response_sha256": "0" * 64,
                }
            ),
            encoding="utf-8",
        )
        publish_batch(batch_path(args, "cmci_data_portal"), [(cmci_file, 35)])
        for _ in range(2):
            cmci.bronze(spark, args)
            cmci.silver(spark, args)
            cmci.gold(spark, args)
            cmci.platinum(spark, args)
        assert spark.table("silver.cmci_reject").count() == 1
        assert spark.table("gold.fact_cmci_indicator").count() == 34
        assert (
            spark.table("platinum.cmci_pillar_score")
            .where(
                "pillar_name = 'Economic Dynamism' AND coverage_status = 'INSUFFICIENT_DATA'"
            )
            .count()
            == 1
        )
    # Run the actual contextual mapper and fiscal SQL with zero/overspent cases.
    from pyspark.sql import functions as F

    fiscal = spark.createDataFrame(
        [
            (
                "Region 0",
                "Province 0_0",
                "Town 0_0_0",
                "Municipality",
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
            ),
            (
                "Region 0",
                "Other province",
                "Town 0_0_0",
                "Municipality",
                70.0,
                140.0,
                30.0,
                60.0,
                100.0,
                200.0,
            ),
        ],
        "region string, province string, lgu_name string, lgu_type string, `70pct_ldrrmf_budget_appropriation` double, `70pct_ldrrmf_expenditures` double, `30pct_quick_response_fund_budget_appropriation` double, `30pct_quick_response_fund_expenditures` double, total_budget_appropriation double, total_expenditures double",
    )
    fiscal = (
        fiscal.withColumn("_source_name", F.lit("synthetic_blgf"))
        .withColumn("_source_ref", F.lit("fixture/FY2024-LDRRMF-by-LGU.xlsx"))
        .withColumn("_batch_id", F.lit(args.batch_id))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_row_hash", F.sha2(F.to_json(F.struct(*fiscal.columns)), 256))
        .withColumn("_observation_id", F.col("_row_hash"))
    )
    upsert(
        spark,
        fiscal,
        f"{args.catalog}.bronze.blgf_ldrrmf_annual_lgu",
        ["_observation_id"],
    )
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
    invoke("src/sql/platinum/build_evidence.py", args)
    assert (
        spark.table("platinum.lgu_evidence_status")
        .where("risk_level IS NOT NULL OR risk_preparedness_gap IS NOT NULL")
        .count()
        == 0
    )
    # Check persistence happens before a blocking failure; next work never runs.
    advanced = False
    try:
        check(spark, args.catalog, args.batch_id, "synthetic_failure", "fixture", 1)
        advanced = True
    except ValueError:
        pass
    assert not advanced
    assert (
        spark.table("monitoring.dq_result")
        .where("check_id = 'synthetic_failure' AND disposition = 'BLOCKED'")
        .count()
        == 1
    )
    check(spark, args.catalog, args.batch_id, "integration_suite", "fixture", 0)
    print(
        json.dumps(
            {
                "status": "PASS",
                "catalog": args.catalog,
                "run_id": args.batch_id,
                "checks": [
                    "Delta replay",
                    "source correction",
                    "population arithmetic",
                    "quarantine replay",
                    "contextual join cardinality",
                    "earthquake observation ordinals",
                    "CMCI missing and invalid values",
                    "blocking failure persistence",
                    "fiscal zero denominator and overspending",
                    "withheld risk and gap",
                ],
            }
        )
    )


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    verify(SparkSession.builder.getOrCreate(), parser(__doc__).parse_args())
