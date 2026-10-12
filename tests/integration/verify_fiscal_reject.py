"""Contract-invalid financial rows must be saved before failing, with safe recovery."""

import csv
import json
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src/sql/common"))
sys.path.insert(0, str(Path(__file__).parent))
from quality import check, upsert
from runtime import batch_path, parser, publish_batch
from verify_delta import invoke, module


def verify(spark, args):
    from pyspark.sql import functions as F

    if "integration" not in args.catalog:
        raise ValueError("Use prior isolated synthetic fiscal fixtures")
    spark.sql(f"USE CATALOG {args.catalog}")
    clean = [
        "--file",
        "src/sql/datasets/blgf_ldrrmf_annual_lgu/silver/clean_blgf_ldrrmf_annual_lgu.sql",
    ]
    gold = ["--file", "src/sql/gold/build_fact_ldrrmf.sql"]
    invoke("src/sql/common/run_sql.py", args, clean)
    invoke("src/sql/common/run_sql.py", args, gold)
    assert spark.table("gold.fact_ldrrmf").count() == 2
    original = (
        spark.table("bronze.blgf_ldrrmf_annual_lgu")
        .where("total_budget_appropriation = 0")
        .limit(1)
    )

    def correction(total, suffix):
        return (
            original.withColumn("total_budget_appropriation", F.lit(total))
            .withColumn("_source_ref", F.lit(f"fixture/FY2023-{suffix}.xlsx"))
            .withColumn("_row_hash", F.lit(f"synthetic-{suffix}"))
            .withColumn("_observation_id", F.col("_row_hash"))
            .withColumn("_batch_id", F.lit(args.batch_id))
            .withColumn("_ingested_at", F.current_timestamp())
        )

    upsert(
        spark,
        correction(1.0, "invalid-total"),
        f"{args.catalog}.bronze.blgf_ldrrmf_annual_lgu",
        ["_observation_id"],
    )
    for _ in range(2):
        advanced = False
        try:
            invoke("src/sql/common/run_sql.py", args, clean)
            advanced = True
        except ValueError:
            pass
        assert not advanced
    assert spark.table("silver.blgf_ldrrmf_annual_lgu_reject").count() == 1
    # Negative magnitude is valid on a logarithmic scale, not an invalid number.
    sys.path.insert(0, str(ROOT / "src/sql/datasets/philvolcs_earthquake"))
    quake = module(
        "negative_magnitude_verify",
        "src/sql/datasets/philvolcs_earthquake/observation_pipeline.py",
    )
    args.source_period = "Q2_2024"
    with tempfile.TemporaryDirectory() as temp:
        file = Path(temp) / "negative_magnitude.csv"
        with file.open("w", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(quake.SOURCE_COLUMNS)
            writer.writerow(
                [
                    "01 January 2024 - 01:00 AM",
                    "10",
                    "120",
                    "5",
                    "-1",
                    "025 km N 20° E of Town 0_0_0 (Province 0_0)",
                    "January",
                    "2024",
                ]
            )
        publish_batch(batch_path(args, "philvolcs_earthquake_data"), [(file, 1)])
        quake.bronze(spark, args)
        quake.silver(spark, args)
        assert (
            spark.table("silver.philvolcs_earthquake_observation_clean")
            .where("magnitude = -1 AND NOT has_dq_issues")
            .count()
            == 1
        )
    assert spark.table("gold.fact_ldrrmf").count() == 2
    upsert(
        spark,
        correction(0.0, "valid-total"),
        f"{args.catalog}.bronze.blgf_ldrrmf_annual_lgu",
        ["_observation_id"],
    )
    for _ in range(2):
        invoke("src/sql/common/run_sql.py", args, clean)
        invoke("src/sql/common/run_sql.py", args, gold)
    assert spark.table("gold.fact_ldrrmf").count() == 3
    assert spark.table("silver.blgf_ldrrmf_annual_lgu_reject").count() == 1
    check(spark, args.catalog, args.batch_id, "fiscal_reject_integration", "fixture", 0)
    print(
        json.dumps(
            {
                "status": "PASS",
                "suite": "fiscal_reject",
                "checks": [
                    "financial replay",
                    "reject before blocking",
                    "stable quarantine on retry",
                    "Gold unchanged after failed Silver",
                    "new valid correction recovery",
                    "finite negative magnitude retained",
                ],
            }
        )
    )


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    verify(SparkSession.builder.getOrCreate(), parser(__doc__).parse_args())
