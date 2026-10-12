"""Materialize supported evidence at municipality/city grain; withhold unsupported scores."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "common"))
from quality import unique, upsert
from runtime import parser


def main():
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    p = parser(__doc__)
    p.add_argument("--source-period", default="Q2_2024")
    p.add_argument("--reporting-year", type=int, default=2024)
    args = p.parse_args()
    spark = SparkSession.builder.getOrCreate()
    frame = spark.table(f"{args.catalog}.gold.dim_lgu").where(
        F.col("geographic_level").isin("CITY", "MUN")
        & (F.col("source_period") == args.source_period)
    )
    frame = frame.withColumn("reporting_year", F.lit(args.reporting_year))
    fiscal = f"{args.catalog}.gold.fact_ldrrmf"
    if spark.catalog.tableExists(fiscal):
        funds = spark.table(fiscal).where(
            (F.col("fiscal_year") == args.reporting_year)
            & (F.col("source_period") == args.source_period)
            & F.col("lgu_type").isin("City", "Municipality")
            & F.col("psgc_code").isNotNull()
        )
        unique(
            spark,
            args.catalog,
            args.batch_id,
            funds,
            ["psgc_code", "fiscal_year"],
            fiscal,
        )
        funds = funds.select(
            "psgc_code",
            "total_appropriation",
            "total_expenditure",
            "utilization_rate_total",
            F.col("_ingested_at").alias("funding_ingested_at"),
            F.col("has_dq_issues").alias("funding_has_dq_issues"),
        )
        frame = frame.join(funds, "psgc_code", "left")
    else:
        for c, t in [
            ("total_appropriation", "decimal(18,2)"),
            ("total_expenditure", "decimal(18,2)"),
            ("utilization_rate_total", "decimal(18,4)"),
            ("funding_ingested_at", "timestamp"),
            ("funding_has_dq_issues", "boolean"),
        ]:
            frame = frame.withColumn(c, F.lit(None).cast(t))
    cmci = f"{args.catalog}.gold.fact_cmci_indicator"
    if spark.catalog.tableExists(cmci):
        resilience = spark.table(cmci).where(
            (F.col("year") == args.reporting_year)
            & (F.col("source_period") == args.source_period)
            & F.col("indicator_code").isin(
                "lup", "drrp", "add", "ews", "drrmp", "lra", "ei", "util", "ep", "ss"
            )
        )
        coverage = resilience.groupBy("psgc_code").agg(
            F.count("raw_score").alias("resiliency_indicator_count"),
            F.max("_ingested_at").alias("cmci_ingested_at"),
        )
        frame = frame.join(coverage, "psgc_code", "left")
    else:
        frame = frame.withColumn(
            "resiliency_indicator_count", F.lit(None).cast("long")
        ).withColumn("cmci_ingested_at", F.lit(None).cast("timestamp"))
    frame = frame.withColumn("risk_level", F.lit(None).cast("string")).withColumn(
        "risk_preparedness_gap", F.lit(None).cast("double")
    )
    frame = frame.withColumn(
        "risk_status", F.lit("INSUFFICIENT_HAZARD_EXPOSURE_VULNERABILITY_EVIDENCE")
    )
    frame = frame.withColumn(
        "preparedness_status",
        F.when(
            (F.col("resiliency_indicator_count") == 10)
            & F.col("total_appropriation").isNotNull(),
            "COMPONENT_EVIDENCE_ONLY",
        ).otherwise("INSUFFICIENT_DATA"),
    )
    frame = frame.withColumn("metric_version", F.lit("evidence_v1")).withColumn(
        "as_of", F.current_timestamp()
    )
    frame = frame.withColumn(
        "uncertainty",
        F.lit(
            "Funding and competitiveness indicators are proxies; earthquake-risk methodology unapproved"
        ),
    )
    unique(
        spark,
        args.catalog,
        args.batch_id,
        frame,
        ["psgc_code", "reporting_year"],
        "platinum.lgu_evidence_status",
    )
    upsert(
        spark,
        frame,
        f"{args.catalog}.platinum.lgu_evidence_status",
        ["psgc_code", "reporting_year"],
        " OR ".join(f"NOT(t.`{c}` <=> s.`{c}`)" for c in frame.columns if c != "as_of"),
    )


if __name__ == "__main__":
    main()
