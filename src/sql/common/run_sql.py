"""Execute repository SQL statements on job compute, stopping on any failure."""

import json
import re
import time
from pathlib import Path

import sqlparse
from quality import check, ensure_monitoring
from runtime import parser


def merge_source(statement):
    """Identify the bounded table/SELECT merge forms used by repository SQL."""
    match = re.search(
        r"MERGE INTO\s+(\S+)\s+\w+\s+USING\s+(\S+)", statement, re.IGNORECASE
    )
    if not match:
        return None
    target, source = match.groups()
    if source.startswith("("):
        selected = re.search(
            r"USING\s+\(\s*(SELECT[\s\S]*?)\)\s+\w+\s+ON", statement, re.IGNORECASE
        )
        if not selected:
            raise ValueError("Unable to verify SQL merge source schema")
        return target, selected.group(1), True
    return target, source, False


def main():
    arguments = parser(__doc__)
    arguments.add_argument("--file", required=True)
    arguments.add_argument("--min-year", type=int)
    arguments.add_argument("--max-year", type=int)
    args = arguments.parse_args()
    if "blgf_ldrrmf_annual_lgu/silver/" in args.file and (args.min_year is None or args.max_year is None):
        raise ValueError("Financial Silver execution requires explicit approved fiscal-year bounds")
    bindings = {"batch_id": args.batch_id, "min_year": args.min_year, "max_year": args.max_year}
    from pyspark.sql import SparkSession

    root = Path(__file__).resolve().parents[3]
    path = (root / args.file).resolve()
    if not path.is_relative_to(root) or path.suffix != ".sql":
        raise ValueError("Expected a repository SQL file")
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    if args.file == "src/sql/00_setup/00_catalog_schema_setup.sql":
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {args.catalog}")
    spark.sql(f"USE CATALOG {args.catalog}")
    if args.file == "src/sql/00_setup/00_catalog_schema_setup.sql":
        spark.sql("CREATE SCHEMA IF NOT EXISTS reference")
        spark.sql(f"CREATE VOLUME IF NOT EXISTS reference.{args.source_volume}")
    ensure_monitoring(spark, args.catalog)
    start = time.monotonic()
    # sqlparse respects comments and quoted semicolons.
    for index, statement in enumerate(sqlparse.split(path.read_text(encoding="utf-8"))):
        if statement.strip():
            print(
                json.dumps(
                    {"sql": args.file, "statement": index, "batch": args.batch_id}
                )
            )
            merge = merge_source(statement)
            if merge:
                target_name, source_name, selected = merge
                target = spark.table(target_name)
                source = (
                    spark.sql(source_name, args=bindings) if selected else spark.table(source_name)
                )
                if {c.name: c.dataType for c in target.schema} != {
                    c.name: c.dataType for c in source.schema
                }:
                    check(
                        spark,
                        args.catalog,
                        args.batch_id,
                        "schema_compatibility",
                        target_name,
                        1,
                    )
            try:
                result = spark.sql(statement.strip(), args=bindings)
                if {"check_id", "table_name", "failed_count", "severity"} <= set(
                    result.columns
                ):
                    for row in result.collect():
                        check(
                            spark,
                            args.catalog,
                            args.batch_id,
                            row.check_id,
                            row.table_name,
                            row.failed_count,
                            row.severity,
                        )
                else:
                    if "assert_true" in statement.lower():
                        result.collect()  # Force evaluation of every blocking assertion.
                    else:
                        result.show(25, truncate=False)
            except Exception:
                check(
                    spark,
                    args.catalog,
                    args.batch_id,
                    f"sql_statement_{index}",
                    args.file,
                    1,
                )
                raise
            if "assert_true" in statement.lower():
                check(
                    spark,
                    args.catalog,
                    args.batch_id,
                    f"sql_statement_{index}",
                    args.file,
                    0,
                )
    print(
        json.dumps(
            {
                "sql": args.file,
                "status": "completed",
                "duration_seconds": round(time.monotonic() - start, 2),
            }
        )
    )


if __name__ == "__main__":
    main()
