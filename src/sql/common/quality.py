"""Persist bounded checks before raising; warnings never block independent datasets."""

from datetime import datetime, timezone


def ensure_monitoring(spark, catalog):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.monitoring")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {catalog}.monitoring.dq_result (
      run_id STRING, check_id STRING, table_name STRING, failed_count BIGINT,
      severity STRING, disposition STRING, checked_at TIMESTAMP) USING DELTA""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {catalog}.monitoring.run_log (
      run_id STRING, task_key STRING, status STRING, message STRING,
      recorded_at TIMESTAMP) USING DELTA""")


def check(spark, catalog, run_id, check_id, table, failed, severity="ERROR"):
    ensure_monitoring(spark, catalog)
    status = "PASS" if failed == 0 else "BLOCKED" if severity == "ERROR" else "WARN"
    row = [
        (
            run_id,
            check_id,
            table,
            int(failed),
            severity,
            status,
            datetime.now(timezone.utc),
        )
    ]
    frame = spark.createDataFrame(
        row,
        "run_id string, check_id string, table_name string, failed_count long, severity string, disposition string, checked_at timestamp",
    )
    frame.createOrReplaceTempView("dq_result_candidate")
    spark.sql(f"""MERGE INTO {catalog}.monitoring.dq_result t
      USING dq_result_candidate s ON t.run_id=s.run_id AND t.check_id=s.check_id AND t.table_name=s.table_name
      WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")
    if status == "BLOCKED":
        raise ValueError(
            f"Blocking quality check {check_id}: {failed} failures in {table}"
        )


def unique(spark, catalog, run_id, frame, keys, table):
    failed = frame.groupBy(*keys).count().where("count > 1").count()
    check(spark, catalog, run_id, "unique_" + "_".join(keys), table, failed)


def upsert(spark, frame, table, keys, changed=None):
    """Delta table transaction; no deletion, schema evolution, or multi-table atomicity."""
    from delta.tables import DeltaTable

    if not spark.catalog.tableExists(table):
        frame.write.format("delta").mode("error").saveAsTable(table)
        return
    existing = spark.table(table)
    if existing.groupBy(*keys).count().where("count > 1").limit(1).count():
        raise ValueError(f"Existing target has duplicate merge keys: {table}")
    if {f.name: f.dataType for f in existing.schema} != {
        f.name: f.dataType for f in frame.schema
    }:
        raise ValueError(f"Schema migration requires review: {table}")
    condition = " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in keys)
    merge = (
        DeltaTable.forName(spark, table).alias("t").merge(frame.alias("s"), condition)
    )
    if changed:
        merge = merge.whenMatchedUpdateAll(condition=changed)
    merge.whenNotMatchedInsertAll().execute()
