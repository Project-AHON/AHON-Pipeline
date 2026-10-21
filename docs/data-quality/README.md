# Data quality

## How checks work

Builders and layer gates persist checks in `monitoring.dq_result`. The pipeline has 40 tasks, including 12 gates and `dashboard_views`, which creates the data quality dashboard views; setup and development verification are separate jobs. Gate tasks follow their corresponding builds. Census and OpenSTAT population gates run independently.

An ERROR-severity rule that fails records BLOCKED and stops dependents. WARN findings retain evidence without stopping the task. A gate execution exception records ERROR when GX returns an exception result, and stops the task. Earlier successful writes are not rolled back.

The failure metric can count failing rows, a row-count difference, or one failed contract. It is not always a count of source rows. Scripts executed through the Python/SQL runners also record execution markers; direct extractors do not necessarily do so.

## Dashboard

The AHON Data Quality Monitoring dashboard (`resources/dashboards/ahon_data_quality.lvdash.json`) answers: how many checks passed, what is broken, whether anything stopped the run, which datasets are affected and when data was last checked. Its Check detail page drills down from dataset to check to the failing rows' sample keys. It reads two views that the `dashboard_views` task creates (`src/sql/00_setup/07_dq_check_views.sql`):

- `monitoring.dq_check_result`: every result in `dq_result` with dataset, layer, check type and owner added. Execution markers and the removed OSM checks are left out.
- `monitoring.dq_check_status`: the current result of each check (latest by `checked_at`). A check that has not run within an hour of its table's newest check (for example a renamed rule) drops out.

Build checks have no rule type yet, so their check type is derived from the check name. The dashboard is defined in `databricks.yml`, and `bundle deploy` creates it with the bundle's `catalog` and `warehouse_id`.

Reject behavior is dataset-specific. See [handling findings](handling-rejects.md), [gate contracts](dq-gates.md), [results](dq-result.md), the [dictionary](../data/data-dictionary/README.md), and the [runbook](../operations/runbook.md).
