# Data quality gates

How the pipeline checks data between layers with Great Expectations, and where each dataset's gate lives. The design is in [decision 0012](../decisions/0012-dq-gates-with-great-expectations.md); the results table is described in [dq-results](dq-results.md).

The gates are being built. Today the job runs the checks inside each build (see the [data quality overview](README.md)); the gate files below run in a notebook and are not yet tasks in `ahon_pipeline`.

## Where the checks run

| Point | Runs after | Tool | Code lives in |
| --- | --- | --- | --- |
| DQ Inspect (landing to bronze) | the load, before bronze is written | plain Python or SQL | the dataset's load script (`src/sql/datasets/<dataset>/bronze/`) |
| DQ Gate: bronze to silver | the silver build | Great Expectations | `src/sql/monitoring/<dataset>/silver_dq_gate.py` |
| DQ Gate: silver to gold | the gold build | Great Expectations | `src/sql/monitoring/<dataset>/gold_dq_gate.py` |
| DQ Gate: gold to platinum | the platinum build | Great Expectations | `src/sql/monitoring/<dataset>/platinum_dq_gate.py` |

Every gate calls `run_dq_gate()` from `src/sql/common/dq_gate.py`. It runs the rules as one GX suite directly on the Spark table, records one row per rule in `monitoring.dq_result`, the table the job's checks use ([dq-results](dq-results.md)), and stops the job on a FAIL or ERROR.

> The gate code does not write to `dq_result` yet. `dq_gate.py` still writes its own table, `monitoring.dq_results`, which has different columns. It has to be aligned with `dq_result` before the gates become job tasks.

A gate checks the table its layer just wrote, so a gate file is named after the layer it checks: `silver_dq_gate.py` checks the silver table.

## Rules

Each rule is one Great Expectations expectation, tagged with a name, a type and an action. The action sets the GX severity and the status the rule gets when it breaks.

| Action | What happens when the rule breaks | Severity | Status |
| --- | --- | --- | --- |
| `stop` | the job stops | critical | FAIL |
| `quarantine` | the job stops (moving rows to a quarantine table is not built yet) | critical | FAIL |
| `flag` | rows are kept and marked `has_dq_issues` by the build; the gate records them | warning | WARN |
| `fix` | the build corrected the value; the gate records how many | warning | WARN |
| `report` | recorded only | info | WARN |

A check that cannot run (missing column, bad query) is `ERROR` and stops the job.

`rule_type` is one of: `null`, `unique`, `range`, `accepted_values`, `referential_integrity`, `volume`, `reconciliation`, `spatial`.

## Naming

- **Gate file:** `src/sql/monitoring/<dataset>/<layer>_dq_gate.py`. `<dataset>` is the dataset name used in `src/sql/datasets/`.
- **Rule name:** what must be true, in snake_case, e.g. `fiscal_year_valid`, `row_count_matches_bronze`, `psgc_code_in_dim_lgu`.
- **`dq_rule_id`:** set by the gate as `<dataset>_<layer>_<rule name>`, e.g. `blgf_ldrrmf_annual_lgu_silver_overspent`. When one dataset has several tables in the same layer (CMCI), start the rule name with the table, e.g. `fact_cmci_indicator_score_valid`, so ids stay unique.
- **Rules that hold a value that changes with new data** (year range, expected row count) say so in a comment, and are updated in the same PR as the new data.

## Gate files by dataset

Every dataset has a gate for each layer it feeds. Gold tables are listed under the dataset that builds them; checks against another dataset's table (for example a join to `dim_lgu`) go in the gate of the table being checked.

The planned `platinum.lgu_risk_profile` (one city or municipality per row, for the dashboard) combines all datasets. Each dataset's platinum gate checks only the columns that dataset brings into it, so a failure points at one owner.

| Dataset | Owner | Gate file (`src/sql/monitoring/...`) | Table checked | Status |
| --- | --- | --- | --- | --- |
| BLGF LDRRMF | Crystal | `blgf_ldrrmf_annual_lgu/silver_dq_gate.py` | `silver.blgf_ldrrmf_annual_lgu_clean` | Run in a notebook on `ahon_dev`: 14 PASS, 3 WARN |
| | | `blgf_ldrrmf_annual_lgu/gold_dq_gate.py` | `gold.fact_ldrrmf` | Run in a notebook on `ahon_dev`: no FAIL; WARNs are the known unmatched, cityhood and new BARMM rows |
| | | `blgf_ldrrmf_annual_lgu/platinum_dq_gate.py` | `platinum.lgu_risk_profile`: LDRRMF columns | Placeholder, table not built |
| PSGC | Yanna | `psa_psgc/silver_dq_gate.py` | `silver.psa_psgc_clean` | Drafted, not run |
| | | `psa_psgc/gold_dq_gate.py` | `gold.dim_lgu` | Drafted, not run |
| | | `psa_psgc/platinum_dq_gate.py` | `platinum.lgu_risk_profile`: `psgc_code`, names and levels | Placeholder, table not built |
| PSA Population | Crizza | `psa_population/silver_dq_gate.py` | `silver.psgc_city_population_clean` | Drafted, not run |
| | | `psa_population/gold_dq_gate.py` | `gold.fact_population` | Drafted, not run |
| | | `psa_population/platinum_dq_gate.py` | `platinum.lgu_risk_profile`: population columns | Placeholder, table not built |
| CMCI | Maki | `cmci/silver_dq_gate.py` | `silver.cmci_indicator_clean` | Drafted, not run |
| | | `cmci/gold_dq_gate.py` | `gold.dim_cmci_indicator`, `gold.fact_cmci_indicator` | Drafted, not run |
| | | `cmci/platinum_dq_gate.py` | `platinum.cmci_pillar_coverage`; later `platinum.lgu_risk_profile`: CMCI columns | Drafted, not run |
| PHIVOLCS earthquakes | Jessy | `philvolcs_earthquake/silver_dq_gate.py` | `silver.philvolcs_earthquake_observation_clean` | Drafted, not run |
| | | `philvolcs_earthquake/gold_dq_gate.py` | gold earthquake table | Placeholder, table not built |
| | | `philvolcs_earthquake/platinum_dq_gate.py` | `platinum.lgu_risk_profile`: earthquake columns | Placeholder, table not built |

Shared files, used by every gate:

| File | Purpose |
| --- | --- |
| `src/sql/common/dq_gate.py` | `rule()` and `run_dq_gate()`: runs the rules, records the results, stops on FAIL or ERROR |
| `src/sql/00_setup/07_dq_results.sql` | Creates the gates' current results table (removed once the gates write to `dq_result`) |

The older checks in `src/sql/monitoring/psa_psgc/*_dq.sql`, `philvolcs_earthquake/*_dq.sql`, `psa_population/*_dq_check.py` and `tests/sql/monitoring/cmci_quality.py` keep running until their gate replaces them. The bronze ones become DQ Inspect checks in the load scripts or stay where they are.

## Adding a gate for a dataset

1. Copy the template below to `src/sql/monitoring/<dataset>/<layer>_dq_gate.py`.
2. Fill in the settings, the helper columns (if a rule spans several columns) and the rules. Start from the checks already in the dataset's build and DQ files.
3. Run it on your development catalog and compare the counts with what you expect. Write the date and the counts in the file's docstring.
4. List the rules on the [data dictionary](../data/data-dictionary/README.md) page of the table checked.
5. Add the gate as a job task after the build in `resources/jobs/ahon_job.yml`.

## Gate file template

```python
# %pip install great_expectations==1.23.1
"""
DQ gate, <from layer> to <layer>: <dataset> (decision 0012).

Run after <build file>. Expected on <catalog> (<date>): <n> PASS, <n> WARN (<which>).
"""

import os
import sys

import great_expectations as gx
from pyspark.sql import functions as F

# Let Python find src/sql/common/dq_gate.py
try:
    _here = os.path.dirname(os.path.abspath(__file__))
except NameError:  # notebooks have no __file__
    _here = os.getcwd()
sys.path.append(os.path.abspath(os.path.join(_here, "..", "..", "common")))

from dq_gate import rule, run_dq_gate  # noqa: E402

# Settings
CATALOG = "ahon_dev"
SOURCE_NAME = "<dataset>"
TABLE = f"{CATALOG}.<layer>.<table>"
RESULTS_TABLE = f"{CATALOG}.monitoring.dq_results"  # until dq_gate.py writes to dq_result

# Helper columns: one column per rule that spans several columns
df = spark.table(TABLE)
check_df = df  # e.g. .withColumn("min_amount", F.least(...))

# Rules: rule(expectation, rule name, rule_type, action)
E = gx.expectations
rules = [
    rule(E.ExpectColumnValuesToNotBeNull(column="<key>"), "<key>_not_null", "null", "stop"),
    rule(E.ExpectCompoundColumnsToBeUnique(column_list=["<key1>", "<key2>"]), "unique_<grain>", "unique", "stop"),
    rule(E.ExpectColumnValuesToBeBetween(column="<col>", min_value=0), "<col>_not_negative", "range", "flag"),
]

run_dq_gate(
    spark, check_df, rules,
    source_name=SOURCE_NAME, layer="<layer>", table_name="<table>",
    key_columns=["<columns that name a row for a person>"],
    results_table=RESULTS_TABLE,
)
```

Expectations used so far: `ExpectTableRowCountToEqual`, `ExpectColumnValuesToNotBeNull`, `ExpectColumnValuesToBeBetween`, `ExpectColumnValuesToBeInSet`, `ExpectColumnValuesToNotBeInSet`, `ExpectColumnValuesToBeUnique`, `ExpectCompoundColumnsToBeUnique`, `ExpectColumnPairValuesToBeEqual`, `ExpectColumnPairValuesAToBeGreaterThanB`. Full list: <https://greatexpectations.io/expectations/>.

## Tips from the LDRRMF pilot

- On serverless, GX's Spark data source needs `persist=False` (handled in `dq_gate.py`).
- GX counts failures per column. A rule over several columns (e.g. "no amount is negative") uses one helper column (`F.least(...)`), so a row counts once.
- Cross-table checks: left-join the other table first and check the joined column. GX skips NULLs, so make the helper column NULL for rows another rule already covers.
- `run_dq_gate()` returns every failing row's key columns to the driver (`result_format` COMPLETE). That is fine for tables of thousands of rows; use a smaller result format for very large tables.
- In a notebook, run one gate per cell: Databricks shows only one results table per cell.
