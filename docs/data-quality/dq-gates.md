# Data quality gates

The pipeline runs 12 gates in `resources/jobs/ahon_job.yml`. Each gate checks its just-built layer, writes results, and blocks dependent tasks on BLOCKED or ERROR. WARNs do not stop a run. A post-build gate cannot roll back its build or earlier committed statements.

The gates add to the checks that run inside each dataset build (see the [data quality overview](README.md)). Both write to the same table, [`monitoring.dq_result`](dq-result.md).

## Where the checks run

| Point | Runs after | Tool | Code lives in |
| --- | --- | --- | --- |
| DQ Inspect (landing to bronze) | the load, before bronze is written | plain Python or SQL | the dataset's load script (`src/sql/datasets/<dataset>/bronze/`) |
| DQ Gate: bronze to silver | the silver build | Great Expectations | `src/sql/monitoring/<dataset>/silver_dq_gate.py` |
| DQ Gate: silver to gold | the gold build | Great Expectations | `src/sql/monitoring/<dataset>/gold_dq_gate.py` |
| DQ Gate: gold to platinum | the platinum build | Great Expectations | `src/sql/monitoring/<dataset>/platinum_dq_gate.py` |

## How a gate runs in the job

Each gate is a job task that runs `src/sql/common/run_python.py` on a gate file. The runner sets `PIPELINE_CATALOG`, `PIPELINE_RUN_ID` (the job's `batch_id`) and `PIPELINE_PERIOD`, so a gate checks the same catalog and batch as the build before it.

1. The gate task runs after its build task.
2. `run_dq_gate()` in `src/sql/common/dq_gate.py` runs the rules as one GX suite and writes one row per rule to `monitoring.dq_result`.
3. If a `stop` or `quarantine` rule fails (BLOCKED) or a check cannot run (ERROR), the gate raises an error and the task fails.
4. The next layer's build depends on the gate (`run_if: ALL_SUCCESS`), so it is skipped. Everything downstream of it is skipped too.
5. WARN results are recorded and the run continues.

A gate runs after its build has written the table, so a failed gate does not undo that write. The table keeps the new rows, and the next layer keeps its last accepted state. See [handling findings](handling-rejects.md) and the [runbook](../operations/runbook.md#recover-from-a-blocked-check).

To run a gate on its own, use `--only <gate task>` (for example `--only dq_ldrrmf_silver`). In a notebook, set `PIPELINE_CATALOG` and `PIPELINE_RUN_ID` first; otherwise the gate uses `ahon_dev` and run ID `interactive`.

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
- **`check_id`:** set by the gate as `<dataset>_<layer>_<rule name>`, e.g. `blgf_ldrrmf_annual_lgu_silver_overspent`. When one dataset has several tables in the same layer (CMCI), start the rule name with the table, e.g. `fact_cmci_indicator_score_valid`, so IDs stay unique.
- **Rules that hold a value that changes with new data** (year range, expected row count) say so in a comment, and are updated in the same PR as the new data.

## Gate files by dataset

Every dataset has a gate for each layer it feeds. Gold tables are listed under the dataset that builds them; checks against another dataset's table (for example a join to `dim_lgu`) go in the gate of the table being checked.

The planned `platinum.lgu_risk_profile` (one city or municipality per row, for the dashboard) combines all datasets. Each dataset's platinum gate checks only the columns that dataset brings into it, so a failure points at one owner.

| Dataset | Owner | Job task | Gate file (`src/sql/monitoring/...`) | Table checked | Runs after |
| --- | --- | --- | --- | --- | --- |
| PSGC | Yanna | `dq_psgc_silver` | `psa_psgc/silver_dq_gate.py` | `silver.psa_psgc_clean` | `psgc_silver` |
| | | `dq_psgc_gold` | `psa_psgc/gold_dq_gate.py` | `gold.dim_lgu` | `dim_lgu` |
| PSA Population | Crizza | `dq_population_silver` | `psa_population/silver_dq_gate.py` | `silver.psgc_city_population_clean` | `psgc_population_silver` |
| | | `dq_population_gold` | `psa_population/gold_dq_gate.py` | `gold.fact_population` | `population_gold` |
| PSA OpenSTAT population | Crizza | `dq_population_openstat_silver` | `psa_population/openstat_silver_dq_gate.py` | `silver.psa_population_clean` | `population_silver` |
| | | `dq_population_openstat_gold` | `psa_population/openstat_gold_dq_gate.py` | `gold.fact_population_total` | `population_total_gold` |
| BLGF LDRRMF | Crystal | `dq_ldrrmf_silver` | `blgf_ldrrmf_annual_lgu/silver_dq_gate.py` | `silver.blgf_ldrrmf_annual_lgu_clean` | `ldrrmf_silver` |
| | | `dq_ldrrmf_gold` | `blgf_ldrrmf_annual_lgu/gold_dq_gate.py` | `gold.fact_ldrrmf` | `ldrrmf_gold` |
| PHIVOLCS earthquakes | Jessy | `dq_earthquake_silver` | `philvolcs_earthquake/silver_dq_gate.py` | `silver.philvolcs_earthquake_observation_clean` | `earthquake_silver` |
| CMCI | Maki | `dq_cmci_silver` | `cmci/silver_dq_gate.py` | `silver.cmci_indicator_clean` | `cmci_silver` |
| | | `dq_cmci_gold` | `cmci/gold_dq_gate.py` | `gold.dim_cmci_indicator`, `gold.fact_cmci_indicator` | `cmci_gold` |
| | | `dq_cmci_platinum` | `cmci/platinum_dq_gate.py` | `platinum.cmci_pillar_coverage` | `cmci_platinum` |

The census and OpenSTAT population gates are separate tasks, so an OpenSTAT finding never blocks the census branch.

All 12 gates ran in a full pipeline run on `ahon_dev_crystal` (batch `crystal-run-001`, 2026-10-20, from the PR #109 branch before it merged) with no BLOCKED result. The warnings are listed in [results](dq-result.md#known-warnings).

### Placeholders, not job tasks

These files are kept for tables that are not built yet. They are not in the job.

| Gate file | For |
| --- | --- |
| `philvolcs_earthquake/gold_dq_gate.py` | A gold earthquake table (none yet; earthquakes stop at silver and the platinum views) |
| `psa_psgc/platinum_dq_gate.py`, `psa_population/platinum_dq_gate.py`, `blgf_ldrrmf_annual_lgu/platinum_dq_gate.py`, `philvolcs_earthquake/platinum_dq_gate.py` | The planned `platinum.lgu_risk_profile`, one city or municipality per row. Each dataset's gate checks only the columns that dataset brings in, so a failure points at one owner |

Shared files, used by every gate:

| File | Purpose |
| --- | --- |
| `src/sql/common/dq_gate.py` | `rule()` and `run_dq_gate()`: runs the rules, records the results, stops on FAIL or ERROR |
| `src/sql/common/run_python.py` | Runs a gate file as a job task and sets `PIPELINE_CATALOG`, `PIPELINE_RUN_ID` and `PIPELINE_PERIOD` |

Older check files are still in the same folders (`psa_psgc/bronze_dq.sql`, `silver_dq.sql`, `philvolcs_earthquake/bronze_dq.sql`, `silver_dq.sql`, `psa_population/bronze_dq_check.py`, `silver_dq_check.py`). They are not job tasks and are to be removed or folded into the load scripts. Only the tasks in `resources/jobs/ahon_job.yml` run.

## Adding a gate for a dataset

1. Copy the template below to `src/sql/monitoring/<dataset>/<layer>_dq_gate.py`.
2. Fill in the settings, the helper columns (if a rule spans several columns) and the rules. Start from the checks already in the dataset's build and DQ files.
3. Run it on your development catalog and compare the counts with what you expect. Write the date and the counts in the file's docstring.
4. List the rules on the [data dictionary](../data/data-dictionary/README.md) page of the table checked.
5. Add a `dq_<dataset>_<layer>` task in `resources/jobs/ahon_job.yml` that runs the gate file through `run_python.py`, depending on the build. Make the next layer's build depend on the gate.
6. Update `tests/common/test_job_contract.py` and add the gate to the table above.

## Gate file template

```python
# %pip install great_expectations==1.24.0
"""
DQ gate, <from layer> to <layer>: <dataset> (decisions 0018 and 0020).

Run after <build file>. Tested on <catalog> (<date>): <n> PASS, <n> WARN (<which>).
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

# Settings: set by src/sql/common/run_python.py in the job; set them yourself in a notebook
CATALOG = globals().get("PIPELINE_CATALOG", "ahon_dev")
RUN_ID = globals().get("PIPELINE_RUN_ID", "interactive")
SOURCE_NAME = "<dataset>"
TABLE = f"{CATALOG}.<layer>.<table>"
RESULTS_TABLE = f"{CATALOG}.monitoring.dq_result"

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
    run_id=RUN_ID,
)
```

Population entry points are separate files under `src/sql/monitoring/psa_population/`. Each population gate receives the configured census year via the runner's widgets. Census components have no dependency on OpenSTAT extraction, builds or gates. Placeholder gates for unbuilt tables are not job tasks.

The shared helper uses a MERGE keyed by run_id, check_id and table_name. Row-count checks persist the distance outside their bounds; other failed aggregate contracts persist at least one failure. PASS metrics are zero. Tests exercise the actual GX engine with a recording writer; full Spark/Delta execution and source-run evidence remain separate validation steps.

See [results](dq-result.md), [finding recovery](handling-rejects.md), and [decision 0020](../decisions/0020-one-dq-results-table.md).
