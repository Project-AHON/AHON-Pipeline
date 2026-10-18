# Data quality gates

How the pipeline checks data between layers, and where each dataset's checks live. The design is in [decision 0010](../../decisions/0010-dq-gates-with-great-expectations.md); the results table is described in [dq_results](../../architecture/data-dictionary/dq_results.md).

## Where the checks run

| Point | Runs after | Tool | Code lives in |
| --- | --- | --- | --- |
| DQ Inspect (landing to bronze) | the load, before bronze is written | plain Python or SQL | the dataset's load script (`src/sql/datasets/<dataset>/bronze/`) |
| DQ Gate: bronze to silver | the silver build | Great Expectations | `src/sql/monitoring/<dataset>/silver_dq_gate.py` |
| DQ Gate: silver to gold | the gold build | Great Expectations | `src/sql/monitoring/<dataset>/gold_dq_gate.py` |
| DQ Gate: gold to platinum | the platinum build | Great Expectations | `src/sql/monitoring/<dataset>/platinum_dq_gate.py` |

Every gate calls `run_dq_gate()` from `src/sql/common/dq_gate.py`. It writes one row per rule to `monitoring.dq_results` and stops the job on a FAIL or ERROR.

A gate checks the table its layer just wrote, so a gate file is named after the layer it checks: `silver_dq_gate.py` checks the silver table.

## Rules

Each rule is one Great Expectations expectation, tagged with a name, a type and an action.

| Action | What happens when the rule breaks | Status |
| --- | --- | --- |
| `stop` | the job stops | FAIL |
| `quarantine` | the job stops (moving rows to a quarantine table is not built yet) | FAIL |
| `flag` | rows are kept and marked `has_dq_issues` by the build; the gate records them | WARN |
| `fix` | the build corrected the value; the gate records how many | WARN |
| `report` | recorded only | WARN |

A check that cannot run (missing column, bad query) is `ERROR` and stops the job.

`rule_type` is one of: `null`, `unique`, `range`, `accepted_values`, `referential_integrity`, `volume`, `reconciliation`, `spatial`.

## Naming

- **Gate file:** `src/sql/monitoring/<dataset>/<layer>_dq_gate.py`. `<dataset>` is the dataset name used in `src/sql/datasets/`.
- **Rule name:** what must be true, in snake_case, e.g. `fiscal_year_valid`, `row_count_matches_bronze`, `psgc_code_in_dim_lgu`.
- **`dq_rule_id`:** set by the gate as `<dataset>_<layer>_<rule name>`, e.g. `blgf_ldrrmf_annual_lgu_silver_overspent`. When one dataset has several tables in the same layer (CMCI), start the rule name with the table, e.g. `fact_cmci_indicator_score_valid`, so ids stay unique.
- **Rules that hold a value that changes with new data** (year range, expected row count) say so in a comment, and are updated in the same PR as the new data.

## File map by dataset

Planned gate files for the five datasets. This map is a starting template: files may be added, renamed or removed as each dataset is built. Every dataset has a gate for each layer it feeds: silver, gold and platinum. Gold tables are listed under the dataset that builds them; checks against another dataset's table (for example a join to `dim_lgu`) go in the gate of the table being checked.

The planned `platinum.lgu_risk_profile` (one city or municipality per year; proposed in the data model draft, not in `data-model.md` yet) combines all datasets. Each dataset's platinum gate checks only the columns that dataset brings into it, so a failure points at one owner. CMCI also checks its own `platinum.cmci_pillar_score`.

| Dataset | Owner | Gate | Gate file (`src/sql/monitoring/...`) | Table checked | Status |
| --- | --- | --- | --- | --- | --- |
| `blgf_ldrrmf_annual_lgu` | Crystal | bronze to silver | `blgf_ldrrmf_annual_lgu/silver_dq_gate.py` | `silver.blgf_ldrrmf_annual_lgu_clean` | Piloted in a notebook |
| | | silver to gold | `blgf_ldrrmf_annual_lgu/gold_dq_gate.py` | `gold.fact_ldrrmf` | Piloted in a notebook |
| | | gold to platinum | `blgf_ldrrmf_annual_lgu/platinum_dq_gate.py` | `platinum.lgu_risk_profile`: LDRRMF spending columns | Later, table not built yet |
| `psa_psgc` | Crizza | bronze to silver | `psa_psgc/silver_dq_gate.py` | `silver.psa_psgc_clean` | To do |
| | | silver to gold | `psa_psgc/gold_dq_gate.py` | `gold.dim_lgu` | To do |
| | | gold to platinum | `psa_psgc/platinum_dq_gate.py` | `platinum.lgu_risk_profile`: `psgc_code`, names and levels (every row has a valid LGU) | Later, table not built yet |
| `psa_population` | Crizza | bronze to silver | `psa_population/silver_dq_gate.py` | `silver.psa_population` | To do |
| | | silver to gold | `psa_population/gold_dq_gate.py` | `gold.fact_population` | To do |
| | | gold to platinum | `psa_population/platinum_dq_gate.py` | `platinum.lgu_risk_profile`: population columns | Later, table not built yet |
| `cmci` | Maki | bronze to silver | `cmci/silver_dq_gate.py` | `silver.cmci_*` (5 pillar tables) | To do (move from `tests/sql/monitoring/cmci_quality.py`) |
| | | silver to gold | `cmci/gold_dq_gate.py` | `gold.dim_cmci_indicator`, `gold.fact_cmci_indicator` | To do (move from `cmci_quality.py`) |
| | | gold to platinum | `cmci/platinum_dq_gate.py` | `platinum.cmci_pillar_score`; `platinum.lgu_risk_profile`: CMCI resiliency columns | To do (move from `cmci_quality.py`); `lgu_risk_profile` later |
| `philvolcs_earthquake` | Jessy | bronze to silver | `philvolcs_earthquake/silver_dq_gate.py` | `silver.philvolcs_earthquake_data_clean` | To do |
| | | silver to gold | `philvolcs_earthquake/gold_dq_gate.py` | gold earthquake table (not built yet) | Later |
| | | gold to platinum | `philvolcs_earthquake/platinum_dq_gate.py` | `platinum.lgu_risk_profile`: earthquake count columns | Later, table not built yet |

Shared files, used by every gate:

| File | Purpose |
| --- | --- |
| `src/sql/common/dq_gate.py` | `rule()` and `run_dq_gate()`: runs the rules, writes `dq_results`, stops on FAIL |
| `src/sql/00_setup/07_dq_results.sql` | Creates `monitoring.dq_results` |
| `docs/operations/monitoring/dq-checks-<dataset>.md` | The dataset's list of checks: name, rule, action, file that runs it, last counts |

The existing checks in `src/sql/monitoring/psa_psgc/*_dq.sql`, `philvolcs_earthquake/*_dq.sql` and `psa_population/*_dq_check.py` keep running until their gate replaces them. The bronze ones become DQ Inspect checks in the load scripts or stay where they are.

## Adding a gate for a dataset

1. Copy the template below to `src/sql/monitoring/<dataset>/<layer>_dq_gate.py`.
2. Fill in the settings, the helper columns (if a rule spans several columns) and the rules. Start from the checks already in the dataset's build and DQ files.
3. Run it on `ahon_dev` and compare the counts with what you expect.
4. List the rules in `docs/operations/monitoring/dq-checks-<dataset>.md`.
5. Add the gate as a job task after the build, once the job YAML exists.

## Gate file template

```python
# %pip install great_expectations==1.23.1
"""
DQ gate, <from layer> to <layer>: <dataset> (decision 0010).

Run after <build file>. Expected on ahon_dev (<date>): <n> PASS, <n> WARN (<which>).
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
RESULTS_TABLE = f"{CATALOG}.monitoring.dq_results"

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
- In a notebook, run one gate per cell: Databricks shows only one results table per cell.
