# Handling rejects and DQ findings

What to do when a run leaves rows in a `_reject` table, or a check in `monitoring.dq_result` ends in WARN or BLOCKED. The same steps apply to the checks inside the builds and to the DQ gates.

## Reject tables

Silver builds move rows they cannot clean into a `_reject` table instead of the clean table.

| Reject table | Dataset | When a row lands here, the build |
| --- | --- | --- |
| `silver.psa_psgc_reject` | PSGC | continues (WARN) |
| `silver.psa_psgc_code_reject` | PSGC | stops |
| `silver.psa_population_reject` | PSA Population | continues (WARN) |
| `silver.philvolcs_earthquake_reject` | PHIVOLCS Earthquake | continues (WARN) |
| `silver.cmci_reject` | CMCI | continues (WARN) |
| `silver.blgf_ldrrmf_annual_lgu_reject` | LDRRMF | stops |

Each row keeps its provenance columns and a `disposition` that says why it was rejected. The table pages in the [data dictionary](../data/data-dictionary/README.md) list the reasons.

Reject tables are only added to, so earlier rejects stay visible after a corrected delivery loads.

## Process

### 1. Find

After each run, the dataset owner (see [ownership](../project/ownership.md)) lists the WARN and BLOCKED rows for the run:

```sql
SELECT table_name, check_id, disposition, failed_count,
       get_json_object(validation_result, '$.meta.rule_type')   AS rule_type,
       get_json_object(validation_result, '$.meta.sample_keys') AS sample_keys
FROM monitoring.dq_result
WHERE run_id = '<batch_id>' AND disposition <> 'PASS'
ORDER BY disposition, table_name, check_id;
```

Gate rows have a `rule_type` and up to five `sample_keys`; build checks have neither. A BLOCKED row means the run stopped: the tasks after it were skipped and their tables keep their last accepted state.

### 2. Compare with the known findings

Look the check up in [Known findings](#known-findings). If it is there and `failed_count` is the same, there is nothing to do. A new check, or a known check with a different count, goes to step 3.

### 3. Find the cause

Read the failing rows: the `sample_keys`, the reject table grouped by `disposition`, or the table filtered on the rule's condition. Decide which kind of cause it is:

| Cause | Meaning | Example signs |
| --- | --- | --- |
| **Source** | The data as received is wrong, missing or a placeholder | The same values are in bronze; the source file shows them too |
| **Pipeline** | A valid row was cleaned, matched or rejected wrongly | Bronze looks fine; the count changed after a code change |
| **Rule** | The rule expects something the data does not have to meet | The rows are correct, but the threshold or value set is too strict |

### 4. Act

| Cause | Action |
| --- | --- |
| Source | Keep the rows (rejected or flagged). Add the count and reason to "Known issues" in the dataset's [data profile](../data/data-profile/README.md) and to [Known findings](#known-findings). Tell the source if it can be fixed upstream. |
| Pipeline | Open a `bugfix/` issue with the `check_id`, the count and a few `sample_keys`, and fix the build. |
| Rule | Change the rule in the gate file (`src/sql/monitoring/<dataset>/<layer>_dq_gate.py`) and record why in its docstring. A stop rule only becomes a report rule with the dataset owner's agreement. |

For a BLOCKED row, the run stays stopped until the fix is in. Do not change a stop rule to report just to get a run through.

### 5. Rerun and close

Rerun the dataset's tasks (see the [runbook](../operations/runbook.md)) and run the step 1 query again:

- the check is PASS, or
- its `failed_count` matches what was recorded in [Known findings](#known-findings).

For silver, the gate's volume rule should show bronze = silver + rejects again. Update the decision column in Known findings once the finding is closed.

## Examples

One finding per gate layer, from a full run on 2026-10-20.

### Silver: PHIVOLCS locality not parsed (pipeline)

- **Find:** `philvolcs_earthquake_silver_candidate_coverage` on `silver.philvolcs_earthquake_observation_clean` is WARN with 1,896 rows. That is every observation.
- **Cause:** all rows have `candidate_status = 'unparsed'`. Bronze `Location` values look like `048  km N 60Â° E of ...`, with two spaces before `km` and a mis-encoded degree sign. `LOCALITY_PATTERN` in the earthquake contract does not allow either, so this is a pipeline problem.
- **Act:** a `bugfix/` issue for the dataset owner. Widen the pattern, and have the silver upsert also update rows whose `candidate_status` changed.
- **Close:** after the rerun, `candidate_coverage` should drop to the rows that are really ambiguous or unmatched. That number then goes in Known findings.

### Gold: LDRRMF rows without a PSGC code (pipeline)

- **Find:** `blgf_ldrrmf_annual_lgu_gold_psgc_matched` on `gold.fact_ldrrmf` is WARN with 235 rows (42 LGUs). The known count was 5 (old Maguindanao).
- **Cause:** the count changed after a change to `build_blgf_ldrrmf_lgu_map.py`. Cities with no province in PSGC (NCR, highly urbanized cities, Isabela City) and LGUs whose city or municipality status changed are now `needs_review` instead of matched. Bronze is unchanged, so this is a pipeline problem.
- **Act:** a `bugfix/` issue to match those LGUs again, by rule or through `blgf_ldrrmf_lgu_map_overrides.csv`.
- **Close:** after the rerun, `psgc_matched` should be back to 5, and `dim_lgu_has_ldrrmf_latest_year` should go down with it.

### Platinum: CMCI pillars with missing indicators (source)

- **Find:** `cmci_platinum_cmci_pillar_coverage_complete` on `platinum.cmci_pillar_coverage` is WARN with 84 rows.
- **Cause:** these LGU, year and pillar rows are `INSUFFICIENT_DATA`, because some of the pillar's indicators have no score. The source shows `-` or a blank for those indicators. The gold gate's `cmci_gold_fact_cmci_indicator_raw_score_present` WARN (312 rows) is the same gap one layer down. This is a source problem.
- **Act:** keep the rows. Record both counts in Known findings and in the CMCI data profile, and show the pillar as incomplete on the dashboard.
- **Close:** nothing to fix. Check that the counts stay the same on the next run.

## Known findings

Counts from the full run on 2026-10-20 (`ahon_dev_crystal`; CMCI in test mode with 5 LGUs).

| Dataset | Check or reject table | Rows | Cause | Decision |
| --- | --- | --- | --- | --- |
| PSGC | `silver.psa_psgc_reject` | 12 (2 each in Q2_2024, April_2024 and Q4_2023; 6 in Q2_2021) | Source: rows with no geographic level | Keep rejected; to confirm |
| PSGC | `psa_psgc_silver_area_name_no_broken_enye` | 1,824 | `ñ` shows as `Ã±` in names | Open: cause not known yet |
| PSGC | `psa_psgc_silver_region_found` | 30,648 | Pipeline: Q2_2021 codes are the old 9-digit format padded to 10 | Open; Q2_2021 is not used downstream |
| LDRRMF | `blgf_ldrrmf_annual_lgu_gold_psgc_matched` | 235 | Pipeline: LGU map change (example above) | Bugfix planned |
| PHIVOLCS | `philvolcs_earthquake_silver_candidate_coverage` | 1,896 | Pipeline: locality pattern (example above) | Bugfix planned |
| PHIVOLCS | `philvolcs_earthquake_silver_in_ph_bounds` | 22 | Epicentres outside the rule's approximate Philippine box (offshore or nearby events) | To check: source or rule |
| CMCI | `cmci_platinum_cmci_pillar_coverage_complete` | 84 | Source: missing indicator scores (example above) | Keep; shown as incomplete |