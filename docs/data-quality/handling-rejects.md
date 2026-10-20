# Handling rejected rows

Silver builds move rows they cannot clean into a `_reject` table instead of the clean table. This page says what to do when a reject table has rows.

## Reject tables

| Reject table | Dataset | When a row lands here, the build |
| --- | --- | --- |
| `silver.psa_psgc_reject` | PSGC | continues (WARN) |
| `silver.psa_psgc_code_reject` | PSGC | stops |
| `silver.psa_population_reject` | PSA Population | continues (WARN) |
| `silver.philvolcs_earthquake_reject` | PHIVOLCS Earthquake | continues (WARN) |
| `silver.cmci_reject` | CMCI | continues (WARN) |
| `silver.blgf_ldrrmf_annual_lgu_reject` | LDRRMF | stops |

Each row keeps its provenance columns and a `disposition` that says why it was rejected. The table pages in [data dictionary](../data/data-dictionary/README.md) list the reasons.

## What to do

1. **Check after each run.** The dataset owner looks at the reject count for their dataset in `monitoring.dq_result` (the WARN row for the reject table, or the stop that ended the run).

   ```sql
   SELECT run_id, check_id, table_name, failed_count, disposition
   FROM monitoring.dq_result
   WHERE table_name LIKE '%reject%' AND failed_count > 0
   ORDER BY checked_at DESC;
   ```

2. **Read the rows and decide.** Group the reject table by `disposition` and look at a few rows.
   - **Source problem** (the data as received is wrong or a placeholder): leave the rows rejected. Add a line to "Known issues" in the dataset's [data profile](../data/data-profile/README.md) with the count and the reason.
   - **Pipeline problem** (a valid row was rejected): open a `bugfix/` issue with the `disposition`, the count and a few `_observation_id` or key values.

3. **Reload after a fix.** After the build is fixed or the source sends a corrected file, rerun the dataset with a new `batch_id`. The silver DQ gate's volume rule should show bronze = silver + rejects for every period.

Reject tables are only added to, so earlier rejects stay visible after a corrected delivery loads.

## Known rejects

| Dataset | Rows | Reason | Decision |
| --- | --- | --- | --- |
| PSGC | 2 per period (6 in Q2_2021) | Rows with no geographic level | Source; to confirm |
