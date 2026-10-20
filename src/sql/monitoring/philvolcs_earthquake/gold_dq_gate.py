"""
DQ gate, silver to gold: philvolcs_earthquake. NOT WRITTEN YET.

The gold earthquake table is not built. run_earthquake.py --stage gold calls
src/sql/gold/build_fact_earthquake_event.sql, which is not in main, and ADR 0009
keeps gold BLOCKED until the event grain, keys, correction/dedup policy and the
geographic meaning of the PSGC candidates are approved. Write this gate when the
build exists, from the columns that build actually creates.

Expected on ahon_dev: not run yet (no table).

Planned rules (names and columns to confirm against the build):
- Stop: the build's own stop checks repeated after the write, for example
  - event key not null and unique (one row per approved event grain);
  - row accounting: every in-scope silver observation has exactly one gold
    disposition (included, excluded or merged as a correction), so
    silver rows = gold rows + excluded rows (volume / reconciliation);
  - psgc_code in gold.dim_lgu on the same PSGC snapshot (referential_integrity,
    helper in_dim_lgu from a left join, NULL when psgc_code is NULL);
  - no row with has_dq_issues = true among included events, if the build
    excludes invalid observations.
- Report:
  - psgc_code NULL (valid events with no approved locality, offshore included);
  - in_ph_bounds false (spatial; same assumed box as the silver gate);
  - event signature duplicates still present after the correction policy.

Unsure: whether gold keeps invalid observations, the final key column name,
and whether matching is by locality candidate or by boundary (point in polygon).
"""
