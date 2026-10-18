"""
DQ gate, gold to platinum: philvolcs_earthquake (decision 0012). NOT WRITTEN YET.

Checks the earthquake count columns of platinum.lgu_risk_profile. That table and
the gold earthquake table it reads from are not built yet, so there is no code
here. Write this gate when the platinum build exists, from its real columns.

Expected on ahon_dev: not run yet (no table).

Planned rules (column names to confirm against the build):
- Stop:
  - psgc_code not null and unique (one row per LGU in lgu_risk_profile);
  - psgc_code in gold.dim_lgu (referential_integrity, helper in_dim_lgu);
  - earthquake count columns not null and >= 0 (range; LGUs without events get 0);
  - reconciliation: sum of each count column = number of matching gold events
    in the same period and magnitude band (helper totals compared in one row).
- Report:
  - LGUs in dim_lgu with no row in lgu_risk_profile (coverage);
  - count columns far above the previous run (volume), only once a baseline exists.

Unsure: which count columns exist (all events, by magnitude band, by period),
the time window they cover, and whether events without an approved psgc_code
are left out of the counts or shown in a separate unassigned row.
"""
