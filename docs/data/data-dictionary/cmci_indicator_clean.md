# cmci_indicator_clean

- **Table:** `silver.cmci_indicator_clean`
- **One row is:** one indicator value for one city or municipality and year
- **Key:** `psgc_code`, `year`, `indicator_code`
- **Built from bronze by:** `src/sql/datasets/cmci/pipeline.py --stage silver`, from `bronze.cmci_raw_indicator_batch_html`
- **Owner:** Maki

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `psgc_code` | string | Canonical PSGC LGU identifier | Part of the key |
| `cmci_name` | string | Exact CMCI source locality name | Preserved for source traceability |
| `year` | int | CMCI reporting year | Part of the key |
| `indicator_code` | string | Indicator code | Part of the key; see `resources/cmci_indicators.csv` |
| `raw_value` | string | Value as returned by CMCI | |
| `raw_score` | double | Numeric value | Missing `-` or blank remains `NULL` |
| `source_period` | string | PSGC snapshot of the mapping | |
| `_source_ref` | string | Bronze provenance | Carried from bronze |
| `_row_hash` | string | Bronze provenance | Carried from bronze |
| `_observation_id` | string | Bronze observation identity | Carried from bronze |
| `_ingested_at` | timestamp (UTC) | When the source row was loaded into bronze | Carried from bronze |
| `_batch_id` | string | The load run | Carried from bronze |
| `_processed_at` | timestamp (UTC) | When the silver row was written | |
| `has_dq_issues` | boolean | True when the value is nonnumeric, negative or nonfinite | Always false here; flagged rows go to [`silver.cmci_reject`](cmci_reject.md) |

When the same key arrives more than once, the latest observation wins (first by ingestion time, then by observation identity).

## Known issues

- Missing CMCI values are represented as `NULL`, not zero.
- Some indicators may be unavailable for particular LGUs or years.
- The portal's numeric value is kept as `raw_score`; units, direction and scoring method are not published per indicator. A lower raw value across different indicators does not mean weaker preparedness.
- Eight PSGC municipalities in the BARMM Special Geographic Area have no rows. See the [CMCI data profile](../data-profile/cmci-profile.md#known-psgc-only-source-gaps).
