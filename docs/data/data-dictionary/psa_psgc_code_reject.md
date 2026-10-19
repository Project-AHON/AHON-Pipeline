# psa_psgc_code_reject

- **Table:** `silver.psa_psgc_code_reject`
- **One row is:** one bronze observation with an invalid PSGC code
- **Key:** `_observation_id`
- **Built from bronze by:** `src/sql/datasets/psa_psgc/silver/psa_psgc_clean.py`
- **Owner:** Yanna

Same columns as [`bronze.psa_psgc`](psa_psgc.md), kept as received, plus:

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `disposition` | string | Why the row was rejected | Always `invalid_psgc_code` |

A code is invalid when it is blank, nonnumeric, or not 8, 9 or 10 digits before padding.

## Notes

- Any row here stops the silver build for that delivery. Earlier rejected observations stay here, so a later corrected delivery can load without erasing them.
