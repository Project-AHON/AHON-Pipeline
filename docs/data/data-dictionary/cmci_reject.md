# cmci_reject

- **Table:** `silver.cmci_reject`
- **One row is:** one flagged CMCI indicator value from one source observation
- **Key:** `_observation_id`, `psgc_code`, `year`, `indicator_code`
- **Built from bronze by:** `src/sql/datasets/cmci/pipeline.py --stage silver`
- **Owner:** Maki

Same columns as [`silver.cmci_indicator_clean`](cmci_indicator_clean.md), with `has_dq_issues` always true.

A value is rejected when it is not blank or `-` and is nonnumeric, negative or nonfinite. Rejected values stay visible here without entering the clean table.

## Notes

- If the latest observation for a key is rejected, gold withholds the earlier accepted value (`raw_score` is `NULL`) until a later accepted observation recovers it. See [`gold.fact_cmci_indicator`](fact_cmci_indicator.md).
