# philvolcs_earthquake_reject

- **Table:** `silver.philvolcs_earthquake_reject`
- **One row is:** one flagged earthquake observation
- **Key:** `_observation_id`
- **Built from bronze by:** `src/sql/datasets/philvolcs_earthquake/observation_pipeline.py --stage silver`
- **Owner:** Jessy

Same columns as [`silver.philvolcs_earthquake_observation_clean`](philvolcs_earthquake_observation_clean.md), with `has_dq_issues` always true.

See the clean table's rules for why a row is flagged.

## Notes

- Flagged rows are also kept in the clean table. Rows here raise a warning; they do not stop the build.
