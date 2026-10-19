# blgf_ldrrmf_annual_lgu_reject

- **Table:** `silver.blgf_ldrrmf_annual_lgu_reject`
- **One row is:** one LDRRMF row that broke a silver stop rule
- **Key:** `_source_ref`, `_row_hash`
- **Built from bronze by:** `src/sql/datasets/blgf_ldrrmf_annual_lgu/silver/clean_blgf_ldrrmf_annual_lgu.sql`
- **Owner:** Crystal

Same columns as [`silver.blgf_ldrrmf_annual_lgu_clean`](blgf_ldrrmf_annual_lgu_clean.md), plus:

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `disposition` | string | Why the row was rejected | Always `invalid_financial_contract_or_missing_reference` |

A row is rejected when its fiscal year or LGU type is invalid, an amount is missing, a total does not equal its parts, or the LGU is missing from the map.

## Notes

- Invalid rows are retained here before the silver build stops. Rows are only added, never updated, so earlier rejects stay visible.
- Rows flagged with `has_dq_issues` (negative, zero budget, overspent) are not rejects; they stay in the clean table.
