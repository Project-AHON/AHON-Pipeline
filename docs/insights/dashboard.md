# Dashboard status and import prerequisites

The export is `resources/dashboard/Project AHON LGU Command Center for DRRM Plans.lvdash.json`. The latest upload ending at commit `1fd90da` adds a Baseline Comparison page and selection-status counters, and removes the old filename. This CI repair preserves all six pages, 47 widgets, eleven dataset identities, visible titles, text, relationship graph, theme, positions, chart types and filter queries from that upload. The reviewed presentation fingerprint is recorded with that commit in the test fixture. Existing output-column names are preserved, and funding adds the missing `utilization_tier` column required by the new counter. Some original labels still refer to readiness or unsupported totals; correct those labels before publication.


## Review fixes without a layout redesign

| Finding | Change |
| --- | --- |
| Personal catalog | Queries use the review's quick-fix catalog `ahon_dev`; switch to the intended deployed catalog, such as `ahon`, for production. This export spans `gold` and `silver`, so a single dataset-schema default does not replace its qualified references. No bundle dashboard resource or per-deploy catalog parameterization is claimed. |
| CMCI names and cleanup | References use `fact_cmci_indicator` and `dim_cmci_indicator`. Gold PSGC codes and years are already typed; padding and year quote stripping are removed. |
| Funding rematching | Funding uses `fact_ldrrmf.psgc_code` with snapshot-aligned dimension joins. No regex name matching or encoding repair remains. Only city/municipality reports with no DQ issues enter analytical summaries; province reports are not added again to city reports. |
| Earthquake input | Reads valid rows from maintained `silver.philvolcs_earthquake_observation_clean`: typed coordinates, magnitude, depth, source year, event time and retained `Location`. Observations are not deduplicated events. Geographic region assignment remains NULL. |
| Missing boundary table | City widgets remain in place. Their dataset starts from the maintained LGU registry instead of an unbuilt boundary table; `geometry_wkt` remains NULL. City choropleths cannot draw until a licensed, reviewed boundary source and loader exist. |
| Percentages | Coverage measures use ratios for percentage formatting, without summing percentages or multiplying them by 100 twice. Coverage is completeness of the selected indicator subset, not readiness. |
| Unsupported scores | Cross-indicator totals, average scores across unlike indicators, readiness tiers and geographic risk outputs remain unavailable. Individual indicator measurements remain available. No maximum of 70 or additive score is asserted. See the [analytics method](../architecture/analytics-method.md). |
| Windows checkout | The uploaded filename `Project AHON LGU Command Center for DRRM Plans.lvdash.json` contains no colon and is retained. Spaces in its README link are URL-encoded. |

The maps and score widgets retain their original types and positions but may show no marks or values for the unavailable fields. This is intentional; preserving structure does not supply missing geometry or establish approved scoring methods. Original visual labels alone do not establish those metrics' validity.

## Periods and filters

City funding aggregates the loaded fiscal-year reports; utilization is the unweighted mean of reported LGU-year ratios. Province bands describe mean utilization below 50%, 50% to below 65%, and at least 65%, with a separate missing category. They are not disaster-risk or preparedness tiers. Monetary totals across years are not a single-year budget.

The existing fiscal-year filter applies only to funding sources exposing `fiscal_year`. It does not constrain preaggregated city/province/region datasets, census year or earthquake observation year. Use funding-detail datasets for fiscal-year-specific reporting. Earthquake points are national observations; regional filters cannot imply geographic attribution. CMCI uses the latest reporting year per LGU with explicit missing indicator rows, rather than carrying forward each indicator's last non-NULL value independently.

## Validation on 21 October 2026

All ten dataset SELECT queries were submitted using the user-selected Databricks profile and its running SQL warehouse. Population, LGU registry and earthquake observations succeeded. Seven datasets failed because `ahon_dev.gold.fact_ldrrmf` and/or `ahon_dev.gold.fact_cmci_indicator` are absent. This is a deployment prerequisite, not a successful dashboard release. The CMCI indicator dimension also needs to be present with the matching selected indicator set.

After the renamed upload, all eleven repaired queries, including Baseline Comparison, compiled and executed against typed empty source-contract fixtures in the same Databricks warehouse. This checks SQL syntax and column/type resolution against the documented inputs; it does not establish availability or correctness of actual missing tables. Local tests check the reviewed six-page presentation fingerprint, dataset output-column contracts, selection-counter field references and NULL handling for unavailable geometry and metrics. Neither check establishes live rendering. The export has not been deployed, published or visually verified in Databricks by this change.

Before importing or publishing:

1. Preserve an export of the current Databricks draft and identify the intended deployed catalog.
2. Build or verify all referenced tables in that catalog using the maintained pipeline. Do not substitute arbitrary tables to suppress errors.
3. Execute every dataset query against the intended warehouse and inspect missingness, reporting periods and scope.
4. Import into a separate draft and check all pages, charts, filters, relationship behavior, NULL geometry and unavailable-score handling. Add clear unavailable/coverage labels before publication. Original home navigation contains links to its original dashboard ID; update them to the new draft's page URLs if importing under a new ID.
5. Publish only after query and visual checks pass. Repository changes alone do not restore or update an existing Databricks dashboard.
