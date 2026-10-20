# Dashboard export candidate

The export in `resources/dashboard/lgu_command_center.lvdash.json` contains four component pages: reported population, funding reports, selected CMCI coverage and national earthquake observation summaries. The previous maps, readiness/risk tiers, cross-indicator raw-score averages and name-based geographic rematching have been removed because their inputs or methods are not maintained pipeline contracts.

Each page has its own reference-year selector and PSGC snapshot selector. Raw rows remain disaggregated, so clearing a selector does not create a misleading sum over years. Missing values stay NULL. There is no global year filter that treats census year, fiscal year and observation year as equivalent.

## Binding and verification

All four dataset queries use bare table names from the selected catalog's `platinum` schema. No catalog or workspace identifier is embedded. After choosing a catalog containing the maintained component outputs, bind dataset catalog and dataset schema explicitly on every create/update:

```powershell
$dashboardJson = Get-Content -Raw -Encoding UTF8 'resources/dashboard/lgu_command_center.lvdash.json'
databricks lakeview create --display-name 'Project AHON: LGU Command Center for DRRM' --warehouse-id <warehouse-id> --dataset-catalog <catalog> --dataset-schema platinum --serialized-dashboard $dashboardJson --profile <profile>
```

Before executing that create command, test every dataset query against the selected catalog/schema using the CLI and a reviewed warehouse. Then verify field resolution, selector scope, missingness, raw-row rendering and permissions. Local contract tests verify SQL parsing and widget bindings; they are not warehouse or visual validation. This candidate has not been deployed or published. No managed dashboard resource or refresh schedule was added to the bundle.

CMCI completeness is not readiness. Funding utilization is not measured preparedness. Earthquake observations are not unique events or assigned LGU exposure. Composite measures require approved methodology before release.

## PR #105 review resolutions

| Review comment | Resolution in this export |
| --- | --- |
| Personal catalog in queries | All four queries use bare table names. Supply the selected catalog and `platinum` schema explicitly on every import or update; no development catalog is substituted or committed. This is a manually imported export, not a new bundle dashboard resource. |
| Obsolete CMCI table names | The export reads `cmci_pillar_coverage`, built by the maintained pipeline from `gold.fact_cmci_indicator` and `gold.dim_cmci_indicator`. It no longer queries `gold.fact_cmci` or `gold.dim_cmci`. |
| External earthquake table and missing boundaries | Earthquakes read `earthquake_observation_summary`, built from `silver.philvolcs_earthquake_observation_clean`. The suggested `silver.philvolcs_earthquake_data_clean` name is not a current pipeline output. The boundary-dependent city map and geographic assignment have been removed. |
| Windows filename | The file is `lgu_command_center.lvdash.json`. The display title is supplied separately through `--display-name` and can retain its colon. |
| Cleaning and PSGC matching in dashboard SQL | Queries project maintained component fields. Funding uses its existing `psgc_code`; no regex name matching, encoding repair, LPAD, quote stripping or TRY_CAST cleanup remains in dataset SQL. |
| Undefined metrics and misleading labels | CMCI is labelled data coverage. Funding reports retain their reported ratios without priority tiers. Earthquakes remain national observation summaries with no nearest-centroid assignment. Proposed analytical definitions remain separate in the [analytics method](../architecture/analytics-method.md). |

The dataset-column and widget-binding checks in `tests/common/test_dashboard_contract.py` validate these maintained serving contracts. They do not establish live query execution or rendered dashboard behavior.
