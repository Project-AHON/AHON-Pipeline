# Dashboard export candidate

The export in `resources/dashboard/Project AHON LGU Command Center for DRRM.lvdash.json` contains four component pages: reported population, funding reports, selected CMCI coverage and national earthquake observation summaries. The previous maps, readiness/risk tiers, cross-indicator raw-score averages and name-based geographic rematching have been removed because their inputs or methods are not maintained pipeline contracts.

Each page has its own reference-year selector and PSGC snapshot selector. Raw rows remain disaggregated, so clearing a selector does not create a misleading sum over years. Missing values stay NULL. There is no global year filter that treats census year, fiscal year and observation year as equivalent.

## Binding and verification

All four dataset queries use bare table names from the selected catalog's `platinum` schema. No catalog or workspace identifier is embedded. After choosing a catalog containing the maintained component outputs, bind dataset catalog and dataset schema explicitly on every create/update:

```powershell
$dashboardJson = Get-Content -Raw 'resources/dashboard/Project AHON LGU Command Center for DRRM.lvdash.json'
databricks lakeview create --display-name 'AHON data components' --warehouse-id <warehouse-id> --dataset-catalog <catalog> --dataset-schema platinum --serialized-dashboard $dashboardJson --profile <profile>
```

Before executing that create command, test every dataset query against the selected catalog/schema using the CLI and a reviewed warehouse. Then verify field resolution, selector scope, missingness, raw-row rendering and permissions. Local contract tests verify SQL parsing and widget bindings; they are not warehouse or visual validation. This candidate has not been deployed or published. No managed dashboard resource or refresh schedule was added to the bundle.

CMCI completeness is not readiness. Funding utilization is not measured preparedness. Earthquake observations are not unique events or assigned LGU exposure. Composite measures require approved methodology before release.
