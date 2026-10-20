# Dashboard status

The pipeline publishes components intended for dashboard consumption. The dashboard export is currently proposed in [PR #105](https://github.com/Project-AHON/AHON-Pipeline/pull/105) and is not deployed by the main-branch bundle.

## Available components

- `platinum.population_components`: reported city/municipality census population.
- `platinum.funding_components`: city/municipality funding reports and ratios.
- `platinum.cmci_pillar_coverage`: completeness of the selected CMCI subset.
- `gold.fact_cmci_indicator` and `gold.dim_cmci_indicator`: measurements and definitions.
- `platinum.earthquake_observation_summary`: monthly observation accounting.

## Interpretation

CMCI completeness is not readiness. Funding utilization is not measured preparedness. Missing measurements remain NULL. Earthquake observations are not deduplicated events or assigned LGU exposure.

## Prerequisites for dashboard release

The dashboard must use maintained table contracts and the selected catalog. Every additional input needs a documented source and reproducible loader. Validate dataset queries, filters, joins and rendering before publication. Boundary maps and composite risk/readiness rankings require approved inputs and methods. The current bundle defines no dashboard publication or refresh schedule.

See [analytics method](../architecture/analytics-method.md) for proposed methods and current boundaries.
