# Available evidence for the four business questions

Hazard describes the earthquake process; exposure describes people/assets in affected areas; vulnerability describes susceptibility to harm; preparedness describes capacity and actions. The attached conceptual diagrams describe intended questions, not a validated scoring method.

| Question | Supported evidence | Missing inputs and limits |
| --- | --- | --- |
| Each LGU's earthquake risk level | PHIVOLCS observation parameters; resident counts; source OSM geometry | Ground-motion/hazard model, defensible event identity, spatial exposure, vulnerability model, validated risk scale and cutoffs. No LGU risk level is published |
| Preparedness relative to risk | Selected CMCI measurements and coverage; reported BLGF amounts and utilization | Validated preparedness constructs, CMCI interpretation/scales, reference-period alignment and risk methodology. No risk-minus-preparedness calculation |
| Greater risk with weaker preparedness | Component tables permit evidence review by codes, years and coverage | Comparable risk/preparedness scores and justified ranking. No vulnerability or highest-priority classification |
| Components to prioritize | Inspect reviewed source CMCI indicators for a selected LGU/year, including missingness | Directionality, units, benchmarks and domain review. Do not rank different indicators by raw numeric value or label the smallest as weakest |

`gold.fact_population` and `platinum.population_components` use one city/municipality × census year row, keyed by `(psgc_code, population_year)`. `population` is the reported PSGC CITY/MUN measurement in people. Barangay rows, regional totals and provincial totals are not added to it. The full same-snapshot city/municipality reference grid remains visible: `AVAILABLE` means a reported measurement exists, and `MISSING` means the reported value or the city/year source observation is absent. Missing remains NULL, never zero. This is resident population, not earthquake-exposed population. The earlier barangay sum/coverage contract is superseded by decision 0012.

`platinum.funding_components` exposes city/municipality reporting units; provincial source reports remain retained outside this analytical view. Ratios are `expenditure / appropriation`; zero denominator yields NULL. Ratios above one remain above one with a warning. Missing financial fields or inconsistent component totals block Gold; prior accepted outputs remain until recovery and must be read alongside the latest DQ run. Funding utilization alone does not establish preparedness.

`platinum.cmci_pillar_coverage` counts available versus expected selected indicators by LGU/year/pillar. Counts and `COMPLETE`/`INSUFFICIENT_DATA` describe completeness of the selected subset, not official CMCI pillar completeness or a capacity score. The Gold `raw_score` is the portal's numeric value; units, polarity and official scoring remain unspecified. Invalid latest corrections are withheld as NULL rather than exposing an older accepted value as current.

`platinum.earthquake_observation_summary` counts source observations by reported year/month and snapshot. Repeated identical rows and possible event revisions remain separate observations. Maximum magnitude is a reported parameter over valid observations, not LGU hazard intensity or risk. Listing wall-clock time is stored as `TIMESTAMP_NTZ`; UTC interpretation is withheld pending an explicit time contract.

Example read-only queries after the relevant tasks succeed (choose your catalog explicitly):

```sql
-- Only city/municipality rows; a missing source measurement remains NULL.
SELECT psgc_code, population_year, population, coverage_status, disposition
FROM platinum.population_components;

-- Inspect a selected LGU and year, not a ranking across unlike indicators.
SELECT f.psgc_code, f.year, d.indicator_name, d.pillar_name,
       f.raw_score, f.has_dq_issues, f.source_period
FROM gold.fact_cmci_indicator f
JOIN gold.dim_cmci_indicator d USING (indicator_code)
WHERE f.psgc_code = :selected_psgc_code AND f.year = :selected_year;

-- Compare only municipal reporting units for a selected fiscal year.
SELECT psgc_code, fiscal_year, total_appropriation, total_expenditure,
       utilization_rate_total, match_status, has_dq_issues
FROM platinum.funding_components
WHERE lgu_type = 'Municipality' AND fiscal_year = :selected_year;
```
