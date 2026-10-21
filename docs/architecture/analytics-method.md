# Analytics method

Proposed methods for answering the four analytical questions, alongside current component contracts. Every measure is computed per city or municipality (`CITY`, `MUN` in `gold.dim_lgu`) and joined on `psgc_code`.

## Status

The current pipeline does not materialize composite LGU risk, preparedness, gap, quadrant or priority rankings. The formulas, weights, cut-offs and peer groups below require methodological approval and implementation before publication as production results. No proposed default overrides the current missing-value or geographic-assignment contracts.

Implemented outputs are reported population, uncapped funding ratios, selected CMCI indicator measurements and coverage, and monthly earthquake observation summaries. Geographic earthquake assignment remains withheld.

## Terms

| Term | Meaning in AHON |
| --- | --- |
| Hazard | The earthquake process: how often, how strong and how deep earthquakes are near an LGU |
| Exposure | People in the LGU (resident population) |
| Risk | Hazard combined with exposure |
| Preparedness | An LGU's capacity and actions to reduce and respond to disasters, from CMCI resiliency indicators and LDRRMF spending |
| Gap | Risk minus preparedness, on the same 0 to 100 scale |
| Vulnerability | High risk with low preparedness. This is the project's working definition, not the people-based meaning used in disaster science. |

## Proposed measures by analytical question

### 1. What is each LGU's earthquake risk level?

| Measure | Calculation | Source tables |
| --- | --- | --- |
| Strong earthquake count | Valid observations with magnitude at or above the agreed cut-off, linked to the LGU, per year or averaged over the period | `silver.philvolcs_earthquake_observation_clean` |
| Maximum magnitude | Largest valid magnitude linked to the LGU in the period | `silver.philvolcs_earthquake_observation_clean` |
| Population | Reported residents for the census year | `gold.fact_population` |
| **Risk score** | Scaled earthquake index (strong count and maximum magnitude) × scaled population, then ranked 0 to 100 | All of the above |
| Risk level | Low, Medium or High from the risk score | Risk score |

### 2. How prepared is each LGU relative to its risk?

| Measure | Calculation | Source tables |
| --- | --- | --- |
| CMCI resiliency | The LGU's resiliency indicators, each ranked against other LGUs and averaged | `gold.fact_cmci_indicator`, `gold.dim_cmci_indicator` |
| Preparedness budget per person | 70% LDRRMF appropriation ÷ population | `gold.fact_ldrrmf`, `gold.fact_population` |
| 70% fund utilization | 70% expenditure ÷ 70% appropriation over FY2018 to FY2024, capped at 100% | `gold.fact_ldrrmf` |
| **Preparedness score** | Scaled CMCI resiliency + scaled preparedness budget per person + scaled 70% utilization, ranked 0 to 100 | All of the above |
| **Gap** | Risk score − preparedness score. Positive means risk is ahead of preparedness. | Both scores |

Only the 70% LDRRMF part is used for preparedness. The 30% Quick Response Fund pays for response after a disaster.

### 3. Which LGUs have high risk and low preparedness?

| Measure | Calculation |
| --- | --- |
| Quadrant | Split risk and preparedness at the median of the peer group: **Prioritize** (high risk, low preparedness), **Reinforce** (high risk, high preparedness), **Build capacity** (low risk, low preparedness), **Sustain** (low risk, high preparedness) |
| Gap rank | LGUs ranked by gap, nationally and within their province or region |

### 4. Which areas should be strengthened first?

| Measure | Calculation | Source tables |
| --- | --- | --- |
| Weakest indicators | The LGU's resiliency indicators with the lowest rank among peers | `gold.fact_cmci_indicator` |
| Weakest pillar | The CMCI pillar where the LGU's percentile among peers is lowest | `gold.fact_cmci_indicator`, `gold.dim_cmci_indicator` |
| Unused preparedness funds | 70% appropriation − 70% expenditure, in pesos | `gold.fact_ldrrmf` |

## Proposed scaling

Inputs have different units (counts, magnitudes, pesos per person, ratios), so each one is turned into a percentile rank from 0 to 100 among all cities and municipalities before it is combined. Ranking also keeps a few extreme values from dominating. Components are weighted equally unless the team agrees otherwise.

CMCI indicators are compared only by rank within the same indicator. Units, direction and scoring method are not published per indicator, so raw values of different indicators must not be compared, summed or ranked against each other.

## Periods

| Input | Period |
| --- | --- |
| Earthquakes | Months loaded from PHIVOLCS (see the [PHIVOLCS profile](../data/data-profile/phivolcs-earthquake-profile.md)) |
| Population | Census year `census_year` (2024) |
| CMCI | Latest edition with values (2024) |
| LDRRMF | FY2018 to FY2024 for utilization; latest year for budget per person |
| PSGC | Snapshot `Q2_2024` |

A 2024 census is not annual exposure for every CMCI or fiscal year. Each measure states its year. Select an explicit reference year for each measure. Do not select the latest non-NULL value independently for each indicator: that can substitute an older measurement for a missing value and mix editions.

## Rules for reading the results

- **Population** is the reported PSGC CITY/MUN measurement in people. Barangay rows, regional totals and provincial totals are not added to it. `AVAILABLE` means a reported measurement exists; `MISSING` means it is absent. Missing remains NULL, never zero. This is resident population, not earthquake-exposed population.
- **Funding** uses city and municipality reports only (`platinum.funding_components`). Ratios are `expenditure / appropriation`; a zero denominator yields NULL. Funding utilization alone does not establish preparedness.
- **CMCI coverage** (`platinum.cmci_pillar_coverage`) describes completeness of the selected indicator subset, not official CMCI pillar completeness or a capacity score.
- **Earthquakes** are observations, not deduplicated events. Repeated rows and revisions stay separate. Magnitude is a stand-in for shaking intensity, which is not in the data.
- **Linking earthquakes to LGUs** uses the town named in the PHIVOLCS location text (`candidate_status = single_candidate`). This is a locality mentioned in publisher text, not a verified nearest-town assignment or the area that felt the earthquake. The pipeline reports these candidates but does not yet assign `psgc_code` ([decision 0014](../decisions/0014-earthquake-observations-and-locality-candidates.md)).
- **Missing inputs** leave a score NULL rather than zero, and the LGU is shown as "insufficient data".

## Decisions required before implementation

The [dashboard export](../insights/dashboard.md) preserves the original presentation while withholding unsupported metrics. Its compatibility field `cmci_readiness` means selected-indicator data coverage, not readiness. Cross-indicator score totals and averages remain NULL because unlike raw units have no agreed additive scale. City geometry, geographic earthquake attribution and composite risk outputs remain unavailable pending reviewed inputs and methods. Its province utilization bands (below 50%, 50% to below 65%, at least 65%, and missing) describe the unweighted mean of reported city/municipality LGU-year utilization ratios; they are not approved risk or preparedness thresholds.

| Decision | Proposed option (not an implemented default) |
| --- | --- |
| Strong earthquake cut-off | Magnitude 5.0 |
| Weights inside the risk and preparedness scores | Equal |
| Risk level cut-offs | Thirds of the national distribution |
| One year or a multi-year average for earthquakes | Average over the loaded period |
| Default peer group for quadrants and ranks | Province |
| Approval of the earthquake locality link | Candidates reported, not assigned |

## Example queries

Choose your catalog explicitly.

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
