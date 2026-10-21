# 0019: Risk and preparedness scores for the dashboard

- **Status:** Proposed
- **Date:** 2026-10-16
- **Builds on:** [0017](0017-city-municipality-analysis-grain.md); decision [0015](0015-reconciled-source-contracts.md) left scoring for a later record

## Context

The dashboard has to answer which cities and municipalities have high earthquake risk and low preparedness, and what to strengthen first. Until now the pipeline only published separate measures (population, funding use, CMCI coverage, earthquake counts) and the earlier dashboard mixed them up: its "risk tiers" were fund utilization only, and CMCI "readiness" counted coverage, not scores. The team needed one agreed, explainable method that local leaders and the TWG can check.

## Decision

- **Unit:** every score is per city or municipality, joined on `psgc_code`.
- **Scaling:** each input is turned into a percentile rank from 0 to 100 among all cities and municipalities before it is combined. Inputs are weighted equally.
- **Risk score:** scaled earthquake index (count of strong earthquakes and maximum magnitude near the LGU) combined with scaled population, then ranked 0 to 100. **Risk level** is Low, Medium or High from the score.
- **Preparedness score:** the average of scaled CMCI resiliency (each indicator ranked within itself), scaled 70% LDRRMF budget per person, and scaled 70% fund use over FY2018 to FY2024 (capped at 100%), ranked 0 to 100. Only the 70% part of the LDRRMF counts; the 30% Quick Response Fund pays for response after a disaster.
- **Gap:** risk score minus preparedness score. Positive means risk is ahead of preparedness.
- **Quadrants:** split risk and preparedness at the median of the peer group: **Prioritize** (high risk, low preparedness), **Reinforce** (high, high), **Build capacity** (low, low), **Sustain** (low risk, high preparedness).
- **Missing inputs** leave a score NULL, shown as "insufficient data", never zero.
- **Defaults until the team decides otherwise:** strong earthquake = magnitude 5.0 or more; risk level cut-offs = thirds of the national distribution; earthquakes averaged over the loaded period; peer group = province.
- The scores are built in one LGU-level platinum table for the dashboard. The full method is in the [analytics method](../architecture/analytics-method.md).

## Why

- Risk and preparedness on the same 0 to 100 scale make the gap meaningful.
- Percentile ranks put counts, magnitudes, pesos per person and ratios on one scale, and stop a few extreme values from dominating.
- Equal weights and simple cut-offs are easy to explain and to change later.
- Ranking CMCI indicators within themselves respects that their units and scoring are not published per indicator.

## Alternatives considered

- **Min-max scaling:** sensitive to outliers such as very large cities.
- **Fund utilization alone as preparedness:** high spending does not show capacity; a small budget fully spent can still be too little.
- **Official CMCI pillar scores:** not available for the selected indicators in a comparable form.
- **Fixed score bands for risk levels:** more stable across reruns, but no agreed thresholds exist yet.

## Consequences

- A new platinum table (planned `platinum.lgu_risk_profile`) with one row per city or municipality, and DQ gates for its columns.
- The dashboard's "How scores work" panel explains this method.
- Earthquake inputs use locality candidates until the link in [0014](0014-earthquake-observations-and-locality-candidates.md) is approved; the dashboard says so.

## Open

- Strong earthquake cut-off, weights, risk level cut-offs and default peer group (defaults above).
- Whether earthquakes use one year or the average over the loaded period.
