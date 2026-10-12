# Analytical evidence definitions

Version `evidence_v1` supports source components and missing-input status. It does not authorize a scientific earthquake-risk model.

| Business question | Available evidence / transformation | Output and example query | Constraint |
| --- | --- | --- | --- |
| Earthquake risk level of each LGU | Versioned LGU geography, contextual census counts, auditable earthquake observations | `platinum.lgu_evidence_status`: `SELECT psgc_code, risk_status FROM <catalog>.platinum.lgu_evidence_status WHERE reporting_year=2024` | Risk stays NULL. Hazard intensity/probability, spatial exposure and vulnerability are insufficient |
| Preparedness shortfall relative to risk | BLGF funding ratios and CMCI component coverage, same reporting year | `SELECT psgc_code, preparedness_status, utilization_rate_total, resiliency_indicator_count FROM <catalog>.platinum.lgu_evidence_status WHERE reporting_year=2024` | No comparable scales or approved preparedness score; gap stays NULL |
| Highest-risk / least-prepared LGUs | Transparent evidence inventory across CITY/MUN entities | `SELECT psgc_code, area_name, risk_status, preparedness_status FROM <catalog>.platinum.lgu_evidence_status` | No combined risk/preparedness ranking; missing data cannot be ranked as zero |
| Preparedness aspects to prioritize | Reported CMCI resiliency indicators and funding evidence | `SELECT psgc_code, indicator_code, raw_score FROM <catalog>.gold.fact_cmci_indicator WHERE year=2024 AND indicator_code IN ('lup','drrp','add','ews','drrmp','lra','ei','util','ep','ss')` | Component evidence supports investigation, not an approved priority ranking of vulnerable LGUs |

## Metrics

| Metric | Formula / units / direction | Grain, reference and coverage | Limits |
| --- | --- | --- | --- |
| Utilization ratio | expenditure / appropriation; decimal ratio, 1 = 100% | Source LGU/fiscal year, separate 70%, QRF 30%, total | Zero/missing denominator → NULL. Overspending >1 retained and flagged. Higher spending is not necessarily better preparedness |
| Rural population | total − urban; persons, no desirability direction | Source contextual geography/census year, 2024 only | Whole-number validation, urban ≤ total. No interpolation or aggregation over overlapping levels |
| CMCI raw indicator | Source selected-indicator value, stored double; source-defined scoring meaning | City/municipality + indicator + year; 35 selected codes, 2014–2024 | Blank / `-` stays NULL, invalid values quarantined. Score comparability across years and indicator directions require external methodology review |
| Selected pillar raw sum | Sum of available indicator scores in that selected pillar | LGU/year/pillar, `selected_35_v1`; expected count from indicator dimension | Partial sums stay accompanied by INSUFFICIENT_DATA; no zero imputation. Neither official CMCI 0–20 nor final preparedness |
| Resiliency coverage | Count of non-null selected resiliency indicators; expected 10 | City/municipality/reporting year | 10 available means component coverage, not strong preparedness. Missing source locality and indicators remain distinguishable through map/source evidence |
| Observation count | Source observation records | Immutable earthquake file/version/ordinal, listing month | Neither deduplicated event count nor hazard/risk. Corrected source files can contain repeated descriptions of an event |

`lgu_evidence_status` includes source ingested times, `as_of`, metric version, coverage statuses and an uncertainty message. Ingestion time measures ingestion freshness, not publication freshness. No freshness SLA or risk thresholds were approved; consumers must inspect source periods and the latest reject disposition. Source values in prior periods must not be silently carried into a later reporting year.

There are no weights, normalization scales, score thresholds or risk ranking rules in this version. Such parameters require an approved methodology version, sufficient coverage criteria and tests before becoming outputs. A reported low/zero drill, plan or warning-system indicator can prompt source verification; it is not itself proof that the capability is absent. Missing indicators first call for evidence collection.

Needed next: hazard maps/probabilistic ground motion with observation windows; verified LGU boundaries; building/facility exposure and stable facility identity; vulnerability measures; approved preparedness indicator interpretation and weighting; historical geographic crosswalks and source freshness rules.
