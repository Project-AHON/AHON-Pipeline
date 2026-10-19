# Analytical data model

`gold.dim_lgu` contains one current PSGC entity per string `psgc_code`, with its selected `source_period`, names, geographic level and standardized hierarchy codes. Codes, period and level are checked before facts reference it.

| Table | Grain / unique business key | Meaning |
| --- | --- | --- |
| `gold.fact_population` | city/municipality × census year; `(psgc_code, population_year)` | Direct CITY/MUN source measurement; missing records/values remain NULL; full LGU/year grid |
| `gold.fact_population_total` | PXWeb hierarchical source geography × census year; deterministic `population_fact_key` | Separate source context, not the canonical city/municipality analysis fact; contextual mapping and DQ flags |
| `gold.fact_cmci_indicator` | `(psgc_code, year, indicator_code)` | Source indicator measurement; missing or invalid latest values are NULL; reviewed source identity retained |
| `gold.dim_cmci_indicator` | `indicator_code` | Existing 35-code label/pillar metadata; `metric_version=selected_35_v1` identifies the selected subset |
| `gold.fact_ldrrmf` | original BLGF reporting entity × fiscal year; `(fiscal_year, region, province, lgu_name, lgu_type)` | Separate 70%/30% amounts and totals in pesos; utilization ratios; contextual geographic disposition |

Earthquake tables contain observations, not established deduplicated events. Textual candidates are arrays in Silver and never fact relationships to LGUs. Event-level Gold is withheld until identity, duplicate and correction policy are approved.

Join facts only at aligned geographic grains and explicitly selected reference periods. A 2024 census is not annual exposure for every CMCI or fiscal year. Historical fiscal entities can differ from the current PSGC geography; preserve unmatched codes and source names. Never sum national, regional, province and municipal census rows together, or provincial funding with municipal funds to infer a common LGU resource total. Use [component queries](metrics.md) with their coverage fields.
