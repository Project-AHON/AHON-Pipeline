# Assumptions and limitations

## Assumptions

- **Unit of analysis:** the city or municipality, identified by its PSGC code. Provinces, regions and barangays are not analysis rows.
- **One PSGC snapshot:** all datasets are matched to one selected PSGC release (currently Q2 2024). Only reviewed mappings support that release; unmatched records and ambiguous candidates remain explicit.
- **Sources as published:** PHIVOLCS, PSA, CMCI and BLGF data are taken as published. AHON does not correct source values; it flags problems.
- **Missing is not zero:** a missing value means no data, not zero people, zero funds or a low score.
- **Population:** use the explicitly configured census year. Population is not interpolated or silently reused as annual exposure for other reference years.
- **Preparedness:** LGU capacity (CMCI) and use of the 70% LDRRMF fund are used as indicators of preparedness. They do not measure it fully.

## Limitations

### Data

| Dataset | Limitation |
| --- | --- |
| PHIVOLCS earthquakes | Records are observations, not confirmed unique events; repeats and revisions may appear. Locations are text and coordinates, not assigned LGUs by PHIVOLCS. |
| PSA population | Resident population only, not people exposed during an earthquake (for example, workers or visitors). |
| PSGC | Some names do not match other sources (spelling, ñ encoding); cityhood and new provinces change codes over time. |
| CMCI | Only a selected set of indicators is used. Raw scores have no published direction or unit for every indicator; not every LGU is covered every year. |
| LDRRMF (BLGF) | Some LGUs changed status during 2018–2024 (for example, towns that became cities); 5 old Maguindanao province rows cannot be matched; 8 BARMM Special Geographic Area towns created in 2024 have no FY2024 rows. Spending above budget is kept as reported. |

### Method

- Composite risk and preparedness scores are proposed and unimplemented. They require approved methods and geographic meaning before comparison or publication.
- No ground-shaking model, soil, fault distance or building data is used.
- Datasets cover different periods. Every comparison must state its fiscal, census and CMCI reference years and earthquake observation window. Temporal alignment requires an explicit analytical decision.

### Pipeline

- Batch only: data is refreshed when the job is run, not in real time.
- Some source files are loaded manually through Hugging Face.

## Out of scope

- Barangay-level analysis.
- Other hazards (floods, landslides, storm surge, typhoons).
- Building-level damage or casualty estimates.
- Real-time alerts.
