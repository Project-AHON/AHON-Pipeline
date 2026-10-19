# Data sources

All sources are public Philippine government data. Each is matched to the city or municipality PSGC code before analysis. Column-level detail and known issues for each dataset are in the [data dictionary](data-dictionary/README.md).

## Summary

| Dataset | Publisher | What it tells us | Format and access | Coverage | Grain | Used for |
| --- | --- | --- | --- | --- | --- | --- |
| PSGC | Philippine Statistics Authority (PSA) | Official geographic codes and names, with census population per city and municipality | PSA classification API (token required) | Snapshot Q2 2024 | One geographic unit (region to barangay) | Shared key for all datasets; population |
| PSA population | Philippine Statistics Authority (PSA) | Total and urban population | CSV from PSA OpenSTAT, table 0241A6DPUP1 | 2024 census; 1,744 rows (national, region, province, city/municipality) | Geographic area × census year | Cross-check of population totals |
| PHIVOLCS earthquakes | Philippine Institute of Volcanology and Seismology (PHIVOLCS) | Where and when earthquakes happened: date-time, latitude, longitude, depth, magnitude, location text | Monthly listings from the PHIVOLCS website, saved as CSV | Selected year-month window | One earthquake observation | Risk |
| CMCI | Department of Trade and Industry (DTI) | LGU capacity indicators across economy, government efficiency, infrastructure, resiliency and innovation | HTML tables from the CMCI data portal | 2014–2024; 35 selected indicators; 1,642 LGUs profiled | LGU × year × indicator | Preparedness |
| LDRRMF | Bureau of Local Government Finance (BLGF) | Local DRRM fund budget and spending, split into the 70% fund and the 30% Quick Response Fund | Seven Excel workbooks, uploaded to the team's Hugging Face dataset | FY2018–2024; 11,434 rows | Reporting LGU × fiscal year | Preparedness |

## How sources are loaded

- File-based sources (LDRRMF) are uploaded unchanged to the team's Hugging Face dataset, then copied into the `source` volume in the `reference` schema. See [decision 0004](../decisions/0004-source-file-landing-hugging-face.md).
- API and web sources (PSGC, PSA population, PHIVOLCS, CMCI) are pulled by each dataset's `extract/` code and saved as raw files in the same volume.
- Raw files are never changed. Bronze tables read from the volume and record where each row came from.


