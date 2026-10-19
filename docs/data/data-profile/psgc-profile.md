# PSGC

## Purpose

The Philippine Standard Geographic Code (PSGC) provides the canonical geographic reference for Project AHON.

Project AHON uses PSGC codes to identify Philippine cities and municipalities consistently across datasets. Source-specific locality names, including CMCI and BLGF names, are mapped to PSGC codes before downstream processing.

The PSGC source also carries census population for cities and municipalities, which feeds the population fact.

## Source

| Property | Value |
|---|---|
| Source | [PSA PSGC classification API](https://classification.psa.gov.ph/psgc) |
| Provider | Philippine Statistics Authority (PSA) |
| Source format | Paginated JSON, saved as one JSONL file per snapshot |
| Access | PSA-issued API token, stored as a Databricks secret |
| Snapshots loaded | `Q2_2024`, `April_2024`, `Q4_2023`, `Q2_2021` |
| Selected snapshot | `Q2_2024` (`source_period`) |
| Geographic levels | Region, province, city, municipality, sub-municipality, special geographic unit, barangay |
| Population years in `Q2_2024` | 2015, 2020, 2024 |

The selected snapshot is an input, independent of the census year. Each snapshot is checked against an approved expected row count before it is loaded. The four snapshots landed 175,079 rows.

## Coverage

The selected `Q2_2024` snapshot contains:

| Measure | Count |
|---|---:|
| Cities | 149 |
| Municipalities | 1,493 |
| Total LGUs | 1,642 |
| Duplicate PSGC codes | 0 |
| LGUs with 2024 population | 1,623 |
| LGUs without 2024 population | 19 |

The count represents the current PSGC reference scope used by Project AHON. Source counts may change when new LGUs are created, renamed, merged, or reclassified.

## Canonical LGU identity

Project AHON uses `psgc_code` as the canonical LGU identifier.

Names are descriptive attributes and must not be used as primary keys because:

- Different LGUs may share the same name.
- Source systems may use abbreviations or alternative spellings.
- City and municipality prefixes may differ.
- Names may change over time.
- Source systems may use historical names.

Downstream datasets should retain their original locality names for traceability while linking records to the canonical PSGC code.

## Fields as received

Each API record describes one geographic entity in one snapshot.

| API field | Loaded as | Description |
|---|---|---|
| `code` | `psgc_code` | PSGC code |
| `area_name` | `area_name` | Name of the geographic entity |
| `geographic_level` | `geographic_level` | `Reg`, `Prov`, `City`, `Mun`, `SubMun`, `SGU` or `Bgy` |
| `reg`, `prv`, `mun`, `bgy` | `region_code`, `province_code`, `municipality_code`, `barangay_code` | Parent codes as published |
| `correspondence_code` | `correspondence_code` | Correspondence code |
| `old_name` | `old_name` | Former or previous name, when available |
| `city_class` | `city_class` | City classification, such as HUC or ICC |
| `income_classification` | `income_classification` | Income classification |
| `urban_rural` | `urban_rural` | Urban or rural tag |
| `island_region` | `island_region` | Luzon, Visayas or Mindanao |
| `status` | `status` | Status as published |
| `version` | `version` | Publisher's version |
| `populations` | `populations_json` | Array of census population records by year |

Things to know about the values as received:

- Codes arrive as 8, 9 or 10 digits because leading zeros are dropped. They are standardized to ten-digit strings in silver.
- Some names carry PSA footnote markers such as `Name 1/` and non-breaking spaces. These are removed in silver.
- Population values are text with formatting characters such as commas.

## Known source cases

These are known source or hierarchy cases, not data-quality failures.

### Cities and municipalities without a conventional province

Some independent or special cities may not have a conventional province value. A missing `province_name` does not automatically indicate invalid data. Cases include NCR cities, highly urbanized and independent component cities, Pateros, and the Special Geographic Area municipalities.

### City of Isabela

The City of Isabela is in Region IX by its PSGC code, but it is geographically part of Basilan (BARMM). Project AHON uses Basilan as its province name, although its code does not fall under Basilan's province code.

### Pateros

Pateros is a municipality in the National Capital Region. Its PSGC hierarchy provides province code `13817`, but no conventional province name. This is preserved from the source.

### Newly created BARMM municipalities

The current PSGC reference contains eight newer municipalities in the BARMM Special Geographic Area:

```text
1999901000 | Kapalawan
1999902000 | Old Kaabakan
1999903000 | Kadayangan
1999904000 | Nabalawag
1999905000 | Pahamuddin
1999906000 | Malidegao
1999907000 | Ligawasan
1999908000 | Tugunan
```

These municipalities are valid members of the PSGC reference scope. Their province name is NULL because they do not belong to a conventional province.

Their presence in the PSGC master does not guarantee that every downstream source provides separate records for them. For example, the current CMCI portal does not expose separate locality records for these eight municipalities, and BLGF has no FY2024 LDRRMF rows for them.

Downstream pipelines must treat this as a known source-coverage gap rather than a PSGC data-quality failure.

### Sulu population coverage

Nineteen Sulu municipalities (`province_code = 19066`) do not have a 2024 population value in the selected `Q2_2024` snapshot. Their missing 2024 population is preserved as NULL rather than replacing it with an older population value.

## Downstream use

The PSGC reference supports downstream workflows by providing a stable geographic identifier for:

- CMCI locality mapping
- LDRRMF locality mapping
- Earthquake locality candidates
- City and municipality population
- Authorized-PSGC validation
- National and source-specific coverage measurement
- Name changes and source-specific aliases

The PSGC reference denominator and each source-specific denominator must remain separate.

Example:

```text
PSGC reference scope: 1,642 LGUs
CMCI-supported scope: 1,634 LGUs
Known CMCI source gap: 8 LGUs
```

A downstream source should not be expected to contain all 1,642 LGUs unless the source itself supports the complete PSGC scope.

## Data governance

### Authorized changes

Changes to the PSGC reference should come from:

- An updated official source
- A reviewed correction
- A documented administrative change
- A controlled reload of the reference workflow

A snapshot change requires a reviewed reference decision.

### Prohibited changes

Do not:

- Reassign a PSGC code to a different LGU.
- Generate PSGC codes from locality names.
- Remove valid LGUs because a downstream source does not contain them.
- Overwrite canonical names with source-specific names.
- Infer province relationships without reviewed administrative evidence.

## Limitations

- The API requires a PSA-issued token; extraction cannot run without it.
- All analysis uses one snapshot (`Q2_2024`). LGUs created, renamed or reclassified after it are not reflected.
- The snapshot is not the same as the census year. Population in `Q2_2024` covers 2015, 2020 and 2024 only.
- Nineteen Sulu municipalities have no 2024 population.
- Display labels such as province name are not geographic join keys. Joins use `psgc_code`.
