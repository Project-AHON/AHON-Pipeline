# CMCI

## Purpose

The Cities and Municipalities Competitiveness Index (CMCI) provides indicators describing local economic conditions, government capacity, infrastructure, resiliency, and innovation.

Project AHON uses CMCI to provide context on:

- Socioeconomic vulnerability
- LGU preparedness and response capacity
- Disaster risk reduction planning
- Communication and digital capability

> CMCI does not measure hazard occurrence, intensity, frequency, or geographic exposure. Hazard datasets remain the primary sources for hazard analysis. CMCI complements those datasets by describing local capacity and preparedness.

## Source

| Property | Value |
|---|---|
| Source | [CMCI Data Portal](https://cmci.dti.gov.ph/data-portal.php) |
| Provider | Department of Trade and Industry |
| Source format | Dynamically generated HTML |
| Access | Public website; one request per batch of LGUs, years and indicators |
| Geographic unit | Cities and municipalities |
| Years requested | 2014 to 2024 |
| Years included | 11 |
| Selected indicators | 35 |

## Coverage

The official PSGC reference contains:

```text
Cities: 149
Municipalities: 1,493
Total PSGC LGUs: 1,642
```

The live CMCI portal contains:

```text
Distinct CMCI locality values: 1,634
```

| Measure | Count |
| --- | ---: |
| PSGC LGUs profiled | 1,642 |
| Live CMCI locality values | 1,634 |
| Approved CMCI mappings | 1,634 |
| Known PSGC-only LGUs | 8 |
| Approved mapping coverage | 99.51% |

All 1,642 PSGC cities and municipalities were considered during CMCI locality reconciliation. Every live CMCI locality is now assigned to exactly one approved PSGC record.

At full scope the source returns `1,634 LGUs × 11 years × 35 indicators` values.

## Fields as received

The portal returns one HTML table per indicator. Each table has a `Province / LGU` column followed by one column per requested year. Each cell holds the indicator value for that LGU and year; `-` or a blank cell means no value.

## Indicators

The pipeline retains 35 CMCI indicators:

| Pillar | Indicator count |
|---|---:|
| Economic Dynamism | 4 |
| Government Efficiency | 10 |
| Infrastructure | 10 |
| Resiliency | 10 |
| Innovation | 1 |
| **Total** | **35** |

All 35 indicators are retained because requesting additional indicators adds little processing overhead after LGUs and years are batched. Gold models and dashboards may use a smaller business-relevant subset without removing other indicators from Bronze or Silver.

The selected indicators, with their codes and pillars, are listed in `resources/cmci_indicators.csv`:

| Pillar | Indicators (code) |
| --- | --- |
| Economic Dynamism | Local Economy Size (`les`), Local Economy Growth (`leg`), Active Establishments in the Locality (`sle`), Employment Generation (`job`) |
| Government Efficiency | Compliance to National Directives (`cnd`), Presence of Investment Promotion Unit (`pipu`), Compliance to ARTA Citizens Charter (`bre`), Capacity to Generate Local Resource (`rat`), Capacity of Health Services (`chs`), Capacity of School Services (`css`), Recognition of Performance (`lgua`), Getting Business Permits (`bpls`), Peace and Order (`pao`), Social Protection (`sp`) |
| Infrastructure | Road Network (`road`), Distance to Ports (`dtp`), Availability of Basic Utilities (`abu`), Transportation Vehicles (`trans`), Education (`edu`), Health (`hea`), LGU Investment (`inv`), Accommodation Capacity (`acc`), Information Technology Capacity (`ict`), Financial Technology Capacity (`atm`) |
| Resiliency | Land Use Plan (`lup`), Disaster Risk Reduction Plan (`drrp`), Annual Disaster Drill (`add`), Early Warning System (`ews`), Budget for DRRMP (`drrmp`), Local Risk Assessments (`lra`), Emergency Infrastructure (`ei`), Utilities (`util`), Employed Population (`ep`), Sanitary System (`ss`) |
| Innovation | Internet Capability (`intc`) |

## Matching to PSGC

Project AHON uses the Philippine Standard Geographic Code (PSGC) as the canonical LGU identifier. CMCI locality names are matched to PSGC codes through a reviewed mapping, stored as [`reference.cmci_lgu_map`](../data-dictionary/cmci_lgu_map.md). Each mapping records its reviewer and evidence. The ingestion workflow does not perform fuzzy matching. Name reconciliation is completed and reviewed before ingestion.

The approved mappings were built with the following methods:

| Method | Count | Description |
|---|---:|---|
| `EXACT` | 1,113 | PSGC and CMCI names matched directly. |
| `NORMALIZED` | 136 | Names matched after safe cleanup such as removing city prefixes, accents, punctuation, or spacing differences. |
| `COMPACT` | 7 | Names matched after removing spaces and punctuation, only when unique in both sources. |
| `MANUAL` | 7 | Earlier reviewed exceptions were resolved using contextual evidence. |
| `PROVINCE_SUFFIX` | 347 | Duplicate locality names were resolved using reviewed CMCI suffixes and PSGC province context. |
| `PROVINCE_SUFFIX_EXCEPTION` | 4 | The conflicting `DS` suffix was resolved using reviewed locality and province combinations. |
| `MANUAL_ALIAS` | 20 | Alternate names, abbreviations, spelling differences, and special-city names were verified manually against CMCI profiles. |
| **Total approved** | **1,634** | One approved mapping for every live CMCI locality. |

Mapping reconciliation checks confirm:

```text
Portal suffixes found: 77
Reviewed suffixes configured: 77
Unknown portal suffixes: 0
Configured suffixes not currently used: 0
Duplicate PSGC codes: 0
Duplicate active CMCI names: 0
Ambiguous active mappings: 0
Unused CMCI locality names: 0
Approved CMCI names missing from portal: 0
```

## Known PSGC-only source gaps

The eight LGUs without separate CMCI locality records are:

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

These are valid PSGC municipalities in the BARMM Special Geographic Area. The current CMCI portal does not expose separate locality values for them.

This is treated as a known source-coverage gap, not a matching failure or malformed-data condition.

The pipeline does not:

- Map these LGUs to former parent municipalities
- Copy historical values from former parent municipalities
- Assign zero values for unavailable CMCI data
- Remove the LGUs from the canonical PSGC reference

If CMCI later adds separate locality records, they can be reviewed and added to the reviewed CMCI mapping.

Coverage is measured with separate denominators so the source gap does not read as a pipeline failure:

```text
PSGC reference coverage: 1,642 LGUs
CMCI source and mapping coverage: 1,634 LGUs
```

## Limitations

- Eight of the 1,642 PSGC LGUs do not have separate locality records in the current CMCI portal.
- Some indicators are unavailable for particular LGUs or historical years.
- CMCI describes competitiveness and capacity, not hazard occurrence or geographic exposure.
- CMCI values are the portal's published scores. Units, direction and scoring method are not published per indicator, so values must not be interpreted as raw counts or amounts, or compared across indicators.
- The CMCI portal is a public website rather than a documented high-volume API.
