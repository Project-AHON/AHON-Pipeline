## CMCI

### Source Summary

| Property | Value |
|---|---|
| Source | Cities and Municipalities Competitiveness Index Data Portal |
| Provider | Department of Trade and Industry |
| Source format | Dynamically generated HTML |
| Geographic unit | Cities and municipalities |
| Available period used | 2014 to 2024 |
| Selected indicators | 35 |
| PSGC LGUs profiled | 1,642 |
| Approved CMCI mappings | 1,262 |
| Unmatched PSGC LGUs | 380 |

Source portal:

[CMCI Data Portal](https://cmci.dti.gov.ph/data-portal.php)

### Project AHON Use

CMCI provides context on:

- Local economic conditions
- Government capacity
- Infrastructure and essential services
- Disaster preparedness and resiliency
- Communication and digital capability

CMCI does not measure hazard occurrence, intensity, frequency, or geographic exposure. Hazard datasets remain the primary sources for hazard analysis.

### Profiling Outcome

All 1,642 PSGC cities and municipalities were considered during CMCI locality mapping.

Current mapping outcome:

```text
Approved active mappings: 1,262
Unmatched PSGC LGUs: 380
Ambiguous active mappings: 0
```

The 380 unmatched LGUs remain in the PSGC reference and CMCI mapping layers for future review. They are intentionally excluded from CMCI ingestion because no approved source identity is available.

### Ingested Scope

Production ingestion includes only mappings that meet all three conditions:

```text
match_status = MATCHED
reviewed = true
is_active = true
```

The ingested scope is:

```text
Approved LGUs: 1,262
Years: 2014 to 2024
Indicators: 35
```

### Storage

Bronze target:

```text
ahon.bronze.cmci_raw_indicator_batch_html
```

Silver targets:

```text
ahon.silver.cmci_economic_dynamism
ahon.silver.cmci_government_efficiency
ahon.silver.cmci_infrastructure
ahon.silver.cmci_resiliency
ahon.silver.cmci_innovation
```

### Coverage Note

Use different denominators for reference and pipeline coverage:

```text
Reference coverage: 1,642 PSGC LGUs
CMCI pipeline coverage: 1,262 approved mappings
```

The 380 unmatched LGUs are a known source-mapping coverage gap, not failed CMCI ingestion records.

For the complete CMCI profile, pipeline design, and quality scope, see [CMCI Dataset](cmci.md).