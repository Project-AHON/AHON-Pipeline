# Philippine Standard Geographic Code

## Purpose

The Philippine Standard Geographic Code (PSGC) provides the canonical geographic reference for Project AHON.

Project AHON uses PSGC codes to identify Philippine cities and municipalities consistently across datasets. Source-specific locality names, including CMCI names, are mapped to PSGC codes before downstream processing.

The PSGC reference layer currently contains:

- `ahon.reference.lgu_master`
- LGU boundary records loaded by the boundary workflow

## Source Scope

The reference scope includes active Philippine cities and municipalities from the configured PSGC source.

Current validated scope:

| Geographic level | Count |
|---|---:|
| Cities | 149 |
| Municipalities | 1,493 |
| Total active LGUs | 1,642 |

The count represents the current PSGC reference scope used by Project AHON. Source counts may change when new LGUs are created, renamed, merged, or reclassified.

## Canonical LGU Identity

Project AHON uses `psgc_code` as the canonical LGU identifier.

Names are descriptive attributes and must not be used as primary keys because:

- Different LGUs may share the same name.
- Source systems may use abbreviations or alternative spellings.
- City and municipality prefixes may differ.
- Names may change over time.
- Source systems may use historical names.

Downstream datasets should retain their original locality names for traceability while linking records to the canonical PSGC code.

## LGU Master Table

### Table

```text
ahon.reference.lgu_master
```

### Grain

One row per active PSGC city or municipality.

### Primary Key

```text
psgc_code
```

### Key Attributes

The table contains attributes used for canonical identity, administrative context, source reconciliation, and activity status.

Important attributes include:

| Column | Purpose |
|---|---|
| `psgc_code` | Canonical 10-digit PSGC identifier |
| `lgu_name` | Current canonical LGU name |
| `old_name` | Historical or former LGU name, when available |
| `geographic_level` | City or municipality classification |
| `province_code` | Parent province code, when applicable |
| `province_name` | Parent province name, when applicable |
| `is_active` | Indicates whether the LGU belongs to the current active reference scope |

Some independent or special cities may not have a conventional province value. A missing `province_name` does not automatically indicate invalid data.

## LGU Master Loading

The LGU master is loaded by:

```text
load_lgu_master.py
```

The workflow:

1. Reads the configured PSGC source.
2. Selects active cities and municipalities.
3. Standardizes identifiers and geographic attributes.
4. Preserves historical names when available.
5. Validates PSGC-code uniqueness.
6. Writes the canonical LGU reference table.

The loader should be rerunnable without creating duplicate PSGC records.

## LGU Boundaries

LGU boundary data is loaded by:

```text
load_lgu_boundaries.py
```

The boundary workflow provides geographic representations for supported cities and municipalities.

Boundary records must link to the canonical LGU master through:

```text
psgc_code
```

Name-based boundary matching may be used only as a controlled reconciliation step. The persisted relationship must use the PSGC code.

## Boundary Validation

The boundary workflow should validate that:

- Required spatial fields are present.
- PSGC codes are not blank.
- Boundary PSGC codes exist in the LGU master.
- Duplicate boundary keys are reported.
- Invalid or empty geometries are reported.
- Unsupported geographic levels are excluded or documented.
- Source and loaded record counts reconcile.

A boundary coverage gap must remain visible. Missing geometry must not be replaced with another LGU's boundary.

## Reference Validation

### PSGC-Code Uniqueness

Each active city or municipality must have exactly one canonical PSGC code.

Expected result:

```text
Duplicate active PSGC codes: 0
```

### Geographic-Level Coverage

The active reference contains the configured city and municipality levels.

Current validated counts:

```text
Cities: 149
Municipalities: 1,493
Total active LGUs: 1,642
```

### Required Attributes

Active LGU records must contain the required canonical identity fields.

At minimum:

```text
psgc_code
LGU name
geographic level
activity status
```

Province fields may be absent for independent cities or special administrative cases.

### Source Reconciliation

The loaders should report:

- Source record count
- Loaded active LGU count
- City count
- Municipality count
- Duplicate PSGC-code count
- Missing required-field count
- Boundary coverage count
- Boundary records without an LGU-master match

## Newly Created BARMM Municipalities

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

These municipalities are valid members of the PSGC reference scope.

Their presence in the PSGC master does not guarantee that every downstream source provides separate records for them. For example, the current CMCI portal does not expose separate locality records for these eight municipalities.

Downstream pipelines must treat this as a known source-coverage gap rather than a PSGC data-quality failure.

## Downstream Use

The PSGC reference supports downstream workflows by providing a stable geographic identifier for:

- CMCI locality mapping
- Hazard and environmental datasets
- LGU boundary joins
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

## Data Governance

### Authorized Changes

Changes to the PSGC reference should come from:

- An updated official source
- A reviewed correction
- A documented administrative change
- A controlled reload of the reference workflow

### Prohibited Changes

Do not:

- Reassign a PSGC code to a different LGU.
- Generate PSGC codes from locality names.
- Replace missing boundaries with nearby LGU boundaries.
- Remove valid LGUs because a downstream source does not contain them.
- Overwrite canonical names with source-specific names.
- Infer province relationships without reviewed administrative evidence.

### Traceability

Reference-loading workflows should preserve enough metadata to identify:

- Source used
- Load timestamp
- Transformation applied
- Validation outcome
- Known exclusions or coverage gaps

## Running the Loaders

Run the LGU master loader before the boundary loader:

```text
1. load_lgu_master.py
2. load_lgu_boundaries.py
```

The LGU master must exist first because boundary records are validated against the canonical PSGC scope.

The exact Databricks task configuration depends on the deployment environment. 

## Expected Output

A successful PSGC reference run should confirm:

```text
Active PSGC LGUs: 1,642
Duplicate PSGC codes: 0
Cities: 149
Municipalities: 1,493
```

The boundary workflow should additionally report:

```text
Boundary records loaded
Unique boundary PSGC codes
Boundary PSGC codes missing from LGU master
Active LGUs without boundary records
Invalid or empty geometries
```

Any difference from the expected reference scope should be investigated before dependent pipelines run.
