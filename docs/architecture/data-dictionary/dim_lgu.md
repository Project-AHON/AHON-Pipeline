### Gold Table

`ahon_dev.gold.dim_lgu`

### Purpose

The `dim_lgu` dimension provides a standardized geographic reference table for AHON's Gold layer. It organizes Philippine geographic entities using PSGC codes and hierarchy codes derived in Silver.

It supports downstream analytics by allowing fact tables and other datasets to join to a consistent geographic reference instead of repeatedly processing raw PSGC records.

### Grain

One record represents one unique PSGC geographic entity from the selected PSGC API snapshot.

The dimension includes geographic records at multiple levels, such as regions, provinces, cities, municipalities, sub-municipalities, barangays, and special geographic units, depending on the records available in the selected Silver snapshot.

Unlike Silver, Gold `dim_lgu` does not retain multiple versions of the same geographic entity across API periods.

### Business Key

`psgc_code`

The dimension uses `psgc_code` as its business key because the table represents only one selected snapshot. Each geographic entity must have one unique PSGC code within that snapshot.

Silver uses a composite business key of `psgc_code + geographic_level + _api_period` because it retains records from multiple snapshots.

### Selected PSGC Snapshot

**Selected snapshot: `Q2_2024`**

The Gold transformation filters Silver using the configurable `source_period` widget, whose default value is `Q2_2024`.

```python
dbutils.widgets.text("source_period", "Q2_2024")
SOURCE_PERIOD = dbutils.widgets.get("source_period")

snapshot_df = source_df.filter(
    F.col("_api_period") == SOURCE_PERIOD
)
```

### Why Only Q2_2024?

The project uses `Q2_2024` as the selected reference snapshot for the initial Gold geographic dimension. The purpose is to provide one consistent geographic baseline for downstream analytics.

The decision is based on the following design considerations:

- **Avoid duplicate geographic entities:** Silver contains multiple PSGC snapshots. Loading all snapshots into a single dimension could produce repeated PSGC codes and violate the dimension's intended grain.
- **Maintain a consistent reference:** Fact tables and analytical datasets should join against one selected geographic reference rather than an uncontrolled mixture of historical snapshots.
- **Keep historical versions in Silver:** Silver retains snapshot-level history, allowing the project to investigate changes across API periods without duplicating every historical version in the initial Gold dimension.
- **Make the reference configurable:** The `source_period` widget allows the selected snapshot to be changed deliberately without hardcoding the filter throughout the transformation.

**Important limitation:** `Q2_2024` is the current configured baseline, not necessarily the newest available PSGC snapshot. The available Bronze data contains `April_2024`, `Q2_2021`, `Q2_2024`, and `Q4_2023`. The project should document and confirm the choice of `Q2_2024` with its stakeholders before treating it as the formally approved reference snapshot.

The current transformation performs an upsert and intentionally does not delete existing target rows that are absent from the selected source snapshot. Therefore, changing `source_period` does not automatically make the target an exact replacement of the new snapshot.

### Current Row Counts

The current `dim_lgu` row count has not been confirmed. Query the live Gold table after a successful run rather than inferring its count from Bronze or Silver.

```sql
SELECT
    COUNT(*) AS total_records,
    COUNT(DISTINCT psgc_code) AS distinct_psgc_codes
FROM ahon_dev.gold.dim_lgu;
```

To inspect the geographic-level distribution:

```sql
SELECT
    geographic_level,
    COUNT(*) AS records
FROM ahon_dev.gold.dim_lgu
GROUP BY geographic_level
ORDER BY geographic_level;
```

To validate the selected source snapshot:

```sql
SELECT
    _api_period,
    COUNT(*) AS records,
    COUNT(DISTINCT psgc_code) AS distinct_psgc_codes
FROM ahon_dev.silver.psa_psgc_clean
WHERE _api_period = 'Q2_2024'
GROUP BY _api_period;
```

### Gold Columns

| Column | Type | Description | Notes |
|---|---|---|---|
| `psgc_code` | STRING | Standardized PSGC code identifying the geographic entity. | Business key. Stored as a string to preserve leading zeros. The transformation validates that it contains exactly 10 numeric digits. |
| `area_name` | STRING | Cleaned name of the geographic entity. | Sourced from Silver. PSA footnote markers and unwanted whitespace should already be handled in Silver. |
| `geographic_level` | STRING | Geographic classification of the record. | Sourced from Silver. Identifies the level of the geographic entity, such as `REG`, `PROV`, `CITY`, `MUN`, `SUBMUN`, `BGY`, or `SGU`, where present in the source. |
| `region_code` | STRING | PSGC code identifying the region associated with the record. | Derived from Silver's `region_psgc` hierarchy field. May be NULL when no applicable region code is available. |
| `province_code` | STRING | PSGC code identifying the province associated with the record. | Derived from Silver's `province_psgc` hierarchy field. May be NULL for records where a province is not applicable or resolved. |
| `municipality_code` | STRING | PSGC code identifying the city or municipality associated with the record. | Derived from Silver's `citymun_psgc` hierarchy field. May be NULL when no applicable city or municipality parent exists. |
| `barangay_code` | STRING | PSGC code identifying the barangay. | Set to the full `psgc_code` for records whose `geographic_level` is `BGY`; set to NULL for other geographic levels. |

**Schema note:** These are the seven columns defined by the current `dim_lgu` transformation. Confirm the actual data types and schema using `DESCRIBE TABLE ahon_dev.gold.dim_lgu`.

### Gold Transformations

The Gold `dim_lgu` transformation:

- Reads cleaned and standardized records from `ahon_dev.silver.psa_psgc_clean`.
- Filters Silver to the configured `SOURCE_PERIOD`, defaulting to `Q2_2024`.
- Uses Silver as the source of truth for geographic names and hierarchy codes.
- Projects the seven columns required by the initial dimension model.
- Maps `region_psgc` to `region_code`.
- Maps `province_psgc` to `province_code`.
- Maps `citymun_psgc` to `municipality_code`.
- Sets `barangay_code` to the full PSGC code only for `BGY` records.
- Preserves PSGC codes as strings to avoid losing leading zeros.
- Validates the source grain before creating or updating the dimension.
- Creates the Gold table when it does not exist and uses Delta `MERGE` to update existing entities or insert new ones.
- Does not automatically delete existing target records absent from the selected snapshot.
- Excludes Silver audit fields, ingestion provenance fields, population JSON, and `_api_period` from the initial Gold dimension because they are not part of its seven-column geographic reference model.

### Data Quality Rules

The Gold transformation checks that:

- The Silver source table exists.
- All required Silver source columns are present.
- The selected snapshot contains records.
- `psgc_code` is populated and contains exactly 10 numeric digits.
- `area_name` is populated and is not blank.
- `geographic_level` is populated.
- Each `psgc_code` occurs only once within the selected source snapshot.
- The resulting dimension contains no duplicate `psgc_code` values.

If duplicate PSGC codes are found in the selected Silver snapshot, the transformation stops and displays the duplicates rather than silently dropping records.

The current transformation does not independently validate every hierarchy relationship or confirm that every non-NULL parent code resolves to a corresponding dimension row. Those checks can be added as further Gold data quality rules.

### Relationship to Silver

| Aspect | Silver: `psa_psgc_clean` | Gold: `dim_lgu` |
|---|---|---|
| Purpose | Clean, standardize, and retain snapshot-level PSGC data. | Provide a simplified geographic reference for analytics. |
| Grain | One geographic record per business key and API snapshot. | One geographic entity per PSGC code in the selected reference snapshot. |
| Business key | `psgc_code + geographic_level + _api_period` | `psgc_code` |
| Snapshot handling | Retains multiple API periods. | Filters to one configured period, defaulting to `Q2_2024`. |
| Geographic names | Includes cleaned names and additional derived display fields. | Retains `area_name`. |
| Hierarchy | Includes standardized hierarchy codes and supporting fields. | Projects region, province, municipality, and barangay codes. |
| Audit and provenance | Retains source provenance and Silver processing fields. | Excludes those fields from the initial dimension. |
| Write strategy | Idempotent Silver processing keyed by its composite business key. | Delta upsert keyed by `psgc_code`; no automatic deletion of target-only rows. |

### Design Decisions and Rationale

**1. One selected snapshot rather than all snapshots**

Gold `dim_lgu` is designed as a single-reference geographic dimension. Filtering to one snapshot avoids mixing different versions of the geographic reference in the initial dimension. Historical snapshot analysis remains possible in Silver.

**2. PSGC code as the business key**

The dimension contains one selected snapshot, so `psgc_code` is sufficient as the intended business key. This depends on the source snapshot having unique PSGC codes; the transformation checks that assumption before proceeding.

**3. Use Silver's standardized hierarchy codes**

Gold reuses the hierarchy codes derived in Silver rather than reimplementing the hierarchy logic. This keeps geographic normalization and special-case handling centralized in Silver.

**4. Use `area_name` instead of introducing another name column**

The dimension retains the cleaned `area_name` field from Silver. This keeps the initial Gold schema aligned with the existing transformation and avoids introducing a redundant `geographic_name` alias.

**5. Store geographic codes as strings**

PSGC codes are identifiers, not quantities. Keeping them as strings preserves leading zeros and supports exact code-based joins.

**6. Make the source period configurable**

The `source_period` widget allows the project to select a different reference snapshot deliberately. `Q2_2024` is the current default; any change should be documented and validated.

**7. Use a non-destructive upsert**

The current Delta `MERGE` inserts new PSGC codes and updates existing codes. It does not delete target-only records. This avoids accidental data loss, but changing the source period requires an explicit decision about whether the dimension should be reconciled to an exact snapshot.

### Future Considerations

The following are not part of the current seven-column implementation:

- Add a surrogate key if required by the wider Gold dimensional model.
- Consider a type-2 slowly changing dimension if historical changes to geographic attributes must be tracked over time in Gold.
- Add hierarchy-integrity checks for region, province, municipality, and barangay relationships.
- Add an explicit snapshot replacement or reconciliation strategy if `source_period` changes.
- Confirm and document the formally approved PSGC reference snapshot with the project stakeholders.
- Consider adding `province_name` or `province_city` only if reporting requirements justify including those convenience fields in Gold.

The initial `dim_lgu` is therefore a **single-snapshot geographic reference dimension**, not a complete historical geographic dimension or a slowly changing dimension.