# philvolcs_earthquake_observation

- **Table:** `bronze.philvolcs_earthquake_observation`
- **Source:** PHIVOLCS monthly earthquake listings. See the [PHIVOLCS earthquake data profile](../data-profile/phivolcs-earthquake-profile.md).
- **One row is:** one row of one landed monthly CSV file, as received
- **Loaded by:** `src/sql/datasets/philvolcs_earthquake/extract/philvolcs_earthquake_ingest.py` (website to one CSV per month under `/Volumes/<catalog>/reference/<source_volume>/philvolcs_earthquake_data/<batch_id>/`), then `src/sql/datasets/philvolcs_earthquake/observation_pipeline.py --stage bronze`
- **Key:** `_observation_id`
- **Row hash covers:** the eight source columns
- **Owner:** Jessy

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `Date-Time` | string | Date and time as listed | Wall-clock text, no time zone |
| `Latitude` | string | Latitude as listed | |
| `Longitude` | string | Longitude as listed | |
| `Depth` | string | Depth in kilometres as listed | |
| `Magnitude` | string | Magnitude as listed | Scale not specified |
| `Location` | string | Distance and bearing from the nearest town, with its province | |
| `Month` | string | Month of the listing page | Added by the extract |
| `Year` | string | Year of the listing page | Added by the extract |
| `_source_row` | long | Row number within the landed file | Starts at 1 |
| `_source_name` | string | The dataset the row came from | Always `philvolcs_earthquake_data` |
| `_source_ref` | string | Where the row was loaded from: the landed CSV path | One file per month |
| `_source_digest` | string | SHA-256 of the landed file | |
| `_ingested_at` | timestamp (UTC) | When the row was loaded into bronze | |
| `_batch_id` | string | The load run that wrote the row | |
| `_row_hash` | string | SHA-256 of the eight source columns | Null-aware JSON; not an event key |
| `_observation_id` | string | Identity of this observation | Hash of `_source_digest` and `_source_row`, so even identical duplicate rows are kept |

## Notes

- The load stops if a file's header differs from the eight columns, a row has the wrong number of fields, or the loaded row count differs from the count recorded at landing.
- Completed landed files carry SHA-256 and source-row counts; incomplete downloads are not readable by bronze.
