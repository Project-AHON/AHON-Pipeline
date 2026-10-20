# PSGC reference observations

The PSA classification API is read for explicitly configured snapshots and expected row counts. The selected snapshot is an input, independent of the census year. Extraction requires an approved HTTPS endpoint and token secret scope/key; neither credentials nor snapshot defaults are committed.

## Bronze

`bronze.psa_psgc` retains observations from completed JSONL deliveries under `/Volumes/<catalog>/reference/<source_volume>/psa_psgc/<batch_id>/`. Source codes are strings, never numeric warehouse keys. API fields `code,reg,prv,mun,bgy` become `psgc_code,region_code,province_code,municipality_code,barangay_code`. The remaining source fields are `area_name,correspondence_code,geographic_level,old_name,city_class,income_classification,urban_rural,island_region,status,version,_api_period`; all are nullable strings. `populations_json` preserves the source population array as JSON text.

Provenance includes `_source_name,_source_ref,_source_digest,_row_hash,_observation_id,_batch_id,_ingested_at`. The raw hash is null-aware JSON of all aliased source values and populations. The observation ID includes delivery batch, file digest and raw hash. Same-batch replay preserves first lineage; corrected deliveries require new batch IDs. Identical rows in one delivery are counted in DQ before collapsing by observation ID. File checksums and configured row counts gate reads.

## Silver

`silver.psa_psgc_clean` has one `(psgc_code,geographic_level,_api_period)` record. Valid codes become ten-digit strings, retaining leading zeros. Levels are uppercase `REG,PROV,CITY,MUN,SUBMUN,BGY,SGU`. Names have PSA footnote markers and surplus whitespace removed.

Code-based lookups in the same snapshot derive `region_psgc,province_psgc,citymun_psgc`, `province_name` and the display label `province_city`. Existing documented independent-city and special-area handling is retained. Display labels are not geographic join keys. `source_version` retains the publisher's version, separately from `_api_period`. Source fields/provenance remain available and `_processed_at` records accepted changes.

Invalid codes are retained in `silver.psa_psgc_code_reject` and block the delivery. Unknown or missing levels are retained in `silver.psa_psgc_reject` with a DQ warning. Conflicting tied business-key observations block instead of selecting an arbitrary winner; superseded observations remain in Bronze and are counted. Reference uniqueness and join cardinality are checked before writing.

The job validates the requested completed delivery, allowing a later correction without erasing previous rejected observations. Silver merges business keys without absence-based deletion or schema evolution. An older first-ingested observation cannot replace a newer accepted row. Replay leaves unchanged processing timestamps. Incompatible existing schemas require a fresh catalog or reviewed migration.

## Downstream and verification

`gold.dim_lgu` is the existing canonical dimension, gated by the selected snapshot. The former `reference.lgu_master` loader is retired. City/municipality population arrays feed `silver.psgc_city_population_clean` and the [canonical population fact](fact_population.md). Barangay observations remain in the source layers; they are not canonical analytical rows. Other datasets use contextual reference lookups with explicit periods.

See the [dimension dictionary](dim_lgu.md), [source inventory](../architecture/source-to-target.md), [runbook](../operations/runbook.md) and [development verification evidence](validation.md). Live PSA authentication, extraction, Bronze and Silver passed on 2026-10-15. The four configured snapshots landed 175,079 Bronze observations and produced 175,067 Silver rows, with one warning rule and no blocking checks. These are geographic reference observations across snapshots, not population totals or exposure measures.
