-- ============================================================================ --
-- bronze_critical_infrastructure.sql
-- ============================================================================ --
-- Bronze layer for the critical-infrastructure pipeline.
--
-- The Python script extract_critical_infrastructure.py reads OSM shapefiles
-- from /Volumes/earthquake/criticalinfra/openstreetmap and writes the raw
-- extracted rows to earthquake.criticalinfra.infrastructure_boundaries.
--
-- This bronze SQL wraps that raw table with:
--   • CREATE TABLE IF NOT EXISTS with explicit schema and PRIMARY KEY
--   • MERGE for idempotent loading (no data loss on re-runs)
--   • Provenance metadata (_source_name, _source_ref, _ingested_at,
--     _batch_id, _row_hash)
--   • Data-quality flags for downstream filtering
--   • Retains ALL raw columns for traceability — no type coercion or dedup
--
-- Source table : earthquake.criticalinfra.infrastructure_boundaries  (Python extract)
-- Bronze table : earthquake.criticalinfra.bronze_infrastructure
--
-- Run after the Python extraction pipeline has completed.
-- ============================================================================ --

CREATE TABLE IF NOT EXISTS ahon.bronze.critical_infrastructures (
    id                BIGINT   NOT NULL PRIMARY KEY,
    amenity           STRING,
    name              STRING,
    source_file       STRING,
    geometry_wkt      STRING,
    geometry_geojson  STRING,
    centroid_lat      DOUBLE,
    centroid_lng      DOUBLE,
    _source_loaded_at TIMESTAMP,
    _source_name      STRING,
    _source_ref       STRING,
    _ingested_at      TIMESTAMP,
    _batch_id         STRING,
    _row_hash         STRING,
    _dq_valid_lat     BOOLEAN,
    _dq_valid_lng     BOOLEAN,
    _dq_valid_amenity BOOLEAN,
    _dq_valid_name    BOOLEAN
);

MERGE INTO ahon.bronze.critical_infrastructures AS target
USING (
    WITH batch AS (
        SELECT uuid() AS _batch_id
    )
    SELECT
        xxhash64(
            COALESCE(CAST(amenity      AS STRING), ''),
            COALESCE(CAST(name         AS STRING), ''),
            COALESCE(CAST(source_file  AS STRING), ''),
            COALESCE(CAST(centroid_lat AS STRING), ''),
            COALESCE(CAST(centroid_lng AS STRING), '')
        )                                                          AS id,
        amenity,
        name,
        source_file,
        geometry_wkt,
        geometry_geojson,
        centroid_lat,
        centroid_lng,
        loaded_at                                                    AS _source_loaded_at,
        'openstreetmap_infrastructure'                               AS _source_name,
        CONCAT('/Volumes//criticalinfra/openstreetmap/', source_file) AS _source_ref,
        current_timestamp()                                          AS _ingested_at,
        b._batch_id                                                  AS _batch_id,
        sha2(concat_ws('|',
            COALESCE(CAST(amenity          AS STRING), ''),
            COALESCE(CAST(name             AS STRING), ''),
            COALESCE(CAST(source_file      AS STRING), ''),
            COALESCE(CAST(geometry_wkt     AS STRING), ''),
            COALESCE(CAST(geometry_geojson AS STRING), ''),
            COALESCE(CAST(centroid_lat     AS STRING), ''),
            COALESCE(CAST(centroid_lng     AS STRING), ''),
            COALESCE(CAST(loaded_at        AS STRING), '')
        ), 256)                                                    AS _row_hash,
        CASE WHEN centroid_lat IS NOT NULL
              AND centroid_lat BETWEEN -90  AND  90  THEN true ELSE false
        END                                                         AS _dq_valid_lat,
        CASE WHEN centroid_lng IS NOT NULL
              AND centroid_lng BETWEEN -180 AND  180 THEN true ELSE false
        END                                                         AS _dq_valid_lng,
        CASE WHEN amenity  IS NOT NULL AND TRIM(amenity)  != '' THEN true ELSE false
        END                                                         AS _dq_valid_amenity,
        CASE WHEN name     IS NOT NULL AND TRIM(name)     != '' THEN true ELSE false
        END                                                         AS _dq_valid_name
    FROM earthquake.criticalinfra.infrastructure_boundaries
    CROSS JOIN batch b
    WHERE COALESCE(amenity, name, centroid_lat, centroid_lng) IS NOT NULL
) AS source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;


-- Optimise for downstream filtered scans on amenity and source_file     
OPTIMIZE earthquake.criticalinfra.bronze_infrastructure
  ZORDER BY (amenity, source_file);