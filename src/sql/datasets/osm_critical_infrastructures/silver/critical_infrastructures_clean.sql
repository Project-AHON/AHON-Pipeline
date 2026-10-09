-- ============================================================================ --
-- silver_critical_infrastructure.sql
-- ============================================================================ --
-- Silver layer for the critical-infrastructure pipeline.
--
-- Builds one silver table from the bronze layer:
--
--   1. silver_infrastructure
--      Cleansed, deduplicated, typed infrastructure catalogue.
--      Standardises amenity values, validates coordinate ranges, extracts
--      geometry type from WKT, and drops rows that fail core DQ checks.
--      Uses CREATE TABLE IF NOT EXISTS + MERGE for idempotent loading.
--      Carries provenance metadata (_source_name, _source_ref, _ingested_at,
--      _batch_id, _row_hash) through from the bronze layer.
--
-- Bronze table  : earthquake.criticalinfra.bronze_infrastructure
-- Silver table  : earthquake.criticalinfra.silver_infrastructure
--
-- Run after bronze_critical_infrastructure.sql has completed.
-- ============================================================================ --

-- --------------------------------------------------------------------------- --
-- Silver table — Cleansed infrastructure catalogue                              --
-- --------------------------------------------------------------------------- --
CREATE TABLE IF NOT EXISTS ahon.silver.critical_infrastructures (
    id               BIGINT   NOT NULL PRIMARY KEY,
    amenity          STRING   NOT NULL,
    name             STRING,
    source_file      STRING   NOT NULL,
    geometry_type    STRING,
    centroid_lat     DOUBLE   NOT NULL,
    centroid_lng     DOUBLE   NOT NULL,
    geometry_wkt     STRING,
    geometry_geojson STRING,
    _source_name     STRING,
    _source_ref      STRING,
    _ingested_at     TIMESTAMP,
    _batch_id        STRING,
    _row_hash        STRING
);

MERGE INTO ahon.silver.critical_infrastructures AS target
USING (
    WITH parsed AS (
        SELECT
            bronze._source_name,
            bronze._source_ref,
            bronze._ingested_at,
            bronze._batch_id,
            bronze._row_hash,
            LOWER(TRIM(COALESCE(bronze.amenity, '')))                 AS amenity,
            nullif(TRIM(bronze.name), '')                             AS name,
            LOWER(TRIM(bronze.source_file))                           AS source_file,
            CAST(bronze.centroid_lat AS DOUBLE)                       AS centroid_lat,
            CAST(bronze.centroid_lng AS DOUBLE)                       AS centroid_lng,
            bronze.geometry_wkt,
            bronze.geometry_geojson,
            REGEXP_EXTRACT(bronze.geometry_wkt, '^([A-Z]+)', 1)       AS geometry_type
        FROM earthquake.criticalinfra.bronze_infrastructure AS bronze
        WHERE bronze._dq_valid_lat
          AND bronze._dq_valid_lng
          AND bronze._dq_valid_amenity
    ),
    invalid AS (
        SELECT
            *,
            CASE
                WHEN centroid_lat IS NULL OR centroid_lat < -90 OR centroid_lat > 90 THEN 1
                WHEN centroid_lng IS NULL OR centroid_lng < -180 OR centroid_lng > 180 THEN 1
                WHEN amenity IS NULL OR amenity = '' THEN 1
                WHEN source_file IS NULL OR source_file = '' THEN 1
                ELSE 0
            END AS is_invalid
        FROM parsed
    ),
    valid AS (
        SELECT
            *
        FROM invalid
        WHERE is_invalid = 0
    )
    SELECT
        xxhash64(amenity, name, source_file, centroid_lat, centroid_lng) AS id,
        amenity,
        name,
        source_file,
        geometry_type,
        centroid_lat,
        centroid_lng,
        geometry_wkt,
        geometry_geojson,
        _source_name,
        _source_ref,
        _ingested_at,
        _batch_id,
        _row_hash
    FROM valid
) AS source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- --------------------------------------------------------------------------- --
-- Optimise silver table for common query patterns                              --
-- --------------------------------------------------------------------------- --
OPTIMIZE ahon.silver.critical_infrastructures
  ZORDER BY (amenity, source_file);