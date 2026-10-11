
-- CREATE SILVER TABLE

CREATE TABLE IF NOT EXISTS ahon_dev.silver.psa_psgc (

    -- Geographic identity
    psgc_code STRING NOT NULL,
    geographic_name STRING NOT NULL,
    geographic_level STRING NOT NULL,

    -- Geographic hierarchy
    region_code STRING,
    province_code STRING,
    municipality_code STRING,
    barangay_code STRING,

    -- PSGC attributes
    correspondence_code STRING,
    old_name STRING,
    status STRING,
    income_classification STRING,
    city_class STRING,
    urban_rural STRING,
    island_region STRING,

    -- Population information
    populations_json STRING,

    -- Source information
    source_version STRING,
    _api_period STRING NOT NULL,

    -- Provenance
    _source_name STRING,
    _source_ref STRING,
    _ingested_at TIMESTAMP,
    _batch_id STRING,
    _row_hash STRING
)
USING DELTA;

MERGE INTO ahon_dev.silver.psa_psgc AS target

USING (


    WITH cleaned AS (

        SELECT

            -- ------------------------------------------------
            -- Geographic identity
            -- ------------------------------------------------

            TRIM(psgc_code) AS psgc_code,

            TRIM(area_name) AS geographic_name,

            TRIM(geographic_level) AS geographic_level,


            CAST(region_code AS STRING) AS region_code,

            CAST(province_code AS STRING) AS province_code,

            CAST(municipality_code AS STRING) AS municipality_code,

            CAST(barangay_code AS STRING) AS barangay_code,


            -- ------------------------------------------------
            -- PSGC attributes
            -- ------------------------------------------------

            NULLIF(
                TRIM(correspondence_code),
                ''
            ) AS correspondence_code,

            NULLIF(
                TRIM(old_name),
                ''
            ) AS old_name,

            NULLIF(
                TRIM(status),
                ''
            ) AS status,

            NULLIF(
                TRIM(income_classification),
                ''
            ) AS income_classification,

            NULLIF(
                TRIM(city_class),
                ''
            ) AS city_class,

            NULLIF(
                TRIM(urban_rural),
                ''
            ) AS urban_rural,

            NULLIF(
                TRIM(island_region),
                ''
            ) AS island_region,


            -- ------------------------------------------------
            -- Population information
            -- ------------------------------------------------

            populations_json,


            -- ------------------------------------------------
            -- Source information
            -- ------------------------------------------------

            NULLIF(
                TRIM(version),
                ''
            ) AS source_version,

            TRIM(_api_period) AS _api_period,


            -- ------------------------------------------------
            -- Bronze provenance
            -- ------------------------------------------------

            _source_name,

            _source_ref,

            _ingested_at,

            _batch_id,

            _row_hash

        FROM ahon_dev.bronze.psa_psgc

        -- ----------------------------------------------------
        -- Required-field validation
        -- ----------------------------------------------------

        WHERE psgc_code IS NOT NULL
          AND TRIM(psgc_code) <> ''

          AND area_name IS NOT NULL
          AND TRIM(area_name) <> ''

          AND geographic_level IS NOT NULL
          AND TRIM(geographic_level) <> ''

          AND _api_period IS NOT NULL
          AND TRIM(_api_period) <> ''
    ),



    deduplicated AS (

        SELECT

            psgc_code,
            geographic_name,
            geographic_level,

            region_code,
            province_code,
            municipality_code,
            barangay_code,

            correspondence_code,
            old_name,
            status,
            income_classification,
            city_class,
            urban_rural,
            island_region,

            populations_json,

            source_version,
            _api_period,

            _source_name,
            _source_ref,
            _ingested_at,
            _batch_id,
            _row_hash

        FROM (

            SELECT

                *,

                ROW_NUMBER() OVER (
                    PARTITION BY
                        psgc_code,
                        geographic_level,
                        _api_period
                    ORDER BY
                        _ingested_at DESC
                ) AS row_num

            FROM cleaned

        ) ranked

        WHERE row_num = 1
    )


    SELECT

        psgc_code,
        geographic_name,
        geographic_level,

        region_code,
        province_code,
        municipality_code,
        barangay_code,

        correspondence_code,
        old_name,
        status,
        income_classification,
        city_class,
        urban_rural,
        island_region,

        populations_json,

        source_version,
        _api_period,

        _source_name,
        _source_ref,
        _ingested_at,
        _batch_id,
        _row_hash

    FROM deduplicated

) AS source

ON target.psgc_code = source.psgc_code
AND target.geographic_level = source.geographic_level
AND target._api_period = source._api_period


WHEN MATCHED
     AND NOT (
         target._row_hash <=> source._row_hash
     )

THEN UPDATE SET

    target.geographic_name = source.geographic_name,

    target.region_code = source.region_code,

    target.province_code = source.province_code,

    target.municipality_code = source.municipality_code,

    target.barangay_code = source.barangay_code,

    target.correspondence_code = source.correspondence_code,

    target.old_name = source.old_name,

    target.status = source.status,

    target.income_classification = source.income_classification,

    target.city_class = source.city_class,

    target.urban_rural = source.urban_rural,

    target.island_region = source.island_region,

    target.populations_json = source.populations_json,

    target.source_version = source.source_version,

    target._source_name = source._source_name,

    target._source_ref = source._source_ref,

    target._ingested_at = source._ingested_at,

    target._batch_id = source._batch_id,

    target._row_hash = source._row_hash


WHEN NOT MATCHED

THEN INSERT (

    psgc_code,
    geographic_name,
    geographic_level,

    region_code,
    province_code,
    municipality_code,
    barangay_code,

    correspondence_code,
    old_name,
    status,
    income_classification,
    city_class,
    urban_rural,
    island_region,

    populations_json,

    source_version,
    _api_period,

    _source_name,
    _source_ref,
    _ingested_at,
    _batch_id,
    _row_hash
)

VALUES (

    source.psgc_code,
    source.geographic_name,
    source.geographic_level,

    source.region_code,
    source.province_code,
    source.municipality_code,
    source.barangay_code,

    source.correspondence_code,
    source.old_name,
    source.status,
    source.income_classification,
    source.city_class,
    source.urban_rural,
    source.island_region,

    source.populations_json,

    source.source_version,
    source._api_period,

    source._source_name,
    source._source_ref,
    source._ingested_at,
    source._batch_id,
    source._row_hash
);
