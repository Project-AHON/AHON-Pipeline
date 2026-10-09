CREATE TABLE IF NOT EXISTS ahon.bronze.philvolcs_earthquake_data (
    `Date-Time` STRING,
    Latitude DOUBLE,
    Longitude DOUBLE,
    Depth DOUBLE,
    Magnitude DOUBLE,
    Location STRING,
    Month STRING,
    Year INT,
    _source_name STRING,
    _source_ref STRING,
    _ingested_at TIMESTAMP,
    _batch_id STRING,
    _row_hash STRING
);

MERGE INTO ahon.bronze.philvolcs_earthquake_data AS target
USING (
    SELECT
        raw.`Date-Time` AS `Date-Time`,
        try_cast(raw.Latitude AS DOUBLE) AS Latitude,
        try_cast(raw.Longitude AS DOUBLE) AS Longitude,
        try_cast(raw.Depth AS DOUBLE) AS Depth,
        try_cast(raw.Magnitude AS DOUBLE) AS Magnitude,
        nullif(trim(raw.Location), '') AS Location,
        nullif(trim(raw.Month), '') AS Month,
        try_cast(raw.Year AS INT) AS Year,
        'philvolcs_earthquake_data' AS _source_name,
        '/Volumes/ahon/reference/source/philvolcs_earthquake/phivolcs_earthquake_all_years.csv' AS _source_ref,
        current_timestamp() AS _ingested_at,
        uuid() AS _batch_id,
        sha2(concat_ws(
            '||',
            raw.`Date-Time`,
            raw.Latitude,
            raw.Longitude,
            raw.Depth,
            raw.Magnitude,
            raw.Location,
            raw.Month,
            raw.Year
        ), 256) AS _row_hash
    FROM read_files(
        '/Volumes/ahon/reference/source/philvolcs_earthquake/phivolcs_earthquake_all_years.csv',
        format => 'csv',
        header => true,
        inferColumnTypes => true
    ) AS raw
) AS source
ON target._row_hash = source._row_hash
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;