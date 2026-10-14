-- Runner validates landing and target schemas first. Legacy hash identity stays.
CREATE OR REPLACE TEMP VIEW earthquake_bronze_observations AS
SELECT `Date-Time`, try_cast(Latitude AS DOUBLE) AS Latitude,
  try_cast(Longitude AS DOUBLE) AS Longitude, try_cast(Depth AS DOUBLE) AS Depth,
  try_cast(Magnitude AS DOUBLE) AS Magnitude,
  nullif(trim(Location), '') AS Location, nullif(trim(Month), '') AS Month,
  try_cast(Year AS INT) AS Year,
  'philvolcs_earthquake_data' AS _source_name, c.source_path AS _source_ref,
  c.as_of AS _ingested_at, c.batch_id AS _batch_id,
  sha2(concat_ws('||', `Date-Time`, Latitude, Longitude, Depth, Magnitude,
                Location, Month, Year), 256) AS _row_hash,
  to_json(named_struct('Date-Time', `Date-Time`, 'Latitude', Latitude,
    'Longitude', Longitude, 'Depth', Depth, 'Magnitude', Magnitude,
    'Location', Location, 'Month', Month, 'Year', Year),
    map('ignoreNullFields', 'false')) AS source_signature
FROM earthquake_raw CROSS JOIN earthquake_config c;
-- COMMAND ----------
-- Identical typed source signatures only; hash ambiguity is a pre-write stop rule.
-- Repeated raw rows remain in immutable landing and counts, not extra Bronze rows.
CREATE OR REPLACE TEMP VIEW earthquake_bronze_candidate AS
SELECT `Date-Time`, Latitude, Longitude, Depth, Magnitude, Location, Month, Year,
  _source_name, _source_ref, _ingested_at, _batch_id, _row_hash,
  count(*) AS raw_occurrences
FROM earthquake_bronze_observations
GROUP BY ALL;
