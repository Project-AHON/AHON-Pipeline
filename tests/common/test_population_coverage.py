"""Execute the canonical city/year query against absent and NULL measurements."""

import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_city_population_grain_excludes_barangays_and_preserves_missingness():
    query = (ROOT / "src/sql/gold/fact_population_candidate.sql").read_text(encoding="utf-8")
    view = (ROOT / "src/sql/platinum/population_components.sql").read_text(encoding="utf-8").split(
        "CREATE OR REPLACE VIEW platinum.population_components AS", 1
    )[1]
    with sqlite3.connect(":memory:") as connection:
        connection.row_factory = sqlite3.Row
        connection.execute("ATTACH DATABASE ':memory:' AS gold")
        connection.execute("ATTACH DATABASE ':memory:' AS silver")
        connection.executescript("""
          CREATE TABLE gold.dim_lgu (psgc_code TEXT, area_name TEXT, geographic_level TEXT,
            region_code TEXT, province_code TEXT, source_period TEXT);
          INSERT INTO gold.dim_lgu VALUES
            ('town_a', 'Measured', 'MUN', 'region', 'province', 'fixture'),
            ('town_b', 'Null measure', 'CITY', 'region', NULL, 'fixture'),
            ('town_c', 'Removed observation', 'MUN', 'region', 'province', 'fixture'),
            ('town_d', 'No observation', 'CITY', 'region', 'province', 'fixture'),
            ('bgy_a', 'Barangay', 'BGY', 'region', 'province', 'fixture'),
            ('province', 'Province total', 'PROV', 'region', 'province', 'fixture');
          CREATE TABLE silver.psgc_city_population_clean (psgc_code TEXT, _api_period TEXT,
            population_year INT, population INT, disposition TEXT,
            _source_ref TEXT, _ingested_at TEXT, _batch_id TEXT, _row_hash TEXT);
          INSERT INTO silver.psgc_city_population_clean VALUES
            ('town_a', 'fixture', 2024, 10, 'accepted', 'source', 'time', 'batch', 'hash_a'),
            ('town_b', 'fixture', 2024, NULL, 'missing_population', 'source', 'time', 'batch', 'hash_b'),
            ('town_c', 'fixture', 2024, 999, 'accepted', 'old_source', 'old_time', 'old_batch', 'old_hash_c'),
            ('town_a', 'fixture', 2020, 8, 'accepted', 'source', 'time', 'batch', 'hash_a');
          CREATE TABLE silver.psa_psgc_clean (psgc_code TEXT, _api_period TEXT, _row_hash TEXT, geographic_level TEXT);
          INSERT INTO silver.psa_psgc_clean VALUES ('town_a', 'fixture', 'hash_a', 'MUN'),
            ('town_b', 'fixture', 'hash_b', 'CITY'), ('town_c', 'fixture', 'new_hash_c', 'MUN'),
            ('town_d', 'fixture', 'hash_d', 'CITY');
        """)
        rows = {row["psgc_code"]: row for row in connection.execute(query, {"source_period": "fixture", "census_year": 2024})}
        assert set(rows) == {"town_a", "town_b", "town_c", "town_d"}
        assert rows["town_a"]["population"] == 10
        assert rows["town_a"]["coverage_status"] == "AVAILABLE"
        assert rows["town_b"]["population"] is None
        assert rows["town_b"]["disposition"] == "missing_population"
        assert rows["town_c"]["population"] is None
        assert rows["town_c"]["disposition"] == "missing_census_observation"
        assert rows["town_c"]["_source_ref"] is None
        assert rows["town_c"]["coverage_status"] == "MISSING"
        assert rows["town_d"]["population"] is None
        assert rows["town_d"]["disposition"] == "missing_census_observation"
        assert all(row["population_year"] == 2024 for row in rows.values())
        historical = list(connection.execute(query, {"source_period": "fixture", "census_year": 2020}))
        assert len(historical) == 4 and sum(row["population"] or 0 for row in historical) == 8
        connection.execute("CREATE TABLE gold.fact_population AS " + query, {"source_period": "fixture", "census_year": 2024})
        assert len(list(connection.execute(view))) == 4


def test_funding_components_excludes_provincial_reports_from_city_analysis():
    query = (ROOT / "src/sql/platinum/funding_components.sql").read_text(encoding="utf-8").split(
        "CREATE OR REPLACE VIEW platinum.funding_components AS", 1
    )[1]
    with sqlite3.connect(":memory:") as connection:
        connection.execute("ATTACH DATABASE ':memory:' AS gold")
        connection.executescript("""
          CREATE TABLE gold.fact_ldrrmf (psgc_code TEXT, source_period TEXT, lgu_type TEXT);
          INSERT INTO gold.fact_ldrrmf VALUES ('city', 'fixture', 'City'),
            ('town', 'fixture', 'Municipality'), ('province', 'fixture', 'Province');
          CREATE TABLE gold.dim_lgu (psgc_code TEXT, source_period TEXT, area_name TEXT, geographic_level TEXT);
          INSERT INTO gold.dim_lgu VALUES ('city', 'fixture', 'City', 'CITY'),
            ('town', 'fixture', 'Town', 'MUN'), ('province', 'fixture', 'Province', 'PROV');
        """)
        rows = connection.execute(query).fetchall()
    assert {row[0] for row in rows} == {'city', 'town'}
