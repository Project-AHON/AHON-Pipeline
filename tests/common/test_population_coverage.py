"""Execute the component SELECT against missing and NULL census observations."""

import sqlite3
from pathlib import Path


def test_population_coverage_includes_lgus_with_no_measurements():
    sql = (Path(__file__).resolve().parents[2] / "src/sql/platinum/population_components.sql").read_text(encoding="utf-8")
    # The SELECT uses standard SQL; only SQLite's CREATE VIEW syntax differs.
    query = sql.split("CREATE OR REPLACE VIEW platinum.population_components AS", 1)[1]
    with sqlite3.connect(":memory:") as connection:
        connection.row_factory = sqlite3.Row
        connection.execute("ATTACH DATABASE ':memory:' AS gold")
        connection.executescript("""
          CREATE TABLE gold.dim_lgu (psgc_code TEXT, area_name TEXT, geographic_level TEXT,
            municipality_code TEXT, source_period TEXT);
          INSERT INTO gold.dim_lgu VALUES
            ('town_a', 'Measured', 'MUN', 'town_a', 'fixture'),
            ('town_b', 'Null measure', 'CITY', 'town_b', 'fixture'),
            ('town_c', 'No observation', 'MUN', 'town_c', 'fixture'),
            ('bgy_a', 'Barangay A', 'BGY', 'town_a', 'fixture'),
            ('bgy_b', 'Barangay B', 'BGY', 'town_b', 'fixture'),
            ('bgy_c', 'Barangay C', 'BGY', 'town_c', 'fixture');
          CREATE TABLE gold.fact_population (municipality_code TEXT, source_period TEXT,
            population_year INT, population INT);
          INSERT INTO gold.fact_population VALUES ('town_a', 'fixture', 2024, 10),
            ('town_b', 'fixture', 2024, NULL), ('town_a', 'fixture', 2020, 8);
        """)
        rows = {(row["psgc_code"], row["population_year"]): row for row in connection.execute(query)}
    assert len(rows) == 6
    assert rows["town_a", 2024]["complete_population_total"] == 10
    assert rows["town_b", 2024]["observed_barangay_count"] == 1
    assert rows["town_b", 2024]["measured_barangay_count"] == 0
    assert rows["town_b", 2024]["available_population_subtotal"] is None
    for year in (2020, 2024):
        missing = rows["town_c", year]
        assert missing["observed_barangay_count"] == missing["measured_barangay_count"] == 0
        assert missing["complete_population_total"] is None
        assert missing["available_population_subtotal"] is None
        assert missing["coverage_status"] == "PARTIAL_OR_MISSING"
