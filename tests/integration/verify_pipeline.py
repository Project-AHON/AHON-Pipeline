"""Run actual transformations on isolated synthetic fixtures, with replay and corrections."""

import base64
import hashlib
import importlib.util
import json
import runpy
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src/sql/common"))
sys.path.insert(0, str(ROOT / "src/sql/datasets/cmci"))
from quality import check, upsert
from runtime import batch_path, parser, publish_batch


def module(name, relative):
    sys.path.insert(0, str((ROOT / relative).parent))
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


def invoke(relative, args, remaining=()):
    previous = sys.argv
    path = ROOT / relative
    sys.path.insert(0, str(path.parent))
    remaining = list(remaining)
    if relative == "src/sql/common/run_python.py":
        remaining += ["--source-period", "Q2_2024"]
        if "src/sql/gold/fact_population.py" in remaining:
            remaining += ["--census-year", "2024"]
    if relative == "src/sql/common/run_sql.py" and any("blgf_ldrrmf_annual_lgu/silver/" in item for item in remaining):
        remaining += ["--min-year", "2023", "--max-year", "2024"]
    sys.argv = [str(path), "--catalog", args.catalog, "--source-volume", args.source_volume,
                "--batch-id", args.batch_id, *remaining]
    try:
        runpy.run_path(str(path), run_name="__main__")
    finally:
        sys.argv = previous


def verify_definition_guard(spark, scope, cmci):
    before = set(map(tuple, spark.table('gold.fact_cmci_indicator').collect()))
    changed_definition = SimpleNamespace(**vars(scope))
    changed_definition.metric_version = scope.metric_version + '_changed_definition'
    try:
        cmci.gold(spark, changed_definition)
    except ValueError as error:
        assert 'cmci_definition_requires_review' in str(error)
    else:
        raise AssertionError('Changing the CMCI definition must require review')
    assert before == set(map(tuple, spark.table('gold.fact_cmci_indicator').collect()))


def verify(spark, args):
    from pyspark.sql import functions as F

    if not args.catalog.startswith("ahon_integration_"):
        raise ValueError("Fixture job requires an isolated ahon_integration_* catalog")
    if args.existing_definition_check:
        spark.sql(f"USE CATALOG {args.catalog}")
        cmci = module("cmci_definition_verify", "src/sql/datasets/cmci/pipeline.py")
        cmci.configure_indicators(ROOT / "resources/cmci_indicators.csv")
        definitions = spark.table('gold.dim_cmci_indicator').select('metric_version').distinct().collect()
        periods = spark.table('gold.dim_lgu').select('source_period').distinct().collect()
        assert len(definitions) == len(periods) == 1
        scope = SimpleNamespace(**vars(args), source_period=periods[0].source_period,
                                metric_version=definitions[0].metric_version)
        cmci.gold(spark, scope)
        verify_definition_guard(spark, scope, cmci)
        print(json.dumps({'status': 'PASS', 'catalog': args.catalog,
                          'checks': ['unchanged CMCI definition replay', 'changed definition blocks and preserves facts']}))
        return
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {args.catalog}")
    spark.sql(f"USE CATALOG {args.catalog}")
    invoke("src/sql/common/run_sql.py", args, ["--file", "src/sql/00_setup/00_catalog_schema_setup.sql"])
    names = [
        ("0100000000", "Region fixture", "REG", None),
        ("0100100000", "Province fixture", "PROV", None),
        ("100101000", "Town fixture 1/", "MUN", None),
        ("0100101001", "Barangay fixture", "BGY", json.dumps([{"population": "1,200", "year": "2024", "psgc": "0100101001"}])),
        ("0100101002", "Missing fixture", "BGY", json.dumps([{"population": "-", "year": "2024", "psgc": "0100101002"}])),
        ("0100109999", "Unknown fixture", "UNKNOWN", None),
    ]

    def reference_frame(rows):
        frame = spark.createDataFrame(rows, "psgc_code string, area_name string, geographic_level string, populations_json string")
        frame = frame.withColumn("_row_hash", F.sha2(F.to_json(F.struct(*frame.columns), options={"ignoreNullFields": "false"}), 256))
        for column in ["region_code", "province_code", "municipality_code", "barangay_code", "city_class"]:
            frame = frame.withColumn(column, F.lit(None).cast("string"))
        return (frame.withColumn("_observation_id", F.col("_row_hash"))
                .withColumn("_api_period", F.lit("Q2_2024")).withColumn("_source_name", F.lit("synthetic_psgc"))
                .withColumn("_source_ref", F.lit("fixture/Q2_2024.jsonl")).withColumn("_batch_id", F.lit(args.batch_id))
                .withColumn("_ingested_at", F.current_timestamp()))

    upsert(spark, reference_frame(names), f"{args.catalog}.bronze.psa_psgc", ["_observation_id"])
    for relative in ["src/sql/datasets/psa_psgc/silver/psa_psgc_clean.py", "src/sql/gold/dim_lgu.py"]:
        invoke("src/sql/common/run_python.py", args, ["--file", relative])
    assert spark.table("gold.dim_lgu").count() == 5
    assert spark.table("silver.psa_psgc_reject").count() == 1
    pop = module("barangay_population_verify", "src/sql/datasets/psa_psgc/silver/clean_barangay_population.py")
    scope = SimpleNamespace(**vars(args), source_period="Q2_2024", metric_version="fixture_v1")
    pop.transform(spark, scope)
    invoke("src/sql/common/run_python.py", args, ["--file", "src/sql/gold/fact_population.py"])
    before = set(map(tuple, spark.table("gold.fact_population").collect()))
    for _ in range(2):
        pop.transform(spark, scope)
        invoke("src/sql/common/run_python.py", args, ["--file", "src/sql/gold/fact_population.py"])
        assert before == set(map(tuple, spark.table("gold.fact_population").collect()))
    assert spark.table("gold.fact_population").where("population IS NULL AND disposition = 'missing_population'").count() == 1
    invoke("src/sql/common/run_sql.py", args, ["--file", "src/sql/platinum/population_components.sql"])
    assert spark.table("platinum.population_components").where("available_population_subtotal = 1200 AND complete_population_total IS NULL").count() == 1

    # Source correction updates the stable barangay/census key and keeps two Bronze observations.
    corrected = reference_frame([(*names[3][:3], json.dumps([{"population": "1300", "year": "2024", "psgc": "0100101001"}]))])
    upsert(spark, corrected, f"{args.catalog}.bronze.psa_psgc", ["_observation_id"])
    invoke("src/sql/common/run_python.py", args, ["--file", "src/sql/datasets/psa_psgc/silver/psa_psgc_clean.py"])
    pop.transform(spark, scope)
    invoke("src/sql/common/run_python.py", args, ["--file", "src/sql/gold/fact_population.py"])
    assert spark.table("gold.fact_population").where("population = 1300").count() == 1
    assert spark.table("gold.fact_population").count() == 2

    # Financial warnings survive into facts. Zero budgets return NULL; overspending is not clipped.
    fiscal_rows = [("Region fixture", "Province fixture", "Town fixture", "Municipality", 0., 0., 0., 0., 0., 0., "fixture/FY2023-LDRRMF-by-LGU.xlsx"),
                   ("Region fixture", "Province fixture", "Town fixture", "Municipality", 7., 14., 3., 6., 10., 20., "fixture/FY2024-LDRRMF-by-LGU.xlsx")]
    columns = "region string, province string, lgu_name string, lgu_type string, `70pct_ldrrmf_budget_appropriation` double, `70pct_ldrrmf_expenditures` double, `30pct_quick_response_fund_budget_appropriation` double, `30pct_quick_response_fund_expenditures` double, total_budget_appropriation double, total_expenditures double, _source_ref string"
    fiscal = spark.createDataFrame(fiscal_rows, columns)
    fiscal = fiscal.withColumn("_row_hash", F.sha2(F.to_json(F.struct(*fiscal.columns)), 256))
    fiscal = fiscal.withColumn("_observation_id", F.col("_row_hash")).withColumn("_source_name", F.lit("synthetic_blgf")).withColumn("_batch_id", F.lit(args.batch_id)).withColumn("_ingested_at", F.current_timestamp())
    upsert(spark, fiscal, f"{args.catalog}.bronze.blgf_ldrrmf_annual_lgu", ["_observation_id"])
    # The packaged overrides describe real LGUs; a fixture uses a separate empty reviewed override file.
    mapping = module("funding_mapping_verify", "src/sql/datasets/blgf_ldrrmf_annual_lgu/silver/build_blgf_ldrrmf_lgu_map.py")
    result = mapping.build_map(
        fiscal.select("region", "province", "lgu_name", "lgu_type").distinct().toPandas(),
        __import__('pandas').DataFrame([("0100101000", "Town fixture", "Mun", "Province fixture", None)], columns=["psgc_code", "name", "level", "province_name", "old_name"]), {})
    result["first_year"], result["last_year"] = 2023, 2024
    schema = "region string, province string, lgu_name string, lgu_type string, psgc_code string, psgc_name string, match_method string, match_status string, candidate_count int, first_year int, last_year int"
    map_frame = spark.createDataFrame(result.astype(object).where(result.notna(), None).values.tolist(), schema)
    map_frame = map_frame.withColumn("source_period", F.lit("Q2_2024"))
    upsert(spark, map_frame, f"{args.catalog}.reference.blgf_ldrrmf_lgu_map", ["region", "province", "lgu_name", "lgu_type"])
    for _ in range(2):
        invoke("src/sql/common/run_sql.py", args, ["--file", "src/sql/datasets/blgf_ldrrmf_annual_lgu/silver/clean_blgf_ldrrmf_annual_lgu.sql"])
        invoke("src/sql/common/run_sql.py", args, ["--file", "src/sql/gold/build_fact_ldrrmf.sql"])
    assert spark.table("gold.fact_ldrrmf").where("total_appropriation = 0 AND utilization_rate_total IS NULL").count() == 1
    assert spark.table("gold.fact_ldrrmf").where("utilization_rate_total = 2 AND has_dq_issues").count() == 1

    # Real CMCI parser, typed Silver, facts, coverage and valid → invalid → corrected replay.
    cmci = module("cmci_verify", "src/sql/datasets/cmci/pipeline.py")
    cmci.configure_indicators(ROOT / "resources/cmci_indicators.csv")
    earthquake = module("earthquake_verify", "src/sql/datasets/philvolcs_earthquake/observation_pipeline.py")
    with tempfile.TemporaryDirectory() as temp:
        temp = Path(temp)
        for state, value in [("valid", "4"), ("invalid", "oops"), ("corrected", "4")]:
            current = SimpleNamespace(**vars(scope))
            current.batch_id = args.batch_id + "_cmci_" + state
            mapping_file = temp / 'mapping.csv'
            mapping_file.write_text("psgc_code,cmci_name,source_period,reviewed,reviewer,evidence\n0100101000,Fixture locality,Q2_2024,true,synthetic-test,fixture\n")
            current.mapping_file = str(mapping_file)
            mappings = cmci.reviewed_mapping(mapping_file, 'Q2_2024')
            html = '<title>Data Portal Results</title>' + ''.join(
                f'<h2>{label}</h2><table><tr><th>Province / LGU</th><th>2024</th></tr><tr><td>Fixture locality</td><td>{value if index == 0 else "-"}</td></tr></table>'
                for index, label in enumerate(cmci.APPROVED_INDICATORS))
            file = temp / f'{state}.json'
            file.write_text(json.dumps({'mappings':mappings, 'years':['2024'], 'html':html,
                'response_bytes_base64':base64.b64encode(html.encode()).decode(), 'response_sha256':hashlib.sha256(html.encode()).hexdigest(), 'source_period':'Q2_2024'}))
            publish_batch(batch_path(current, 'cmci_data_portal'), [(file,35)])
            for _ in range(2):
                cmci.bronze(spark,current)
                cmci.silver(spark,current)
                cmci.gold(spark,current)
                cmci.platinum(spark,current)
            assert spark.table('gold.fact_cmci_indicator').count() == 35
            assert spark.table('gold.fact_cmci_indicator').where('raw_score IS NOT NULL').count() == (0 if state == 'invalid' else 1)
            if state == 'corrected':
                assert spark.table('gold.fact_cmci_indicator').where('raw_score = 4').count() == 1
            assert spark.table('platinum.cmci_pillar_coverage').count() == 5

        verify_definition_guard(spark, current, cmci)

        current = SimpleNamespace(**vars(scope))
        current.batch_id = args.batch_id + '_earthquake'
        file = temp / 'phivolcs_earthquake_2024_01.csv'
        file.write_text('Date-Time,Latitude,Longitude,Depth,Magnitude,Location,Month,Year\n01 January 2024 - 01:00 PM,10,120,5,3,1 km N of Town fixture (Province fixture),January,2024\n01 January 2024 - 01:00 PM,10,120,5,3,1 km N of Town fixture (Province fixture),January,2024\nbad,91,120,-1,3,Unparsed,January,2024\n')
        publish_batch(batch_path(current,'philvolcs_earthquake_data'), [(file,3)])
        for _ in range(2):
            earthquake.bronze(spark,current)
            earthquake.silver(spark,current)
        assert spark.table('bronze.philvolcs_earthquake_observation').count() == 3
        assert spark.table('silver.philvolcs_earthquake_observation_clean').where('psgc_code IS NOT NULL').count() == 0
        assert spark.table('silver.philvolcs_earthquake_observation_clean').where("candidate_status='single_candidate'").count() == 2
        assert spark.table('silver.philvolcs_earthquake_reject').count() == 1

    # Cardinality and failure propagation: duplicated dimension never reaches a dependent fact write.
    before = set(map(tuple, spark.table('gold.fact_population').collect()))
    spark.table('gold.dim_lgu').unionByName(spark.table('gold.dim_lgu')).createOrReplaceTempView('duplicate_dimension_fixture')
    try:
        check(spark, args.catalog, args.batch_id, 'fixture_duplicate_dimension', 'duplicate_dimension_fixture',
              spark.table('duplicate_dimension_fixture').groupBy('psgc_code').count().where('count > 1').count())
    except ValueError:
        pass
    else:
        raise AssertionError('Blocking checks must raise')
    assert before == set(map(tuple, spark.table('gold.fact_population').collect()))
    check(spark, args.catalog, args.batch_id, 'pipeline_integration', 'synthetic_fixtures', 0)
    print(json.dumps({'status':'PASS', 'catalog':args.catalog, 'checks':[
        'PSGC leading zeros, snapshot, unknown dispositions', 'barangay grain and source correction',
        'NULL population and partial aggregation', 'funding ratio edges and SQL replay',
        'CMCI response validation, valid-invalid-reverted corrections, missingness and coverage',
        'changed CMCI definition blocks without altering facts',
        'earthquake duplicate observations, invalid retention and withheld assignment',
        'blocking failure propagation and preserved target']}))


if __name__ == '__main__':
    from pyspark.sql import SparkSession

    arguments = parser(__doc__)
    arguments.add_argument('--existing-definition-check', action='store_true',
                           help='Check only the definition guard against completed synthetic fixtures')
    verify(SparkSession.builder.getOrCreate(), arguments.parse_args())
