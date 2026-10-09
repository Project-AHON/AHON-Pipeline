from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# Configuration

BRONZE_TABLE = "ahon_dev.bronze.psa_psgc"
MASTER_TABLE = "ahon_dev.reference.lgu_master"

SOURCE_PERIOD = "Q2_2024"
SOURCE_NAME = "PSA PSGC API"
SOURCE_REFERENCE = "https://classification.psa.gov.ph/psgc"


# 1. Read Q2_2024 PSGC snapshot

psgc_df = (
    spark.table(BRONZE_TABLE)
    .filter(F.col("_api_period") == SOURCE_PERIOD)
)


# 2. Get Province reference

province_df = (
    psgc_df
    .filter(F.col("geographic_level") == "Prov")
    .select(
        F.substring("psgc_code", 1, 5).alias("province_code"),
        F.col("area_name").alias("province_name"),
    )
    .dropDuplicates(["province_code"])
)

# 3. Get Cities and Municipalities

lgu_df = (
    psgc_df
    .filter(
        F.col("geographic_level").isin("City", "Mun")
    )
    .withColumn(
        "province_code",
        F.when(
            F.substring("psgc_code", 1, 5) == "19999",
            F.lit("999"),
        ).otherwise(
            F.substring("psgc_code", 1, 5)
        ),
    )
)


# 4. Join Province and extract 2024 population

population_schema = ArrayType(
    StructType([
        StructField("id", LongType(), True),
        StructField("population", StringType(), True),
        StructField("psgc", StringType(), True),
        StructField("year", LongType(), True),
    ])
)

population_df = (
    lgu_df
    .withColumn(
        "population_records",
        F.from_json(
            F.col("populations_json"),
            population_schema,
        ),
    )
    .withColumn(
        "population_record",
        F.explode_outer("population_records"),
    )
    .filter(
        F.col("population_record.year") == 2024
    )
    .select(
        "psgc_code",
        F.regexp_replace(
            F.col("population_record.population"),
            ",",
            "",
        ).cast("BIGINT").alias("population_2024"),
    )
)

lgu_master_df = (
    lgu_df
    .join(
        province_df,
        on="province_code",
        how="left",
    )
    .join(
        population_df,
        on="psgc_code",
        how="left",
    )
)

# 5. Select final LGU Master columns


final_lgu_master_df = (
    lgu_master_df
    .select(
        "psgc_code",
        "correspondence_code",
        F.col("area_name").alias("lgu_name"),
        "province_code",
        "province_name",
        "geographic_level",
        "old_name",
        "city_class",
        "income_classification",
        "population_2024",
        F.lit(SOURCE_NAME).alias("source_name"),
        F.lit(SOURCE_PERIOD).alias("source_period"),
        F.lit(SOURCE_REFERENCE).alias("source_reference"),
        F.lit(True).alias("is_active"),
        F.current_timestamp().alias("created_timestamp"),
        F.current_timestamp().alias("updated_timestamp"),
    )
)


# 6. Create Reference Schema and Table

spark.sql("""
CREATE SCHEMA IF NOT EXISTS ahon_dev.reference
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {MASTER_TABLE} (
    psgc_code STRING NOT NULL,
    correspondence_code STRING,
    lgu_name STRING NOT NULL,
    province_code STRING,
    province_name STRING,
    geographic_level STRING NOT NULL,
    old_name STRING,
    city_class STRING,
    income_classification STRING,
    population_2024 BIGINT,
    source_name STRING NOT NULL,
    source_period STRING NOT NULL,
    source_reference STRING NOT NULL,
    is_active BOOLEAN NOT NULL,
    created_timestamp TIMESTAMP NOT NULL,
    updated_timestamp TIMESTAMP NOT NULL
)
USING DELTA
""")


# 7. Load LGU Master

(
    final_lgu_master_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(MASTER_TABLE)
)

print(f"Created {MASTER_TABLE} from PSA PSGC {SOURCE_PERIOD}.")
