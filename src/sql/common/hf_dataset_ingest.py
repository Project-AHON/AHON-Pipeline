# %pip install huggingface_hub openpyxl
import os
from huggingface_hub import snapshot_download
import pandas as pd

# 1. Bulk download the entire 'data' directory to Databricks local storage
local_dir = snapshot_download(
    repo_id="Jess-Christine/Project-AHON",
    repo_type="dataset",
    allow_patterns="data/*",
    local_dir="/Volumes/ahon/reference/source/huggingface"

)

# 2. Function to auto-ingest tabular files into Delta tables
def mass_ingest_to_delta(base_path, target_schema="default"):
    for root, dirs, files in os.walk(base_path):
        for file in files:
            file_path = os.path.join(root, file)
            
            # Skip hidden files or READMEs
            if file.startswith(".") or file.endswith(".md"):
                continue
                
            # Construct table name from folder + filename (e.g., ldrrmf_data_file)
            folder_name = os.path.basename(root)
            clean_filename = os.path.splitext(file)[0].replace("-", "_").replace(" ", "_")
            table_name = f"{target_schema}.{folder_name}_{clean_filename}"

            print(f"Processing: {file_path} -> Table: {table_name}")

            try:
                # Read based on extension
                if file.endswith(".csv"):
                    df = spark.read.option("header", "true").option("inferSchema", "true").csv(f"file:{file_path}")
                elif file.endswith(".json") or file.endswith(".geojson"):
                    df = spark.read.option("multiline", "true").json(f"file:{file_path}")
                elif file.endswith(".parquet"):
                    df = spark.read.parquet(f"file:{file_path}")
                elif file.endswith(".xlsx"):
                    pdf = pd.read_excel(file_path, header=None)
                    pdf.columns = [f"col_{i}" for i in range(len(pdf.columns))]
                    pdf = pdf.astype(str)
                    df = spark.createDataFrame(pdf)
                else:
                    print(f"Skipping unsupported file type: {file}")
                    continue

                # Write to Delta
                df.write.format("delta").mode("overwrite").saveAsTable(table_name)
                print(f"Successfully created table: {table_name}")

            except Exception as e:
                print(f"Failed to process {file}: {e}")

# Run mass ingestion
mass_ingest_to_delta("/Volumes/ahon/reference/source/huggingface/data")