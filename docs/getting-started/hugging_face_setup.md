# Hugging Face dataset repo setup

## Overview

This project uses a Hugging Face dataset repository as the source-of-truth for raw source files that need to be downloaded into Databricks and ingested into Delta tables. The repository is configured for dataset access, not model hosting, because the primary use case is file distribution and versioned data retrieval.

The dataset repo used by the ingestion workflow is:

- `Jess-Christine/Project-AHON`
- repo type: `dataset`

This matches the pattern in the ingestion script:

```python
local_dir = snapshot_download(
    repo_id="Jess-Christine/Project-AHON",
    repo_type="dataset",
    allow_patterns="data/*",
    local_dir="/Volumes/ahon/reference/source/huggingface"
)
```

## Why this decision was chosen

We selected a Hugging Face dataset repo for this workflow because it gives us a simple, reliable way to:

- store raw data files in a versioned, shareable repository
- download the exact dataset folders needed using `snapshot_download()`
- keep the project organized around `data/` content rather than model artifacts
- make the data available to Databricks without custom infrastructure for every file transfer
- support a repeatable ingestion pipeline for CSV, JSON, Parquet, and Excel files

A standard Git repository would work for code, but it is not the best fit for data-first distribution. A dataset repo is more suitable because it is designed for file-based dataset management and integrates cleanly with the Hugging Face Hub API.

## Recommended repo structure

Use a folder layout similar to this:

```text
Project-AHON/
├── README.md
├── data/
│   ├── folder_1/
│   │   ├── file_1.csv
│   │   └── file_2.json
│   ├── folder_2/
│   │   └── file_3.parquet
│   └── ...
└── .gitattributes
```

The key pattern is that all raw source files live under `data/`, and the ingestion script downloads only that subtree via `allow_patterns="data/*"`.

## Setup steps

### 1. Create the Hugging Face dataset repo

On the Hugging Face Hub:

- create a new repository
- set it as a dataset repository, not a model repository
- choose a descriptive, stable repo name such as `Project-AHON`
- use the full namespace format when referencing it in code, for example `Jess-Christine/Project-AHON`

### 2. Authenticate locally

Install the Hub client and log in:

```bash
pip install huggingface_hub
huggingface-cli login
```

If needed, create a read/write token in Hugging Face settings and use it for authentication.

### 3. Prepare the data directory

Add the source files under `data/`:

```bash
git clone https://huggingface.co/datasets/Jess-Christine/Project-AHON
cd Project-AHON
mkdir -p data
# add raw files into data/
```

If files are large, enable Git LFS for datasets and large binaries:

```bash
git lfs install
git lfs track "data/**"
git add .gitattributes
```

### 4. Push the repo

```bash
git add .
git commit -m "Initial dataset upload"
git push
```

### 5. Download and ingest in Databricks

The existing script uses the Hub SDK to download the relevant files into a Databricks volume:

```python
from huggingface_hub import snapshot_download

local_dir = snapshot_download(
    repo_id="Jess-Christine/Project-AHON",
    repo_type="dataset",
    allow_patterns="data/*",
    local_dir="/Volumes/ahon/reference/source/huggingface"
)
```

Then the pipeline walks the downloaded directory and ingests supported files into Delta tables.

## Why this is a good fit for the pipeline?

This approach fits the project because the ingestion job is data-centric and file-oriented:

- raw files are stored centrally and versioned
- the Hub API allows deterministic downloads into Databricks
- the repo can be reused for re-runs without rebuilding data storage
- file structure is easy to traverse and normalize into table names
- the setup keeps data and code separation clean and maintainable

## Operational guidance

- Keep raw source files under `data/` and avoid mixing code assets with dataset content.
- Use clear folder names so the auto-ingest script can generate stable Delta table names.
- Validate file types before pushing to ensure the ingestion step is predictable.
- Prefer repository versioning over one-off manual uploads when data changes over time.

## Summary

Using a Hugging Face dataset repository is the right decision for this project because it creates a clean, versioned, and accessible data landing zone that Databricks can consume directly. It is a better fit than a generic code repo or ad hoc local storage because the workflow depends on reliable dataset retrieval, folder-based organization, and repeatable ingestion.
