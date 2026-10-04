# 0004: Land source files through Hugging Face

- **Status:** Accepted
- **Date:** 2026-10-04

## Context

Several source datasets reach the team as files, for example the BLGF Excel files, and later shapefiles, JSON or CSV. The pipeline needs one agreed way to get those files from a team member's computer into the Databricks workspace, into the `source` volume (raw files exactly as received, see 0001). Without one, each dataset would be loaded by hand in a different way.

## Decision

- Source files are uploaded to one Hugging Face dataset repository, in one folder per dataset under `data/`. Both structured files (Excel, CSV, Parquet) and unstructured files (JSON, GeoJSON, shapefiles) go there.
- A Python extract script per dataset downloads that dataset's folder from Hugging Face (`huggingface_hub.snapshot_download`) and copies the files, unchanged, into the `source` volume, in one folder named after the dataset.
- Bronze code reads from the volume only. It never reads from Hugging Face directly.
- The extract script lives with the dataset's code, in `src/.../DATASET_NAME/extract/`.

## Why

- It needs no cloud storage account, access key or external location, so any team member can add files with a login and the upload page.
- A Hugging Face dataset repository is a version-controlled Git repository, so uploaded files have a history.
- It follows the same pattern as the external-volume approach (files in a volume, then loaded), so bronze code does not change if the landing place changes later.
- Because bronze reads only from the volume, the raw files stay in `source` exactly as received, as the naming standard requires.

## Alternatives considered

- **External volume on cloud storage:** the same end result, but it needs a storage location and credentials set up by a workspace admin. Kept as the fallback if the team later wants raw files in its own storage.
- **Uploading files by hand into the volume:** fastest for one file, but not repeatable and leaves no record of where the file came from.
- **Committing the files to this GitHub repository:** rejected. Data does not belong in the code repository (see `.gitignore`).
- **Downloading directly from each publisher's website in code:** not always possible, as some portals block automated downloads or change their pages, and it leaves no copy of what was loaded.

## Consequences

- Each file dataset gets an extract script and a folder under `data/` in the Hugging Face repository.
- The `source` volume holds a copy of every landed file. Hugging Face is the hand-off place and the volume is the record the pipeline uses.
- The naming standard's volume-folder rule (one folder per dataset, named after it) applies to these landings.
- The extract scripts need `huggingface_hub` available on the compute that runs them.
- Downsides accepted: another service to depend on, and files exist in two places.

## Open

- Who owns the Hugging Face repository. It is currently under one member's account, and it should move to an organization or shared account so it does not depend on one person.
- Whether the repository is public or private. Private needs an access token stored as a secret, never in code. Public data must be allowed to be shared publicly, so licences need checking per dataset.
- Size and file-type limits on Hugging Face for the largest files, such as shapefiles.
- The final catalog, schema and volume names for `source`, and whether the extract step runs as a scheduled job.
