# 0001: CMCI locality mapping and incremental ingestion

- **Status:** Accepted
- **Date:** 2026-10-06

## Context

Project AHON integrates Cities and Municipalities Competitiveness Index (CMCI) data with the canonical Philippine Standard Geographic Code (PSGC) LGU reference.

The sources use different locality names and scopes. The PSGC reference contains 1,642 active cities and municipalities, while the live CMCI portal exposes 1,634 locality values. Duplicate locality names, abbreviations, historical names, spelling differences, and source-specific province suffixes prevent safe name-only joins.

CMCI ingestion also needs to preserve raw source responses, avoid repeating completed work, and support safe recovery when a request fails.

## Decision

Project AHON will use the following CMCI design:

1. Use `psgc_code` as the canonical LGU identifier and preserve `cmci_name` as the exact CMCI source identity.
2. Store reviewed relationships in `ahon.reference.cmci_lgu_map` and ingest only records where:

   ```text
   match_status = MATCHED
   reviewed = true
   is_active = true
   ```

3. Keep the eight PSGC municipalities without separate CMCI locality records explicitly unmatched. Do not assign values from former parent municipalities and do not replace missing values with zero.
4. Separate read-only source profiling from table-changing mapping updates:

   ```text
   cmci_source_profile.py
   → profiles and validates without writing

   cmci_mapping_update.py
   → applies reviewed mappings through a guarded Delta merge
   ```

5. Preserve CMCI data in two Bronze forms:

   ```text
   ahon.bronze.cmci_raw_indicator_batch_html
   → one row per source request batch

   ahon.bronze.cmci_raw_indicator
   → one row per batch, PSGC LGU, indicator, and year
   ```

6. Determine completed ingestion scope from `requested_psgc_codes` in batch Bronze. Ingest only approved PSGC codes not already represented there.
7. Use deterministic batch IDs, keep `test` as the default run mode, and require an explicit `full` run mode for full ingestion.
8. Stop after the first failed batch. Preserve successful batches and recalculate the remaining scope on the next run.
9. Merge Silver records using the logical key:

   ```text
   psgc_code + year
   ```

10. Keep the PSGC and CMCI denominators separate:

    ```text
    PSGC reference scope: 1,642 LGUs
    CMCI-supported scope: 1,634 LGUs
    Known PSGC-only source gaps: 8 LGUs
    ```

## Why

- PSGC codes provide stable identities when locality names differ across sources or change over time.
- Reviewed mapping prevents valid CMCI values from being assigned to the wrong LGU.
- Keeping known source gaps explicit distinguishes unavailable source data from zero performance or pipeline failure.
- Separating profiling and updates prevents routine quality checks from modifying reference data.
- Raw HTML supports audit, reprocessing, and parser recovery, while row-level Bronze supports incremental Silver processing.
- Pending-only ingestion avoids unnecessary CMCI requests and duplicate Bronze records.
- Deterministic, restartable batches preserve completed work and simplify recovery.
- A safe test default reduces the risk of triggering a full source ingestion unintentionally.

## Alternatives considered

- **Join PSGC and CMCI directly by locality name:** Not chosen because duplicate locality names, spelling differences, abbreviations, and historical names can produce incorrect assignments.
- **Use fuzzy matching during ingestion:** Not chosen because ingestion should consume reviewed mappings rather than make identity decisions while writing source data.
- **Map the eight PSGC-only LGUs to former parent municipalities:** Not chosen because historical CMCI values describe the full former municipality and are not geographically equivalent to the new LGUs.
- **Assign zero to LGUs without CMCI records:** Not chosen because zero would represent measured performance rather than unavailable source data.
- **Store only parsed indicator rows:** Not chosen because removing raw HTML would reduce auditability and make parser recovery harder.
- **Rebuild all batches on every run:** Not chosen because changes to the approved mapping set can shift batch membership and repeat already completed ingestion.
- **Continue after a failed batch:** Not chosen because later successes could change pending-batch boundaries on restart.
- **Use one script for profiling and mapping updates:** Not chosen because a profiler should remain read-only and safe to run regularly.

## Consequences

- `ahon.reference.cmci_lgu_map` is a required dependency for CMCI ingestion.
- The final approved mapping scope is 1,634 LGUs, with eight PSGC records remaining unmatched as known source gaps.
- Mapping methods and review status remain auditable in the reference table.
- `cmci_mapping_update.py` is a controlled update utility, not a routine ingestion task.
- `cmci_source_profile.py` may be run before and after ingestion to validate mapping, Bronze, and Silver coverage.
- Bronze setup must create both CMCI Bronze tables and include the standard provenance columns.
- `cmci_batch_ingest.py` must use batch Bronze, not the row-level indicator table alone, to determine which PSGC codes have already been processed.
- The committed ingestion default remains `test`; Databricks jobs must pass `--run-mode full` explicitly for full ingestion.
- Silver tables should ultimately contain 17,974 unique LGU-year rows each:

  ```text
  1,634 LGUs × 11 years = 17,974 rows
  ```

- Analyses requiring all 1,642 PSGC LGUs must start from `ahon.reference.lgu_master` and left-join CMCI data using `psgc_code`.
- A reproducible baseline mapping bootstrap remains separate from the reviewed mapping-update workflow and must not overwrite reviewed production mappings.

## Open

- Add and validate a reproducible baseline CMCI mapping bootstrap for fresh environment deployment and disaster recovery.
- Complete incremental Silver parsing for the newly ingested CMCI scope.
- Add automated quality gates for mapping uniqueness, Bronze provenance, and final Silver coverage.
