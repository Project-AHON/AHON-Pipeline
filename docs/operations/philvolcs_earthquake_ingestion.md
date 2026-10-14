# Earthquake run guide and review evidence

Part of [#82](https://github.com/Project-AHON/AHON-Pipeline/issues/82). Reviewed
12 October 2026 (Asia/Manila), against actual main
`e1e8b883a1780b3150e6c7cd880530f4d7e42e66`. The original
`feature/baseline-pipeline-reliability` checkout was preserved; this focused work
uses an isolated `feature/82-earthquake-quality-and-candidates` clone. Its prior
unmerged bundle/raw-text changes were inspected but not copied wholesale.

The broader goals remain LGU earthquake risk, preparedness shortfall,
high-risk/low-preparedness ranking and indicator priorities. Preserve **normalized
population exposure multiplied by normalized earthquake activity**. This change
provides earthquake observations only. Locality candidates do not measure impact;
no activity definition, Platinum score, infrastructure or income input is added.

## Audit and dependencies

The refreshed open set was #54, #60, #66, #69, #72, #73, #75, #81. All eight
file lists/diffs, discussion/review comments and inline review threads were read.
The comments are evidence, not authorization to implement unrelated work.

| PR | Findings and smallest earthquake response |
| --- | --- |
| #54 OSM | Source/table, overwrite, lineage/hash and CRS reviews; no validated administrative boundary workflow in main. Do not use facility centroids. |
| #60 CMCI Bronze | Separate source, code and reference conventions; leave its mapping and measurements untouched. ADR 0005 number is occupied. |
| #66 CMCI Silver | Separate parser; CI changes overlap #60/#69; its ADR numbering also conflicts with main 0006. Do not copy the framework. |
| #69 DAB | Proposed prod-only bundle and placeholder jobs. Main has no actual bundle/job definitions despite README's diagram. Use the one Python task below; coordinate integration with its owner. ADR 0007 is reserved. |
| #72 Population Bronze | Conditional matched updates/source validation are useful patterns; no population implementation here. |
| #73 Population Silver | Review objects to target deletion and append-only rejects; use retained observations and no delete clause. |
| #75 Population DQ | Earthquake extractor overlap is only removal of an unnecessary f-string prefix. Leave extractor unchanged; dictionary index may need conflict resolution. Reporting-only checks do not stop writes. |
| #81 LDRRMF | Borrow distinct locality/province candidate map -> Silver -> Gold flow. Avoid its table replacements, aliases, overrides, confidence numbers and LGU/year fact grain. ADR 0008 remains Proposed/reserved. |

Historical #48 earthquake, #61 reference and #62 PSGC are merged; #59 population
is closed without merge. #48 review explicitly requested one ingestion batch ID
and distinguished Bronze hashes from Silver signatures; current main SQL still
uses per-row UUIDs and unconditional matched updates. The runner fixes future
behavior but cannot reconstruct lost first-ingestion lineage. Proposed ADR 0006
is not approved by this change. ADR 0009 records the minimal schema proposal.

README, CONTRIBUTING, naming, all main decision statuses, earthquake/PSGC
dictionaries/profiling, dim_lgu, both reference loaders, monitoring, tests, job
paths and model documents were inspected. `data-model.md` is empty in main and
its history only introduces the empty page. Issue #15's checked grain boxes and
#81's mention of Schema v1 do not supply an earthquake field/key contract.
Issue #82 has no schema attachment/comments. The available review artifact says
the earlier supplied image is a DQ slide, not an ERD. No missing ERD was claimed
as inspected. Existing local spatial outputs have no established approved
boundary/snapshot contract and are not used.

## Parameters and run order

Use `src/sql/datasets/philvolcs_earthquake/run_earthquake.py` as **one Spark Python
file task** on configured development Spark compute (Databricks Runtime 15.4 LTS
or later with Unity Catalog and Volume access). It executes the SQL files in the
same session; the map needs no standalone driver job or persisted table. Do not
configure these SQL files as Python tasks or call a notebook placeholder runnable.

1. Land an explicit newly retrieved/changed CSV, validate its eight headers and
   row widths, nonempty input and pinned SHA-256, and preserve its bytes. Optional
   `--land-from` validates the entire file then publishes a new immutable copy;
   an existing destination with different bytes is refused. Do not point at the
   old scraper's mutable `phivolcs_earthquake_all_years.csv` output directly.
2. Validate pinned inference schema and existing Bronze schema; build/reconcile
   a candidate and run Bronze stop/flag/report rules **before** insert-only MERGE.
   Verify persisted landing hashes/typed values after the write.
3. Read the configured PSGC snapshot and build the temporary distinct-locality
   candidate map. Missing reference blocks candidates but does not drop events.
4. Build all scoped Silver observations, check schema/lineage/counts/no fan-out,
   then MERGE new/changed observations and rerun Silver DQ. Invalids remain flagged.
5. Before eventual Gold materialization, separately validate/build `dim_lgu` on
   the same snapshot. This runner never creates or rewrites the dimension.
6. `--stage gold` currently reports candidate dispositions, verifies their count,
   then raises BLOCKED before any Gold target write. Gold verification/MERGE is
   not runnable until the decisions in the fact dictionary are approved.

| CLI parameter | Contract |
| --- | --- |
| `--catalog` | Required safe identifier; no hardcoded production target |
| `--source-path` | Required immutable CSV under `/Volumes/<catalog>/reference/`; one explicit batch scope |
| `--source-sha256` | Required lowercase SHA-256 of exact file bytes, verified before every target write |
| `--land-from` | Optional validated source file to copy once; no scrape, no destination replacement |
| `--batch-id` | Required one ID supplied per ingestion run; reuse on retries; unchanged rows retain first IDs |
| `--hash-schema` | Required local/workspace JSON `{source_column: Spark simpleString type}` of REVIEWED legacy inference types, all eight columns |
| `--source-period` | Configured PSGC snapshot; default Q2_2024 matches existing dim_lgu baseline, which is not formally approved |
| `--expected-periods` | Required comma-separated YYYY-MM scope; missing periods reported explicitly |
| `--stage` | `bronze`, `silver` (default), `gold` (diagnostic and blocked) |
| `--reprocess` | Deliberate historical Bronze-to-Silver reprocessing after reviewed reference/schema decisions; otherwise only explicit landing hashes |
| `--check-only` | Candidate validation without table writes; existing first-ingestion provenance used when available |

The runner adds a durable `<source-path>.manifest.json` with file hash, counts,
header, inferred-type/hash contract and periods after validation. Keep immutable
raw files/manifests and structured job outputs together. Raw rows are streamed for
structure checks; only bounded DQ aggregates are collected, never growing event
data. Candidate map work occurs once per distinct locality/province in scope.
Incremental MERGEs change only new/different rows; no DELETE, full-table overwrite,
OPTIMIZE or ZORDER runs. Historical reprocessing deliberately scans history;
restore original raw files before claiming historical hash recomputation.

Supply task parameters as a list, for example:

```text
--catalog <dev_catalog>
--source-path /Volumes/<dev_catalog>/reference/source/philvolcs_earthquake/<immutable_file>.csv
--source-sha256 <verified_sha256>
--batch-id <run_id>
--source-period Q2_2024
--expected-periods <YYYY-MM[,YYYY-MM]>
--hash-schema <reviewed_workspace_json_path>
--stage silver
--check-only
```

The placeholders are inputs, not runnable defaults. Obtain the hash-schema JSON
by inspecting `read_files` on the original immutable main landing with identical
CSV options and recording all eight `dataType.simpleString()` values; compare
existing hashes before adopting it. Do not fill in numeric types from guesswork
or re-infer a smaller routine batch and silently accept changed identity.

For #69 integration, add a `spark_python_task` pointing at this .py workspace
file and pass the list above as `parameters`. Use the existing team's configured
compute; set `max_concurrent_runs: 1`. Required settings include a reviewed dev
target, Spark compute, Volume access, source/schema inputs, and log retention.
This guide deliberately adds no competing databricks.yml, compute IDs, credentials,
production schedule, job deployment or infrastructure. Databricks documents
[Python script task configuration](https://docs.databricks.com/aws/en/jobs/tasks/python-script)
and [bundle task types](https://docs.databricks.com/aws/en/dev-tools/bundles/job-task-types).

The legacy extractor's main block still requests eight years, rewrites combined
files, filters some source banners and disables TLS verification. Routine operation
must retrieve only requested months/changed files, using its monthly function or
an approved upstream landing process. Its fallback is only for the current month.
Do not repeatedly run its all-history default. Extraction hardening is a separate
follow-up; this change validates and pins the supplied landing instead of claiming
complete successful source-month acquisition.

## Explicit migration boundaries

Bronze types and hash identity are unchanged. If existing hashes used different
inference/session settings, restore those original files/settings or approve a
raw-text/hash migration separately. Never pad PSGC codes or accept silent hash
drift. Null/delimiter ambiguity and raw-text-to-inferred collapse stop the batch;
invalid event observations remain in the immutable file and existing tables.

Silver changes from event-signature grain/NOT NULL constraints to observation
grain/nullable event fields, adds flags/processing and candidate columns, and
removes the event primary key assumption. Automatic schema migration is refused.
The safe path is a **fresh dev catalog** with the same table names, or a reviewed
migration creating a separate nullable Silver target from Bronze, reconciling
every hash/lineage/signature, then approving the intended cutover. Existing Silver
may already have lost observations and overwritten lineage, so merely dropping
constraints/adding columns is insufficient. No migration/cutover was executed.

PSGC Q2_2024 API data used by dim_lgu is not the Q2 2026 workbook/master loaded
by `load_lgu_master.py`. A snapshot change requires a reviewed reference decision
and crosswalk; no boundary crosswalk was verified. `dim_lgu` has no snapshot field
and retains old target-only rows, so eventual Gold needs full code/level/name/
hierarchy equivalence with the selected snapshot, not merely code existence.

## DQ and row accounting

`bronze_dq.sql` and `silver_dq.sql` reuse SQL aggregate/result patterns without
`dq_ruleset`/`dq_results` dependencies. Each rule outputs stable `check_name`,
attribute, table/scope, action, failed_rows, denominator, expected_result, status
and as_of. PASS/WARN/FAIL indicate measured rules; missing SLA is NOT_RUN and
unapproved assignment/missing candidate reference is BLOCKED. The runner raises
any non-PASS stop rule before the respective target MERGE. Flag/report rules keep
observations. An empty expected event batch, changed schema, missing lineage,
ambiguous hash, duplicate merge identity or join fan-out is not silently tolerated.

| Attribute | Layer evidence |
| --- | --- |
| Consistency | Raw schema and Month/Year vs parsed wall time; unchanged provenance; snapshot, candidate method/status and null assignment agreement; no fan-out |
| Accuracy | Raw-text vs inferred distinct counts; recomputed landing hashes and typed reconciliation; physical bounds are plausibility, not source accuracy |
| Completeness | Nonempty/header/required-field checks; all scoped Bronze hashes reach Silver; requested periods and candidate coverage use explicit denominators |
| Auditability | Five provenance fields, SHA-256 format/contract, one new batch ID, immutable raw manifest, _processed_at; old overwritten lineage is not recoverable automatically |
| Validity | Timestamp parsing, finite coordinates within [-90,90]/[-180,180], finite nonnegative depth/magnitude, integer period, 10-digit PSGC reference existence/level for candidates |
| Uniqueness | Repeated raw rows accounted separately; distinct observation hashes, map keys, signature duplicates reported, idempotent insert/update conditions |
| Timeliness | As-of, maximum source/event/ingestion times and converted UTC lag reported; missing/late periods require documented cadence/SLA, no old-backfill age failure |

Accounting equations: `raw_rows = distinct_observations + repeated_raw_rows`;
scoped distinct Bronze hashes = scoped Silver hashes before/after enrichment;
every scoped Silver observation = exactly one Gold diagnostic disposition.
Actual event inclusion/exclusion/correction totals are BLOCKED until policy approval.
Mapping denominator = all scoped observations, including invalid/unparsed rows;
no expectation of an event in every LGU/month.

## Read-only issue lookup

The query below identifies broken rules by source reference and row hash. Run it
in the **same Spark session** after the runner has created `earthquake_scope`
and `earthquake_silver_check`. These are temporary views: a separate SQL warehouse
or a later job session cannot read them. For an investigation, use an interactive
dev session running the task; the query does not create or update any tables.
It reports issues and mapping blockers; it does not replace the aggregate stop
checks enforced by the runner.

```sql
-- Read-only rule finder, scoped by runner; no stored per-rule framework required.
WITH paired AS (
  SELECT s.*, b.Month AS source_month, b.Year AS source_year,
    b._raw_period_invalid AS raw_period_invalid,
    count(*) OVER (PARTITION BY s.id) AS signature_observations
  FROM earthquake_silver_check s LEFT JOIN earthquake_scope b ON s._row_hash = b._row_hash
), issues AS (
  SELECT _source_ref, _row_hash,
    filter(array(
      CASE WHEN event_time IS NULL THEN 'event_time_parse' END,
      CASE WHEN latitude IS NULL OR isnan(latitude) OR NOT latitude BETWEEN -90 AND 90
        THEN 'latitude_finite_range' END,
      CASE WHEN longitude IS NULL OR isnan(longitude) OR NOT longitude BETWEEN -180 AND 180
        THEN 'longitude_finite_range' END,
      CASE WHEN depth IS NULL OR isnan(depth) OR depth < 0 OR depth = double('Infinity')
        THEN 'depth_finite_nonnegative' END,
      CASE WHEN magnitude IS NULL OR isnan(magnitude) OR magnitude < 0
        OR magnitude = double('Infinity') THEN 'magnitude_finite_nonnegative' END,
      CASE WHEN source_year IS NULL OR source_month IS NULL OR event_time IS NULL
        OR coalesce(raw_period_invalid, false)
        OR source_year <> year(event_time)
        OR lower(trim(source_month)) <> lower(date_format(event_time, 'MMMM'))
        THEN 'source_period_consistency' END,
      CASE WHEN event_time IS NOT NULL AND signature_observations > 1
        THEN 'event_signature_duplicate_review' END,
      CASE WHEN psgc_candidate_status <> 'single_candidate' THEN 'locality_candidate_gap' END,
      CASE WHEN psgc_match_status = 'blocked_meaning' THEN 'geographic_assignment_blocked' END
    ), x -> x IS NOT NULL) AS broken_rules
  FROM paired
)
SELECT _source_ref, _row_hash, explode(broken_rules) AS rule_name FROM issues;
```

## Validation evidence

Local: 27 focused pytest cases passed, including SHA/structure failures, immutable
landing and its second unchanged run, normal and same-name/province matches,
malformed/nested/blank descriptions, independent cities, unmatched candidates,
timestamps, NaN/infinity, physical bounds beyond historical maxima, schema mismatch
and stop rules. Scoped Ruff passed. SQL parsed in Databricks dialect using sqlglot.

A read-only Databricks development warehouse fixture query executed production
Bronze/Silver/Gold candidate SQL with 17 raw rows / 16 distinct observations.
Observed: one repeated raw row, four invalid physical/time observations, five
flagged observations including one period mismatch, three single locality candidates,
zero analytical PSGC assignments, and all stop rules PASS. Gold diagnostic counts
were 7 eligible signatures, 2 duplicate observations, 2 possible corrections and
5 invalid/non-event observations, totaling 16. These are **synthetic fixture counts**,
not live earthquake or reference coverage. Fake reference codes test mechanics.

At the time of validation, a local generator combined actual candidate SQL with
tiny fixtures into one read-only SQL statement. The generator and SQL fixtures
have since been removed from this PR; the retained Python tests do not reproduce
these warehouse checks. The results below record that earlier fixture run.
Seven scenarios completed in the development warehouse, returning 321 bounded
summary rows. Normal stop checks passed. Injected missing lineage failed the
provenance rule for 16 observations; a wrong snapshot failed for 16; a duplicated
map key failed both map uniqueness and observation fan-out (one extra row); missing
flags failed for five observations. A missing reference returned BLOCKED candidate
reporting while retaining all 16 observations and five flags. Original fractional
Year-text evidence flagged the additional observation (six flags) despite the
typed Year remaining an integer. The helper stop gate
is unit-tested to raise on FAIL, NOT_RUN or BLOCKED for stop actions. No
pipeline source/target table, production data or reference snapshot was modified.

Not run: full Spark Python file task/read_files integration, real-source pinned
inference/hash audit, production/history counts, actual target schema migration,
Delta MERGE rerun/atomic-failure integration, approved-boundary tests, Gold target
MERGE/verification, bundle deployment. These remain required dev acceptance checks
once inputs/schema decisions are supplied; passing fixture SQL is not equivalent
to an end-to-end ingestion run. Repository-wide Ruff reports five existing errors:
the extractor's unnecessary f-string (already changed in #75), an unused PSGC Delta
import, and import order in PSGC extract/Silver and dim_lgu. Those unrelated files
remain untouched. Existing workflow YAML lint and diff whitespace checks passed.
The task is not fully materialized
Gold, and issue #82 should not be marked done based on this diff alone.
