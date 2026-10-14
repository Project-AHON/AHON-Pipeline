# 0010: Reconcile executable source and analytical contracts

- Status: Proposed for team review; implemented locally on `feature/pipeline-reconciliation`
- Date: 2026-10-14
- Baseline: `4d82f6b5010c715ed14eca490c5415692e8fc34e`

The executable path follows `resources/jobs/ahon_job.yml`. This decision updates the implementation portions of decisions 0005, 0006 CMCI Silver, 0007 DAB, 0008 CMCI Gold, and 0009 CMCI quality; their historical rationale remains available in Git. It does not establish a scientific risk model.

`gold.fact_population` retains PR #91's barangay × census-year key. PXWeb national, regional, province and city/municipality observations instead use `gold.fact_population_total`; they cannot be loaded into the barangay fact. Population is a count of residents, not measured earthquake exposure. Missing barangay measurements remain NULL, with a disposition.

The canonical LGU dimension remains `gold.dim_lgu`, keyed by a ten-digit PSGC string, using one selected API snapshot. Hierarchy columns use standardized PSGC codes. Snapshot changes require a reviewed migration or a fresh catalog; absent entities are not deleted automatically. The separate 2026 publication workbook is not silently substituted for the 2024 API snapshot.

CMCI uses the existing 35 indicator definitions and strict HTML contract, with a compact long-form Silver path to eliminate hardcoded all-country coverage gates and competing writers. Its logical key is PSGC × year × indicator. A CSV must supply an exact source name, selected PSGC snapshot, reviewer and evidence; source observations carry this mapping. The five old wide Silver pillar tables and older CMCI setup/quality scripts are historical and are not deployed entry points. Existing incompatible schemas are never automatically replaced. Use a fresh development catalog; production migration needs separate review.

Platinum CMCI reports coverage, not raw pillar sums or official scores. No new weights, thresholds, normalized population scores, risk levels, preparedness scores or risk-minus-preparedness formulas are introduced.

Earthquake Bronze stores raw text observations identified by file digest and source row ordinal. Silver retains every observation, invalid rows and exact textual locality candidates. Candidate codes never become LGU exposure assignments. Event-level Gold remains blocked by the absence of a publisher event identity and an approved duplicate/correction policy. Changes in reported depth or magnitude cannot be assumed to be either a new event or a corrected event.

Completed source batches are immutable and carry file digests and row counts. A completion marker exists to prevent a Bronze task reading an interrupted download. A new source correction requires a new batch ID. Silver and Gold use checked Delta MERGEs without absence-based deletion or automatic schema evolution. DQ results reuse the monitoring schema; invalid observations remain in source/row dispositions or existing reject paths.

The existing three job files become an executable pipeline, standalone setup, and synthetic quality verification. A `dev` target is added because repeatable bundle execution in a personal workspace is needed to verify asset paths, task parameters and dependencies; its state path is isolated from other development deployments. It implements the personal-workspace workflow described in decision 0007 rather than introducing a shared development environment. Production has no default target. All schedules remain absent.

Standard PSGC, PXWeb, BLGF and CMCI Bronze identities include the immutable delivery batch ID, file digest and raw row hash. Same-batch replay preserves identities; a new delivery of identical content creates a new observation, permitting source-value reversions. Earlier file-only identity tables are guarded and require a fresh catalog or reviewed migration. Earthquake and OSM preserve their explicit file/ordinal observation grains separately.

Following the user instruction to avoid hardcoded settings, operational values are mandatory bundle/script inputs with no committed defaults. Verification uses ignored local overrides. Reviewed CMCI definitions are a CSV source asset; schemas, units and deterministic parsing remain stable contracts.
