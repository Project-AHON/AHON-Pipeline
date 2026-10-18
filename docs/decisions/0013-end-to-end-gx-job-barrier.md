# 0013: Complete the portable pipeline with a GX completion barrier

- Status: Implemented; runtime evidence is recorded in the operations review
- Date: 2026-10-19
- Extends: 0011 (shared GX metrics); does not activate the draft native-Spark gates in 0012

PR #97 exported a complete workflow tied to a personal workspace release. Current main already supplies the portable source-to-Platinum pipeline and inline GX checks. Retain those contracts and resolve the PR conflicts in favor of current executable behavior; the three CMCI changes in #97 contain formatting changes only relative to the merge base.

Keep a single GX 1.24.0 dependency in the Python project and every task environment. Python normalizes `great-expectations` and `great_expectations` to the same distribution, so pinning both 1.23.1 and 1.24.0 makes project installation unsatisfiable.

Add one `quality_checks` task after every terminal branch, using the existing core environment. It requires valid same-delivery GX records and nonempty, uniquely keyed terminal outputs. Persist a GX failure before raising; advisory source issues remain warnings. Candidate validation inside writers continues to protect downstream publication. No duplicate GX service, metadata registry, native-Spark validation framework or extra result table is added. Draft gates with old schemas and hard-coded catalogs stay unscheduled.

Use STANDARD serverless, one configured writer, no recurring schedule and no retries for the completion audit. Verify in an isolated development bundle/catalog, preserving the original source windows, reviewed mapping, source provenance and row-count requirements. Source outages are failures to diagnose, not reasons to fabricate inputs. Report unit tests, fixture checks and actual live-source execution separately.
