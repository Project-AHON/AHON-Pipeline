# CMCI execution

Use `src/sql/datasets/cmci/pipeline.py` and the CMCI branch of `resources/jobs/ahon_job.yml`. The prior `cmci_*_gx` workflow and hardcoded 1,634-LGU completeness gates are historical implementations, not configured jobs. Data quality is enforced directly before dependent outputs; no Great Expectations package is required by the supported path.

Provide a reviewed CSV and matching `source_period`; see the [main runbook](runbook.md) for the exact schema, deployment and selective-execution commands. Run mode and test size are explicit caller inputs; no source scope is embedded in code. Direct script execution with `--run-mode full` selects all reviewed mappings. Requested source years are explicit, and the response must validate the approved source window.

Stage order: extract → Bronze → Silver → Gold → Platinum coverage. Raw HTML and source bytes are retained in immutable landed JSON; all batches validate before publication. Replays use the same batch ID. Source corrections use a new batch ID, update stable logical keys, and preserve raw predecessors. Missing measurements stay NULL; invalid latest corrections withhold old analytical values until recovery.

Inspect `monitoring.dq_result` and `silver.cmci_reject` on failure. Reviewed source-name/code relationships must be one-to-one and exist in the selected dimension. Existing incompatible tables are not replaced; use a fresh development catalog or obtain a reviewed migration. No real approved mapping for Q2_2024 was present in the selected personal workspace during this task; synthetic fixtures test code only.
