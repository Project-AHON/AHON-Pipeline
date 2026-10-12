# Earthquake operations

Use the [shared runbook](runbook.md) for explicit monthly windows, verified landing, replay and recovery. Active Bronze/Silver entry point: src/sql/datasets/philvolcs_earthquake/observation_pipeline.py. [Observation contract](../architecture/data-dictionary/philvolcs_earthquake_data.md). Older SQL/runner files are guarded legacy diagnostics; no active job builds an event fact or assumes exposure from a locality.
