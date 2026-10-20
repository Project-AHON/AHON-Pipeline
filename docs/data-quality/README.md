# Data quality

## How checks work

Builders and layer gates persist checks in `monitoring.dq_result`. The pipeline has 39 tasks, including 12 gates; setup and development verification are separate jobs. Gate tasks follow their corresponding builds. Census and OpenSTAT population gates run independently.

An ERROR-severity rule that fails records BLOCKED and stops dependents. WARN findings retain evidence without stopping the task. A gate execution exception records ERROR when GX returns an exception result, and stops the task. Earlier successful writes are not rolled back.

The failure metric can count failing rows, a row-count difference, or one failed contract. It is not always a count of source rows. Scripts executed through the Python/SQL runners also record execution markers; direct extractors do not necessarily do so.

Reject behavior is dataset-specific. See [handling findings](handling-rejects.md), [gate contracts](dq-gates.md), [results](dq-result.md), the [dictionary](../data/data-dictionary/README.md), and the [runbook](../operations/runbook.md).
