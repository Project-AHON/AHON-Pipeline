# 0016: Use GX for dataset quality gates

- Status: Implemented following the user's request; team review remains required
- Date: 2026-10-15
- Updates: the DQ-engine choice in 0010; does not restore the retired CMCI-specific GX job hierarchy

All supported datasets use pinned GX Core through the existing shared quality helper. Spark computes complete-input rule metrics; GX validates their failure counts. This preserves the existing transformations, SQL checks and serverless execution without collecting growing source datasets into pandas or introducing another framework.

The existing `monitoring.dq_result` table retains its keys, counts and severities. A nullable `validation_result` JSON string records GX evidence; adding that audit column preserves historical rows. No additional result table or GX service is needed. ERROR failures persist and block dependent writes; WARN failures preserve flagged observations. GX failures cannot fall back to a non-GX pass. Ingestion integrity, transport, parsing and schema guards remain fail-fast contracts.

Financial SQL emits named rule metrics to the same helper instead of owning a separate assertion engine. Existing per-dataset entry points and dependencies remain. The synthetic verification resource is development-only. Production requires an explicitly configured job identity and its source/secret permissions; no identity or credential is invented.

PSGC credentials are entered locally into a Databricks secret. Only scope/key names are configuration inputs. Production deployment and execution are not authorized by this implementation request; validation and execution evidence distinguish development fixtures, real-source replay and unavailable live prerequisites.
