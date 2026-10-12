# AHON Pipeline

AHON processes Philippine geographic reference, LDRRMF funding, 2024 population, CMCI components and PHIVOLCS earthquake observations on Databricks. Funding and CMCI are preparedness evidence; earthquake risk classifications remain withheld.

Based on main `e1e8b883a1780b3150e6c7cd880530f4d7e42e66`. The original checkout is preserved. See [all-PR assessment](docs/architecture/pr-assessment.md), [validation evidence](docs/operations/validation.md) and [deployment/recovery runbook](docs/operations/runbook.md).

```powershell
python -m pip install -r requirements.txt -r requirements-dev.txt
python tools/validate_repository.py
ruff check src tests tools
pytest tests -q
databricks bundle validate --strict --target dev --profile 7474651621867565
databricks bundle validate --strict --target prod --profile 7474651621867565
```

[Architecture](docs/architecture/overview.md), [source-to-target inventory](docs/architecture/source-to-target.md), [data model](docs/architecture/data-model.md), [data dictionaries](docs/architecture/data-dictionary/README.md), [metrics](docs/architecture/metrics.md), [engineering decision](docs/decisions/0010-coherent-medallion-contract.md), [LGU guide](docs/stakeholder/lgu-guide.md).

Serverless jobs include pinned runtime dependencies. Every landing batch has a verified completion manifest; retries preserve raw versions, accepted lineage and rejects. All schedules ship paused. The full source workflow needs a PSA API secret and a reviewed CMCI mapping. Critical infrastructure needs a source/asset/CRS contract. Scientific risk classification needs hazard, exposure, vulnerability and an approved method. Dataset jobs run independently when their own prerequisites are available.

Runtime pins are kept consistently in requirements.txt, pyproject.toml and job environments. The obsolete openpyxl-only uv.lock referenced an unavailable private package proxy and was removed; install the pinned declarations instead. YAML formatting accepts Windows/Linux line endings and the existing mixed list indentation while still checking YAML validity and bundle structure.
