"""Actual GX evidence must be present, consistent and from every terminal branch."""

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src/sql/common"))
from quality import validate_metric
from verify_quality import REQUIRED_RESULTS, evidence_failures


def result(check_id, table, count=0, severity="ERROR"):
    status, payload = validate_metric(check_id, table, count, severity)
    return {"check_id": check_id, "table_name": table, "failed_count": count,
            "severity": severity, "disposition": status, "validation_result": payload}


@pytest.fixture
def evidence():
    return [result(*key) for key in sorted(REQUIRED_RESULTS)]


def test_all_branches_with_real_gx_evidence_pass(evidence):
    assert evidence_failures(evidence) == []


def test_empty_or_missing_branch_cannot_use_existing_tables(evidence):
    assert len(evidence_failures([])) == len(REQUIRED_RESULTS)
    assert evidence_failures(evidence[1:])


def test_advisory_source_failures_are_retained_without_blocking(evidence):
    evidence.append(result("known_missing_population", "gold.fact_population", 19, "WARN"))
    assert evidence_failures(evidence) == []


def test_blocking_source_failure_blocks_the_pipeline(evidence):
    evidence.append(result("duplicate_key", "gold.fact_population", 1))
    assert evidence_failures(evidence)


def test_repair_reevaluates_prior_audit_verdict(evidence):
    evidence.append(result("pipeline_audit_output_nonempty", "gold.fact_population_total", 1))
    assert evidence_failures(evidence) == []


@pytest.mark.parametrize("change", [
    {"validation_result": None}, {"validation_result": "not json"},
    {"validation_result": "{}"}, {"failed_count": -1}, {"failed_count": True},
    {"failed_count": 1}, {"disposition": "WARN"}, {"severity": "IGNORE"},
])
def test_malformed_or_inconsistent_evidence_fails_closed(evidence, change):
    evidence[0].update(change)
    assert evidence_failures(evidence)


@pytest.mark.parametrize("field,value", [("engine", "mock"), ("gx_version", "1.23.1")])
def test_wrong_engine_or_version_cannot_claim_success(evidence, field, value):
    payload = json.loads(evidence[0]["validation_result"])
    payload["meta"][field] = value
    evidence[0]["validation_result"] = json.dumps(payload)
    assert evidence_failures(evidence)
