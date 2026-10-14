"""Regression for schema checking multi-line financial quarantine MERGEs."""

import sys
from pathlib import Path

import sqlglot
import sqlparse

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src/sql/common"))
from run_sql import merge_source


def test_all_financial_merge_sources_can_be_schema_checked():
    files = [
        ROOT / "src/sql/gold/build_fact_ldrrmf.sql",
        ROOT
        / "src/sql/datasets/blgf_ldrrmf_annual_lgu/silver/clean_blgf_ldrrmf_annual_lgu.sql",
    ]
    targets = []
    for file in files:
        for statement in sqlparse.split(file.read_text(encoding="utf-8")):
            result = merge_source(statement)
            if result:
                target, source, selected = result
                assert selected
                assert len(sqlglot.parse(source, read="databricks")) == 1
                targets.append(target)
    assert set(targets) == {
        "gold.fact_ldrrmf",
        "silver.blgf_ldrrmf_annual_lgu_clean",
        "silver.blgf_ldrrmf_annual_lgu_reject",
    }
