"""Behavioral tests for source response validation and conservative geographic matching."""

import importlib.util
import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src/sql/datasets/cmci"))
sys.path.insert(0, str(ROOT / "src/sql/common"))


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, ROOT / path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


CMCI = load("cmci_contract_test", "src/sql/datasets/cmci/pipeline.py")
CMCI.configure_indicators(ROOT / "resources/cmci_indicators.csv")
MATCH = load(
    "ldrrmf_contract_test",
    "src/sql/datasets/blgf_ldrrmf_annual_lgu/silver/build_blgf_ldrrmf_lgu_map.py",
)


def html(value="-", name="Fixture locality"):
    return "<title>Data Portal Results</title>" + "".join(
        f"<h2>{label}</h2><table><tr><th>Province / LGU</th><th>2024</th></tr><tr><td>{name}</td><td>{value}</td></tr></table>"
        for label in CMCI.APPROVED_INDICATORS
    )


def test_cmci_keeps_missing_text_for_silver_disposition():
    rows = CMCI.parse_response(
        html(), [{"cmci_name": "Fixture locality", "psgc_code": "0100101000"}], ["2024"]
    )
    assert len(rows) == 35
    assert all(row[-1] == "-" for row in rows)


@pytest.mark.parametrize(
    "payload",
    [
        html(name="Wrong locality"),
        html().replace("2024", "2023"),
        html() + html(),
        "<html>Error</html>",
    ],
)
def test_cmci_response_drift_blocks_landing(payload):
    with pytest.raises(ValueError):
        CMCI.parse_response(
            payload,
            [{"cmci_name": "Fixture locality", "psgc_code": "0100101000"}],
            ["2024"],
        )


def test_cmci_mapping_requires_review_and_unique_source_names(tmp_path):
    file = tmp_path / "mapping.csv"
    file.write_text(
        "psgc_code,cmci_name,source_period,reviewed,reviewer,evidence\n0100101000,Fixture,Q2_2024,false,,\n"
    )
    with pytest.raises(ValueError, match="requires"):
        CMCI.reviewed_mapping(file, "Q2_2024")


def reference(rows):
    frame = pd.DataFrame(
        rows, columns=["psgc_code", "name", "level", "province_name", "old_name"]
    )
    frame["key"] = frame.name.map(MATCH.name_key)
    frame["province_key"] = frame.province_name.map(MATCH.name_key)
    frame["old_key"] = frame.old_name.map(MATCH.name_key)
    return frame


def test_same_names_require_province_and_ambiguity_stays_unassigned():
    refs = reference(
        [
            ("0100101000", "San Jose", "Mun", "Alpha", None),
            ("0200101000", "San Jose", "Mun", "Beta", None),
        ]
    )
    row = {"province": "Alpha", "lgu_name": "San Jose", "lgu_type": "Municipality"}
    assert MATCH.match_one(row, refs, {})[:4] == (
        "0100101000",
        "San Jose",
        "province_name",
        "matched",
    )
    assert MATCH.match_one({**row, "province": "Unknown"}, refs, {})[3] == "ambiguous"
    refs = pd.concat(
        [refs, refs.iloc[:1].assign(psgc_code="0100102000")], ignore_index=True
    )
    assert MATCH.match_one(row, refs, {})[3] == "ambiguous"


def test_name_only_and_changed_geographic_level_need_review():
    refs = reference([("0100101000", "San Jose", "City", None, None)])
    row = {"province": "Unknown", "lgu_name": "San Jose", "lgu_type": "Municipality"}
    assert MATCH.match_one(row, refs, {})[3] == "needs_review"


def test_override_normalization_collision_is_rejected(tmp_path):
    file = tmp_path / "overrides.csv"
    file.write_text(
        "province,lgu_name,lgu_type,psgc_code,reason\nAlpha,San Jose,Municipality,0100101000,fixture\nAlpha,SAN JOSE,Municipality,0100102000,fixture\n"
    )
    with pytest.raises(ValueError, match="collide"):
        MATCH.read_overrides(file)


def test_packaged_reviewed_aliases_have_no_conflicting_targets():
    overrides = MATCH.read_overrides(
        ROOT
        / "src/sql/datasets/blgf_ldrrmf_annual_lgu/silver/blgf_ldrrmf_lgu_map_overrides.csv"
    )
    assert overrides
    assert (
        overrides[
            (
                MATCH.name_key("Masbate"),
                MATCH.name_key("Pio V. Corpuz (Limbuhan)"),
                "MUNICIPALITY",
            )
        ]
        == "0504116000"
    )
