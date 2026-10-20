# cmci_lgu_map

- **Table:** `reference.cmci_lgu_map`
- **One row is:** one reviewed PSGC LGU to CMCI locality mapping for one PSGC snapshot
- **Key:** `psgc_code`, `source_period`
- **Loaded by:** `src/sql/datasets/cmci/pipeline.py --stage bronze`, from the reviewed mapping CSV
- **Owner:** Maki

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `psgc_code` | string | Canonical PSGC LGU identifier | 10 digits; must exist at city/municipality level in `gold.dim_lgu` for the same snapshot |
| `cmci_name` | string | Exact locality value submitted to CMCI | Unique per snapshot |
| `source_period` | string | PSGC snapshot | |
| `reviewer` | string | Who reviewed the mapping | Required |
| `evidence` | string | Evidence for the mapping | Required |

## Notes

- Unique code and source-name relationships are checked. Only reviewed mappings are used; there is no fuzzy matching.
- How the 1,634 approved mappings were built is described in the [CMCI data profile](../data-profile/cmci-profile.md#matching-to-psgc).

## Known issues

- Eight valid PSGC municipalities in the BARMM Special Geographic Area have no CMCI locality record, so they have no mapping. See the [CMCI data profile](../data-profile/cmci-profile.md#known-psgc-only-source-gaps).
