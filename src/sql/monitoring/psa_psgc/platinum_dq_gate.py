"""
DQ gate, gold to platinum: psa_psgc (decision 0012).

Not active yet: platinum.lgu_risk_profile is not built. When it is, this gate
checks the PSGC columns it brings into that table (psgc_code, LGU names and
geographic levels: every row is a valid city or municipality in dim_lgu),
following the template in docs/operations/monitoring/dq-gates.md.
"""
