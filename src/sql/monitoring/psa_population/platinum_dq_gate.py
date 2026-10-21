"""
DQ gate, gold to platinum: psa_population.

Not built yet: platinum.lgu_risk_profile does not exist. This file holds the
plan only, so there is no code to run.

lgu_risk_profile is planned as one city or municipality per year and combines
all datasets. This gate will check only the population columns that
psa_population brings into it, so a failure points at one owner (Crizza).
The psgc_code, name and level columns are checked by the psa_psgc gate.

Planned rules, once the table and its column names exist:
- population columns not null for every city and municipality (null, report
  at first; stop once coverage is known).
- population not negative (range, stop).
- if urban and rural population are carried: urban not above total, and rural
  equal to total minus urban (range and reconciliation, stop).
- if percent urban is carried: between 0 and 100 (range, stop).
- city or municipality population equal to the sum of its barangays in
  gold.fact_population for the same year (reconciliation, report; PSA
  rounding and barangays without a value may leave small gaps).
- population year present and within the loaded census years (accepted_values,
  report; update when a new census year is loaded).

Expected on ahon_dev: not run yet.
"""
