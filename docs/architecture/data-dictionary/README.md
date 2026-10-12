# Active table dictionaries

All names are relative to the configured catalog. [Source-to-target](../source-to-target.md) and [data model](../data-model.md) define source coverage and keys. Code schema compatibility checks reject unapproved changes.

- [Shared observation and monitoring columns](pipeline-contracts.md)
- [PSGC](psa_psgc_data.md) and [LGU dimension](dim_lgu.md)
- [LDRRMF Bronze/Silver](blgf_ldrrmf_annual_lgu.md), [funding fact](fact_ldrrmf.md)
- [Population](psa_population.md)
- [CMCI](cmci_dictionary.md)
- [Earthquake observations](philvolcs_earthquake_data.md), [blocked event fact](fact_earthquake_event.md)
- [Analytical evidence](lgu_evidence_status.md)

The older workbook [LGU master](lgu_master.md) is superseded by dim_lgu and is not in the executable bundle. Templates do not define deployed tables.
