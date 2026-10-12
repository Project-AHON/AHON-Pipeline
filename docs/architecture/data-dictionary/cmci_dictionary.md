# CMCI selected indicator contract

bronze.cmci_raw_indicator_batch_html: response_html, mappings_json, years_json, source_period, response_sha256 STRING plus shared lineage. Raw landing JSON also preserves exact response_bytes_base64. One request-response file version; manifest count represents indicator values rather than Bronze response rows.

reference.cmci_lgu_map: psgc_code,cmci_name,source_period,reviewer,evidence STRING; key code/period; reviewed CSV requires unique code and portal name and verified snapshot membership.

silver.cmci_indicator_clean and gold.fact_cmci_indicator: psgc_code/cmci_name/indicator_code/raw_value/source_period STRING, year INT, raw_score DOUBLE, has_dq_issues BOOLEAN plus selected source lineage. Physical merge key code/year/indicator within one guarded snapshot (logical reference-period grain). Missing '-' or empty → NULL, nonfinite/negative/unparseable values quarantine. silver.cmci_reject retains fields, keyed observation/code/year/indicator. No automatic source deletion. Silver retains the prior accepted value after an invalid correction. Gold withholds its raw_score (NULL) and marks has_dq_issues until a newer valid source version arrives; inspect rejects.

gold.dim_cmci_indicator: indicator_code/indicator_name/pillar_name/metric_version STRING, 35 selected components, selected_35_v1. Additional indicators require reviewed version changes.

platinum.cmci_pillar_score: psgc_code STRING, year INT, source_period STRING, pillar_name STRING, expected_indicator_count/available_indicator_count BIGINT, pillar_score_raw DOUBLE nullable, source_ingested_at TIMESTAMP, coverage_status STRING COMPLETE/INSUFFICIENT_DATA, metric_version STRING. Sum available values; observed nonnull count must equal selected expected count to be complete. Empty score is not converted to zero. Raw sum is not an official score or risk/preparedness index.
