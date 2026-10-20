# cmci_raw_indicator_batch_html

- **Table:** `bronze.cmci_raw_indicator_batch_html`
- **Source:** Department of Trade and Industry, CMCI Data Portal. See the [CMCI data profile](../data-profile/cmci-profile.md).
- **One row is:** one immutable source-response observation: one CMCI request containing a batch of LGUs, all requested years and all selected indicators
- **Loaded by:** `src/sql/datasets/cmci/pipeline.py --stage bronze`
- **Row hash covers:** `response_html`, `mappings_json`, `years_json`, `source_period`, `response_sha256`
- **Owner:** Maki

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `response_html` | string | Complete raw HTML returned by CMCI | Retained for audit and reprocessing |
| `mappings_json` | string | Reviewed PSGC code to CMCI name mappings sent in the request | JSON |
| `years_json` | string | CMCI years requested | JSON; currently 2014 to 2024 |
| `source_period` | string | PSGC snapshot the mappings belong to | For example `Q2_2024` |
| `response_sha256` | string | SHA-256 of the response bytes | Checked against the landed bytes before loading |
| `_source_name` | string | The dataset the row came from | Always `cmci_data_portal` |
| `_source_ref` | string | Where the row was loaded from: the landed JSON file path | The landed JSON retains source bytes in base64 |
| `_source_digest` | string | SHA-256 of the landed file | |
| `_ingested_at` | timestamp (UTC) | When the row was loaded into bronze | |
| `_batch_id` | string | The load run that wrote the row | |
| `_row_hash` | string | SHA-256 of the raw source columns as received | Covers the source columns above, not the provenance columns |
| `_observation_id` | string | Identity of this observation | Hash of `_batch_id`, `_source_digest` and `_row_hash`, so an identical response in a new release is a new observation |

## Known issues

- The CMCI portal is a public website rather than a documented high-volume API.
