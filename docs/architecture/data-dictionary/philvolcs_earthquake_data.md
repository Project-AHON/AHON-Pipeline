# PHILVOLCS earthquake observations

- **Source:** Philippine Institute of Volcanology and Seismology (PHIVOLCS), Department of Science and Technology (DOST). The raw extract is the combined CSV `phivolcs_earthquake_all_years.csv` generated from PHIVOLCS monthly earthquake pages and landed in the `ahon.reference.source` volume under the 'philippine_earthquake' folder.
- **Coverage:** Monthly earthquake listings from 2019-01-01 through 2026-09-29 in the current extract, covering seismic events reported in the Philippines and nearby offshore areas; the source is refreshed as PHIVOLCS publishes new monthly tables.
- **Owner:** Department of Science and Technology - Philippine Institute of Volcanology and Seismology (DOST-Philvolcs)

## Bronze: `<catalog>.bronze.philvolcs_earthquake_data`

One row is a distinct source observation under the existing `_row_hash` contract,
including invalid/non-event rows that can be represented by the typed schema.
Repeated identical source rows stay in the immutable CSV; reports explicitly count
raw rows, distinct observations and repetitions. A revised magnitude/depth or
source description may produce a new observation.

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `Date-Time` | STRING | Source timestamp text | `dd MMMM yyyy - hh:mm AM/PM`; can be invalid/blank |
| `Latitude` | DOUBLE | Published latitude, decimal degrees | Nullable; original numeric text remains in raw CSV |
| `Longitude` | DOUBLE | Published longitude, decimal degrees | Nullable; original numeric text remains in raw CSV |
| `Depth` | DOUBLE | Published depth in km | Nullable; NaN/infinity are reported |
| `Magnitude` | DOUBLE | Published magnitude | Nullable; no arbitrary upper limit |
| `Location` | STRING | Published description | Legacy trim/blank-to-null remains; raw text retained in file |
| `Month` | STRING | Source page month name | Legacy trim/blank-to-null remains |
| `Year` | INT | Source page year | Legacy try_cast remains; original text separately checked |
| `_source_name` | STRING | Dataset name | `philvolcs_earthquake_data` |
| `_source_ref` | STRING | Immutable landing CSV path | First source containing this hash; file digest/type manifest beside it |
| `_ingested_at` | TIMESTAMP | First Bronze ingestion time, UTC | Unchanged on a rerun |
| `_batch_id` | STRING | First ingestion run ID | One supplied ID per run; unchanged rows retain earlier IDs |
| `_row_hash` | STRING | Observation fingerprint | Lowercase 64-character SHA-256; merge identity |

**Legacy hash contract:** main computes `sha2(concat_ws('||', Date-Time, Latitude,
Longitude, Depth, Magnitude, Location, Month, Year), 256)` from `read_files`
**inferred source values before Bronze casts/trims**, in exactly that order.
`concat_ws` skips nulls and does not escape delimiters. Therefore the old dictionary
claim of original numeric text fidelity was incorrect. The runner pins reviewed
inferred types, checks recomputed hashes against landed/persisted observations,
and stops for distinct signatures sharing a hash, textual observations collapsing
under inference, or inference drift. It does not introduce a new identity contract.

Typed Bronze cannot distinguish all original numeric strings, restore previously
trimmed values, or recover first lineage already changed by old main reruns.
The immutable CSV and `<csv>.manifest.json` retain its SHA-256, raw count, source
columns, inference types and source period counts. These files must be retained
as long as the observations. Recomputing hashes for history requires each original
file and its inferred-type contract; it cannot be done faithfully from typed
Bronze alone. An incompatible target or missing original contract requires a
fresh development target or separately reviewed migration, never a production
overwrite. Invalid numeric text stays auditable in raw even when a legacy cast
produces null. A schema-inference change can block a batch pending that decision.

## Silver: `<catalog>.silver.philvolcs_earthquake_data_clean`

One row is one distinct Bronze observation, including flagged rows. The merge key
is `_row_hash`; `id` remains an event signature and is **not unique**. Enrichment
must not multiply rows. Existing Silver's event primary key/NOT NULL constraints
must be migrated explicitly before adopting this observation contract.

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `id` | BIGINT | `xxhash64(event_time, latitude, longitude, depth, magnitude)` | Same signature expression; never invalid-observation identity |
| `event_time` | TIMESTAMP, nullable | Parsed source wall-clock value under legacy signature convention | Not labeled UTC; see time semantics below |
| `latitude`, `longitude` | DOUBLE, nullable | Bronze coordinates | Kept even when implausible; flagged |
| `depth`, `magnitude` | DOUBLE, nullable | Bronze depth/magnitude | Finite/nonnegative rules; no 800/7.8 upper cutoff |
| `location_description` | STRING, nullable | Trimmed description | Blank Location allowed |
| `month`, `year` | INT, nullable | Derived from parsed source wall time | Compared with Bronze page metadata |
| `has_dq_issues` | BOOLEAN | Invalid timestamp/numeric values or inconsistent source period | Mapping gaps are separate |
| `_source_name`, `_source_ref`, `_batch_id`, `_row_hash` | STRING | Unchanged Bronze lineage | Same meanings as above |
| `_ingested_at` | TIMESTAMP | Unchanged Bronze ingestion time | UTC |
| `_processed_at` | TIMESTAMP | Silver processing time, UTC | Unchanged rows keep their earlier value |
| `reported_locality`, `reported_province` | STRING | Conservatively parsed/normalized tokens | Empty/unparsed tokens retained as empty or null |
| `psgc_candidate_codes` | ARRAY<STRING> | Exact CITY/MUN locality+province candidates | Zero, one or multiple; no invented/padded codes |
| `psgc_candidate_status` | STRING | `single_candidate`, `ambiguous`, `unmatched`, `unparsed`, `blocked_reference` | Proposal, not approved Schema v1 values |
| `psgc_candidate_method` | STRING, nullable | `exact_locality_province_candidate` | Method category, not confidence probability |
| `_psgc_snapshot` | STRING | Configured reference period | Default Q2_2024; no Q2 2026 substitution |
| `psgc_code` | STRING, nullable | Analytical PSGC assignment | Always null pending approved meaning |
| `psgc_match_status` | STRING | `blocked_meaning` | Minimal proposed extension; no guessed assignment |
| `psgc_match_method` | STRING, nullable | Approved assignment method | Always null while blocked |

**Candidate meaning:** the reference locality named in `025 km S 58° W of
Sipalay (Negros Occidental)`, not the epicenter's containing/affected LGU.
Only the supported simple distance/bearing, locality and one province-parenthesis
pattern is parsed. Nested island/municipality descriptions remain unparsed.
Normalize case and whitespace only. Do not remove significant tokens, `City of`,
accents, or repair names without reviewed evidence. Such conservative rules leave
some obvious-looking names unmatched intentionally. Independent cities without
conventional province relationships need evidence before exceptions; no name-only
fallback or fabricated overrides exist. Unmatched does not mean offshore.

One temporary `earthquake_lgu_candidates` row per normalized locality/province
is enough for the bounded input scope. This is a narrow reference enrichment
exception, not a combination of earthquake with another measure dataset. Silver
uses `silver.psa_psgc_clean`, never Gold. Missing reference blocks candidates but
allows independent cleaning. Snapshot changes or reviewed reference corrections
need deliberate historical reprocessing. Historical events are candidate-matched
to the chosen reference vintage, not claimed to have those historic boundaries.

**Time semantics:** [PHIVOLCS](https://earthquake.phivolcs.dost.gov.ph/) labels
listing times Philippine Time and explains UTC+8. The runner fixes its session to
UTC solely to reproduce a stable legacy wall-clock TIMESTAMP/signature expression;
`event_time` is not thereby a UTC event instant. Timeliness output separately uses
`to_utc_timestamp(event_time, 'Asia/Manila')`. Main's original session convention
and the full historical landing headers were not recovered. Changing existing
event_time/IDs requires reviewed reconciliation, rather than relabeling history.

The [read-only issue lookup](../../operations/philvolcs_earthquake_ingestion.md#read-only-issue-lookup)
is keyed by `_source_ref` and `_row_hash` and runs in the runner's Spark session.
DQ aggregates cover all seven attributes;
stop rules are raised before writes by the runner. Read-only reports alone do
not enforce a build. See proposed [ADR 0009](../../decisions/0009-earthquake-observations-and-locality-candidates.md)
and [blocked event fact](fact_earthquake_event.md).
