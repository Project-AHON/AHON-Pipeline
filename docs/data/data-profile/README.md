# Data profiles

What each source looks like as it arrives: who publishes it, how it is accessed, what it covers, the fields as received, how it links to PSGC, and its known issues and limits. One page per dataset, named `<dataset>-profile.md`.

Profiles describe the source. The tables built from it are described in the [data dictionary](../data-dictionary/README.md). For a one-table summary of all sources and what is in or out of scope, see [sources](../sources.md).

## Profiles

| Profile | Dataset | Publisher | Owner |
| --- | --- | --- | --- |
| [psgc-profile](psgc-profile.md) | Philippine Standard Geographic Code | Philippine Statistics Authority | Crizza |
| [psa-population-profile](psa-population-profile.md) | PSA Population by city and municipality | Philippine Statistics Authority | Crizza |
| [phivolcs-earthquake-profile](phivolcs-earthquake-profile.md) | Earthquake listings | PHIVOLCS | Jessy |
| [cmci-profile](cmci-profile.md) | Cities and Municipalities Competitiveness Index | Department of Trade and Industry | Maki |
| [blgf-ldrrmf-profile](blgf-ldrrmf-profile.md) | LDRRMF annual utilization by LGU | Bureau of Local Government Finance | Crystal |

## How to add a profile

1. Copy the template below into `<dataset>-profile.md`.
2. Fill in each section from the source itself and from profiling queries. Remove sections that do not apply.
3. Add a row to the table above, and add the source to [sources](../sources.md).

## Rules for every profile

- **Describe the source as received,** not the cleaned tables. Cleaning rules belong in the dictionary.
- **Give numbers with their basis:** which snapshot, year or run they come from.
- **Keep denominators separate.** A source that does not cover every PSGC LGU is a source-coverage gap, not a pipeline failure. Say what it covers against the PSGC total.
- **Known issues are documented, not fixed here.** Say what was found and how many rows, and whether the pipeline flags, fixes or keeps it.
- **Say what the data does not measure,** so it is not over-read (for example, funding is not preparedness, an earthquake listing is not exposure).
- **Keep it true to the source.** Update the profile when the source, period or access method changes.

## Template

```markdown
# Dataset name

## Purpose

What the dataset is and what Project AHON uses it for.

> What it does not measure, if it could be over-read.

## Source

| Property | Value |
|---|---|
| Source | Name and link |
| Provider | Publisher |
| Source format | File or API format |
| Access | How it is retrieved (API token, website, Hugging Face) |
| Period | Years, snapshot or window |
| Geographic unit | What one record describes |
| Units | If amounts |

## Coverage

Counts by year, level or snapshot, against the PSGC total where relevant.

## Fields as received

| Source field | Loaded as | Description |
|---|---|---|
| `Field` | `column` | What it holds |

Things to know about the values as received (format, missing values, layout changes).

## Matching to PSGC

How records are linked to PSGC codes, the match statuses, and known unmatched records.

## Known source issues

| Issue | Evidence |
|---|---|
| What was found | Rows or examples |

## Limitations

What the data cannot be used for, or must be used with care.
```
