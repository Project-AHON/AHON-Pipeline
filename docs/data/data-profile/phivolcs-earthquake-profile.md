# PHIVOLCS Earthquake

## Purpose

PHIVOLCS earthquake listings record where and when earthquakes were detected in and around the Philippines. Project AHON uses them as the hazard side of earthquake risk: how often, how strong and how near earthquakes are to each city and municipality.

> The listings are observations. One earthquake can appear more than once, and the location text names the nearest town, not the area affected. Counts are not LGU event counts, exposed population or validated risk.

## Source

| Property | Value |
|---|---|
| Source | PHIVOLCS Earthquake Information website, monthly listings |
| Provider | Philippine Institute of Volcanology and Seismology (PHIVOLCS) |
| Source format | HTML table per month, saved as one CSV per month |
| Access | Public website; one request per month. The current month falls back to the latest-events page when its monthly page is not yet published. |
| Period | Inclusive year-month window, set by `start_month` and `end_month` (`auto` = previous month) |
| Geographic unit | One earthquake observation (a point with a location description) |

Each requested month must return exactly one six-column event table, or the extract stops without publishing any file.

## Fields as received

The extract preserves the publisher's six displayed columns as text and adds the listing's month and year, giving eight fields in CSV:

| Column | Description | Format |
|---|---|---|
| `Date-Time` | Date and time as listed | `dd Month yyyy - hh:mm AM/PM` |
| `Latitude` | Latitude in degrees | Decimal text |
| `Longitude` | Longitude in degrees | Decimal text |
| `Depth` | Depth in kilometres | Decimal text |
| `Magnitude` | Magnitude as published | Decimal text |
| `Location` | Distance and bearing from the nearest town, with its province | `<distance> km <bearing> of <town> (<province>)` |
| `Month` | Month of the listing page | Added by the extract, e.g. `January` |
| `Year` | Year of the listing page | Added by the extract |

Things to know about the values as received:

- All values are kept as text in bronze, exactly as displayed.
- `Date-Time` is the listing's wall-clock time. No time zone is given, so it is not converted to UTC.
- The magnitude scale is not specified per row.
- Banner or invalid rows on the page are kept so silver can flag them.
- The listing has no event identifier. A corrected or repeated listing of the same earthquake is a separate row.

## Matching to PSGC

The `Location` text names the town referenced by the source distance/bearing description. It is neither epicenter containment nor earthquake impact.

Only the single-parenthesis form `<distance> km <bearing> of <town> (<province>)` is parsed. Town and province are compared with PSGC city and municipality names in the selected snapshot, using case and whitespace normalization only. There are no aliases, overrides, nearest-point or fuzzy matching.

| Candidate status | Meaning |
|---|---|
| `single_candidate` | Exactly one PSGC city or municipality has that town and province name |
| `ambiguous` | More than one matches; all codes are kept |
| `unmatched` | Parsed, but no PSGC name matches |
| `unparsed` | The location text does not follow the pattern |

Until the meaning of the location is approved, no PSGC code is assigned (`psgc_code` is NULL). Candidates are reported only.

## Limitations

- Observations are not deduplicated events. No publisher event identity, duplicate or correction policy has been approved, so there is no earthquake gold table yet.
- The location names the nearest town, which is not the same as the area that felt the earthquake. Offshore earthquakes still name a town on land.
- Independent cities without a conventional province may not match by town and province.
- Event time is local wall-clock time; UTC interpretation is withheld.
- The magnitude scale is not stated.
- The source is a public website rather than a documented API, and its page layout can change.
