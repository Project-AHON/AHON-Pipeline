import os
import time
import uuid
from datetime import datetime, timezone
from io import StringIO

import pandas as pd
import requests

try:
    import urllib3
except ImportError:  # pragma: no cover - optional dependency
    urllib3 = None

# Suppress SSL warnings when the dependency is available
if urllib3 is not None:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

MONTH_NAMES = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December"
]

def scrape_current_month_from_main_page():
    """
    Scrapes the latest earthquake data from the main PHIVOLCS page.
    This is used only for the current month when the dedicated monthly page is not yet available.
    """
    url = "https://earthquake.phivolcs.dost.gov.ph/"

    try:
        print("  Fetching from main page (current month)...", end=" ")

        session = requests.Session()
        session.verify = False

        response = session.get(url, timeout=15)
        response.raise_for_status()

        tables = pd.read_html(StringIO(response.text), skiprows=1)

        df = None
        for table in tables:
            if table.shape[1] >= 5:
                df = table
                break

        if df is None or df.empty:
            print("✗ No data found")
            return None

        expected_columns = [
            "Date-Time",
            "Latitude",
            "Longitude",
            "Depth",
            "Magnitude",
            "Location",
        ]

        if df.shape[1] == 6:
            df.columns = expected_columns
        elif df.shape[1] > 6:
            df = df.iloc[:, :6]
            df.columns = expected_columns
        else:
            print(f"✗ Invalid columns ({df.shape[1]})")
            return None

        mask = (
            df["Date-Time"].astype(str).str.contains("Date|Time|Philippine", case=False, na=False)
            | df["Latitude"].astype(str).str.contains("Latitude|ºN|°N", case=False, na=False)
            | df["Longitude"].astype(str).str.contains("Longitude|ºE|°E", case=False, na=False)
        )
        df = df[~mask].reset_index(drop=True)

        if not df.empty:
            first_col = df.iloc[:, 0].astype(str).str.strip()
            summary_mask = first_col.str.lower().str.contains("total|no. of events", na=False, regex=True)
            month_abbrev_mask = first_col.str.match(r"^[A-Z][a-z]{2}-\d{2}$", na=False)
            df = df[~(summary_mask | month_abbrev_mask)]

        df = df.dropna(how="all").reset_index(drop=True)

        current_month = datetime.now(timezone.utc).strftime("%B")
        current_year = datetime.now(timezone.utc).year

        df["Month"] = current_month
        df["Year"] = current_year

        print(f"✓ {len(df)} records")
        return df

    except (requests.RequestException, pd.errors.ParserError) as e:
        print(f"✗ Error: {e}")
        return None


def scrape_phivolcs_data_from_html(year, month_name):
    """
    Fetches earthquake data by reading the HTML table from the PHIVOLCS monthly page.
    If the monthly page returns 404 for the current month only, it falls back to the main page.
    Historical 404s are treated as absent/missing source pages.
    """
    url = (
        f"https://earthquake.phivolcs.dost.gov.ph/EQLatest-Monthly/"
        f"{year}/{year}_{month_name}.html"
    )

    try:
        print(f"  Fetching: {month_name} {year}...", end=" ")

        session = requests.Session()
        session.verify = False

        response = session.get(url, timeout=15)
        response.raise_for_status()

        tables = pd.read_html(StringIO(response.text), skiprows=1)

        df = None
        for table in tables:
            if table.shape[1] >= 5:
                df = table
                break

        if df is None or df.empty:
            print("✗ No data")
            return None

        expected_columns = [
            "Date-Time",
            "Latitude",
            "Longitude",
            "Depth",
            "Magnitude",
            "Location",
        ]

        if df.shape[1] == 6:
            df.columns = expected_columns
        elif df.shape[1] > 6:
            df = df.iloc[:, :6]
            df.columns = expected_columns
        else:
            print(f"✗ Invalid columns ({df.shape[1]})")
            return None

        mask = (
            df["Date-Time"].astype(str).str.contains("Date|Time|Philippine", case=False, na=False)
            | df["Latitude"].astype(str).str.contains("Latitude|ºN|°N", case=False, na=False)
            | df["Longitude"].astype(str).str.contains("Longitude|ºE|°E", case=False, na=False)
        )
        df = df[~mask].reset_index(drop=True)

        if not df.empty:
            first_col = df.iloc[:, 0].astype(str).str.strip()
            summary_mask = first_col.str.lower().str.contains("total|no. of events", na=False, regex=True)
            month_abbrev_mask = first_col.str.match(r"^[A-Z][a-z]{2}-\d{2}$", na=False)
            df = df[~(summary_mask | month_abbrev_mask)]

        df = df.dropna(how="all").reset_index(drop=True)

        df["Month"] = month_name
        df["Year"] = year

        print(f"✓ {len(df)} records")
        return df

    except requests.exceptions.HTTPError as errh:
        if errh.response is not None and errh.response.status_code == 404:
            current_year = datetime.now(timezone.utc).year
            current_month = datetime.now(timezone.utc).strftime("%B")

            if year == current_year and month_name == current_month:
                print("✗ HTTP 404 (trying main page for current month)")
                return scrape_current_month_from_main_page()
            else:
                print(f"✗ HTTP 404 for historical month {month_name} {year} - treated as missing source page")
                return None
        else:
            print(f"✗ HTTP {errh.response.status_code if errh.response is not None else 'unknown'}")
            return None
    except requests.RequestException as e:
        print(f"✗ Error: {e}")
        return None


def scrape_year_data(year, output_dir="data"):
    """
    Scrapes earthquake data for all months in a given year.
    Returns the combined DataFrame for that year.
    """
    print(f"\n{'─'*70}")
    print(f"📅 Scraping Year: {year}")
    print(f"{'─'*70}")

    all_data = []
    successful_months = []
    failed_months = []

    for month_name in MONTH_NAMES:
        df = scrape_phivolcs_data_from_html(year, month_name)

        if df is not None and not df.empty:
            all_data.append(df)
            successful_months.append(month_name)
        else:
            failed_months.append(month_name)

        time.sleep(0.5)

    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)

        os.makedirs(output_dir, exist_ok=True)
        output_filename = os.path.join(output_dir, f"phivolcs_earthquake_{year}.csv")
        combined_df.to_csv(output_filename, index=False, encoding="utf-8-sig")

        print(f"\n✓ Year {year} Complete:")
        print(f"  • Total records: {len(combined_df)}")
        print(f"  • Successful months: {len(successful_months)}")
        print(f"  • File saved: {output_filename}")

        return combined_df
    else:
        print(f"\n✗ No data retrieved for {year}")
        return None


def scrape_multiple_years(years_back=3, output_dir="data"):
    """
    Scrapes earthquake data for the last N years.
    Each year is saved as a separate CSV file.
    Returns combined data, summary, and a single batch id.
    """
    current_year = datetime.now(timezone.utc).year
    start_year = current_year - years_back + 1
    batch_id = str(uuid.uuid4())

    print(f"\n{'='*70}")
    print("🌏 PHIVOLCS EARTHQUAKE DATA SCRAPER")
    print(f"{'='*70}")
    print(f"📊 Scraping Range: {start_year} - {current_year}")
    print(f"📁 Output Directory: {output_dir}/")
    print(f"🆔 Batch ID: {batch_id}")
    print(f"{'='*70}")

    all_years_data = []
    scrape_summary = {}

    for year in range(start_year, current_year + 1):
        df = scrape_year_data(year, output_dir)

        if df is not None:
            all_years_data.append(df)
            scrape_summary[year] = len(df)
        else:
            scrape_summary[year] = 0

    if all_years_data:
        combined_all = pd.concat(all_years_data, ignore_index=True)
        combined_filename = os.path.join(output_dir, "phivolcs_earthquake_all_years.csv")
        combined_all.to_csv(combined_filename, index=False, encoding="utf-8-sig")

        print(f"\n{'='*70}")
        print("✅ SCRAPING COMPLETE!")
        print(f"{'='*70}")
        print("\n📊 Summary by Year:")
        for year, count in scrape_summary.items():
            print(f"  • {year}: {count:,} earthquakes")
        print(f"\n📈 Total Records: {len(combined_all):,}")
        print(f"\n📁 Files Created:")
        for year in range(start_year, current_year + 1):
            if scrape_summary.get(year, 0) > 0:
                print(f"  • {output_dir}/phivolcs_earthquake_{year}.csv")
        print(f"  • {output_dir}/phivolcs_earthquake_all_years.csv (combined)")
        print(f"\n{'='*70}\n")

        return combined_all, scrape_summary, batch_id

    print("\n✗ No data was retrieved for any year.")
    return None, {}, batch_id


def display_statistics(df):
    """
    Display basic statistics about the scraped data.
    """
    if df is None or df.empty:
        return

    print(f"{'='*70}")
    print("📈 DATA STATISTICS")
    print(f"{'='*70}\n")

    print("🔢 Magnitude Statistics:")
    print(df["Magnitude"].describe())

    print("\n📅 Earthquakes by Year:")
    yearly_counts = df.groupby("Year").size().sort_index()
    for year, count in yearly_counts.items():
        print(f"  • {year}: {count:,} earthquakes")

    print("\n💥 Top 10 Strongest Earthquakes:")
    top_10 = df.nlargest(10, "Magnitude")[["Date-Time", "Magnitude", "Location", "Year"]]
    for _, row in top_10.iterrows():
        print(f"  • Mag {row['Magnitude']} - {row['Location'][:50]} ({row['Year']})")

    print(f"\n{'='*70}\n")


if __name__ == "__main__":
    YEARS_TO_SCRAPE = 8
    OUTPUT_DIR = "/Volumes/ahon/reference/source/philvolcs_earthquake"

    combined_df, summary, batch_id = scrape_multiple_years(
        years_back=YEARS_TO_SCRAPE,
        output_dir=OUTPUT_DIR,
    )

    if combined_df is not None:
        display_statistics(combined_df)

    print(f"Batch ID for this ingestion run: {batch_id}")
