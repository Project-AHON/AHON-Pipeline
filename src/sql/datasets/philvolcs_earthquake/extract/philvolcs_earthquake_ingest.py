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

MONTH_NAMES = ["January", "February", "March", "April", "May", "June",
                "July", "August", "September", "October", "November", "December"]


def scrape_current_month_from_main_page():
    """
    Scrapes the latest earthquake data from the main PHIVOLCS page.
    This is used for the current month that doesn't have a dedicated monthly page yet.
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
            'Date-Time',
            'Latitude',
            'Longitude',
            'Depth',
            'Magnitude',
            'Location'
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
            df['Date-Time'].astype(str).str.contains('Date|Time|Philippine', case=False, na=False) |
            df['Latitude'].astype(str).str.contains('Latitude|ºN|°N', case=False, na=False) |
            df['Longitude'].astype(str).str.contains('Longitude|ºE|°E', case=False, na=False)
        )
        df = df[~mask].reset_index(drop=True)

        if not df.empty:
            first_col = df.iloc[:, 0].astype(str).str.strip()
            summary_mask = first_col.str.lower().str.contains('total|no. of events', na=False, regex=True)
            month_abbrev_mask = first_col.str.match(r'^[A-Z][a-z]{2}-\d{2}$', na=False)
            df = df[~(summary_mask | month_abbrev_mask)]

        df = df.dropna(how='all').reset_index(drop=True)

        current_month = datetime.now(timezone.utc).strftime("%B")
        current_year = datetime.now(timezone.utc).year

        df['Month'] = current_month
        df['Year'] = current_year

        print(f"✓ {len(df)} records")

        return df

    except (requests.RequestException, pd.errors.ParserError) as e:
        print(f"✗ Error: {e}")
        return None


def scrape_phivolcs_data_from_html(year, month_name):
    """
    Fetches earthquake data by reading the HTML table from the PHIVOLCS monthly page.
    If the monthly page returns 404, it will try scraping from the main page.
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
            'Date-Time',
            'Latitude',
            'Longitude',
            'Depth',
            'Magnitude',
            'Location'
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
            df['Date-Time'].astype(str).str.contains('Date|Time|Philippine', case=False, na=False) |
            df['Latitude'].astype(str).str.contains('Latitude|ºN|°N', case=False, na=False) |
            df['Longitude'].astype(str).str.contains('Longitude|ºE|°E', case=False, na=False)
        )
        df = df[~mask].reset_index(drop=True)

        if not df.empty:
            first_col = df.iloc[:, 0].astype(str).str.strip()
            summary_mask = first_col.str.lower().str.contains('total|no. of events', na=False, regex=True)
            month_abbrev_mask = first_col.str.match(r'^[A-Z][a-z]{2}-\d{2}$', na=False)
            df = df[~(summary_mask | month_abbrev_mask)]

        df = df.dropna(how='all').reset_index(drop=True)

        df['Month'] = month_name
        df['Year'] = year

        print(f"✓ {len(df)} records")

        return df

    except requests.exceptions.HTTPError as errh:
        if errh.response.status_code == 404:
            print("✗ HTTP 404 (trying main page)")
            return scrape_current_month_from_main_page()
        else:
            print(f"✗ HTTP {errh.response.status_code}")
            return None
    except requests.RequestException as e:
        print(f"✗ Error: {e}")
        return None
