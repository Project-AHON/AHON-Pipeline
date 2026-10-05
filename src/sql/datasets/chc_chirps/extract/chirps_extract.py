from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

CHIRPS_BASE_URL = (
    "https://data.chc.ucsb.edu/products/"
    "CHIRPS/v3.0/monthly/global/tifs"
)

DEFAULT_OUTPUT_DIR = Path("data/raw/chc_chirps")


def build_chirps_url(year: int, month: int) -> str:
    """
    Build the URL for a CHIRPS v3.0 monthly GeoTIFF.
    """

    filename = f"chirps-v3.0.{year}.{month:02d}.tif"

    return f"{CHIRPS_BASE_URL}/{filename}"


def check_url(url: str) -> bool:
    """
    Check whether a CHIRPS file is available.
    """

    request = Request(
        url,
        method="HEAD",
        headers={
            "User-Agent": "AHON-Pipeline/1.0"
        },
    )

    try:
        with urlopen(request, timeout=30):
            return True

    except HTTPError as error:
        if error.code == 404:
            return False

        raise

    except URLError as error:
        print(f"Connection error: {error}")
        return False


def download_file(url: str, output_path: Path) -> bool:
    """
    Download a file from a URL.
    """

    request = Request(
        url,
        headers={
            "User-Agent": "AHON-Pipeline/1.0"
        },
    )

    try:
        with (
            urlopen(request, timeout=60) as response,
            output_path.open("wb") as file,
        ):
            file.write(response.read())

        return True

    except (HTTPError, URLError) as error:
        print(f"Download failed: {error}")
        return False


def download_chirps_month(
    year: int,
    month: int,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
) -> str:

    url = build_chirps_url(year, month)

    filename = f"chirps-v3.0.{year}.{month:02d}.tif"

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path = output_dir / filename

    print(f"\nChecking: {filename}")

    if output_path.exists():
        print("Status: already downloaded")
        return "already_exists"

    if not check_url(url):
        print("Status: unavailable")
        return "unavailable"

    print("Status: available")
    print(f"Downloading: {url}")

    success = download_file(
        url,
        output_path,
    )

    if success:
        print(f"Saved: {output_path}")
        return "downloaded"

    return "failed"


if __name__ == "__main__":

    test_months = [
        (2018, 1),
        (2018, 2),
        (2018, 3),
    ]

    for year, month in test_months:

        download_chirps_month(
            year=year,
            month=month,
        )