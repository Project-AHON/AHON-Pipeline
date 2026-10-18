"""CMCI HTTP transport; source indicator definitions are provided as an explicit CSV asset."""
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def create_http_session(retries, backoff, portal_url, user_agent):
    """
    Create a reusable HTTP session with controlled retries.
    """

    retry_strategy = Retry(
        total=retries,
        connect=retries,
        read=retries,
        status=retries,
        backoff_factor=backoff,
        status_forcelist=(
            429,
            500,
            502,
            503,
            504,
        ),
        allowed_methods=(
            "GET",
            "POST",
        ),
        respect_retry_after_header=True,
    )

    adapter = HTTPAdapter(
        max_retries=retry_strategy
    )

    session = requests.Session()

    session.mount(
        "https://",
        adapter,
    )

    session.mount(
        "http://",
        adapter,
    )

    session.headers.update(
        {
            "User-Agent": user_agent,
            "Accept": (
                "text/html,"
                "application/xhtml+xml,"
                "application/xml;q=0.9,"
                "*/*;q=0.8"
            ),
            "Accept-Language": (
                "en-US,en;q=0.9"
            ),
            "Referer": portal_url,
        }
    )

    return session
