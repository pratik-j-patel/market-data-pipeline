"""
Step 2 of the market data pipeline: pull ONE ticker's daily prices from the API
and print the closing price in the terminal.

notebooks/step_2_first_api_call.ipynb documents the reasoning behind every line
of this file. To run it:

    python fetch_one_ticker.py

A notebook that works and a script that works are two different claims. Step 4
schedules this file, not the notebook.
"""

import os
import sys
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Reads the .env file next to this script and copies each KEY=value line into
# the environment for the life of this program. This is why the key is never
# in the code: the code asks the environment for it at runtime, and .env is
# gitignored.
#
# NOTE: pass the filename explicitly. Bare load_dotenv() inspects the call
# stack to find the calling file, which fails when code is piped into python
# from stdin. Explicit is more predictable.
load_dotenv(".env")

API_KEY = os.getenv("POLYGON_API_KEY")

# Fail early and clearly. Without this the API returns a confusing 401 rather
# than "the .env file is not being read".
if not API_KEY:
    sys.exit(
        "POLYGON_API_KEY not found.\n"
        "Check that a .env file exists in this folder and contains a line like:\n"
        "  POLYGON_API_KEY=your_key_here"
    )

BASE_URL = "https://api.polygon.io"
TICKER = "AAPL"


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def ms_epoch_to_date(ms: int) -> str:
    # The API sends timestamps as milliseconds since 1 Jan 1970 UTC.
    # datetime wants seconds, hence the / 1000. tz=timezone.utc stops Python
    # from silently using the local timezone and shifting the date.
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# The work
# ---------------------------------------------------------------------------

def main() -> None:

    # The "previous day's aggregate bar" endpoint. The key is deliberately not
    # in this URL -- see the header below.
    url = f"{BASE_URL}/v2/aggs/ticker/{TICKER}/prev"

    # Authentication goes in a HEADER, not in the URL. requests quotes the full
    # URL, query string included, in its exception messages, so a key in the
    # query string leaks into every traceback and log line. A header does not.
    headers = {"Authorization": f"Bearer {API_KEY}"}

    # Query parameters -- the non-secret options. "adjusted" corrects for stock
    # splits; without it a 2-for-1 split reads as a 50% crash. The value is the
    # string "true", not Python's True, because it goes into a URL.
    params = {"adjusted": "true"}

    # The timeout is not optional: without it a hung server hangs this script
    # forever, which in a scheduled job is a silent stall nobody notices.
    response = requests.get(url, params=params, headers=headers, timeout=10)

    # --- Error handling -- provided ----------------------------------------
    if response.status_code == 429:
        sys.exit("Rate limited (429). Free tier is 5 calls/min -- wait a minute and retry.")
    if response.status_code == 401:
        sys.exit("Unauthorized (401). The API key was rejected -- check the value in .env.")
    if response.status_code == 403:
        sys.exit("Forbidden (403). Key is valid but not entitled to this endpoint.")

    # Any other 4xx/5xx becomes an exception rather than carrying on with garbage.
    response.raise_for_status()

    data = response.json()

    # A 200 OK does not guarantee useful data. The API sends its own status
    # field, and on a non-trading day results comes back empty. Check the body,
    # not just the HTTP code.
    if data.get("status") not in ("OK", "DELAYED"):
        sys.exit(f"API returned status={data.get('status')!r}: {data}")

    results = data.get("results") or []
    if not results:
        sys.exit(
            f"No bars returned for {TICKER!r}. "
            f"HTTP {response.status_code}, api status={data.get('status')!r}, "
            f"queryCount={data.get('queryCount')}, "
            f"resultsCount={data.get('resultsCount')}. "
            "Check the symbol is real before assuming a market-calendar issue."
        )

    bar = results[0]  # one day of open/high/low/close/volume -- a "bar"

    # "c" is the close price; "t" is the timestamp in epoch milliseconds.
    # Target output:   AAPL closed at $310.34 on 2026-08-24
    # {value:.2f} formats a float to two decimal places -- right for money.
    close_price = bar["c"]
    trade_date = ms_epoch_to_date(bar["t"])

    print(f"{TICKER} closed at ${close_price:.2f} on {trade_date}")


if __name__ == "__main__":
    main()
