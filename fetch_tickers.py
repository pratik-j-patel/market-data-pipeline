"""
Step 3 of the market data pipeline: fetch daily bars for a list of tickers over a
date range and write them to disk, partitioned by trade date.

notebooks/step_3_ticker_loop.ipynb explains every design decision in this file,
measurement by measurement. Run this with:

    python fetch_tickers.py --backfill     # once: ~2 years of history, ~5 minutes
    python fetch_tickers.py                # every run after that: 7-day trailing window

DESIGN, in one paragraph
------------------------
Every run fetches a trailing WINDOW of days and writes by overwriting, never by
appending. Rerun it and nothing changes; skip three days and the next run backfills
them; a day that published late is picked up automatically. That property is called
idempotency and it is the entire point of this step. The write merges on
(trade_date, ticker), so a ticker that fails on this run keeps the rows it already
had rather than having them wiped.

OUTPUT
------
    data/date=2026-08-24/prices.jsonl        one line per ticker for that TRADE day
    data/_raw/run=<run_id>/AAPL.json         what the API literally returned, per run
    data/_runs/run_<run_id>.json             what this run asked for vs. what it got

A notebook that works and a script that works are two different claims. Step 4
commits this file; cron in step 10 calls this file.
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "data"

# Market dates are Eastern. "What trading day is it?" is always an ET question:
# after 8pm ET the UTC date is already tomorrow, which silently shifts the window
# and inflates the reported lag by a day. Instants (run ids, ingest stamps) stay
# UTC -- on 2026-11-01 clocks fall back and 1:30am ET happens twice.
ET = ZoneInfo("America/New_York")

# Pass the path explicitly. Bare load_dotenv() inspects the call stack to find
# the calling file, which breaks when the script is piped in from stdin or run
# by cron from a different working directory. Cron is step 10; get this right now.
load_dotenv(PROJECT_ROOT / ".env")

API_KEY = os.getenv("POLYGON_API_KEY")
if not API_KEY:
    sys.exit(
        "POLYGON_API_KEY not found.\n"
        "Expected a .env file next to this script containing:\n"
        "  POLYGON_API_KEY=your_key_here\n"
        "Set or rotate it with scripts/set_api_key.sh -- not by hand."
    )

BASE_URL = "https://api.polygon.io"

# Auth in a HEADER, never in the URL. requests quotes the full URL inside its
# exception messages, so a key in ?apiKey= leaks into every traceback and log.
HEADERS = {"Authorization": f"Bearer {API_KEY}"}

# Free tier: 5 calls/min. 60/5 = 12s, plus margin -- the API measures against
# its own clock, not this machine's.
CALLS_PER_MIN = 5
SLEEP_SECONDS = 60 / CALLS_PER_MIN + 0.5

DEFAULT_WINDOW_DAYS = 7      # trailing window: covers a weekend, a holiday, and a lag day
BACKFILL_DAYS = 730          # ~2 years, the one-time history load


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def ms_epoch_to_date(ms: int) -> str:
    # Milliseconds since 1970-01-01 UTC -> YYYY-MM-DD. tz=utc stops Python
    # silently using the local timezone and shifting bars onto the wrong day.
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def load_tickers(path: Path) -> list:
    # One symbol per line; blank lines and # comments ignored.
    out = []
    for line in path.read_text().splitlines():
        line = line.split("#")[0].strip()
        if line:
            out.append(line.upper())
    return out


def bar_to_row(ticker: str, bar: dict, ingested_at: str) -> dict:
    # Rename the API's one-letter keys. o/h/l/c/v is standard in market data,
    # but nobody should have to remember that while debugging a dbt model.
    return {
        "ticker": ticker,
        "trade_date": ms_epoch_to_date(bar["t"]),
        "open": bar.get("o"),
        "high": bar.get("h"),
        "low": bar.get("l"),
        "close": bar.get("c"),
        "volume": bar.get("v"),
        "vwap": bar.get("vw"),
        "transactions": bar.get("n"),
        "ingested_at_utc": ingested_at,
    }


def day_file(trade_date: str) -> Path:
    return DATA_DIR / f"date={trade_date}" / "prices.jsonl"


def write_day_files(rows: list) -> int:
    # Group by trade date, then MERGE each day file on (trade_date, ticker).
    #
    # Why merge rather than overwrite: if NVDA failed this run, its rows are not
    # in `rows`. A blind overwrite would delete NVDA's perfectly good history from
    # every day file it touches -- a failure would destroy data. Merging replaces
    # only the tickers that succeeded and leaves the rest alone.
    by_date = {}
    for row in rows:
        by_date.setdefault(row["trade_date"], []).append(row)

    for trade_date, new_rows in sorted(by_date.items()):
        path = day_file(trade_date)
        path.parent.mkdir(parents=True, exist_ok=True)

        merged = {}
        if path.exists():
            for line in path.read_text().splitlines():
                if line.strip():
                    old = json.loads(line)
                    merged[old["ticker"]] = old

        for row in new_rows:
            merged[row["ticker"]] = row

        # Write to a temp file, then swap it in atomically. A crash mid-write
        # leaves the intact old file rather than a truncated one.
        tmp = path.with_suffix(".jsonl.tmp")
        with tmp.open("w") as f:
            for tkr in sorted(merged):
                f.write(json.dumps(merged[tkr]) + "\n")
        tmp.replace(path)

    return len(by_date)


def write_manifest(run_id, mode, start, end, requested, succeeded, failed, rows) -> dict:
    # Record what was RECEIVED, not what was requested. When the question "is
    # the data current?" comes up in November, the answer is a file rather than
    # a guess. This is also the raw material for the freshness checks in step 11.
    dates = sorted({r["trade_date"] for r in rows})
    newest = dates[-1] if dates else None
    today = datetime.now(ET).date()

    manifest = {
        "run_id": run_id,
        "mode": mode,
        "finished_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "requested_range": {"from": str(start), "to": str(end)},
        "tickers_requested": len(requested),
        "tickers_succeeded": sorted(succeeded),
        "tickers_failed": failed,
        "bars_received": len(rows),
        "trade_dates_touched": len(dates),
        "oldest_trade_date": dates[0] if dates else None,
        "newest_trade_date": newest,
        "lag_days_vs_run": (
            (today - datetime.strptime(newest, "%Y-%m-%d").date()).days if newest else None
        ),
    }

    path = DATA_DIR / "_runs" / f"run_{run_id}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, indent=2))
    return manifest


# ---------------------------------------------------------------------------
# The work
# ---------------------------------------------------------------------------

def fetch_daily_bars(ticker: str, start, end, max_retries: int = 3, verbose: bool = True) -> list:

    # Read the path as: aggregate bars for {ticker}, in buckets of 1 day, from
    # {start} to {end}. The 1/day pair is why the same endpoint gives per-minute
    # bars later without learning a new one.
    url = f"{BASE_URL}/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}"

    # Query parameters:
    #     adjusted -> "true"   corrects for splits; without it a 2-for-1 split
    #                          reads as a 50% crash
    #     sort     -> "asc"    oldest bar first
    #     limit    -> 50000    the response is paginated; a 2-year daily range is
    #                          ~500 bars, comfortably inside one page
    params = {"adjusted": "true", "sort": "asc", "limit": 50000}

    for attempt in range(1, max_retries + 1):
        response = requests.get(url, params=params, headers=HEADERS, timeout=30)

        # 429 means the rate limit was exceeded. Wait, then retry -- and wait
        # LONGER each attempt. That escalation is called backoff: 60s, 120s, 180s.
        if response.status_code == 429:
            wait = 60 * attempt
            if verbose:
                print(f"    429 on {ticker}: waiting {wait}s (attempt {attempt}/{max_retries})")
            time.sleep(wait)
            continue

        # Permanent. Retrying a rejected key 25 times wastes four minutes and
        # produces no new information, so stop the whole run here.
        if response.status_code in (401, 403):
            raise RuntimeError(
                f"HTTP {response.status_code} on {ticker}. The key was rejected or is not "
                f"entitled to this endpoint. Check .env, then scripts/set_api_key.sh."
            )

        # Probably transient -- their side, not yours.
        if response.status_code >= 500:
            wait = 5 * attempt
            if verbose:
                print(f"    HTTP {response.status_code} on {ticker}: retrying in {wait}s")
            time.sleep(wait)
            continue

        response.raise_for_status()
        body = response.json()

        # A 200 OK does not guarantee useful data. An unknown ticker returns
        # 200 with an empty results list, NOT a 404 -- so report the observable
        # facts and let the reader diagnose. Never assert a cause without proof.
        if body.get("status") not in ("OK", "DELAYED"):
            raise RuntimeError(
                f"{ticker}: HTTP {response.status_code}, api status={body.get('status')!r}, "
                f"queryCount={body.get('queryCount')}, resultsCount={body.get('resultsCount')}."
            )

        return body.get("results") or []

    raise RuntimeError(f"{ticker}: no success after {max_retries} attempts.")


def run(tickers: list, days_back: int, mode: str, verbose: bool = True) -> dict:
    run_id = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M")
    ingested_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    end = datetime.now(ET).date()
    start = end - timedelta(days=days_back)

    raw_dir = DATA_DIR / "_raw" / f"run={run_id}"
    raw_dir.mkdir(parents=True, exist_ok=True)

    print(f"Run {run_id} | mode={mode} | {len(tickers)} tickers | {start} -> {end}")
    print(f"Pacing {SLEEP_SECONDS:.1f}s between calls "
          f"(~{len(tickers) * SLEEP_SECONDS / 60:.1f} min)\n")

    all_rows, succeeded, failed = [], [], {}
    t0 = time.time()

    for i, ticker in enumerate(tickers, start=1):
        try:
            bars = fetch_daily_bars(ticker, start, end, verbose=verbose)

            # Validation happens here rather than up front. Batch validation via
            # /v3/reference/tickers?ticker.any_of= was tried on Aug 26 2026: HTTP 200,
            # but the filter was ignored (it returned 1000 rows = the limit). Per-ticker
            # validation would cost 25 calls and double every run. Zero bars over the
            # whole window is the same signal, for free.
            if not bars:
                raise RuntimeError(
                    f"0 bars over {start}..{end}. The request succeeded, so check the "
                    f"symbol in tickers.txt before assuming a market-calendar issue."
                )

            # Crash insurance. Land the raw response on disk BEFORE deriving anything
            # from it, so a failure on ticker 17 does not cost the five minutes
            # already spent on tickers 1-16.
            (raw_dir / f"{ticker}.json").write_text(json.dumps(bars))

            rows = [bar_to_row(ticker, b, ingested_at) for b in bars]
            all_rows.extend(rows)
            succeeded.append(ticker)
            if verbose:
                print(f"  [{i:>2}/{len(tickers)}] {ticker:<6} {len(rows):>4} bars")

        except Exception as exc:
            # One bad symbol must not cost the other 24 and the minutes they took.
            failed[ticker] = f"{type(exc).__name__}: {exc}"
            print(f"  [{i:>2}/{len(tickers)}] {ticker:<6} FAILED -- {failed[ticker]}")

        # No sleep after the last one.
        if i < len(tickers):
            time.sleep(SLEEP_SECONDS)

    files = write_day_files(all_rows)
    manifest = write_manifest(run_id, mode, start, end, tickers, succeeded, failed, all_rows)

    print(f"\nFinished in {time.time() - t0:.0f}s")
    print(f"  {len(succeeded)}/{len(tickers)} tickers ok, {len(failed)} failed")
    print(f"  {len(all_rows)} bars -> {files} day files under {DATA_DIR.name}/")
    print(f"  newest trade date: {manifest['newest_trade_date']} "
          f"({manifest['lag_days_vs_run']} days behind today)")
    print(f"  manifest: data/_runs/run_{run_id}.json")
    if failed:
        print(f"  FAILED: {', '.join(failed)}")

    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    parser.add_argument("--backfill", action="store_true",
                        help=f"one-time load of ~{BACKFILL_DAYS} days of history")
    parser.add_argument("--days", type=int, default=None,
                        help=f"trailing window in days (default {DEFAULT_WINDOW_DAYS})")
    parser.add_argument("--tickers", type=str, default=None,
                        help="comma-separated symbols, overriding tickers.txt")
    parser.add_argument("--quiet", action="store_true", help="one line per ticker only")
    args = parser.parse_args()

    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    else:
        tickers = load_tickers(PROJECT_ROOT / "tickers.txt")

    if not tickers:
        sys.exit("No tickers to fetch. Check tickers.txt.")

    days = args.days or (BACKFILL_DAYS if args.backfill else DEFAULT_WINDOW_DAYS)
    mode = "backfill" if args.backfill else "daily"

    manifest = run(tickers, days_back=days, mode=mode, verbose=not args.quiet)

    # Exit non-zero if anything failed, so cron in step 10 can tell the
    # difference between a run that worked and a run that merely finished.
    sys.exit(1 if manifest["tickers_failed"] else 0)


if __name__ == "__main__":
    main()
