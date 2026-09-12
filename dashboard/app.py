"""
The presentation layer -- the last node in the diagram at the top of the README.

Reads market_data.marts.fct_daily_prices joined to market_data.marts.dim_tickers
and nothing else. Not staging, and not the raw VARIANT table. The star schema in
step 9 was built so that this file could be short, and a dashboard that reaches
past the marts to fix something is a dashboard reporting a missing mart.

Launch it from the repository root, which is where Streamlit looks for
.streamlit/secrets.toml:

    conda deactivate
    source .venv/bin/activate
    streamlit run dashboard/app.py
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st
from cryptography.hazmat.primitives import serialization
from snowflake import connector

# One hour. Streamlit re-executes this entire file top to bottom on every widget
# interaction -- that is its execution model, not a bug -- so an uncached query
# would mean a Snowflake round trip for every dropdown change by every visitor.
# This repository is public and the app is deployed publicly, so that traffic is
# not something this project controls. The warehouse is XSMALL with a 60-second
# minimum charge on each wake, and the pipeline loads once a day: an hour-old
# answer is never more than one load stale, and it is the difference between a
# few cents a month and a bill that scales with strangers clicking.
CACHE_TTL_SECONDS = 60 * 60

MARTS_QUERY = """
select
    f.ticker,
    d.company_name,
    d.sector,
    f.trade_date,
    f.close,
    f.volume,
    f.prior_close,
    f.daily_return_pct,
    f.moving_avg_20d,
    f.high_52w,
    f.low_52w,
    f.dollar_volume,
    f.loaded_at
from market_data.marts.fct_daily_prices as f
join market_data.marts.dim_tickers as d
    on f.ticker = d.ticker
order by f.ticker, f.trade_date
"""


def _private_key_der() -> bytes:
    """
    Load the Snowflake private key and hand back the DER bytes the connector
    wants.

    Two sources, because the two places this app runs differ in exactly one way.
    On this laptop the key is a file in ~/.snowflake and secrets.toml names its
    path. On Streamlit Community Cloud there is no such file and no way to put
    one there, so the PEM text itself is pasted into their secrets store as
    private_key_pem. Everything downstream of this function is identical.

    The key on disk is encrypted with a passphrase that was generated rather
    than chosen, so it is never typed anywhere; it travels with the key.
    """
    config = st.secrets["snowflake"]

    pem = config.get("private_key_pem")
    if pem is None:
        pem = Path(config["private_key_path"]).expanduser().read_bytes()
    else:
        pem = pem.encode()

    passphrase = config.get("private_key_passphrase") or None
    key = serialization.load_pem_private_key(
        pem,
        password=passphrase.encode() if passphrase else None,
    )

    return key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


@st.cache_resource(show_spinner=False)
def _connection():
    """
    A connection is a resource, not data: st.cache_resource hands every visitor
    the same object instead of trying to copy it, which is what st.cache_data
    would do and what a socket cannot survive.
    """
    config = st.secrets["snowflake"]
    return connector.connect(
        account=config["account"],
        user=config["user"],
        role=config["role"],
        warehouse=config["warehouse"],
        database=config["database"],
        schema=config["schema"],
        private_key=_private_key_der(),
    )


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner="Reading the marts layer...")
def load_prices() -> pd.DataFrame:
    """
    Every row of the fact table, once an hour, for the whole app.

    The obvious alternative is to query one ticker at a time and cache per
    ticker. This pulls all twenty-five instead, on purpose: the table is a few
    thousand rows per ticker and roughly a megabyte and a half in total, which
    is nothing to move and nothing to hold, and pulling it in one shot means the
    number of Snowflake queries per hour is one no matter how many people are
    clicking or how many tickers they compare. Switching tickers then costs a
    dataframe filter rather than a warehouse wake.

    That trade stops being right somewhere north of a million rows. At this
    grain -- one row per ticker per trading day, twenty-five tickers -- that is
    over a century away.
    """
    try:
        cursor = _connection().cursor()
    except Exception:
        # A cached connection outlives the session Snowflake gave it. Drop the
        # cached object and let the next call build a fresh one rather than
        # showing a stack trace to whoever happened to arrive first after the
        # timeout.
        _connection.clear()
        cursor = _connection().cursor()

    try:
        cursor.execute(MARTS_QUERY)
        frame = cursor.fetch_pandas_all()
    finally:
        cursor.close()

    # Snowflake returns identifiers folded to upper case. Fold them back once,
    # here, so that nothing downstream has to remember which case it is in.
    frame.columns = [column.lower() for column in frame.columns]
    frame["trade_date"] = pd.to_datetime(frame["trade_date"])
    return frame


# Has to run before any other Streamlit call in the script, including the
# spinner that load_prices() puts on screen while it waits on Snowflake.
st.set_page_config(
    page_title="Market Data Pipeline",
    page_icon=":chart_with_upwards_trend:",
    layout="wide",
)

prices = load_prices()

st.title("Market data pipeline")
st.caption(
    "Daily equity prices for 25 large-cap tickers: Polygon to S3 to Snowflake, "
    "transformed with dbt, orchestrated by Airflow, read from the marts layer."
)

catalogue = (
    prices[["ticker", "company_name", "sector"]]
    .drop_duplicates()
    .sort_values("ticker")
)
labels = {row.ticker: f"{row.ticker} - {row.company_name}" for row in catalogue.itertuples()}

with st.sidebar:
    st.header("Ticker")
    selected = st.selectbox(
        "Ticker",
        options=list(labels),
        format_func=lambda ticker: labels[ticker],
        label_visibility="collapsed",
    )
    st.caption(catalogue.loc[catalogue["ticker"] == selected, "sector"].iloc[0])

history = prices[prices["ticker"] == selected].set_index("trade_date")

st.subheader(f"{labels[selected]} - closing price")
st.line_chart(
    history[["close", "moving_avg_20d"]].rename(
        columns={"close": "Close", "moving_avg_20d": "20-session average"}
    )
)
st.caption(
    "The 20-session average is blank for each ticker's first nineteen sessions. "
    "dbt leaves it null rather than averaging a partial window, and a gap in the "
    "line is what null looks like -- a zero would be a claim the price was zero."
)

latest_trade = prices["trade_date"].max().date()
loaded_at = prices["loaded_at"].max()
st.caption(
    f"Data as of {latest_trade}. Last row written to Snowflake at "
    f"{loaded_at:%Y-%m-%d %H:%M} UTC. Cached for one hour."
)
