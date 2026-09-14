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

import altair as alt
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


def _fetch_marts() -> pd.DataFrame:
    """
    One attempt at the marts query, on whatever connection is cached.

    Split out from load_prices so that the retry there has something to repeat.
    """
    cursor = _connection().cursor()
    try:
        cursor.execute(MARTS_QUERY)
        return cursor.fetch_pandas_all()
    finally:
        cursor.close()


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
        frame = _fetch_marts()
    except connector.errors.DatabaseError:
        # A cached connection outlives the session Snowflake gave it. The object
        # stays valid; the token inside it does not. The failure surfaces here,
        # on execute, because building a cursor is local work that never reaches
        # the server -- so an earlier version of this guard, which wrapped the
        # cursor construction, could not fire. The first visitor after an idle
        # afternoon got 390114 as a stack trace instead.
        #
        # Deliberately not narrowed to the expired-token error code. A code list
        # can be incomplete, and a guard that silently fails to fire is worse
        # than one that occasionally repeats a query it did not need to. A real
        # SQL fault fails the same way the second time and is re-raised; the
        # only cost is one wasted round trip.
        _connection.clear()
        frame = _fetch_marts()

    # Snowflake returns identifiers folded to upper case. Fold them back once,
    # here, so that nothing downstream has to remember which case it is in.
    frame.columns = [column.lower() for column in frame.columns]
    frame["trade_date"] = pd.to_datetime(frame["trade_date"])
    return frame


# Categorical slots 1 and 2 -- blue and orange -- stepped separately for the two
# surfaces Streamlit renders on. Both pairs were run through a palette validator
# against the actual backgrounds (#ffffff and #0e1117) rather than chosen by eye:
# lightness band, chroma floor, contrast, and colour-blind separation under
# protanopia and tritanopia. Worst-case separation is dE 24.7 light / 26.8 dark
# against a floor of 8, so the two lines stay distinguishable to a reader who
# cannot separate red from green -- which the two near-identical blues this
# replaced did not.
SERIES_COLOURS = {
    "light": ["#2a78d6", "#eb6834"],
    "dark": ["#3987e5", "#d95926"],
}


def series_colours() -> list[str]:
    """
    Streamlit infers the theme from the rendered background and exposes it here.
    It can be wrong on the very first paint of a session, which costs one frame
    in the other palette and then corrects itself -- acceptable, given both
    palettes pass their checks on both surfaces.
    """
    try:
        mode = st.context.theme.type
    except Exception:
        mode = "dark"
    return SERIES_COLOURS.get(mode, SERIES_COLOURS["dark"])


def money(value, places: int = 2) -> str:
    """A price, or an em dash. Never a zero standing in for an absent number."""
    if value is None or pd.isna(value):
        return "\u2014"
    return f"${value:,.{places}f}"


def compact_money(value) -> str:
    """Dollar volume runs to eleven digits; nobody reads eleven digits."""
    if value is None or pd.isna(value):
        return "\u2014"
    for cutoff, suffix in ((1e12, "T"), (1e9, "B"), (1e6, "M"), (1e3, "K")):
        if abs(value) >= cutoff:
            return f"${value / cutoff:,.2f}{suffix}"
    return f"${value:,.0f}"


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

latest = history.iloc[-1]

st.subheader(f"{labels[selected]} - {history.index[-1]:%d %b %Y}")

# Four tiles, and three of them can legitimately be empty. high_52w and low_52w
# are null until 252 sessions sit behind the row, and daily_return_pct is null on
# a ticker's first session. Each renders an em dash rather than a zero, for the
# same reason the chart renders a gap.
tiles = st.columns(4)
tiles[0].metric(
    "Close",
    money(latest["close"]),
    None if pd.isna(latest["daily_return_pct"]) else f"{latest['daily_return_pct']:+.2f}%",
    border=True,
)
tiles[1].metric("52-week high", money(latest["high_52w"]), border=True)
tiles[2].metric("52-week low", money(latest["low_52w"]), border=True)
tiles[3].metric("Dollar volume", compact_money(latest["dollar_volume"]), border=True)

# Altair rather than st.line_chart, for one reason: st.line_chart forces the
# y-axis to include zero, and no argument turns that off. On a price series that
# is half the plot spent on a region the data never visits -- nobody compares a
# share price to zero -- so two years of movement gets squeezed into the top
# third. Altair ships with Streamlit, so this costs no dependency.
chart_frame = (
    history[["close", "moving_avg_20d"]]
    .rename(columns={"close": "Close", "moving_avg_20d": "20-session average"})
    .reset_index()
)
SERIES = ["Close", "20-session average"]

# Fit the axis to the data with a little air, and turn `nice` off so the padding
# is the padding rather than Vega rounding outward to a tidy number.
low = chart_frame[SERIES].min().min()
high = chart_frame[SERIES].max().max()
air = (high - low) * 0.04

# The crosshair finds the X: the reader aims at a date, not at a 2px line, and
# gets both series at once whether or not the pointer landed on either.
hover = alt.selection_point(
    nearest=True, on="pointerover", fields=["trade_date"], empty=False
)

base = alt.Chart(chart_frame).transform_fold(SERIES, as_=["series", "value"])

lines = base.mark_line(strokeWidth=2, strokeJoin="round", strokeCap="round").encode(
    x=alt.X("trade_date:T", title=None),
    y=alt.Y(
        "value:Q",
        title=None,
        scale=alt.Scale(domain=[low - air, high + air], nice=False),
    ),
    color=alt.Color(
        "series:N",
        title=None,
        scale=alt.Scale(domain=SERIES, range=series_colours()),
        legend=alt.Legend(orient="bottom", symbolType="stroke"),
    ),
)

# Dots appear only under the crosshair. A marker on every one of 512 points is
# chaos; a marker on the one the reader is pointing at is an answer.
dots = lines.mark_point(size=64, filled=True).encode(
    opacity=alt.condition(hover, alt.value(1), alt.value(0))
)

crosshair = (
    alt.Chart(chart_frame)
    .mark_rule(strokeWidth=1)
    # The average is null for a ticker's first nineteen sessions, and a number
    # format applied to nothing renders the literal string "null" in the
    # tooltip. That is the same mistake as drawing a zero: a machine token
    # standing where a reader expects a statement about the data. Formatting it
    # here, as text, means the absence is spelled the same way the tiles spell
    # it -- and the guard sits next to the only two fields that can be absent.
    .transform_calculate(
        close_label="format(datum['Close'], '$,.2f')",
        average_label=(
            "isValid(datum['20-session average'])"
            " ? format(datum['20-session average'], '$,.2f')"
            " : '\u2014'"
        ),
    )
    .encode(
        x="trade_date:T",
        opacity=alt.condition(hover, alt.value(0.35), alt.value(0)),
        tooltip=[
            alt.Tooltip("trade_date:T", title="Session", format="%d %b %Y"),
            alt.Tooltip("close_label:N", title="Close"),
            alt.Tooltip("average_label:N", title="20-session avg"),
        ],
    )
    .add_params(hover)
)

st.altair_chart(alt.layer(lines, dots, crosshair).properties(height=380))
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
