# Putting a dashboard on the warehouse

Every step before this one moved data toward a shape somebody could look at.
This is the looking. The dashboard is a Streamlit app: a Python file that reads
a table and draws it, where the drawing code is `st.line_chart(frame)` rather
than a chart library's object model.

It reads `marts.fct_daily_prices` joined to `marts.dim_tickers` on `ticker`, and
nothing else. Not the staging model, not the raw VARIANT table. That join is the
entire reason the star schema was built in step 9, and a dashboard that reaches
past the marts to fix something is a dashboard reporting a missing mart.

## What you need before starting

- `~/.dbt/profiles.yml` already written by `scripts/set_dbt_profile.py`, with a
  working key pair. The dashboard does not set up its own authentication; it
  reuses what dbt proved.
- The marts built at least once, so `dim_tickers` and `fct_daily_prices` exist.
- The project venv. A fresh Terminal starts in conda's `base`, which is a
  different Python.

About fifteen minutes.

## 1. Install

```
cd ~/Projects/market-data-pipeline
conda deactivate
source .venv/bin/activate
pip install -r dashboard/requirements.txt
```

That file is separate from the one at the repository root on purpose. The root
file pins dbt-core, dbt-snowflake and boto3 — the ingestion and transformation
stack, none of which the dashboard imports. Streamlit Community Cloud installs
whatever requirements file sits beside the app it is told to run, and pointing
the deployment at the root file would have the free-tier runner build the entire
pipeline in order to draw a line chart.

Two things in it are worth reading rather than skipping. `snowflake-connector-python`
is requested with its `[pandas]` extra, which is what makes `fetch_pandas_all()`
available: Snowflake returns Arrow batches, and the extra adds the pandas and
pyarrow needed to turn those into a dataframe without a Python-level row loop.
And its version is pinned to match what dbt-snowflake already installed into the
same virtual environment, so there is one connector in there rather than two
arguing about which one wins.

## 2. Connection

```
cd ~/Projects/market-data-pipeline
python scripts/set_dashboard_profile.py
```

This writes `.streamlit/secrets.toml`, which is where Streamlit looks for
configuration when the app is launched from the repository root. It is ignored
by git and the script sets it to mode 600.

It derives every value from `~/.dbt/profiles.yml` rather than prompting, and the
reason is the passphrase. `set_dbt_profile.py` generated that passphrase instead
of asking you to choose one, specifically so it would never have to be typed.
Prompting for it here would undo that: it would land in a terminal buffer, and
possibly in a shell history file, for no gain over reading it off a file that is
already on the disk with the right permissions.

The script chmods the file after writing and then re-reads the mode and refuses
to report success unless it is 600. That assertion exists because of a real
failure: the mode argument to `os.open` is a *creation* mode, and when the path
already exists it is ignored entirely, so the file silently keeps whatever
permissions it had. A leftover file at that path is enough to publish a
passphrase to every account on the machine, and nothing else in the flow would
have noticed.

## 3. Run it

```
cd ~/Projects/market-data-pipeline
streamlit run dashboard/app.py
```

It opens on `localhost:8501`. The first load wakes the warehouse, so expect a
few seconds before anything appears; after that, changing tickers is instant.

## What the page shows, and one thing that looks like a bug

A ticker picker, the closing price, and the twenty-session moving average drawn
on top of it.

The moving average does not start where the price does. It begins nineteen
sessions later, and between the two start points only one line exists. That gap
is correct and it is the point: `fct_daily_prices` computes the average only
when a full twenty sessions are behind the row, and leaves it null otherwise. A
partial window is not a smaller answer to the same question, it is an answer to
a different one. The chart renders null as an absence rather than a zero,
because a zero would be a claim that the price was zero.

The same is true of the 52-week high and low, which are null across roughly half
the history for the same reason.

## Why the app pulls all twenty-five tickers at once

Streamlit's execution model is that the entire script re-runs, top to bottom, on
every interaction with every widget. That is not a quirk to work around; it is
how the framework stays simple. It does mean that a query written in the obvious
place runs again on every click.

So every function that touches Snowflake is wrapped in `@st.cache_data(ttl=3600)`,
and the one that does is written to fetch the whole fact table rather than one
ticker at a time. The table is a few thousand rows per ticker and around a
megabyte and a half in total, which is nothing to move and nothing to hold. The
payoff is that the number of Snowflake queries per hour is one, regardless of
how many people are looking at the app or how many tickers they compare, and
switching tickers costs a dataframe filter instead of waking a warehouse.

The trade stops being right somewhere north of a million rows. At one row per
ticker per trading day, with twenty-five tickers, that is more than a century
away — and the alternative is a public app where every stranger's click is
billable.

## Deploying to Streamlit Community Cloud

Free for public repositories. Point it at `dashboard/app.py` as the entrypoint.

The configuration goes into the app's own secrets store in the Streamlit
dashboard, never into the repository. It takes the same keys as the local file
with one difference: there is no `~/.snowflake` directory on the runner, so the
key cannot be referenced by path. Drop `private_key_path` and supply
`private_key_pem` instead, as a TOML multi-line string containing the entire
contents of the `.p8` file — header and footer lines included, exactly as they
appear on disk. `dashboard/app.py` accepts either form and converts both to the
DER bytes the connector wants, so nothing else changes.

Two things to settle before the app is public:

**The account it connects as.** The dbt profile connects with a role that has
far more authority than a dashboard needs. A deployed app should have its own
Snowflake user with its own key pair and a role granted `SELECT` on the marts
schema and nothing else. The app cannot write, so nothing is lost.

**What happens when the warehouse is gone.** A deployed app that cannot reach
Snowflake shows an error, not a blank page. If the account is suspended — a
trial ending, say — the app goes dark until it is reachable again.

## What is missing

**Nothing tells you the data is stale.** The footer reports the latest trade
date and when Snowflake last loaded a row, which is enough for a person reading
carefully, and nothing at all for a person glancing. The freshness check that
exists lives in dbt and fails a build; it does not reach this page.

**The cache has no manual invalidation.** A pipeline run that lands five minutes
after a page load is invisible for the rest of the hour. For a source that
publishes once a day this is the right trade, and it is still a trade.
