# Putting a dashboard on the warehouse

Every step before this one moved data toward a shape somebody could look at.
This is the looking. The dashboard is a Streamlit app: a Python file that reads
a table and draws it, top to bottom, with no callbacks and no component tree.

It reads `marts.fct_daily_prices` joined to `marts.dim_tickers` on `ticker`, and
nothing else. Not the staging model, not the raw VARIANT table. That join is the
entire reason the star schema was built in step 9, and a dashboard that reaches
past the marts to fix something is a dashboard reporting a missing mart.

## What you need before starting

- `~/.dbt/profiles.yml` already written by `scripts/set_dbt_profile.py`. The
  dashboard does not reuse that credential -- it gets its own -- but the account
  identifier is read from there so it does not have to be typed twice.
- `sql/07_dashboard_reader_role.sql` run in Snowsight as ACCOUNTADMIN.
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

## 2. The identity the dashboard connects as

Everything up to this point in the project ran as one Snowflake user holding
ACCOUNTADMIN. That is defensible while the only thing holding the credential is a
laptop. It stops being defensible the moment a key is pasted into a hosting
provider's configuration for an app anyone can open, because the blast radius of
a leak becomes the whole account.

So the dashboard gets its own identity. `sql/07_dashboard_reader_role.sql`
creates a `dash_wh` warehouse, a `dashboard_reader` role granted `SELECT` on the
marts schema and nothing else, and a `dashboard_app` user to hold that role.

That user is created with `TYPE = SERVICE`, which is the part worth reading
twice. Snowflake's service users cannot log in with a password and cannot log in
with SAML -- not by policy, but because the account type has nowhere to put a
password. A credential that does not exist cannot be phished, guessed, reused
from another site, or left in a screenshot. It also means the user cannot be
tested in Snowsight at all, which is why section 4 exists.

Then generate its key pair:

```
cd ~/Projects/market-data-pipeline
python scripts/set_dashboard_profile.py
```

This writes `.streamlit/secrets.toml` -- where Streamlit looks when the app is
launched from the repository root, ignored by git, mode 600 -- and puts an
`ALTER USER ... SET RSA_PUBLIC_KEY` statement on the clipboard to run in
Snowsight. It goes to the clipboard rather than the screen because the statement
is around four hundred characters, and selecting a line that long out of a
terminal is how it arrives truncated.

The passphrase is generated rather than chosen, so it is never typed and cannot
be one reused from somewhere else. The private key and the config are both
created with `O_EXCL` at mode 600, and the script re-reads the mode afterwards
and refuses to report success unless it is 600. Both halves exist because of a
real failure: the mode argument to `os.open` is a *creation* mode, and when the
path already exists it is ignored entirely, so the file silently keeps whatever
permissions it had. A leftover file at that path is enough to publish a
passphrase to every account on the machine, and nothing else in the flow would
notice. `O_EXCL` removes the question by refusing to open an existing path at all.

### Checking the key landed

`DESC USER dashboard_app;` shows an `RSA_PUBLIC_KEY_FP` row -- a SHA-256 hash of
the public key -- which should equal the fingerprint the script printed. Do not
compare them by eye. They are base64, and lowercase `l` and uppercase `I` are the
same pixels in most fonts; that mistake has already been made once here, by the
person who had just finished warning against it.

The stronger check needs no reading at all. Key-pair authentication works by the
client signing a token with the private key and Snowflake verifying it with the
public key it holds, and if those do not match, authentication fails outright.
So a successful connection *is* the proof -- and section 4 makes one.

## 3. Run it

```
cd ~/Projects/market-data-pipeline
streamlit run dashboard/app.py
```

It opens on `localhost:8501`. The first load wakes the warehouse, so expect a
few seconds before anything appears; after that, changing tickers is instant.

## 4. Prove the role is actually restricted

```
cd ~/Projects/market-data-pipeline
python scripts/check_dashboard_grants.py
```

`sql/07_dashboard_reader_role.sql` says what `dashboard_reader` is allowed to do,
and this file could say the same thing. Neither is evidence. A `GRANT` that was
never run, a schema added later, or the role granted somewhere unexpected all
leave the prose looking correct. This script connects as the real user over the
real path and asks the warehouse, which is also the only way to test a `SERVICE`
user at all.

Two things about how it is built:

**The readable tables are checked first, and they are the canary.** A test that
only checks "staging is refused" passes just as happily when the connection is
broken and everything is refused. If the two marts reads fail, the script reports
`INCONCLUSIVE` rather than `PASS` -- it will not claim the refusals proved
anything.

**It also tries to create a table in marts.** `SELECT`-only is a different claim
from "cannot see staging", and a role can be scoped to the right schema and still
be able to write in it.

Measured 2026-09-12: 12,800 rows readable in `fct_daily_prices`, 25 in
`dim_tickers`, `staging.stg_prices` and `raw.raw_prices` both refused, and
`CREATE TABLE` in marts refused.

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

## Why the chart is Altair and not `st.line_chart`

`st.line_chart` is one line and it was the first version of this page. It has one
limit that matters here: it forces the y-axis to include zero, and no argument
turns that off. Apple traded between roughly 180 and 345 over the window, so half
the plot was spent on a region the data never visits and the entire two-year
shape was compressed into the top third. Nobody compares a share price to zero.

Altair ships as a Streamlit dependency, so replacing that one call costs no new
package. The chart sets its own y-domain from the data with four percent of air
on each side and `nice: False`, so the padding stays the padding instead of Vega
rounding outward to a tidy number.

Three other things came with the switch, and each is a decision rather than a
default:

- **The crosshair finds the date.** A vertical rule snaps to the nearest session
  and both series report at once. The reader aims at a date, not at a two-pixel
  line, and never has to land on a line to get its value.
- **Markers appear only under the crosshair.** A dot on every one of five hundred
  points is noise; a dot on the one being pointed at is an answer.
- **The tooltip formats the average as text, not as a number.** A number format
  applied to a null renders the literal string `null`, which is the same mistake
  as drawing a zero -- a machine token standing where a reader expects a
  statement about the data. It renders an em dash, the way the tiles do.

The line colours are categorical slots one and two of a validated palette,
stepped separately for light and dark backgrounds, and the app reads Streamlit's
theme to choose. They were checked against both surfaces for lightness, chroma,
contrast, and separation under protanopia and tritanopia rather than picked by
eye: the first version used two blues that were nearly indistinguishable.

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

The app connects as `dashboard_app`, not as the dbt user -- see section 2. What
travels to Streamlit is a key that can read two tables.

**A deployed app that cannot reach Snowflake shows an error, not a blank page.**
If the account is suspended — a trial ending, say — the app goes dark until it is
reachable again.

## What is missing

**Nothing tells you the data is stale.** The footer reports the latest trade
date and when Snowflake last loaded a row, which is enough for a person reading
carefully, and nothing at all for a person glancing. The freshness check that
exists lives in dbt and fails a build; it does not reach this page.

**The cache has no manual invalidation.** A pipeline run that lands five minutes
after a page load is invisible for the rest of the hour. For a source that
publishes once a day this is the right trade, and it is still a trade.
