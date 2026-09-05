# Building the staging model with dbt

`raw_prices` is a faithful copy of what is in S3: one VARIANT column holding an
untouched JSON object, and three lineage columns. Nothing in it has a type yet.
This step gives it one.

dbt is the tool that does it. The idea is smaller than the vocabulary around it:
you write a SELECT statement in a `.sql` file, and `dbt run` wraps it in a
`CREATE TABLE AS` and executes it against the warehouse. That is essentially the
whole product. What you get on top of plain SQL scripts is that the models know
about each other, so dbt builds them in the right order, and that the SQL is a
file in a repository rather than a query in a browser tab.

This project has one model, `stg_prices`. v1 does not need more.

## What you need before starting

- The Snowflake account identifier: the account locator and region joined by a
  dot, e.g. `abc12345.us-east-1`. Snowsight shows the locator under the account
  menu. It is not committed here, for the same reason the AWS account ID is not.
- Your Snowflake user, and its password.
- `sql/06_snowflake_raw_load.sql` already run, so `market_data.raw.raw_prices`
  exists and has rows in it.

Roughly 45 minutes if nothing fights you, and most of the risk is in the first
two sections. Once `dbt debug` passes, the rest is fast.

## 1. Install

The venv matters here. A fresh Terminal window starts in conda's `base`
environment, and installing into that instead is how you end up with dbt on a
different Python than the one holding `requests` and `boto3`.

```bash
cd ~/Projects/market-data-pipeline
conda deactivate
source .venv/bin/activate
which python
```

That last command must print a path ending in
`market-data-pipeline/.venv/bin/python`. If it prints anything else, stop and
fix it before installing — everything below assumes it.

```bash
pip install -r requirements.txt && dbt --version
```

Expect `Core: installed: 1.12.0` and `snowflake: 1.12.0`. It pulls around eighty
packages, one of which is a 43 MB wheel, so give it a couple of minutes.

**dbt will then say `Your version of dbt-core is out of date!` and point at
1.12.3. Ignore it, and do not upgrade.** 1.12.3 is the version that cannot
install on macOS at all -- it is the whole reason the two pins below exist. dbt
compares against the newest release on PyPI and has no idea that release has no
Mac wheel. The warning will appear on every command; it is not a problem to fix.

Three of those pins are load-bearing and the reasons are in the comments beside
them: dbt Core had no Python 3.13 support before 1.11, and `dbt-core` plus
`dbt-core-experimental-parser` are pinned to the last pair that installs from
wheels alone on macOS. Without those two, pip picks a version with no macOS
wheel, tries to build it from source, and the build step reaches out to GitHub
and fails on a certificate error. Pinning is not caution here; it is the fix.

**Then smoke-test the ingestion path**, because this install downgrades
`certifi` — the list of certificate authorities `requests` trusts — to satisfy a
cap in dbt-snowflake:

```bash
python fetch_one_ticker.py
```

A normal successful pull means the older CA list still validates the API's
certificate and nothing upstream was disturbed. If it fails on a certificate
error, that is this downgrade, and the fix is a separate venv for dbt rather
than fighting the pin. The S3 uploads are unaffected either way: botocore
carries its own CA list and ignores certifi entirely.

Unrelated but worth ten seconds while you are here — python.org's Python
installs without a CA bundle wired up, which is what made the failure above look
like a network problem. Running `/Applications/Python 3.13/Install
Certificates.command` fixes that for anything using plain `urllib`. Nothing in
this project needs it, but the next tool that does will fail the same confusing
way.

## 2. Connection, with a key pair instead of a password

dbt reads connection details from `~/.dbt/profiles.yml` — your home directory,
not this repository. That default is worth keeping rather than overriding: a
credential in the home directory cannot end up in a commit no matter what gets
staged by accident, which is the same invariant `.env` holds for the API key.

What goes in it here is not a password. Snowflake is retiring password-only
sign-ins, so this project authenticates with a **key pair**: two matching files,
where the private one stays on this laptop and never moves, and the public one
is handed to Snowflake once. Snowflake then proves the connection by asking a
question only the private key can answer. Nothing that can be typed, phished or
pasted into a commit is involved, and key-pair sign-in sits outside the
deprecation entirely.

It is the same shape as the way this project already reaches S3 — Snowflake
assumes an IAM role rather than storing AWS keys — arrived at for the same
reason from a different direction.

```bash
./scripts/set_dbt_profile.sh
```

It prompts for the account identifier, user and role, generates a 2048-bit key
pair into `~/.snowflake/`, writes the dbt profile pointing at it, and sets every
file it touches to mode `600`. The private key is encrypted with a passphrase it
generates itself — nothing you have to invent or remember, and nothing reused
from another account. If you ever lose the profile, regenerate the pair; there
is nothing there to recover, only to replace.

The key is generated with Python's `cryptography` library rather than `openssl`,
deliberately: macOS ships LibreSSL under the name `openssl`, and it differs from
OpenSSL on exactly the PKCS#8 flags Snowflake's own documented commands use.
`cryptography` arrives with dbt and behaves identically on both, so the wrapper
script checks the venv is active and hands off to Python.

Nothing about your account is baked into either script — same reasoning as
`S3_BUCKET` on Aug 29. Values that would need an exception in
`check_secrets.sh` do not go in files that `check_secrets.sh` scans.

The script finishes by printing an `ALTER USER ... SET RSA_PUBLIC_KEY=...`
statement, which is what tells Snowflake about the public half of the pair.

**Do not select that line out of the terminal.** It is around 400 characters and
selecting a wrapped line by hand is how a key arrives truncated — which then
fails later, as an authentication error that says nothing about a bad paste.
Build it from the key file instead and put it straight on the clipboard:

```bash
echo "ALTER USER <YOUR_USER> SET RSA_PUBLIC_KEY='$(grep -v '^-----' ~/.snowflake/market_data_dbt_key.pub | tr -d '\n')';" | pbcopy
```

That strips the `-----BEGIN PUBLIC KEY-----` header and footer, joins the
remaining lines into one, and wraps the result in the statement. Paste it into a
Snowsight worksheet with Cmd-V and run it.

Then verify the key arrived whole:

```sql
DESC USER <YOUR_USER>;
SELECT "value" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE "property" = 'RSA_PUBLIC_KEY_FP';
```

`DESC USER` returns about forty rows; the second statement pulls out just the
one that matters. Everything after `SHA256:` must equal the fingerprint the
script printed. If they match, the whole key made it across.

If a key or a `market_data` profile already exists, the script refuses and says
so. It will not overwrite either.

**Your own Snowsight login is a separate thing.** This key pair is how *dbt*
connects. Signing in to the Snowflake website with your username and password is
covered by the same deprecation and needs a second factor on your user —
Snowsight will walk you through enrolling one. Do that too, or you will be
locked out of the console while dbt carries on working fine.

## 3. Prove the connection before building anything

```bash
cd dbt && dbt debug
```

Every line should read `OK`. The failures worth recognising:

- **`Credentials in profile "market_data", target "dev" invalid`** — wrong user,
  wrong account identifier, or the `ALTER USER` in section 2 was never run.
  Check the account identifier first; the locator-plus-region form is the one
  people get wrong. Then re-check the fingerprint against `DESC USER`.
- **`Could not find profile named 'market_data'`** — the `profile:` key in
  `dbt_project.yml` and the top-level key in `~/.dbt/profiles.yml` have to be
  the same string. This is the most common `dbt debug` failure there is.
- **`JWT token is invalid`** or **`Runtime Error ... 250001`** — Snowflake
  refused the key. Almost always the `ALTER USER` statement was run against a
  different user than the profile names, or the public key was pasted with a
  line break in it. Compare the fingerprints; they are the whole point of that
  check.
- **`Could not deserialize key data`** — dbt found the key file but could not
  open it, which means `private_key_passphrase` in the profile no longer matches
  the key at `private_key_path`. That happens if the pair was regenerated
  without the profile being rewritten. Move both aside and run the script again.

## 4. One pre-flight query

Run this in Snowsight, not in dbt. Every cast in the model is a cast of a JSON
scalar to a number or a date, except one, and that one is worth two seconds of
checking before it fails inside a model run:

```sql
select payload:ingested_at_utc::string                                        as raw_value,
       convert_timezone('UTC', payload:ingested_at_utc::timestamp_tz)::timestamp_ntz as parsed
from market_data.raw.raw_prices
limit 3;
```

`ingested_at_utc` arrives as an ISO string carrying a `+00:00` offset. If it
parses, `parsed` shows the same instant with the zone dropped and the model will
build. If it errors, the problem is that one expression and you have found it in
isolation rather than in a stack trace.

## 5. Build it

```bash
dbt run
```

One model, a `CREATE TABLE AS`, a few seconds of warehouse time. dbt creates the
`staging` schema itself if it does not exist.

## 6. Verify against raw

The staging model neither adds nor drops rows, so every number below has to
match what section 6 of `sql/06_snowflake_raw_load.sql` reported for the raw
table. Any drift means a cast silently dropped something.

```sql
select count(*)                  as rows_out,
       count(distinct price_key) as distinct_keys,
       count(distinct ticker)    as tickers,
       min(trade_date)           as first_day,
       max(trade_date)           as last_day
from market_data.staging.stg_prices;
```

`rows_out` and `distinct_keys` must be equal. If they are not, the same file was
loaded twice, and the model deliberately does not hide that — see the closing
comment in `stg_prices.sql`.

Then the thing this whole design was built around:

```sql
describe table market_data.staging.stg_prices;

select ticker, trade_date, open, close, volume
from market_data.staging.stg_prices
where ticker = 'AAPL' and trade_date = '2024-08-30';
```

`describe` must show `FLOAT` for open, high, low, close and volume. AAPL's close
on that day is exactly `229` — it is stored in the JSON as an integer, and it is
the first row of the raw table. A tool that inferred types from a sample would
have called that column INTEGER and truncated every fractional close after it.
That is the entire reason the raw table is VARIANT and every cast in the model
is written by hand.

## 7. Commit

```bash
git status
```

Read it before staging. `.env`, `data/`, `dbt/target/` and `dbt/logs/` must all
be absent — the last two are new to this step and are now in `.gitignore`.

```bash
git add . && ./scripts/check_secrets.sh && git commit
```

Chained with `&&` on purpose: if the secret scan finds something, the commit
does not happen.

## What dbt generated, and what to ignore

- `dbt/target/` — compiled SQL and run artifacts. Look in
  `target/compiled/market_data/models/staging/stg_prices.sql` if you want to see
  exactly what was sent to Snowflake, with the `source()` reference rendered
  into a real three-part name. Gitignored.
- `dbt/logs/dbt.log` — the full run log, more detail than the terminal shows.
  Gitignored.
- Both are rebuilt from the models on every run. `dbt clean` deletes them.

## Two things about the layout that look like mistakes and are not

**There is no `+schema: staging` config anywhere.** dbt's default behaviour for
a custom schema is to *append* it to the profile's schema, so a profile on `raw`
plus a model config of `staging` builds into `raw_staging`, not `staging`.
Rather than override the macro that does that, the profile targets `staging`
directly and `_sources.yml` declares `raw` as where the source lives. Reading
from one schema and writing to another, with no macro involved.

**`stg_prices` is a table, not a view.** The dbt convention for a staging layer
is a view — no storage, never stale. A table is the choice here because the
step's definition of done says one clean table, because 12,529 rows of storage
is measured in kilobytes, and because the Streamlit dashboard in step 12 reads
this object on every page load. It is one word in `dbt_project.yml` if that
changes.

## Why there is no password anywhere in this

Snowflake is retiring single-factor password sign-ins in phases, and Phase 3 —
the one that ends password-only sign-in for everyone — lands in the Aug–Oct 2026
window, on a per-account date each account is notified of individually.

The original plan for this step was a password now and a key pair later, on the
theory that trial accounts are exempt. Snowflake's documentation does say that.
The account's own console said otherwise within hours, naming a date in the same
week as this build. The console is the better evidence, and in any case the
decision does not depend on which reading is right: a key pair costs about the
same fifteen minutes either way, and it is unaffected by every phase of the
rollout. So there was never a reason to find out the hard way.

The `.env` file is untouched by all of this. The API key and the AWS keys still
live there and are still set by `scripts/set_api_key.sh` and
`scripts/set_aws_keys.sh`. Snowflake is now the one credential in this project
that is not a secret string at all.
