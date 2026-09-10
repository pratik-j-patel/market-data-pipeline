"""
Daily equity prices: API -> local files -> S3 -> Snowflake -> dbt.

Four tasks, in order, and each one runs a command this repository already has.
Nothing here imports the pipeline's code. Airflow is a caller, not an owner --
so `python fetch_tickers.py` on a laptop and the `fetch_prices` task below run
byte-identical files, and the README's instructions cannot drift away from what
the scheduler actually does.

WHY THIS RUNS AT 07:00 AND EXPECTS YESTERDAY
    A trading day's close is never available on that day. Measured three times
    in August and again on 2026-09-04: day D's bar appears somewhere between
    about 9pm ET on D and about 11am ET on D+1. A 7am run therefore usually has
    yesterday's close and occasionally does not.

    That is deliberate rather than tolerated. fetch_tickers.py pulls a trailing
    *window* of days, not "yesterday", so a bar that publishes late is simply
    picked up by the next morning's run. Waiting until noon to guarantee the
    bar would trade a self-healing design for a slower one.

WHY THE TIMEZONE IS NAMED AND NOT UTC
    Airflow schedules in UTC unless a timezone is attached to start_date. "07:00
    in New York" and "11:00 UTC" are the same instant today and different
    instants on 2026-11-01, when US clocks change -- which is inside this
    project's window. The ingestion code already draws this distinction (trading
    days in ET, instants in UTC); the schedule should not undo it.

WHY WEEKDAYS ONLY, AND WHY THERE IS NO MARKET CALENDAR HERE
    Saturday and Sunday have no bars to fetch, so a weekend run would spend 25
    API calls and a warehouse resume to change nothing. Public holidays still
    fire and still find nothing new, and that is fine: the trailing window means
    a closed market and a missed run are the same situation, and neither needs
    special handling. A holiday calendar in this DAG would be a dependency that
    exists to prevent a harmless no-op.

WHY catchup IS OFF
    Airflow's default is to fire one run for every schedule interval since
    start_date. That is right for a job whose work is a function of its logical
    date. This one is not: every run fetches the same trailing window regardless
    of when it was meant to happen, so ten catch-up runs would do the same work
    ten times. max_active_runs=1 exists for the same reason -- two of these
    overlapping would have both writing the same day files.

WHAT THIS STEP DID NOT HAVE TO ADD
    Re-running is already safe at every stage: the file writer merges on
    (trade_date, ticker) and leaves unchanged bytes alone, the uploader compares
    MD5 to the S3 ETag, and COPY INTO consults its own load history. This DAG
    schedules and observes; it does not make the pipeline correct. Running it
    twice should move nothing the second time, and that is worth checking rather
    than assuming -- it was not true until 2026-09-07, and the README explains
    why in "Each layer was correct and the pipeline still was not".
"""

import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

# Trading days are a New York concept; see the module docstring.
ET = pendulum.timezone("America/New_York")

# Set in airflow/Dockerfile. Named here so the commands below read as commands.
# The pipeline's Python is a virtualenv separate from Airflow's own, because
# Airflow and dbt pin overlapping sets of libraries and neither should have to
# win. /opt/repo is this repository, bind-mounted from the host.
REPO = "/opt/repo"
PY = "/opt/pipeline/venv/bin/python"
DBT = "/opt/pipeline/venv/bin/dbt"

with DAG(
    dag_id="market_data_pipeline",
    description="Daily equity prices: API -> S3 -> Snowflake -> dbt",
    schedule="0 7 * * 1-5",
    # IN THE PAST, and that matters more than it looks. A manual trigger takes
    # "now" as its logical date; if that falls before start_date, Airflow creates
    # the run, finds no task instances to create for it, and marks it SUCCESSFUL
    # in about twenty milliseconds. A green run that did nothing is worse than a
    # red one, because nothing asks you to look at it. Found exactly that way on
    # 2026-09-07, with start_date set one day into the future to keep the
    # schedule quiet -- which also silenced the trigger.
    #
    # catchup=False is what stops a past start_date from backfilling every
    # interval since; the two settings are only safe together.
    start_date=pendulum.datetime(2026, 9, 7, tz=ET),
    catchup=False,
    max_active_runs=1,
    tags=["market-data"],
    default_args={
        # One retry, because the failures worth retrying here are transient:
        # a dropped connection, a container restarting mid-task. A second
        # attempt costs a few minutes and every task is safe to re-run.
        # Anything that fails twice is a real fault and should stay red.
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
    },
) as dag:
    dag.doc_md = __doc__

    # ~5 minutes: 25 tickers paced 12.5s apart to stay under 5 calls/min.
    # Exits non-zero if ANY ticker failed, so a run that merely finished is
    # distinguishable from a run that worked.
    fetch_prices = BashOperator(
        task_id="fetch_prices",
        bash_command=f"cd {REPO} && {PY} fetch_tickers.py",
        # Comfortably past the ~5 minute pacing floor. A task with no timeout
        # can hold max_active_runs=1 closed forever on a hung connection.
        execution_timeout=pendulum.duration(minutes=20),
        doc_md="Trailing 7-day window from the Massive/Polygon API into "
               "`data/date=YYYY-MM-DD/prices.jsonl`, merged on (trade_date, ticker).",
    )

    upload_to_s3 = BashOperator(
        task_id="upload_to_s3",
        bash_command=f"cd {REPO} && {PY} upload_to_s3.py",
        execution_timeout=pendulum.duration(minutes=15),
        doc_md="Uploads changed partitions only: one ListObjectsV2 returns every "
               "ETag, and an unchanged file's ETag already equals its MD5.",
    )

    snowflake_copy = BashOperator(
        task_id="snowflake_copy",
        bash_command=f"cd {REPO} && {PY} scripts/snowflake_copy.py",
        execution_timeout=pendulum.duration(minutes=15),
        doc_md="COPY INTO from the S3 stage, then the verification query. Zero "
               "files processed is success. Exits non-zero if the row count and "
               "the distinct (ticker, trade_date) count ever disagree.",
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        # `dbt build`, not `dbt seed && dbt run && dbt test`.
        #
        # build interleaves the three: it loads the seed, builds a model, runs
        # that model's tests, and only then builds what depends on it. The
        # ordering is the entire point. A duplicate (ticker, trade_date) in
        # stg_prices does not merely appear twice in fct_daily_prices -- it
        # shifts every 20-day moving average whose window spans it. `run` then
        # `test` would compute those wrong averages first and report the fault
        # afterwards, leaving a mart that is wrong and a test that says so.
        # build refuses to construct the mart at all.
        #
        # Measured 2026-09-10 by planting a duplicate row in raw_prices: the
        # uniqueness test failed and 13 downstream nodes were SKIPPED, both
        # marts among them. Removing the row put all 29 back to green.
        #
        # The seed still loads on every run: 25 static rows cost nothing to
        # reload, and it means editing ticker_reference.csv takes effect without
        # anyone remembering a second command.
        #
        # `dbt source freshness` is deliberately NOT a fifth task here. It asks
        # a looser version of a question tests/assert_prices_are_current.sql
        # already answers inside this command, and answers more precisely --
        # counting weekdays behind the newest bar rather than hours since the
        # last load. Two tasks for one invariant means two alerts and one fix.
        bash_command=f"cd {REPO}/dbt && {DBT} build",
        execution_timeout=pendulum.duration(minutes=15),
        doc_md="Seeds, builds and TESTS in dependency order: staging.stg_prices, "
               "then marts.dim_tickers and marts.fct_daily_prices, with 25 data "
               "tests interleaved. A failed test on staging skips the marts "
               "rather than building them from rows already known to be bad. "
               "Ignore dbt's out-of-date banner; 1.12.0 is pinned deliberately "
               "and requirements.txt says why.",
    )

    fetch_prices >> upload_to_s3 >> snowflake_copy >> dbt_build
