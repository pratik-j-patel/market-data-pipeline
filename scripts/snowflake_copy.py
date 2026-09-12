#!/usr/bin/env python3
"""
Load the S3 partitions into Snowflake's raw table.

    python scripts/snowflake_copy.py --dry-run   # report what is staged, load nothing
    python scripts/snowflake_copy.py             # run the COPY INTO
    python scripts/snowflake_copy.py --quiet     # totals only

WHY THIS FILE EXISTS
    Steps 1-9 left the pipeline as four commands. Three of them are scripts;
    this one was SQL pasted into a Snowsight worksheet by hand. An orchestrator
    cannot paste into a web page, so the load had to become callable before a
    DAG could exist. That is the only thing this file adds -- the SQL is the
    same SQL, lifted from sql/06_snowflake_raw_load.sql section 5.

    It is a standalone script rather than an Airflow-only operator on purpose.
    The README's answer to warehouse lock-in is that S3 is the source of truth
    and the warehouse can be rebuilt from the same files in about twenty
    minutes. That claim stops being true the moment the load only exists inside
    a DAG, so this runs the same way from a terminal with or without Airflow
    installed.

WHERE THE CONNECTION COMES FROM
    ~/.dbt/profiles.yml, the file scripts/set_dbt_profile.py already wrote and
    dbt already uses. It holds the account, user, role, warehouse, database, the
    path to the private key and the key's passphrase -- which was generated, not
    chosen, and has never been printed. Reading it here means there is exactly
    one place those values live. Copying them into .env would have created a
    second copy of a credential, and two copies drift.

    Nothing about the account is in this file, which is also what keeps
    scripts/check_secrets.sh quiet: it blocks a commit when any .env value of
    twelve characters or more appears in staged content, and the fix for that
    has always been to stop putting values where they need an exception.

WHAT IDEMPOTENCY MEANS HERE
    Snowflake keeps a per-table record of which files it has already loaded and
    skips them. A second run in a row therefore processes zero files, and
    ZERO FILES IS SUCCESS, not a failure -- getting that backwards would make a
    correctly behaving pipeline go red every morning after the first.

    That load history expires after 64 days. A file re-copied after that window
    duplicates its rows, which is why this script checks that the row count and
    the distinct (ticker, trade_date) count still match and fails if they do
    not. That check is deliberately crude; the real ones are dbt tests in
    step 11. It is here because it costs one query and it is the invariant the
    whole grain of the table rests on.
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"

PROFILES_PATH = Path(os.path.expanduser("~/.dbt/profiles.yml"))
PROFILE_NAME = "market_data"

# The raw layer's objects, all created by sql/06_snowflake_raw_load.sql. The
# database name comes from the profile; these three do not, because they are
# properties of this pipeline rather than of whoever is connecting.
RAW_SCHEMA = "raw"
TABLE = "raw_prices"
STAGE = "prices_stage"
FILE_FORMAT = "jsonl_format"

# Shows up in Snowflake's query history, so a load run is distinguishable from
# a dbt build or a worksheet by someone poking around.
QUERY_TAG = "market_data_pipeline/snowflake_copy"


def die(observed: str, check: str) -> None:
    """State what was observed and what to check. Never a cause we cannot prove."""
    print(f"\nSTOPPED: {observed}", file=sys.stderr)
    print(f"Check:   {check}", file=sys.stderr)
    sys.exit(1)


try:
    import yaml
except ImportError:
    die("the `pyyaml` package is not available.",
        "This script runs inside the project venv, which gets it with dbt:\n"
        "           conda deactivate && source .venv/bin/activate")

try:
    import snowflake.connector
    from snowflake.connector.errors import Error as SnowflakeError
except ImportError:
    die("the `snowflake-connector-python` package is not available.",
        "It arrives with dbt-snowflake. Activate the project venv:\n"
        "           conda deactivate && source .venv/bin/activate")

try:
    from cryptography.hazmat.primitives import serialization
except ImportError:
    die("the `cryptography` package is not available.",
        "It arrives with dbt. Activate the project venv:\n"
        "           conda deactivate && source .venv/bin/activate")


def new_run_id() -> str:
    """UTC instant, matching the run ids fetch_tickers.py and upload_to_s3.py write."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M")


def read_profile() -> dict:
    """The connection block dbt uses, read from ~/.dbt/profiles.yml.

    Every failure here names the file and the key, because the alternative is a
    KeyError traceback out of a script that is holding a private key path.
    """
    if not PROFILES_PATH.exists():
        die(f"no dbt profile at {PROFILES_PATH}.",
            "Run scripts/set_dbt_profile.sh to create it.")

    try:
        loaded = yaml.safe_load(PROFILES_PATH.read_text()) or {}
    except yaml.YAMLError as exc:
        die(f"{PROFILES_PATH} is not valid YAML ({exc.__class__.__name__}).",
            "Open it and look for a mis-indented line.")

    profile = loaded.get(PROFILE_NAME)
    if not profile:
        die(f"no '{PROFILE_NAME}' profile in {PROFILES_PATH}.",
            f"The file holds: {', '.join(sorted(loaded)) or '(nothing)'}")

    target = profile.get("target", "dev")
    outputs = profile.get("outputs") or {}
    conf = outputs.get(target)
    if not conf:
        die(f"profile '{PROFILE_NAME}' has no output named '{target}'.",
            f"Its outputs are: {', '.join(sorted(outputs)) or '(none)'}")

    required = ("account", "user", "private_key_path",
                "private_key_passphrase", "role", "warehouse", "database")
    missing = [k for k in required if not conf.get(k)]
    if missing:
        die(f"the '{PROFILE_NAME}.{target}' profile is missing: {', '.join(missing)}.",
            f"{PROFILES_PATH} -- compare it against the block "
            "scripts/set_dbt_profile.py writes.")

    return conf


def load_private_key(conf: dict) -> bytes:
    """The private key as DER bytes, which is the shape the connector wants.

    The key on disk is encrypted PKCS#8 PEM. Decrypting it here rather than
    handing the connector a file path keeps this on the same `cryptography`
    code path scripts/set_dbt_profile.py used to generate it -- macOS ships
    LibreSSL as `openssl` and the two disagree about exactly these flags.
    """
    key_path = Path(os.path.expanduser(conf["private_key_path"]))
    if not key_path.exists():
        die(f"the private key named in the profile is not at {key_path}.",
            "Either the file moved, or the profile points somewhere stale.")

    try:
        key = serialization.load_pem_private_key(
            key_path.read_bytes(),
            password=str(conf["private_key_passphrase"]).encode(),
        )
    except (ValueError, TypeError) as exc:
        die(f"the private key at {key_path} could not be opened "
            f"({exc.__class__.__name__}).",
            "The passphrase in profiles.yml may not belong to this key file. "
            "Regenerating the pair is the fix; there is nothing here worth "
            "recovering, only replacing.")

    return key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def connect(conf: dict):
    """A session on the raw schema, tagged so the run is findable afterwards."""
    try:
        return snowflake.connector.connect(
            account=conf["account"],
            user=conf["user"],
            private_key=load_private_key(conf),
            role=conf["role"],
            warehouse=conf["warehouse"],
            database=conf["database"],
            schema=RAW_SCHEMA,
            session_parameters={"QUERY_TAG": QUERY_TAG},
        )
    except SnowflakeError as exc:
        die(f"Snowflake refused the connection ({exc.__class__.__name__}: {exc}).",
            "Three things fail this way: the account identifier in profiles.yml, "
            "the public key registered on the user (DESC USER), and the role "
            "having no access. `cd dbt && dbt debug` isolates it.")


def measure(cur, database: str) -> dict:
    """The verification query from sql/06 section 6, run as data rather than read.

    Deliberately not a stored view: the point is that this file asks the
    warehouse the same question the runbook does, so the two cannot drift.
    """
    cur.execute(f"""
        SELECT COUNT(*)                                   AS rows_total,
               COUNT(DISTINCT source_file)                AS files_total,
               COUNT(DISTINCT payload:ticker::string
                              || '|' ||
                              payload:trade_date::string) AS distinct_keys,
               COUNT(DISTINCT payload:ticker::string)     AS tickers,
               MIN(payload:trade_date::date)              AS first_day,
               MAX(payload:trade_date::date)              AS last_day
        FROM {database}.{RAW_SCHEMA}.{TABLE}
    """)
    row = cur.fetchone()
    return {
        "rows_total": row[0],
        "files_total": row[1],
        "distinct_keys": row[2],
        "tickers": row[3],
        "first_day": str(row[4]) if row[4] else None,
        "last_day": str(row[5]) if row[5] else None,
    }


def classify_duplicates(cur, database: str) -> dict:
    """Split duplicate (ticker, trade_date) keys into the two cases that mean
    different things.

    A key whose loads all carry the SAME seven numbers was loaded twice. That is
    this pipeline's fault -- a file re-copied after the 64-day load history
    expires, say -- and stopping is right, because nothing downstream can repair
    it and a green run over it would be a lie.

    A key whose loads carry DIFFERENT numbers is the provider revising a bar
    after the fact. Nothing here is broken, nothing is lost by continuing, and
    stg_prices keeps the newest load. Stopping the pipeline over it puts a red
    DAG in front of someone who cannot fix the upstream data anyway -- the same
    reasoning as the WARN half of the severity rule in dbt/models/_models.yml.

    Measured 2026-09-11: the vwap revision that stopped this script produced 25
    keys in the second category and none in the first.
    """
    cur.execute(f"""
        WITH loads AS (
            SELECT payload:ticker::string              AS ticker,
                   payload:trade_date::date            AS trade_date,
                   HASH(payload:open::float,
                        payload:high::float,
                        payload:low::float,
                        payload:close::float,
                        payload:volume::float,
                        payload:vwap::float,
                        payload:transactions::int)     AS bar_hash
            FROM {database}.{RAW_SCHEMA}.{TABLE}
        ),
        per_key AS (
            SELECT ticker, trade_date,
                   COUNT(*)                 AS loads,
                   COUNT(DISTINCT bar_hash) AS distinct_bars
            FROM loads
            GROUP BY 1, 2
            HAVING COUNT(*) > 1
        )
        SELECT COUNT_IF(distinct_bars = 1)                          AS reloaded_keys,
               COUNT_IF(distinct_bars > 1)                          AS revised_keys,
               COALESCE(SUM(IFF(distinct_bars = 1, loads - 1, 0)), 0) AS reloaded_rows,
               COALESCE(SUM(IFF(distinct_bars > 1, loads - 1, 0)), 0) AS revised_rows,
               MIN(IFF(distinct_bars > 1, trade_date, NULL))        AS first_revised,
               MAX(IFF(distinct_bars > 1, trade_date, NULL))        AS last_revised
        FROM per_key
    """)
    row = cur.fetchone()
    return {
        "reloaded_keys": row[0],
        "revised_keys": row[1],
        "reloaded_rows": row[2],
        "revised_rows": row[3],
        "first_revised": str(row[4]) if row[4] else None,
        "last_revised": str(row[5]) if row[5] else None,
    }


def list_stage(cur, database: str) -> tuple:
    """(file count, total bytes) currently visible under the stage."""
    cur.execute(f"LIST @{database}.{RAW_SCHEMA}.{STAGE}")
    rows = cur.fetchall()
    # LIST returns name, size, md5, last_modified.
    total = sum(int(r[1]) for r in rows) if rows else 0
    return len(rows), total


def run_copy(cur, database: str) -> dict:
    """The COPY INTO from sql/06 section 5, verbatim in behaviour.

    loaded_at is left out of the column list on purpose: a column omitted from
    a COPY takes its table DEFAULT, which is the only way to get a per-row
    SYSDATE(). ON_ERROR = ABORT_STATEMENT is the default and is stated because
    it is a decision -- a half-loaded table is worse than an empty one, because
    it looks like it worked.
    """
    sql = f"""
        COPY INTO {database}.{RAW_SCHEMA}.{TABLE}
                  (payload, source_file, file_row_number)
        FROM (
            SELECT $1,
                   METADATA$FILENAME,
                   METADATA$FILE_ROW_NUMBER
            FROM @{database}.{RAW_SCHEMA}.{STAGE}
        )
        FILE_FORMAT = (FORMAT_NAME = {database}.{RAW_SCHEMA}.{FILE_FORMAT})
        ON_ERROR    = ABORT_STATEMENT
    """
    try:
        cur.execute(sql)
    except SnowflakeError as exc:
        die(f"the COPY INTO failed ({exc.__class__.__name__}: {exc}).",
            "An assume-role error here usually means the storage integration "
            "was recreated without updating the IAM trust policy -- see the "
            "warning in sql/06_snowflake_raw_load.sql section 2.")

    columns = [c[0].lower() for c in cur.description]
    rows = cur.fetchall()

    # Two result shapes. When files are loaded, COPY returns one row per file
    # with a rows_loaded column. When there is nothing left to load it returns
    # a single informational row instead -- fewer columns, no per-file detail.
    # Branching on the column names rather than the row count is what makes
    # "nothing to do" a first-class outcome instead of a parsing accident.
    if "rows_loaded" in columns:
        idx_rows = columns.index("rows_loaded")
        idx_status = columns.index("status") if "status" in columns else None
        loaded_files = [
            r for r in rows
            if idx_status is None or str(r[idx_status]).upper().startswith("LOAD")
        ]
        return {
            "files_processed": len(loaded_files),
            "rows_loaded": sum(int(r[idx_rows] or 0) for r in loaded_files),
            "message": None,
        }

    message = str(rows[0][0]) if rows else "COPY returned no result rows."
    return {"files_processed": 0, "rows_loaded": 0, "message": message}


def write_manifest(run_id, dry_run, database, before, after, copy_result,
                   staged_files, staged_bytes) -> Path:
    manifest = {
        "run_id": run_id,
        "kind": "snowflake_copy",
        "dry_run": dry_run,
        "finished_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "target": f"{database}.{RAW_SCHEMA}.{TABLE}",
        "stage": f"{database}.{RAW_SCHEMA}.{STAGE}",
        "staged_files": staged_files,
        "staged_bytes": staged_bytes,
        "files_processed": copy_result["files_processed"],
        "rows_loaded_this_run": copy_result["rows_loaded"],
        "copy_message": copy_result["message"],
        "rows_before": before["rows_total"],
        "rows_after": after["rows_total"],
        "distinct_keys_after": after["distinct_keys"],
        "files_after": after["files_total"],
        "tickers_after": after["tickers"],
        "first_day": after["first_day"],
        "last_day": after["last_day"],
    }
    path = DATA_DIR / "_runs" / f"snowflake_copy_{run_id}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, indent=2))
    return path


def run(dry_run: bool, verbose: bool) -> int:
    conf = read_profile()
    database = conf["database"]
    run_id = new_run_id()

    if verbose:
        print(f"profile:     {PROFILES_PATH} ({PROFILE_NAME})")
        print(f"account:     {conf['account']}  role {conf['role']}  "
              f"warehouse {conf['warehouse']}")
        print(f"target:      {database}.{RAW_SCHEMA}.{TABLE}")

    conn = connect(conf)
    try:
        cur = conn.cursor()

        staged_files, staged_bytes = list_stage(cur, database)
        before = measure(cur, database)

        if verbose:
            print(f"\nstaged:      {staged_files} files, {staged_bytes:,} bytes")
            print(f"before:      {before['rows_total']:,} rows from "
                  f"{before['files_total']:,} files")

        if dry_run:
            copy_result = {"files_processed": 0, "rows_loaded": 0,
                           "message": "dry run -- nothing was loaded"}
            after = before
        else:
            copy_result = run_copy(cur, database)
            after = measure(cur, database)

        # Asked here, not below, because the connection is closed by the time
        # the check runs -- and only when there is something to explain, so the
        # ordinary run still costs exactly two queries.
        duplicates = None
        if after["rows_total"] != after["distinct_keys"]:
            duplicates = classify_duplicates(cur, database)
    finally:
        conn.close()

    manifest_path = write_manifest(run_id, dry_run, database, before, after,
                                   copy_result, staged_files, staged_bytes)

    verb = "would load from" if dry_run else "processed"
    print(f"\n{verb}:   {copy_result['files_processed']} files")
    if copy_result["message"]:
        print(f"message:     {copy_result['message']}")
    print(f"rows loaded: {copy_result['rows_loaded']:,} this run")
    print(f"rows total:  {after['rows_total']:,} "
          f"({after['rows_total'] - before['rows_total']:+,} vs before)")
    print(f"files:       {after['files_total']:,}")
    print(f"tickers:     {after['tickers']}")
    print(f"range:       {after['first_day']} -> {after['last_day']}")
    print(f"manifest:    {manifest_path.relative_to(PROJECT_ROOT)}")

    # The grain of this table is one row per ticker per trading day, and when
    # the row count and the key count diverge something has loaded twice. Until
    # 2026-09-11 that ended the run. It should not always: a provider revising a
    # number it already published also produces a second row, and on that date
    # one did -- a vwap correction of a hundredth of a cent on seven tickers
    # stopped the whole pipeline. So what happened is established before it is
    # judged.
    if duplicates:
        if duplicates["revised_keys"]:
            print(f"\nWARNING: {duplicates['revised_keys']} "
                  f"(ticker, trade_date) key(s) have more than one load "
                  f"carrying DIFFERENT numbers "
                  f"({duplicates['revised_rows']:,} extra row(s), "
                  f"{duplicates['first_revised']} -> "
                  f"{duplicates['last_revised']}).")
            print("         The provider revised bars it had already published. "
                  "stg_prices keeps")
            print("         the newest load per key; nothing here needs fixing. "
                  "To see what")
            print("         changed, compare the loads for one of those days by "
                  "loaded_at.")

        if duplicates["reloaded_keys"]:
            die(f"{duplicates['reloaded_keys']} (ticker, trade_date) key(s) "
                f"have more than one load carrying IDENTICAL numbers "
                f"({duplicates['reloaded_rows']:,} extra row(s)).",
                "The same file has been loaded more than once. This is not a "
                "provider revision -- the bars are unchanged. Compare "
                "source_file and loaded_at per (ticker, trade_date) before "
                "deleting anything.")

    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load the staged S3 partitions into Snowflake's raw table.")
    parser.add_argument("--dry-run", action="store_true",
                        help="report what is staged and what is loaded, "
                             "without running the COPY")
    parser.add_argument("--quiet", action="store_true", help="totals only")
    args = parser.parse_args()
    sys.exit(run(dry_run=args.dry_run, verbose=not args.quiet))


if __name__ == "__main__":
    main()
