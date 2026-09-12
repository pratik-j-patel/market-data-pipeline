#!/usr/bin/env python3
"""
Prove the dashboard's Snowflake identity can read the marts and nothing else.

WHY THIS EXISTS AS A SCRIPT AND NOT A PARAGRAPH
sql/07_dashboard_reader_role.sql says what dashboard_reader is allowed to do.
A README could say the same thing. Neither is evidence: a GRANT that was never
run, a schema added later, or a role accidentally granted somewhere else all
leave the prose looking correct. This connects as the real user over the real
path and asks the warehouse.

It also cannot be run any other way. dashboard_app is TYPE = SERVICE, so it
cannot log into Snowsight at all -- there is no interactive session in which to
try these queries by hand.

WHY THE POSITIVE CHECKS COME FIRST, AND WHY THEY ARE NOT DECORATION
A negative test that passes because the connection is broken has proved nothing:
every query fails, including the ones that should. The two reads below are the
canary. If they do not succeed, the refusals underneath them are meaningless and
this script says so rather than reporting a pass.

Usage:  python scripts/check_dashboard_grants.py
Exit 0 = the role is scoped as intended. Exit 1 = it is not.
"""

import os
import sys
import tomllib

from cryptography.hazmat.primitives import serialization
from snowflake import connector

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(REPO_ROOT, ".streamlit", "secrets.toml")

ALLOWED = [
    ("market_data.marts.fct_daily_prices", "the fact table the dashboard charts"),
    ("market_data.marts.dim_tickers", "the dimension it joins to"),
]
FORBIDDEN = [
    ("market_data.staging.stg_prices", "the staging model"),
    ("market_data.raw.raw_prices", "the raw landing table"),
]


def load_config():
    if not os.path.exists(CONFIG_PATH):
        sys.exit(f"ERROR: {CONFIG_PATH} does not exist. Run scripts/set_dashboard_profile.py.")
    with open(CONFIG_PATH, "rb") as handle:
        return tomllib.load(handle)["snowflake"]


def private_key_der(config):
    pem = config.get("private_key_pem")
    pem = pem.encode() if pem else open(os.path.expanduser(config["private_key_path"]), "rb").read()
    passphrase = config.get("private_key_passphrase") or None
    key = serialization.load_pem_private_key(
        pem, password=passphrase.encode() if passphrase else None
    )
    return key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def main():
    config = load_config()
    connection = connector.connect(
        account=config["account"],
        user=config["user"],
        role=config["role"],
        warehouse=config["warehouse"],
        database=config["database"],
        schema=config["schema"],
        private_key=private_key_der(config),
    )
    cursor = connection.cursor()

    cursor.execute("select current_user(), current_role(), current_warehouse()")
    user, role, warehouse = cursor.fetchone()
    print(f"connected as   {user}  /  {role}  /  {warehouse}\n")

    failures = []

    print("MUST be readable -- these are the canary. If they fail, nothing below means anything.")
    canary_ok = True
    for table, description in ALLOWED:
        try:
            cursor.execute(f"select count(*) from {table}")
            print(f"  OK       {table:42} {cursor.fetchone()[0]:>7,} rows   ({description})")
        except Exception as error:
            canary_ok = False
            failures.append(f"{table} should be readable and is not: {type(error).__name__}")
            print(f"  FAILED   {table:42} {type(error).__name__}")

    print("\nMUST be refused.")
    for table, description in FORBIDDEN:
        try:
            cursor.execute(f"select count(*) from {table}")
            rows = cursor.fetchone()[0]
            failures.append(f"{table} was READABLE ({rows:,} rows) and must not be")
            print(f"  LEAKED   {table:42} {rows:>7,} rows   ({description})")
        except Exception:
            print(f"  refused  {table:42}         ({description})")

    print("\nMUST be read-only.")
    try:
        cursor.execute("create table market_data.marts.grant_check_scratch (i int)")
        failures.append("the role could CREATE TABLE in marts and must not be able to")
        print("  LEAKED   create table in marts succeeded")
    except Exception:
        print("  refused  create table in marts")

    cursor.close()
    connection.close()

    print()
    if not canary_ok:
        sys.exit("INCONCLUSIVE -- the reads that should work did not, so the refusals prove nothing.")
    if failures:
        for line in failures:
            print(f"  - {line}")
        sys.exit("FAIL -- the role is not scoped as sql/07_dashboard_reader_role.sql intends.")
    print("PASS -- reads the marts, cannot see staging or raw, cannot write.")


if __name__ == "__main__":
    main()
