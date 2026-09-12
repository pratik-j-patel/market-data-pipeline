#!/usr/bin/env python3
"""
Give the dashboard its own Snowflake identity, and write the config it reads.

WHY THE DASHBOARD DOES NOT REUSE THE dbt CREDENTIAL
An earlier version of this script derived everything from ~/.dbt/profiles.yml,
which was right while the dashboard only ran on this laptop. It is wrong for a
deployed app: that profile connects with ACCOUNTADMIN, and pasting a key with
full control of the account into a hosting provider's configuration -- for an
app anyone on the internet can open -- makes the blast radius of a mistake the
entire warehouse. sql/07_dashboard_reader_role.sql creates a role that can read
the marts and nothing else, and a SERVICE user to hold it. This script generates
that user's key pair.

WHAT IT STILL BORROWS
The account identifier, read out of ~/.dbt/profiles.yml. It is not a secret --
it is a locator and a region -- and typing it a second time is a typo waiting to
happen. Everything else is asked for, with the defaults from sql/07.

WHY A SEPARATE KEY PAIR AND NOT A COPY
Two identities sharing one key are one identity wearing two names. Rotating the
dashboard's key should not touch dbt, and a key that leaves this laptop for a
hosting provider should never be the same key that does not.

WHY PYTHON AND NOT openssl
macOS ships LibreSSL as `openssl`, which differs from OpenSSL on exactly the
PKCS#8 encryption flags Snowflake's documented commands use. `cryptography`
arrives with the Snowflake connector and behaves identically on both.

Run sql/07_dashboard_reader_role.sql FIRST -- the ALTER USER this prints has
nothing to attach to until the user exists.

Usage:  python scripts/set_dashboard_profile.py
"""

import base64
import hashlib
import json
import os
import secrets
import stat
import subprocess
import sys

import yaml
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

PROFILE_NAME = "market_data"
PROFILES = os.path.expanduser("~/.dbt/profiles.yml")

KEY_DIR = os.path.expanduser("~/.snowflake")
KEY_PATH = os.path.join(KEY_DIR, "market_data_dashboard_key.p8")
PUB_PATH = os.path.join(KEY_DIR, "market_data_dashboard_key.pub")

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT_DIR = os.path.join(REPO_ROOT, ".streamlit")
OUT_PATH = os.path.join(OUT_DIR, "secrets.toml")

DEFAULT_USER = "dashboard_app"
DEFAULT_ROLE = "dashboard_reader"
DEFAULT_WAREHOUSE = "dash_wh"
DATABASE = "market_data"
SCHEMA = "marts"


def die(message):
    print(f"\nERROR: {message}", file=sys.stderr)
    sys.exit(1)


def ask(prompt, default):
    answer = input(f"{prompt} [{default}]: ").strip()
    return answer or default


def account_from_dbt_profile():
    """The locator and region, or None. Never any credential material."""
    if not os.path.exists(PROFILES):
        return None
    try:
        with open(PROFILES) as handle:
            profiles = yaml.safe_load(handle) or {}
        profile = profiles.get(PROFILE_NAME) or {}
        output = (profile.get("outputs") or {}).get(profile.get("target")) or {}
        return output.get("account")
    except Exception:
        return None


def toml_string(value):
    # A JSON string and a TOML basic string share escaping rules, and json.dumps
    # is in the standard library -- so the quoting is done by something tested.
    return json.dumps(str(value))


def write_private(path, data):
    """
    O_EXCL, so the mode below is always applied.

    The mode argument to os.open is a CREATION mode: if the path already exists
    it is ignored entirely and the file keeps whatever permissions it had. On
    2026-09-11 that put a passphrase into a leftover 0644 file. O_EXCL removes
    the question by refusing to open an existing path at all.
    """
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as handle:
        handle.write(data)


def write_config(path, text):
    """
    Same guarantee for a file that legitimately gets rewritten: create a fresh
    temp file with O_EXCL at 0600, then rename it over the target. os.replace is
    atomic and keeps the temp file's mode, so there is never a moment where the
    real file exists with the wrong permissions.
    """
    temp = path + ".new"
    if os.path.exists(temp):
        os.remove(temp)
    write_private(temp, text.encode())
    os.replace(temp, path)


def main():
    if os.path.exists(KEY_PATH):
        die(
            f"{KEY_PATH} already exists.\n"
            "  Generating a new pair would orphan the public half Snowflake holds.\n"
            "  To rotate deliberately:\n"
            f"      mv {KEY_PATH} {KEY_PATH}.old\n"
            f"      mv {PUB_PATH} {PUB_PATH}.old\n"
            "  then run this again and register the new public key."
        )

    account = account_from_dbt_profile()
    if account:
        print(f"Account identifier from {PROFILES}: {account}")
    else:
        account = input("Snowflake account identifier (e.g. abc12345.us-east-1): ").strip()
        if not account:
            die("an account identifier is required.")

    user = ask("Snowflake user", DEFAULT_USER)
    role = ask("Role", DEFAULT_ROLE)
    warehouse = ask("Warehouse", DEFAULT_WAREHOUSE)

    # Generated, never chosen: it is never typed, so it cannot be a password
    # reused from somewhere else. If it is lost, rotate the pair -- there is
    # nothing here worth recovering, only replacing.
    passphrase = secrets.token_urlsafe(32)

    print("\nGenerating a 2048-bit RSA key pair...")
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.BestAvailableEncryption(passphrase.encode()),
    )
    public_pem = key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    public_der = key.public_key().public_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )

    os.makedirs(KEY_DIR, mode=0o700, exist_ok=True)
    os.chmod(KEY_DIR, 0o700)
    write_private(KEY_PATH, private_pem)
    with open(PUB_PATH, "wb") as handle:
        handle.write(public_pem)
    os.chmod(PUB_PATH, 0o644)

    # ------------------------------------------------------------------ verify
    # A key that cannot be reopened is a silent failure that surfaces later as an
    # unreadable connector error, so it is checked here, where the cause is
    # obvious.
    with open(KEY_PATH, "rb") as handle:
        reread = serialization.load_pem_private_key(handle.read(), password=passphrase.encode())
    if reread.public_key().public_numbers() != key.public_key().public_numbers():
        die("the key on disk does not match the key generated. Nothing here to trust.")

    # Prove the file is really encrypted by confirming it CANNOT be opened
    # without the passphrase. A behavioural check, not a string comparison
    # against a PEM header -- a file can begin with the right words and contain
    # anything, and the header literal is something check_secrets.sh rightly
    # refuses to let into a source file.
    try:
        serialization.load_pem_private_key(private_pem, password=None)
        die("the key on disk is NOT encrypted. Refusing to leave it that way.")
    except TypeError:
        pass  # cryptography raises TypeError when a passphrase is required

    mode = stat.S_IMODE(os.stat(KEY_PATH).st_mode)
    if mode != 0o600:
        die(f"the private key is mode {mode:o}, expected 600.")

    # ---------------------------------------------------------------- secrets
    lines = [
        "# Generated by scripts/set_dashboard_profile.py. Do not commit.",
        "#",
        "# Streamlit reads this when the app is launched from the repository root.",
        "# The deployed copy lives in Streamlit Community Cloud's own secrets store",
        "# and carries private_key_pem -- the contents of the .p8 file -- instead of",
        "# private_key_path, because the runner has no ~/.snowflake to read from.",
        "",
        "[snowflake]",
        f"account = {toml_string(account)}",
        f"user = {toml_string(user)}",
        f"role = {toml_string(role)}",
        f"warehouse = {toml_string(warehouse)}",
        f"database = {toml_string(DATABASE)}",
        f"schema = {toml_string(SCHEMA)}",
        f"private_key_path = {toml_string(KEY_PATH)}",
        f"private_key_passphrase = {toml_string(passphrase)}",
    ]
    os.makedirs(OUT_DIR, exist_ok=True)
    os.chmod(OUT_DIR, 0o700)
    write_config(OUT_PATH, "\n".join(lines) + "\n")

    written_mode = stat.S_IMODE(os.stat(OUT_PATH).st_mode)
    if written_mode != 0o600:
        die(
            f"{OUT_PATH} is mode {written_mode:o}, not 600. It holds the key's "
            "passphrase. Fix the permissions before using it."
        )

    # ------------------------------------------------------------- the paste
    # Snowflake wants the base64 body without the PEM header, footer or newlines.
    # Built straight to the clipboard rather than printed: the line is ~400
    # characters and selecting it out of a terminal is how it gets truncated.
    body = "".join(
        line for line in public_pem.decode().splitlines() if not line.startswith("-----")
    )
    statement = f"ALTER USER {user} SET RSA_PUBLIC_KEY='{body}';"
    try:
        subprocess.run(["pbcopy"], input=statement.encode(), check=True)
        copied = True
    except Exception:
        copied = False

    # Snowflake reports this as RSA_PUBLIC_KEY_FP. Comparing it against DESC USER
    # is how you know the paste landed intact rather than truncated.
    fingerprint = base64.b64encode(hashlib.sha256(public_der).digest()).decode()

    print()
    print(f"  private key       {KEY_PATH}  (mode 600)")
    print(f"  public key        {PUB_PATH}")
    print(f"  config            {OUT_PATH}  (mode 600)")
    print(f"  passphrase        generated, stored in the config, not shown")
    print()
    if copied:
        print("  The ALTER USER statement is ON YOUR CLIPBOARD. Paste it into Snowsight")
        print("  as ACCOUNTADMIN and run it. Nothing was printed here on purpose.")
    else:
        print("  Could not reach pbcopy. Re-run in a terminal on this Mac.")
    print()
    print("  Then confirm the key landed intact:")
    print(f"      DESC USER {user};")
    print("  The RSA_PUBLIC_KEY_FP row, after 'SHA256:', must read:")
    print(f"      {fingerprint}")
    print()
    print("  Then:  streamlit run dashboard/app.py")


if __name__ == "__main__":
    main()
