#!/usr/bin/env python3
"""
Set up dbt's connection to Snowflake using key-pair authentication.

WHY A KEY PAIR AND NOT A PASSWORD
Snowflake is retiring password-only sign-ins. A key pair is not a password, so
it sits outside that deprecation entirely -- and it is the same shape as the way
this project already reaches S3, where Snowflake assumes an IAM role instead of
storing AWS keys. One less credential that can be typed, phished or committed.

The pair is two files. The private key stays on this laptop and is never sent
anywhere. The public half is pasted into Snowflake once. Snowflake then proves
the connection by challenging something only the private key can answer.

WHY PYTHON AND NOT openssl
macOS ships LibreSSL as `openssl`, which differs from OpenSSL on exactly the
PKCS#8 encryption flags Snowflake's documented commands use. The `cryptography`
library arrives with dbt anyway and behaves identically on both, so the key is
generated here instead of shelling out to whichever openssl happens to be first
on PATH.

WHY THIS PROMPTS FOR EVERYTHING
Nothing about the account is baked into this file. scripts/check_secrets.sh
blocks a commit when any .env value of 12 characters or more appears in staged
content; the response to that on Aug 29 was to stop putting values where they
would need an exception, not to teach the checker exceptions. The account
identifier, user and role are typed in at run time and land only in
~/.dbt/profiles.yml, outside this repository.

Usage:  python scripts/set_dbt_profile.py
"""

import base64
import getpass
import hashlib
import os
import secrets
import stat
import sys

PROFILE_NAME = "market_data"
DBT_DIR = os.path.expanduser("~/.dbt")
PROFILES = os.path.join(DBT_DIR, "profiles.yml")
KEY_DIR = os.path.expanduser("~/.snowflake")
KEY_PATH = os.path.join(KEY_DIR, "market_data_dbt_key.p8")
PUB_PATH = os.path.join(KEY_DIR, "market_data_dbt_key.pub")


def die(msg):
    print("ERROR: %s" % msg, file=sys.stderr)
    raise SystemExit(1)


try:
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
except ImportError:
    die(
        "the `cryptography` package is not available.\n"
        "  This script runs inside the project venv, which gets it with dbt:\n"
        "      conda deactivate && source .venv/bin/activate\n"
        "      pip install -r requirements.txt"
    )


def yaml_quote(value):
    """YAML single-quoted scalar. A literal quote is written twice."""
    return "'%s'" % value.replace("'", "''")


# --------------------------------------------------------------- refuse to clobber
# Neither an existing key nor an existing profile block is ever overwritten.
# A script that silently replaces a file the user also edits is a bug waiting
# for a bad evening -- that was Aug 29's lesson and it still applies.
if os.path.exists(KEY_PATH):
    die(
        "a key already exists at %s.\n"
        "  This script will not replace it. To start over, move it aside first:\n"
        "      mv %s %s.old" % (KEY_PATH, KEY_PATH, KEY_PATH)
    )

if os.path.exists(PROFILES):
    with open(PROFILES) as fh:
        if any(line.startswith(PROFILE_NAME + ":") for line in fh):
            die(
                "a '%s' profile already exists in %s.\n"
                "  This script will not overwrite it. Edit that file directly instead."
                % (PROFILE_NAME, PROFILES)
            )

# ------------------------------------------------------------------------ inputs
print("Setting up key-pair authentication for the '%s' dbt profile.\n" % PROFILE_NAME)

def ask(prompt, default=None):
    """Prompt until answered. A cancelled or piped-empty run exits with a
    sentence rather than a traceback -- a stack trace out of a script that
    handles private keys reads as a broken script, not a cancelled one."""
    while True:
        try:
            answer = input(prompt).strip()
        except (EOFError, KeyboardInterrupt):
            print()
            die("cancelled before anything was written.")
        if answer:
            return answer
        if default is not None:
            return default
        print("  That cannot be empty.")


account = ask("Snowflake account identifier (e.g. abc12345.us-east-1): ")
user = ask("Snowflake user: ")
role = ask("Snowflake role [ACCOUNTADMIN]: ", default="ACCOUNTADMIN")

# ------------------------------------------------------------------- generate
# The passphrase is generated rather than chosen. It is never typed, so it
# cannot be a password reused from somewhere else, and it is only ever read by
# dbt out of profiles.yml. If that file is lost, regenerate the pair -- there is
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

# Written at 0600 from the start, not chmod'ed afterwards -- between a default
# create and a later chmod there is a window where the key is world-readable.
fd = os.open(KEY_PATH, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
with os.fdopen(fd, "wb") as fh:
    fh.write(private_pem)
with open(PUB_PATH, "wb") as fh:
    fh.write(public_pem)
os.chmod(PUB_PATH, 0o644)

# --------------------------------------------------------------------- verify
# Read the key back off disk and decrypt it. Generating a key that cannot be
# reopened is a silent failure that would surface later as an unreadable dbt
# error, so it is checked here, at the point where the cause is obvious.
with open(KEY_PATH, "rb") as fh:
    reread = serialization.load_pem_private_key(fh.read(), password=passphrase.encode())
if reread.public_key().public_numbers() != key.public_key().public_numbers():
    die("the key written to disk does not match the key generated. Nothing to trust here.")

# Prove the key on disk is really encrypted, by confirming it CANNOT be opened
# without the passphrase.
#
# An earlier version compared the PEM header text instead. That was weaker -- a
# file can begin with the right words and contain anything -- and it also put a
# literal private-key header into a source file, which scripts/check_secrets.sh
# correctly refused to let past. The checker was right and the code was wrong.
# Teaching the checker an exception would have been the worse fix: exceptions
# are how a checker starts lying to you.
try:
    serialization.load_pem_private_key(private_pem, password=None)
    die("the key written to disk is NOT encrypted. Refusing to leave it that way.")
except TypeError:
    pass  # cryptography raises TypeError when a passphrase is required -- correct

mode = stat.S_IMODE(os.stat(KEY_PATH).st_mode)
if mode != 0o600:
    die("the private key is mode %o, expected 600." % mode)

# Snowflake shows this as RSA_PUBLIC_KEY_FP. Comparing it against DESC USER is
# how you know the paste landed intact rather than truncated.
fingerprint = base64.b64encode(hashlib.sha256(public_der).digest()).decode()

# ------------------------------------------------------------- write the profile
os.makedirs(DBT_DIR, mode=0o700, exist_ok=True)
os.chmod(DBT_DIR, 0o700)

block = "\n".join([
    "%s:" % PROFILE_NAME,
    "  target: dev",
    "  outputs:",
    "    dev:",
    "      type: snowflake",
    "      account: %s" % yaml_quote(account),
    "      user: %s" % yaml_quote(user),
    "      private_key_path: %s" % yaml_quote(KEY_PATH),
    "      private_key_passphrase: %s" % yaml_quote(passphrase),
    "      role: %s" % yaml_quote(role),
    "      warehouse: load_wh",
    "      database: market_data",
    # Models are built here. Sources declare their own schema (raw) in
    # _sources.yml, so reading from raw and writing to staging needs no macro.
    "      schema: staging",
    "      threads: 4",
    "      client_session_keep_alive: false",
    "",
])

existing = ""
if os.path.exists(PROFILES):
    with open(PROFILES) as fh:
        existing = fh.read()
    if existing and not existing.endswith("\n"):
        existing += "\n"

fd = os.open(PROFILES, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
with os.fdopen(fd, "w") as fh:
    fh.write(existing + block)
os.chmod(PROFILES, 0o600)

# ----------------------------------------------------------------------- report
pub_b64 = "".join(
    line for line in public_pem.decode().splitlines()
    if not line.startswith("-----")
)

print("""
Done. Two files were written, both readable only by you:

  %s   the private key   (never leaves this laptop)
  %s   the public key
  %s   the dbt profile, pointing at the private key

The passphrase was generated, not chosen, and lives only in the profile above.
It is not printed here and does not need to be remembered -- if that file is
lost, run this script again and register a new key.

------------------------------------------------------------------------------
NEXT: paste this into a Snowsight worksheet and run it. It tells Snowflake the
public half of the pair, which is what lets the private key prove who you are.
------------------------------------------------------------------------------

ALTER USER %s SET RSA_PUBLIC_KEY='%s';

Then confirm the key arrived intact:

DESC USER %s;

Find the RSA_PUBLIC_KEY_FP row. Everything after "SHA256:" must equal:

%s

If those match, come back and run:  cd dbt && dbt debug
""" % (KEY_PATH, PUB_PATH, PROFILES, user, pub_b64, user, fingerprint))
