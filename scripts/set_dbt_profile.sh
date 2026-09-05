#!/usr/bin/env bash
#
# Thin wrapper around scripts/set_dbt_profile.py.
#
# The real work is in the Python file: it generates an RSA key pair using the
# `cryptography` library rather than shelling out to openssl, because macOS
# ships LibreSSL under that name and LibreSSL differs from OpenSSL on exactly
# the PKCS#8 flags Snowflake's documented commands use.
#
# What this wrapper adds is the check that goes wrong most often -- running in
# conda's `base` environment instead of the project venv, which is where
# `cryptography` actually lives.

set -euo pipefail

cd "$(dirname "$0")/.."

if ! python -c "import cryptography" >/dev/null 2>&1; then
    cat >&2 <<'MSG'
ERROR: this needs the project virtualenv, and it is not active.

    conda deactivate
    source .venv/bin/activate
    which python          # must end in market-data-pipeline/.venv/bin/python
    pip install -r requirements.txt

Then run this again.
MSG
    exit 1
fi

exec python scripts/set_dbt_profile.py "$@"
