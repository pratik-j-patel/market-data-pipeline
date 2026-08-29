#!/usr/bin/env bash
# Set (or rotate) the AWS credentials and S3 target used by upload_to_s3.py.
#
#   bash scripts/set_aws_keys.sh
#
# Writes both keys to .env and to the macOS Keychain, then verifies they match.
# Neither key is ever printed and neither enters shell history:
#   - `read -s` takes them as INPUT, not as a command, so ~/.zsh_history never sees them
#   - -s suppresses echo, so the terminal never draws them -- screenshots are safe
#   - nothing here reads the clipboard, so pasting commands can't clobber it
#
# UNLIKE set_api_key.sh, this script MERGES into .env instead of overwriting it,
# so POLYGON_API_KEY survives. Do not "simplify" this to a plain `>` redirect.
#
# WHY THE BUCKET NAME IS PROMPTED AND NOT HARDCODED:
#   check_secrets.sh takes every value in .env of 12+ characters and greps the
#   staged diff for it verbatim. That check has zero false positives only while
#   everything in .env is absent from committed files. A literal bucket name
#   here failed that check on 2026-08-29. Prompting keeps the bucket in .env
#   and nowhere else -- and keeps the checker honest, which matters more than
#   saving one keystroke. It also means anyone cloning this repo points it at
#   their own bucket without editing code.
#
# Contains no secret. Safe to commit.

set -euo pipefail

cd "$(dirname "$0")/.."

read_env_value() {
  [ -f .env ] || return 0
  awk -F= -v k="$1" '$1 == k {print $2}' .env 2>/dev/null || true
}

DEFAULT_BUCKET="$(read_env_value S3_BUCKET)"
DEFAULT_REGION="$(read_env_value AWS_DEFAULT_REGION)"
DEFAULT_REGION="${DEFAULT_REGION:-us-east-1}"

# --- Non-secret settings: shown as you type, Enter keeps the current value ---
read -r -p "S3 bucket${DEFAULT_BUCKET:+ [$DEFAULT_BUCKET]}: " BUCKET
BUCKET="$(printf '%s' "${BUCKET:-$DEFAULT_BUCKET}" | tr -d '[:space:]')"
read -r -p "AWS region [$DEFAULT_REGION]: " REGION
REGION="$(printf '%s' "${REGION:-$DEFAULT_REGION}" | tr -d '[:space:]')"

if [ -z "$BUCKET" ]; then
  echo "No bucket name given. Aborted; nothing was changed."
  exit 1
fi
if ! printf '%s' "$BUCKET" | grep -Eq '^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$'; then
  echo "'$BUCKET' is not a valid S3 bucket name (3-63 chars, lowercase, digits, . and -)."
  echo "Aborted; nothing was changed."
  exit 1
fi

# --- Secrets: nothing is drawn on screen ---
echo
echo "Now the two credentials. Nothing will appear as you paste. That is correct."
echo

read -rs -p "Access key ID:     " AKID
echo
read -rs -p "Secret access key: " SECRET
echo

AKID="$(printf '%s' "$AKID" | tr -d '[:space:]')"
SECRET="$(printf '%s' "$SECRET" | tr -d '[:space:]')"

if [ -z "$AKID" ] || [ -z "$SECRET" ]; then
  echo "One of the fields was empty. Aborted; nothing was changed."
  exit 1
fi

if ! printf '%s' "$AKID" | grep -Eq '^AKIA[A-Z0-9]{16}$'; then
  echo "Access key ID does not match the expected shape (AKIA + 16 uppercase chars)."
  echo "Length was ${#AKID}. Aborted; nothing was changed."
  exit 1
fi

if ! printf '%s' "$SECRET" | grep -Eq '^[A-Za-z0-9/+=]{40}$'; then
  echo "Secret access key does not match the expected shape (40 base64 chars)."
  echo "Length was ${#SECRET}. Aborted; nothing was changed."
  exit 1
fi

# --- .env: merge, never clobber ---
touch .env
TMP="$(mktemp)"
grep -v -E '^(AWS_ACCESS_KEY_ID|AWS_SECRET_ACCESS_KEY|AWS_DEFAULT_REGION|S3_BUCKET)=' .env > "$TMP" || true
{
  printf 'AWS_ACCESS_KEY_ID=%s\n' "$AKID"
  printf 'AWS_SECRET_ACCESS_KEY=%s\n' "$SECRET"
  printf 'AWS_DEFAULT_REGION=%s\n' "$REGION"
  printf 'S3_BUCKET=%s\n' "$BUCKET"
} >> "$TMP"
mv "$TMP" .env
chmod 600 .env

# --- Keychain ---
security delete-generic-password -s "aws-access-key-id"     >/dev/null 2>&1 || true
security delete-generic-password -s "aws-secret-access-key" >/dev/null 2>&1 || true
security add-generic-password -a "$USER" -s "aws-access-key-id"     -w "$AKID"
security add-generic-password -a "$USER" -s "aws-secret-access-key" -w "$SECRET"

unset AKID SECRET

# --- Verify, without displaying anything secret ---
ENV_ID="$(awk -F= '$1 == "AWS_ACCESS_KEY_ID" {print $2}' .env)"
ENV_SEC="$(awk -F= '$1 == "AWS_SECRET_ACCESS_KEY" {print $2}' .env)"
KC_ID="$(security find-generic-password -s "aws-access-key-id" -w)"
KC_SEC="$(security find-generic-password -s "aws-secret-access-key" -w)"

echo
echo "key id length:     ${#ENV_ID}"
echo "secret length:     ${#ENV_SEC}"
echo "bucket:            $(awk -F= '$1 == "S3_BUCKET" {print $2}' .env)"
echo "region:            $(awk -F= '$1 == "AWS_DEFAULT_REGION" {print $2}' .env)"
echo "polygon key kept:  $(grep -cE '^POLYGON_API_KEY=' .env) line(s)"
echo "env file:          .env (permissions $(stat -f '%Lp' .env))"
if [ "$ENV_ID" = "$KC_ID" ] && [ "$ENV_SEC" = "$KC_SEC" ]; then
  echo "result:            MATCH -- .env and Keychain agree. You are done."
else
  echo "result:            MISMATCH -- the two stores disagree. Re-run this script."
  exit 1
fi
