#!/usr/bin/env bash
# Set (or rotate) the market data API key.
#
#   bash scripts/set_api_key.sh
#
# Merges the key into .env and writes it to the macOS Keychain, then verifies
# both match. Other variables already in .env are preserved.
# The key is never printed to the screen and never enters shell history:
#   - `read -s` takes it as INPUT, not as a command, so ~/.zsh_history never sees it
#   - -s suppresses echo, so the terminal never draws it -- screenshots are safe
#   - nothing here reads the clipboard, so pasting commands can't clobber it
#
# Contains no secret. Safe to commit.

set -euo pipefail

cd "$(dirname "$0")/.."

SERVICE="massive-api-key"
VAR="POLYGON_API_KEY"

echo "Copy your key from massive.com, then paste it at the prompt."
echo "Nothing will appear as you paste. That is correct."
echo

read -rs -p "Key: " NEWKEY
echo

NEWKEY="$(printf '%s' "$NEWKEY" | tr -d '[:space:]')"

if [ -z "$NEWKEY" ]; then
  echo "Nothing entered. Aborted; nothing was changed."
  exit 1
fi

if ! printf '%s' "$NEWKEY" | grep -Eq '^[A-Za-z0-9_-]+$'; then
  echo "That does not look like an API key (unexpected characters)."
  echo "Length was ${#NEWKEY}. Aborted; nothing was changed."
  exit 1
fi

if [ "${#NEWKEY}" -lt 20 ] || [ "${#NEWKEY}" -gt 60 ]; then
  echo "Length ${#NEWKEY} is outside the expected 20-60 range. Aborted."
  exit 1
fi

# --- .env: merge, never clobber ---
# This USED TO BE a plain `> .env` redirect, written when POLYGON_API_KEY was the
# only thing in the file. By 2026-09-19 .env also held AWS_ACCESS_KEY_ID,
# AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION and S3_BUCKET -- so the one script
# whose whole job is rotating a key safely would have silently deleted the four
# credentials upload_to_s3.py needs, and the failure would have surfaced at the
# next 07:00 DAG run rather than here. set_aws_keys.sh already merged for exactly
# this reason and said so in its header. Now both do. Do not "simplify" this back.
touch .env
TMP="$(mktemp)"
grep -v -E "^${VAR}=" .env > "$TMP" || true
printf '%s=%s\n' "$VAR" "$NEWKEY" >> "$TMP"
mv "$TMP" .env
chmod 600 .env

# --- Keychain ---
security delete-generic-password -s "$SERVICE" >/dev/null 2>&1 || true
security add-generic-password -a "$USER" -s "$SERVICE" -w "$NEWKEY"

unset NEWKEY

# --- Verify, without displaying anything secret ---
ENV_VAL="$(awk -F= -v v="$VAR" '$1 == v {print $2}' .env)"
KC_VAL="$(security find-generic-password -s "$SERVICE" -w)"

echo "length:      ${#ENV_VAL}"
echo "env file:    .env (permissions $(stat -f '%Lp' .env))"
if [ "$ENV_VAL" = "$KC_VAL" ]; then
  echo "result:      MATCH -- .env and Keychain agree. You are done."
else
  echo "result:      MISMATCH -- the two stores disagree. Re-run this script."
  exit 1
fi
