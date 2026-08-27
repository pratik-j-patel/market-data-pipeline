#!/usr/bin/env bash
#
# check_secrets.sh — scan what is STAGED for commit and refuse anything
# secret-shaped. Run it after `git add`, before `git commit`.
#
#   ./scripts/check_secrets.sh
#
# Exit 0 = clean, safe to commit.
# Exit 1 = something found. DO NOT COMMIT. Run `git reset` and investigate.
#
# This script NEVER prints a secret's value — only the file and line number
# where it appears. That is deliberate: a checker that echoes the key into
# your scrollback has just created a second copy of the problem.
#
# TWO DELIBERATE BLIND SPOTS, both load-bearing:
#
#  1. It does not scan ITSELF. Its own filename contains "secrets" and its
#     pattern list contains strings like the AWS key name, so scanning itself
#     produces guaranteed false positives on every single run. A checker that
#     always cries wolf is a checker you learn to ignore.
#
#  2. It does not flag bare 32-character hex strings. The API returns an
#     X-Request-Id in exactly that shape, it appears in the step 2 notebook
#     outputs, and it is not a credential. Same reasoning.
#
# PATTERNS REQUIRE REAL KEY MATERIAL, NOT THE SHAPE OF IT. On its first real run
# (Aug 27, 2026) an earlier version fired five times on a clean repo: the URL
# anatomy diagram, a quoted traceback, two comments warning about ?apiKey=, and
# `api_key = os.getenv(...)` -- the correct pattern. It flagged the documentation
# of the fix as if it were the bug. So `?apiKey=` now needs >=16 characters of
# key-like text after it, and a credential assignment needs a QUOTED literal of
# >=12 characters -- which `os.getenv("...")` is not, because no quote follows
# the `=`.

set -uo pipefail

cd "$(git rev-parse --show-toplevel)" 2>/dev/null || {
    echo "Not inside a git repository."; exit 2
}

SELF="scripts/check_secrets.sh"

STAGED=$(git diff --cached --name-only --diff-filter=ACM)
if [ -z "$STAGED" ]; then
    echo "Nothing is staged. Run 'git add' first — there is nothing to check."
    exit 0
fi

SCAN=$(printf '%s\n' "$STAGED" | grep -vxF "$SELF" || true)

FAIL=0
echo "Checking $(printf '%s\n' "$STAGED" | wc -l | tr -d ' ') staged file(s)…"
echo

if [ -z "$SCAN" ]; then
    echo "CLEAN — only the checker itself is staged. Safe to commit."
    exit 0
fi

# ---------------------------------------------------------------- check 1
# Filenames that should never be committed, whatever is inside them.
BADNAMES=$(printf '%s\n' "$SCAN" | grep -E '(^|/)\.env$|(^|/)\.env\.local$|credentials|secrets|\.pem$|\.key$|\.p12$|\.pfx$' || true)
if [ -n "$BADNAMES" ]; then
    echo "FAIL — these staged files should never be committed:"
    printf '%s\n' "$BADNAMES" | sed 's/^/    /'
    FAIL=1
fi

# ---------------------------------------------------------------- check 2
# The highest-signal check: take each real value out of .env and look for it
# verbatim in the staged content. Zero false positives by construction.
if [ -f .env ]; then
    while IFS= read -r line; do
        case "$line" in ''|\#*) continue ;; esac
        case "$line" in *=*) ;; *) continue ;; esac
        value="${line#*=}"
        value="${value%\"}"; value="${value#\"}"
        value="${value%\'}"; value="${value#\'}"
        [ ${#value} -lt 12 ] && continue
        case "$value" in your_key_here|changeme|placeholder*) continue ;; esac

        HITS=$(git grep --cached -l -F -e "$value" -- $SCAN 2>/dev/null || true)
        if [ -n "$HITS" ]; then
            echo "FAIL — a live value from .env appears in these staged files:"
            printf '%s\n' "$HITS" | sed 's/^/    /'
            echo "    (value not shown on purpose)"
            FAIL=1
        fi
    done < .env
fi

# ---------------------------------------------------------------- check 3
# Generic credential shapes. File and line only, never the matching text.
scan_pattern() {
    HITS=$(git grep --cached -n -E -e "$1" -- $SCAN 2>/dev/null | cut -d: -f1,2 || true)
    if [ -n "$HITS" ]; then
        echo "FAIL — possible $2:"
        printf '%s\n' "$HITS" | sed 's/^/    /'
        FAIL=1
    fi
}

scan_pattern '[?&]apiKey=[A-Za-z0-9_-]{16,}'       'live-looking API key in a URL query string'
scan_pattern 'AKIA[0-9A-Z]{16}'                   'AWS access key ID'
scan_pattern 'aws_secret_access_key[[:space:]]*[:=][[:space:]]*["'"'"'][A-Za-z0-9/+=_-]{12,}' 'AWS secret access key'
scan_pattern '-----BEGIN [A-Z ]*PRIVATE KEY-----'  'private key block'
scan_pattern '(password|passwd|secret|token|api_key|apikey)[[:space:]]*[:=][[:space:]]*["'"'"'][A-Za-z0-9/+=_-]{12,}["'"'"']' 'hardcoded credential assignment'

echo
if [ $FAIL -eq 0 ]; then
    echo "CLEAN — nothing secret-shaped in the staged files. Safe to commit."
else
    echo "STOP — do not commit. Run 'git reset' to unstage, then fix the findings above."
    echo "If a key was already committed in an earlier snapshot, ROTATE IT — deleting"
    echo "it in a new commit does not remove it from history."
fi
exit $FAIL
