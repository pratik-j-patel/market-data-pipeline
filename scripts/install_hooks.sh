#!/usr/bin/env bash
#
# install_hooks.sh — install this project's git hooks.
#
#   ./scripts/install_hooks.sh
#
# Run once per clone. Git hooks live in .git/hooks/, which is NOT part of the
# repository, so they do not survive a clone and they are not shared with anyone
# else who clones this. This script is the committed, portable half.
#
# Installs: pre-commit -> scripts/check_secrets.sh
#
# Why a hook rather than remembering to run the checker: on Aug 27, 2026 the
# checker was run manually as one of four pasted commands. It printed STOP and
# exited non-zero, and the shell ran `git commit` immediately afterwards anyway,
# because pasted lines execute independently of each other's exit status. A
# check that has to be remembered is a check that will eventually be skipped.
# Git refuses the commit outright when a pre-commit hook exits non-zero.

set -euo pipefail
ROOT="$(git rev-parse --show-toplevel)"
HOOK="$ROOT/.git/hooks/pre-commit"

cat > "$HOOK" <<'HOOKEOF'
#!/usr/bin/env bash
# Installed by scripts/install_hooks.sh — do not edit here, edit the installer.
exec "$(git rev-parse --show-toplevel)/scripts/check_secrets.sh"
HOOKEOF

chmod +x "$HOOK"
echo "Installed pre-commit hook -> $HOOK"
echo "Every 'git commit' now runs scripts/check_secrets.sh first and aborts if it fails."
echo "To bypass deliberately (you should essentially never need this): git commit --no-verify"
