#!/usr/bin/env bash
# Install commithooks dispatchers and library into .git/
# Usage: bash scripts/setup-hooks.sh [commithooks-source-path]
set -euo pipefail

COMMITHOOKS="${1:-${COMMITHOOKS_DIR:-$HOME/Documents/commithooks}}"

if [ ! -d "$COMMITHOOKS/lib" ]; then
  echo "Commithooks not found at $COMMITHOOKS (skipping)"
  echo "Clone https://github.com/rahulrajaram/commithooks.git to ~/Documents/commithooks/"
  exit 0
fi

GIT_DIR="$(git rev-parse --git-dir)"

# Copy dispatchers
mkdir -p "$GIT_DIR/hooks"
for hook in pre-commit commit-msg pre-push post-checkout post-merge; do
  src="$COMMITHOOKS/$hook"
  [ -f "$src" ] || continue
  cp "$src" "$GIT_DIR/hooks/$hook"
  chmod +x "$GIT_DIR/hooks/$hook"
  echo "[ok]   $hook"
done

# Copy library
rm -rf "${GIT_DIR:?}/lib"
cp -r "$COMMITHOOKS/lib" "$GIT_DIR/lib"
echo "[ok]   lib/ ($(ls "$GIT_DIR/lib/" | wc -l) modules)"

echo ""
echo "Commithooks installed from $COMMITHOOKS"
