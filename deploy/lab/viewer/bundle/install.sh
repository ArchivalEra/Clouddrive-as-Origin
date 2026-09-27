#!/usr/bin/env bash
# Check (and suggest installs for) everything the viewer swarm needs.
# Run this FIRST, on the machine that will host the browsers.
set -u
ok()   { echo "  ok   $1"; }
miss() { echo "  MISS $1"; echo "       -> $2"; FAIL=1; }
FAIL=0

echo "== viewer-swarm environment =="

command -v node >/dev/null 2>&1 && ok "node ($(node -v))" \
  || miss "node" "apt install nodejs  (or: https://nodejs.org, needs >= 18)"

CHROME=""
for c in chromium chromium-browser google-chrome google-chrome-stable; do
  command -v "$c" >/dev/null 2>&1 && { CHROME=$(command -v "$c"); break; }
done
[ -n "$CHROME" ] && ok "chromium ($CHROME)" \
  || miss "chromium" "apt install chromium  (or google-chrome-stable)"

FOUND=""
for d in "${PW:-}" "${PW_DIR:-}" "$HOME/.npm/_npx"/*/node_modules/playwright-core; do
  [ -n "$d" ] && [ -d "$d" ] && { FOUND=$d; break; }
done
if [ -n "$FOUND" ]; then
  ok "playwright-core ($FOUND)"
else
  miss "playwright-core" "cd $(pwd) && npm i playwright-core   (then export PW_DIR=$PWD/node_modules/playwright-core)"
fi

command -v python3 >/dev/null 2>&1 && ok "python3 (serves the page)" \
  || miss "python3" "apt install python3"

echo
[ "$FAIL" -eq 0 ] && echo "environment ready. Next: bash run.sh   (see README.md)" || exit 1
