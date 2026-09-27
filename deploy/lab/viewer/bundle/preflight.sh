#!/usr/bin/env bash
# One short session (up to ~10 minutes: a deep cold seek can be slow) against the media URL: proves the video plays, seeks,
# and the collection works, before the five-hour run is trusted with the time.
set -eu
: "${MEDIA_URL:?set MEDIA_URL to the https URL of the video}"

for d in "${PW:-}" "${PW_DIR:-}" "$HOME/.npm/_npx"/*/node_modules/playwright-core; do
  [ -n "$d" ] && [ -d "$d" ] && { PW_DIR=$d; break; }
done
: "${PW_DIR:?playwright-core not found - bash install.sh tells you how to get it}"
export PW_DIR

PORT=${PORT:-8000}
mkdir -p runs
python3 -m http.server "$PORT" --directory . > runs/page-server.log 2>&1 &
SERVER=$!
trap 'kill $SERVER 2>/dev/null || true' EXIT
page_ok() {
  python3 - "$PORT" <<'PYEOF'
import sys, urllib.request
try:
    urllib.request.urlopen(f"http://127.0.0.1:{sys.argv[1]}/video-page.html", timeout=2).read(64)
except Exception:
    sys.exit(1)
PYEOF
}
for _ in $(seq 1 20); do page_ok && break; sleep 0.5; done
page_ok || { echo "page server not answering on port $PORT - see runs/page-server.log" >&2; exit 2; }

node swarm.mjs \
  --target "http://127.0.0.1:$PORT" \
  --page video-page.html \
  --src "$MEDIA_URL" \
  --slots 1 --hours 0.03 \
  --play-secs 5 --watch-secs 30 \
  --duration-secs "${DURATION_SECS:-86801}" \
  --startup-timeout-secs 240 --session-budget-secs 600 \
  --fanout 0 \
  --out runs/preflight.jsonl --progress-secs 20

echo "--- preflight session ---"
cat runs/preflight.jsonl
