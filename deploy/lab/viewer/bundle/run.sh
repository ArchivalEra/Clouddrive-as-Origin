#!/usr/bin/env bash
# The five-hour viewer run, with the agreed shape fixed here so it cannot drift:
#   ten browser slots, ASYNCHRONOUS starts (each slot draws its own uniform
#   delay inside the first ASYNC_START seconds - never ten at once), each
#   session: start at a RANDOM position of the video, play 5 s, seek to
#   another RANDOM position (over the WHOLE video), play 10 minutes, die; the
#   slot refills immediately with a new random position.
# Everything each session does is appended to runs/sessions.jsonl.
#
# The operator provides: MEDIA_URL (the video's https URL).
# Tunables: HOURS, SLOTS, WATCH_SECS, DURATION_SECS, WINDOW_*/ASYNC_START.
set -eu
: "${MEDIA_URL:?set MEDIA_URL to the https URL of the video (the operator provides it)}"

HOURS=${HOURS:-5}
SLOTS=${SLOTS:-10}
WATCH_SECS=${WATCH_SECS:-600}
PLAY_SECS=${PLAY_SECS:-5}
DURATION_SECS=${DURATION_SECS:-86801}      # the video's duration, seconds
ASYNC_START_SECS=${ASYNC_START_SECS:-1800}  # first starts spread over 30 min
JITTER_SECS=${JITTER_SECS:-60}
STARTUP_TIMEOUT_SECS=${STARTUP_TIMEOUT_SECS:-150}
SESSION_BUDGET_SECS=${SESSION_BUDGET_SECS:-1000}
WINDOW_START=${WINDOW_START:-0}             # ignored while DURATION_SECS > 0
WINDOW_SECS=${WINDOW_SECS:-3600}            # ignored while DURATION_SECS > 0
FANOUT=${FANOUT:-0}

RUN=runs
mkdir -p "$RUN"

for d in "${PW:-}" "${PW_DIR:-}" "$HOME/.npm/_npx"/*/node_modules/playwright-core; do
  [ -n "$d" ] && [ -d "$d" ] && { PW_DIR=$d; break; }
done
: "${PW_DIR:?playwright-core not found - bash install.sh tells you how to get it}"
export PW_DIR

PORT=${PORT:-8000}
python3 -m http.server "$PORT" --directory . > runs/page-server.log 2>&1 &
SERVER=$!
trap 'kill $SERVER 2>/dev/null || true' EXIT

# The page server must ANSWER before the swarm starts: a refused connection
# here reads like a video problem ten sessions later, so it is checked, not
# assumed. (No curl dependency; python3 is already required.)
page_ok() {
  python3 - "$PORT" <<'PY'
import sys, urllib.request
try:
    urllib.request.urlopen(f"http://127.0.0.1:{sys.argv[1]}/video-page.html", timeout=2).read(64)
except Exception:
    sys.exit(1)
PY
}
for _ in $(seq 1 20); do page_ok && break; sleep 0.5; done
page_ok || {
  echo "the page server is not answering on port $PORT - see runs/page-server.log" >&2
  echo "(pick another port: PORT=8123 bash run.sh)" >&2
  exit 2
}

node swarm.mjs \
  --target "http://127.0.0.1:$PORT" \
  --page video-page.html \
  --src "$MEDIA_URL" \
  --slots "$SLOTS" --hours "$HOURS" \
  --play-secs "$PLAY_SECS" --watch-secs "$WATCH_SECS" \
  --duration-secs "$DURATION_SECS" \
  --async-start-secs "$ASYNC_START_SECS" --jitter-secs "$JITTER_SECS" \
  --startup-timeout-secs "$STARTUP_TIMEOUT_SECS" --session-budget-secs "$SESSION_BUDGET_SECS" \
  --window-start "$WINDOW_START" --window-secs "$WINDOW_SECS" \
  --fanout "$FANOUT" \
  --out "$RUN/sessions.jsonl" --progress-secs 60 \
  > "$RUN/swarm.log" 2>&1
