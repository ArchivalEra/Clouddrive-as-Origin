#!/usr/bin/env bash
# Launch the run detached, so nothing depends on the terminal or the ssh
# channel that started it (a five-hour run must survive both). Poll with
# `tail -f runs/swarm.log` and `bash monitor.sh`.
#
#   MEDIA_URL='https://...' bash detach.sh
#
# Extra tunables are passed through: HOURS, SLOTS, WATCH_SECS, DURATION_SECS,
# ASYNC_START_SECS, JITTER_SECS, FANOUT.
set -eu
: "${MEDIA_URL:?set MEDIA_URL to the https URL of the video}"

cd "$(dirname "$0")"
mkdir -p runs
# `setsid` + full redirection: the run is in its own session, and the shell
# that started it returns at once.
setsid nohup env \
  MEDIA_URL="$MEDIA_URL" \
  HOURS="${HOURS:-5}" SLOTS="${SLOTS:-10}" WATCH_SECS="${WATCH_SECS:-600}" \
  PLAY_SECS="${PLAY_SECS:-5}" DURATION_SECS="${DURATION_SECS:-86801}" \
  ASYNC_START_SECS="${ASYNC_START_SECS:-1800}" JITTER_SECS="${JITTER_SECS:-60}" \
  STARTUP_TIMEOUT_SECS="${STARTUP_TIMEOUT_SECS:-150}" \
  SESSION_BUDGET_SECS="${SESSION_BUDGET_SECS:-1000}" \
  WINDOW_START="${WINDOW_START:-0}" WINDOW_SECS="${WINDOW_SECS:-3600}" \
  FANOUT="${FANOUT:-0}" PORT="${PORT:-8000}" \
  bash run.sh > runs/detach.log 2>&1 < /dev/null &

sleep 3
echo "launched in the background; logs:"
echo "  runs/swarm.log     (driver progress, one line a minute)"
echo "  runs/detach.log    (anything run.sh itself printed)"
echo "  runs/monitor.log   (start: bash monitor.sh &)"
echo "  runs/sessions.jsonl (one JSON line per finished session)"
