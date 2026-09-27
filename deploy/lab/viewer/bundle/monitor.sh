#!/usr/bin/env bash
# One status line every 5 minutes while the swarm runs. Write to the run dir,
# on a real disk: a power cut must not take the run's record with it.
RUN=runs
OUT=$RUN/monitor.log
mkdir -p "$RUN"
line() {
  {
    printf '%s  ' "$(date -u +%FT%TZ)"
    tail -1 "$RUN/swarm.log" 2>/dev/null | cut -c1-190
    printf '    load=%s memavail=%sMB sessions=%s\n' \
      "$(cut -d' ' -f1 /proc/loadavg)" \
      "$(awk '/^MemAvailable/{print int($2/1024)}' /proc/meminfo)" \
      "$(wc -l < "$RUN/sessions.jsonl" 2>/dev/null || echo 0)"
  } >> "$OUT"
}
# The README says to start this BEFORE the swarm, so the first thing to do is
# wait for it to appear: `while pgrep ...` alone exits immediately when it has
# not started yet, and the log then holds nothing but the closing line.
for _ in $(seq 1 120); do
  pgrep -f "swarm[.]mjs" >/dev/null 2>&1 && break
  sleep 5
done
if ! pgrep -f "swarm[.]mjs" >/dev/null 2>&1; then
  echo "$(date -u +%FT%TZ)  swarm never appeared (waited 10 min)" >> "$OUT"
  exit 1
fi
line
while pgrep -f "swarm[.]mjs" >/dev/null 2>&1; do
  sleep 300
  line
done
echo "$(date -u +%FT%TZ)  swarm stopped" >> "$OUT"
