#!/usr/bin/env bash
# The node's env file, kept honest against the config that names it.
#
# Why this exists: config-efficient.toml names the environment variables the
# binary reads, and a named-but-unset variable is a BOOT FAILURE (fail-closed
# admission, src/main.rs). The installer's template and that config drifted once
# -- a fresh install was missing ORIGIN_TOKEN and would not start -- so the rule
# "every name the config references must be defined" lives here, in one place,
# with its own exit codes, instead of in a heredoc nobody could test.
#
# Usage: env-file.sh <env-file> <config-file> (--fresh | --keep) [--new-token]
#
#   --fresh      write the template: a real token generated, the rest REPLACE_ME
#   --keep       keep an existing file, after checking it defines every name the
#                config references; refuse (exit 2) otherwise
#   --new-token  with --keep: generate and append ORIGIN_TOKEN if it is the only
#                missing name (the one that can be generated; the operator must
#                put the same value into the edge rule -- see the runbook)
#
# Exit: 0 fine; 1 usage or environment problem; 2 the existing file is incomplete
set -euo pipefail

ENV_FILE="${1:-}"; CFG_FILE="${2:-}"; MODE="${3:-}"
NEW_TOKEN=0
for a in "$@"; do [ "$a" = "--new-token" ] && NEW_TOKEN=1; done
usage() { echo "usage: $0 <env-file> <config-file> (--fresh|--keep) [--new-token]"; }
[ -n "$ENV_FILE" ] && [ -n "$CFG_FILE" ] || { usage; exit 1; }
case "$MODE" in --fresh|--keep) ;; *) usage; exit 1;; esac
[ -r "$CFG_FILE" ] || { echo "FAIL: cannot read $CFG_FILE"; exit 1; }

# Every `*_env = "NAME"` the config names is a variable the binary will read.
required=$(grep -oE '^[[:space:]]*[a-z_]*_env[[:space:]]*=[[:space:]]*"[A-Z0-9_]+"' "$CFG_FILE" \
  | grep -oE '"[A-Z0-9_]+"' | tr -d '"' | sort -u)
[ -n "$required" ] || { echo "FAIL: $CFG_FILE names no *_env variables"; exit 1; }

gen_token() {
  if command -v openssl >/dev/null 2>&1; then openssl rand -hex 32
  else od -An -tx1 -N32 /dev/urandom | tr -d ' \n'; fi
}
defined() { local v; v=$(sed -n "s/^[[:space:]]*$1=//p" "$ENV_FILE" | head -1); [ -n "$v" ]; }
missing() { local n; for n in $required; do defined "$n" || echo "$n"; done; }

if [ "$MODE" = "--fresh" ]; then
  token=$(gen_token)
  cat > "$ENV_FILE" <<ENV
OPENLIST_USERNAME=REPLACE_ME
OPENLIST_PASSWORD=REPLACE_ME
ORIGIN_PREWARM_SECRET=REPLACE_ME
ORIGIN_TOKEN=$token
ORIGIN_TLS_CERT_PATH=/etc/ssl/dib.l.cd/cdn-oracle/cert.pem
ORIGIN_TLS_KEY_PATH=/etc/ssl/dib.l.cd/cdn-oracle/key.pem
ENV
  chmod 600 "$ENV_FILE"
  echo "wrote $ENV_FILE (template; fill the REPLACE_ME entries before first start)"
  echo "ORIGIN_TOKEN=$token"
  echo "  ^ put this value into the edge's ModifyRequestHeader action (runbook: First install)"
  miss=$(missing || true)
  [ -z "$miss" ] || { echo "FAIL: the template does not define: $miss"; exit 1; }
  exit 0
fi

# --keep
[ -f "$ENV_FILE" ] || { echo "FAIL: $ENV_FILE does not exist (use --fresh)"; exit 1; }
miss=$(missing || true)
if [ -z "$miss" ]; then
  echo "keeping $ENV_FILE (defines all $(echo "$required" | wc -w) names the config reads)"
  exit 0
fi

if [ "$NEW_TOKEN" = 1 ] && [ "$miss" = "ORIGIN_TOKEN" ]; then
  token=$(gen_token)
  printf 'ORIGIN_TOKEN=%s\n' "$token" >> "$ENV_FILE"
  chmod 600 "$ENV_FILE"
  echo "appended a new ORIGIN_TOKEN to $ENV_FILE"
  echo "ORIGIN_TOKEN=$token"
  echo "  ^ the front will now require this stamp: put the same value into the"
  echo "    edge's ModifyRequestHeader action BEFORE restarting the unit"
  exit 0
fi

echo "FAIL: $ENV_FILE is missing: $(echo $miss)"
echo "  The config names these variables and the binary refuses to start without"
echo "  them (fail-closed admission). Fill them in, or re-run with --new-token to"
echo "  generate the token here."
exit 2
