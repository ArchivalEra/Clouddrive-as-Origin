#!/usr/bin/env bash
# One command: source -> compile machine -> node, with the checks that would have
# caught the two mistakes this deploy path has already made.
#
# What it prevents, both measured on 2026-09-22:
#   - A local x86_64 binary copied to the aarch64 node (`Exec format error`,
#     units stuck in `activating`, three or four minutes down). So: `file` and a
#     bogus-config run happen on the NODE, before anything is installed.
#   - `install.sh` run for a binary-only update (it rewrites configs and units).
#     So: this script uses `install -o opc -g opc -m 0755` and never install.sh.
#   - The compile machine's own cross-gcc producing a binary that needs
#     GLIBC_2.38 on a 2.34 node. So: the build goes through build-aarch64.sh,
#     which uses the static musl toolchain.
#
# Run this on the WORKSTATION (it has the ssh config for the node and the key for
# the compile machine). Everything is gated: nothing is installed unless the
# binary runs on the node, and a failed accept rolls back to the previous binary.
#
#   bash deploy/oracle/deploy-node.sh            # ask before installing
#   bash deploy/oracle/deploy-node.sh --yes      # unattended
set -euo pipefail

REPO=$(cd "$(dirname "$0")/../.." && pwd)
# Hosts come from ssh-config aliases, never literals: NODE is the production
# node, COMPILE_HOST the cross-build box. Override either from the environment.
COMPILE_HOST=${COMPILE_HOST:-compile}
COMPILE_KEY=${COMPILE_KEY:-$HOME/.ssh/compile-key}
NODE=${NODE:-oracle-cdn}
ASSUME_YES=0
[ "${1:-}" = "--yes" ] && ASSUME_YES=1

say() { printf '\n=== %s\n' "$*"; }
die() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }

say "1/6 package the source"
tar czf /tmp/cds-src.tgz -C "$REPO" Cargo.toml Cargo.lock src front tests deploy config.example.toml
ls -l /tmp/cds-src.tgz

say "2/6 cross-build on the compile machine ($COMPILE_HOST)"
scp -q -i "$COMPILE_KEY" -o IdentitiesOnly=yes /tmp/cds-src.tgz "$COMPILE_HOST:/tmp/cds-src.tgz"
# The build script travels with this one. It used to be assumed already present
# at /tmp/build-aarch64.sh on the compile machine, which held only as long as
# nobody cleaned that machine's /tmp — the deploy then died at step 2 with "no
# such file", the same way a secret passed through the other side's /tmp
# silently arrives empty.
scp -q -i "$COMPILE_KEY" -o IdentitiesOnly=yes "$REPO/deploy/oracle/build-aarch64.sh" "$COMPILE_HOST:/tmp/build-aarch64.sh"
ssh -i "$COMPILE_KEY" -o IdentitiesOnly=yes "$COMPILE_HOST" \
  'bash /tmp/build-aarch64.sh /tmp/cds-src.tgz /tmp/origin-cache-aarch64-musl' | tail -4
BUILT=$(ssh -i "$COMPILE_KEY" -o IdentitiesOnly=yes "$COMPILE_HOST" 'sha256sum /tmp/origin-cache-aarch64-musl | cut -c1-16')
echo "  built sha256: $BUILT"

say "3/6 bring it to the node"
scp -q -i "$COMPILE_KEY" -o IdentitiesOnly=yes "$COMPILE_HOST:/tmp/origin-cache-aarch64-musl" /tmp/origin-cache.new
scp -q /tmp/origin-cache.new "$NODE:/home/opc/origin-cache.new"

say "4/6 HARD GATE: does this binary run on this node?"
ARCH=$(ssh "$NODE" 'file /home/opc/origin-cache.new | head -1')
echo "  $ARCH"
case "$ARCH" in
  *aarch64*|*ARM\ aarch64*) ;;
  *) die "the artifact is not aarch64: $ARCH" ;;
esac
# A config error proves it executes; a GLIBC/exec error means the toolchain or
# the architecture is wrong.
OUT=$(ssh "$NODE" '/home/opc/origin-cache.new /nonexistent.toml 2>&1 | head -2' || true)
echo "  $OUT"
case "$OUT" in
  *"load config"*) ;;
  *) die "the binary did not reach its config loader: $OUT" ;;
esac

if [ "$ASSUME_YES" != 1 ]; then
  printf '\nInstall this binary on %s and restart both units? [y/N] ' "$NODE"
  read -r ans
  [ "$ans" = y ] || [ "$ans" = Y ] || die "declined"
fi

say "5/6 install + restart (configs and units untouched)"
ssh "$NODE" 'sudo -n cp -a /opt/origin-cache/origin-cache /home/opc/origin-cache.prev \
  && sudo -n install -o opc -g opc -m 0755 /home/opc/origin-cache.new /opt/origin-cache/origin-cache \
  && sudo -n systemctl restart origin-cache-efficient origin-cache-nocache && sleep 8 \
  && systemctl is-active origin-cache-efficient origin-cache-nocache | tr "\n" " "'
echo
echo "  running now: $(ssh "$NODE" 'sha256sum /opt/origin-cache/origin-cache | cut -c1-16') (rollback copy: /home/opc/origin-cache.prev)"

say "6/6 acceptance gate"
# One run, and its whole output kept on the node: a verdict that says FAIL while
# the evidence that would explain it was piped through `tail` cannot be acted on,
# and running it twice also runs it under two different moments of the restart.
ssh "$NODE" 'bash /home/opc/repo/deploy/oracle/accept.sh > /home/opc/accept-last.log 2>&1; tail -6 /home/opc/accept-last.log'
if ssh "$NODE" 'grep -q "VERDICT=PASS" /home/opc/accept-last.log'; then
  echo "  VERDICT=PASS (full output: /home/opc/accept-last.log on the node)"
else
  echo "  accept FAILED — full output kept at /home/opc/accept-last.log on the node:"
  ssh "$NODE" 'grep -E "FAIL" /home/opc/accept-last.log | head -6'
  echo "  rolling back to the previous binary"
  ssh "$NODE" 'sudo -n install -o opc -g opc -m 0755 /home/opc/origin-cache.prev /opt/origin-cache/origin-cache \
    && sudo -n systemctl restart origin-cache-efficient origin-cache-nocache && sleep 8 \
    && systemctl is-active origin-cache-efficient origin-cache-nocache'
  die "rolled back (the new binary is at /home/opc/origin-cache.new)"
fi
