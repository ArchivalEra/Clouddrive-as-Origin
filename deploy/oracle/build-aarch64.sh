#!/usr/bin/env bash
# Cross-build the node's aarch64 binary on the COMPILE MACHINE (not the node).
#
# Why here, and why musl:
#   - The oracle node is aarch64 / glibc 2.34 (Oracle Linux 9.8). The compile
#     machine is x86_64 with glibc 2.43 (Debian trixie), and its own
#     `aarch64-linux-gnu-gcc` links against that: the binary asks for
#     `GLIBC_2.38` and dies on the node with "version `GLIBC_2.38' not found"
#     (measured 2026-09-22). A static musl build has no glibc to mismatch.
#   - The toolchain is a STATIC cross toolchain, unpacked on the compile
#     machine's HDD (read-mostly, no IO pressure):
#       /mnt/hdd/crossbuild-tools/aarch64-linux-musl-cross
#     The build's target dir lives on the NVMe home, which is where the IO is.
#   - The node can also build itself (1m47s, no cross toolchain needed) and
#     stays the fallback. A musl binary is a different libc, so it faces the
#     ordinary acceptance gate (`accept.sh`) instead of being trusted for
#     being newer.
#
# Usage (on the compile machine, user archivalera):
#   bash build-aarch64.sh <source-tarball> [output-path]
#
# The source tarball is made on the workstation with:
#   tar czf /tmp/cds-src.tgz Cargo.toml Cargo.lock src front tests deploy config.example.toml
#
# Then, from the workstation (the node is reached through its own ssh config):
#   scp <output> oracle-cdn:/home/opc/origin-cache.new
#   ssh oracle-cdn '/home/opc/origin-cache.new /nonexistent.toml; echo "exit=$?"'   # config error = runs
#   ssh oracle-cdn 'sudo -n install -o opc -g opc -m 0755 /home/opc/origin-cache.new /opt/origin-cache/origin-cache \
#                   && sudo -n systemctl restart origin-cache-efficient origin-cache-nocache'
#   ssh oracle-cdn 'bash /home/opc/repo/deploy/oracle/accept.sh'   # expect VERDICT=PASS
set -euo pipefail

SRC=${1:?usage: build-aarch64.sh <source-tarball> [output-path]}
OUT=${2:-$HOME/origin-cache-aarch64-musl}
TC=/mnt/hdd/crossbuild-tools/aarch64-linux-musl-cross
WORK=$HOME/crossbuild
# The target dir is the IO-heavy part of a build: NVMe home, not the HDD.
TARGET_DIR=$HOME/cds-musl-target

[ -x "$TC/bin/aarch64-linux-musl-gcc" ] || {
  echo "FAIL: no toolchain at $TC (unpack aarch64-linux-musl-cross.tgz there)"
  exit 1
}
command -v cargo >/dev/null || { echo "FAIL: cargo is not on PATH"; exit 1; }

mkdir -p "$WORK"
tar xzf "$SRC" -C "$WORK"
cd "$WORK"

PATH=$TC/bin:$PATH \
CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=$TC/bin/aarch64-linux-musl-gcc \
CARGO_TARGET_DIR=$TARGET_DIR \
  cargo build --release --target aarch64-unknown-linux-musl --locked

BIN=$TARGET_DIR/aarch64-unknown-linux-musl/release/origin-cache
[ -x "$BIN" ] || { echo "FAIL: no binary at $BIN"; exit 1; }
install -m 0755 "$BIN" "$OUT"

echo "built: $OUT"
file "$OUT"
sha256sum "$OUT" | cut -c1-16
