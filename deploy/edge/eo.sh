#!/usr/bin/env bash
# The single door to the EdgeOne (teo) control plane for this project.
#
# It exists to encode two environment facts that have already cost time here.
#
#   1. This workstation exports HTTP_PROXY/HTTPS_PROXY=http://127.0.0.1:2080
#      globally, and the project forbids anything travelling through that
#      proxy. The proxy variables are stripped here, once, rather than at every
#      call site where one of them would eventually be forgotten.
#
#   2. EdgeOne International accounts are served by teo.intl.tencentcloudapi.com.
#      tccli 3.x has no international routing -- it defaults to the domestic
#      endpoint, where an international key answers SecretIdNotFound and looks
#      like a bad key rather than a bad endpoint. Override with EO_ENDPOINT if
#      the account is ever a domestic one.
#
# Credentials are read from the environment only (TENCENTCLOUD_SECRET_ID,
# TENCENTCLOUD_SECRET_KEY, TENCENTCLOUD_TOKEN). Nothing in deploy/edge/ echoes,
# logs or stores them; no file here may ever contain one.
#
# Usage:  eo.sh <Action> [--Param Value ...]
#   e.g.  eo.sh DescribeZones --Offset 0 --Limit 100
#
# Note that teo parameter names are capitalised (--Offset, not --offset): tccli
# does no case folding and answers an unknown option with a bare usage message.

set -euo pipefail

: "${TENCENTCLOUD_SECRET_ID:?set TENCENTCLOUD_SECRET_ID in the environment (never in a file)}"
: "${TENCENTCLOUD_SECRET_KEY:?set TENCENTCLOUD_SECRET_KEY in the environment (never in a file)}"

EO_ENDPOINT="${EO_ENDPOINT:-teo.intl.tencentcloudapi.com}"
EO_REGION="${EO_REGION:-ap-hongkong}"

# The bundled Python warns on import ("'return' in a 'finally' block"), which
# would otherwise decorate every call's output.
export PYTHONWARNINGS=ignore

unset HTTP_PROXY HTTPS_PROXY http_proxy https_proxy ALL_PROXY all_proxy ZCODE_HTTP_PROXY

if [ "$#" -lt 1 ]; then
    echo "usage: eo.sh <Action> [--Param Value ...]" >&2
    exit 2
fi

if command -v tccli >/dev/null 2>&1; then
    TCCLI=$(command -v tccli)
elif [ -x "$HOME/.local/bin/tccli" ]; then
    TCCLI="$HOME/.local/bin/tccli"
else
    echo "tccli not found. Install it with:  uv tool install tccli" >&2
    exit 2
fi

exec "$TCCLI" teo "$@" --endpoint "$EO_ENDPOINT" --region "$EO_REGION"
