#!/bin/bash
set -euo pipefail

WOS_ROOT="${WOS_WORKSPACE_ROOT:-$(git -C "$(dirname "$0")" rev-parse --show-toplevel)}"
REMOTE_SCRIPTS="$WOS_ROOT/scripts/remote"

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <hostname-or-ip> [remote-command ...]" >&2
  exit 1
fi

TARGET="$(${REMOTE_SCRIPTS}/wos_resolve.py target "$1")"
shift

REMOTE_USER="${WOS_SSH_USER:-root}"
REMOTE_PORT="${WOS_SSH_PORT:-}"
SSH_CONNECT_TIMEOUT="${WOS_SSH_CONNECT_TIMEOUT:-10}"
SSH_SERVER_ALIVE_INTERVAL="${WOS_SSH_SERVER_ALIVE_INTERVAL:-15}"
SSH_SERVER_ALIVE_COUNT_MAX="${WOS_SSH_SERVER_ALIVE_COUNT_MAX:-4}"

SSH_ARGS=(
  -F /dev/null
  -o BatchMode=yes
  -o ConnectTimeout="$SSH_CONNECT_TIMEOUT"
  -o ConnectionAttempts=1
  -o ServerAliveInterval="$SSH_SERVER_ALIVE_INTERVAL"
  -o ServerAliveCountMax="$SSH_SERVER_ALIVE_COUNT_MAX"
  -o StrictHostKeyChecking=no
  -o UserKnownHostsFile=/dev/null
  -o LogLevel=ERROR
)

if [[ -n "$REMOTE_PORT" ]]; then
  SSH_ARGS+=(-p "$REMOTE_PORT")
fi

if [[ $# -eq 0 ]]; then
  exec ssh "${SSH_ARGS[@]}" "${REMOTE_USER}@${TARGET}"
fi

exec ssh "${SSH_ARGS[@]}" "${REMOTE_USER}@${TARGET}" "$@"
