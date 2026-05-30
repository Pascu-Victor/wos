#!/bin/bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <hostname-or-ip> [remote-command ...]" >&2
  exit 1
fi

TARGET="$1"
shift

REMOTE_USER="${WOS_LINUX_USER:-user}"
REMOTE_PASS="${WOS_LINUX_PASSWORD:-1234}"
REMOTE_PORT="${WOS_LINUX_SSH_PORT:-}"

SSH_ARGS=(
  -o StrictHostKeyChecking=no
  -o UserKnownHostsFile=/dev/null
  -o LogLevel=ERROR
)

if [[ -n "$REMOTE_PORT" ]]; then
  SSH_ARGS+=(-p "$REMOTE_PORT")
fi

if [[ $# -eq 0 ]]; then
  exec sshpass -p "$REMOTE_PASS" ssh "${SSH_ARGS[@]}" "${REMOTE_USER}@${TARGET}"
fi

exec sshpass -p "$REMOTE_PASS" ssh "${SSH_ARGS[@]}" "${REMOTE_USER}@${TARGET}" "$@"
