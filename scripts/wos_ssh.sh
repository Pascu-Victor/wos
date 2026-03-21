#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <hostname-or-ip> [remote-command ...]" >&2
  exit 1
fi

TARGET="$(${SCRIPT_DIR}/wos_resolve.py target "$1")"
shift

REMOTE_USER="${WOS_SSH_USER:-root}"
REMOTE_PORT="${WOS_SSH_PORT:-}"

SSH_ARGS=(
  -o BatchMode=yes
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
