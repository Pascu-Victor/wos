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

SSH_CONTROL_DIR="${WOS_SSH_CONTROL_DIR:-}"
SSH_CONTROL_COMMAND="${WOS_SSH_CONTROL_COMMAND:-}"
if [[ -n "$SSH_CONTROL_DIR" ]]; then
  if [[ ! -d "$SSH_CONTROL_DIR" ]]; then
    echo "WOS SSH control directory does not exist: $SSH_CONTROL_DIR" >&2
    exit 2
  fi
  SSH_ARGS+=(
    -o ControlMaster=auto
    -o ControlPersist=300
    -o "ControlPath=${SSH_CONTROL_DIR%/}/%C"
  )
fi

if [[ -n "$REMOTE_PORT" ]]; then
  SSH_ARGS+=(-p "$REMOTE_PORT")
fi

if [[ -n "$SSH_CONTROL_COMMAND" ]]; then
  if [[ -z "$SSH_CONTROL_DIR" ]]; then
    echo "WOS_SSH_CONTROL_COMMAND requires WOS_SSH_CONTROL_DIR" >&2
    exit 2
  fi
  case "$SSH_CONTROL_COMMAND" in
    check|exit) ;;
    *)
      echo "Unsupported WOS SSH control command: $SSH_CONTROL_COMMAND" >&2
      exit 2
      ;;
  esac
  exec ssh "${SSH_ARGS[@]}" -O "$SSH_CONTROL_COMMAND" "${REMOTE_USER}@${TARGET}"
fi

if [[ $# -eq 0 ]]; then
  exec ssh "${SSH_ARGS[@]}" "${REMOTE_USER}@${TARGET}"
fi

exec ssh "${SSH_ARGS[@]}" "${REMOTE_USER}@${TARGET}" "$@"
