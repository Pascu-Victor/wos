#!/bin/bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "Usage: $0 <local-path> <hostname-or-ip>:<remote-path>" >&2
  exit 1
fi

SOURCE_PATH="$1"
TARGET_SPEC="$2"

REMOTE_PASS="${WOS_LINUX_PASSWORD:-1234}"
REMOTE_PORT="${WOS_LINUX_SSH_PORT:-}"

SCP_ARGS=(
  -o StrictHostKeyChecking=no
  -o UserKnownHostsFile=/dev/null
  -o LogLevel=ERROR
)

if [[ -n "$REMOTE_PORT" ]]; then
  SCP_ARGS+=(-P "$REMOTE_PORT")
fi

exec sshpass -p "$REMOTE_PASS" scp "${SCP_ARGS[@]}" "$SOURCE_PATH" "$TARGET_SPEC"
