#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

if [[ $# -ne 3 ]]; then
  echo "Usage: $0 <hostname-or-ip> <remote-path> <local-path>" >&2
  exit 1
fi

TARGET="$(${SCRIPT_DIR}/wos_resolve.py target "$1")"
REMOTE_PATH="$2"
LOCAL_PATH="$3"

REMOTE_USER="${WOS_SSH_USER:-root}"
REMOTE_PORT="${WOS_SSH_PORT:-}"

SFTP_ARGS=(
  -q
  -o BatchMode=yes
  -o StrictHostKeyChecking=no
  -o UserKnownHostsFile=/dev/null
  -o LogLevel=ERROR
)

if [[ -n "$REMOTE_PORT" ]]; then
  SFTP_ARGS+=(-P "$REMOTE_PORT")
fi

mkdir -p "$(dirname "$LOCAL_PATH")"
BATCH_FILE="$(mktemp "/tmp/wos-sftp.XXXXXX")"
trap 'rm -f "$BATCH_FILE"' EXIT

printf 'get "%s" "%s"\n' "$REMOTE_PATH" "$LOCAL_PATH" > "$BATCH_FILE"
exec sftp "${SFTP_ARGS[@]}" -b "$BATCH_FILE" "${REMOTE_USER}@${TARGET}"
