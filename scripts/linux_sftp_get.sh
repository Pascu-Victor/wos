#!/bin/bash
set -euo pipefail

if [[ $# -ne 3 ]]; then
  echo "Usage: $0 <hostname-or-ip> <remote-path> <local-path>" >&2
  exit 1
fi

TARGET="$1"
REMOTE_PATH="$2"
LOCAL_PATH="$3"

REMOTE_USER="${WOS_LINUX_USER:-user}"
REMOTE_PASS="${WOS_LINUX_PASSWORD:-1234}"
REMOTE_PORT="${WOS_LINUX_SSH_PORT:-}"

SFTP_ARGS=(
  -q
  -o StrictHostKeyChecking=no
  -o UserKnownHostsFile=/dev/null
  -o LogLevel=ERROR
)

if [[ -n "$REMOTE_PORT" ]]; then
  SFTP_ARGS+=(-P "$REMOTE_PORT")
fi

mkdir -p "$(dirname "$LOCAL_PATH")"
BATCH_FILE="$(mktemp "/tmp/wos-linux-sftp.XXXXXX")"
trap 'rm -f "$BATCH_FILE"' EXIT

printf 'get "%s" "%s"\n' "$REMOTE_PATH" "$LOCAL_PATH" > "$BATCH_FILE"
exec sshpass -p "$REMOTE_PASS" sftp "${SFTP_ARGS[@]}" -b "$BATCH_FILE" "${REMOTE_USER}@${TARGET}"
