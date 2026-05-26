#!/bin/bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <server-hostname-or-ip> <client-hostname-or-ip> [--mode pingpong|stream] [--port N] [--payload-size N] [--iterations N] [--total-bytes N] [--output local-file]" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SERVER_HOST="$1"
CLIENT_HOST="$2"
shift 2

MODE="pingpong"
PORT="9000"
PAYLOAD_SIZE="1024"
ITERATIONS="1000"
TOTAL_BYTES="67108864"
OUTPUT=""
STARTUP_DELAY="${WOS_NETBENCH_STARTUP_DELAY:-1}"
SERVER_PID=""
SERVER_LOG=""

cleanup() {
  if [[ -n "$SERVER_PID" ]]; then
    ${SCRIPT_DIR}/wos_ssh.sh "$SERVER_HOST" "kill $SERVER_PID >/dev/null 2>&1 || true; wait $SERVER_PID >/dev/null 2>&1 || true; rm -f $SERVER_LOG" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      MODE="$2"
      shift 2
      ;;
    --port)
      PORT="$2"
      shift 2
      ;;
    --payload-size)
      PAYLOAD_SIZE="$2"
      shift 2
      ;;
    --iterations)
      ITERATIONS="$2"
      shift 2
      ;;
    --total-bytes)
      TOTAL_BYTES="$2"
      shift 2
      ;;
    --output)
      OUTPUT="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

if [[ "$MODE" != "pingpong" && "$MODE" != "stream" ]]; then
  echo "ERROR: --mode must be 'pingpong' or 'stream'" >&2
  exit 1
fi

SERVER_PEER="$(${SCRIPT_DIR}/wos_resolve.py target "$SERVER_HOST")"

SERVER_LOG="/tmp/wos-netbench-server-${PORT}.log"
printf -v SERVER_REMOTE_CMD '%q ' /mnt/disk/testprog netbench-server --port "$PORT" --sessions 1
SERVER_PID="$(${SCRIPT_DIR}/wos_ssh.sh "$SERVER_HOST" "rm -f $SERVER_LOG; $SERVER_REMOTE_CMD >$SERVER_LOG 2>&1 & echo \$!")"

if [[ -z "$SERVER_PID" ]]; then
  echo "ERROR: failed to start netbench server on $SERVER_HOST" >&2
  exit 1
fi

sleep "$STARTUP_DELAY"

CLIENT_ARGS=(/mnt/disk/testprog netbench-client --host "$SERVER_PEER" --port "$PORT" --mode "$MODE" --payload-size "$PAYLOAD_SIZE")
if [[ "$MODE" == "pingpong" ]]; then
  CLIENT_ARGS+=(--iterations "$ITERATIONS")
else
  CLIENT_ARGS+=(--total-bytes "$TOTAL_BYTES")
fi

printf -v CLIENT_REMOTE_CMD '%q ' "${CLIENT_ARGS[@]}"
RAW_RESULT="$(${SCRIPT_DIR}/wos_ssh.sh "$CLIENT_HOST" "$CLIENT_REMOTE_CMD")"
RESULT="$(printf '%s\n' "$RAW_RESULT" | awk '/^\{.*\}$/ { line = $0 } END { print line }')"
SERVER_STDOUT="$(${SCRIPT_DIR}/wos_ssh.sh "$SERVER_HOST" "cat $SERVER_LOG 2>/dev/null || true")"

if [[ -z "$RESULT" ]]; then
  echo "ERROR: WOS net benchmark produced no JSON output" >&2
  if [[ -n "$RAW_RESULT" ]]; then
    printf '%s\n' "$RAW_RESULT" >&2
  fi
  if [[ -n "$SERVER_STDOUT" ]]; then
    printf '%s\n' "$SERVER_STDOUT" >&2
  fi
  exit 1
fi

if [[ -z "$OUTPUT" ]]; then
  TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
  SAFE_SERVER="${SERVER_HOST//[^A-Za-z0-9._-]/_}"
  SAFE_CLIENT="${CLIENT_HOST//[^A-Za-z0-9._-]/_}"
  OUTPUT="benchmarks/results/wos/${SAFE_CLIENT}-to-${SAFE_SERVER}-netbench-${MODE}-${TIMESTAMP}.json"
fi

mkdir -p "$(dirname "$OUTPUT")"
printf '%s\n' "$RESULT" > "$OUTPUT"
printf '%s\n' "$RESULT"
if [[ -n "$SERVER_STDOUT" ]]; then
  printf '%s\n' "$SERVER_STDOUT" > "${OUTPUT%.json}.server.log"
fi
echo "Saved result to $OUTPUT"
