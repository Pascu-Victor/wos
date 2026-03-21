#!/bin/bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <hostname-or-ip> --mode read|stat --path <remote-path> [--read-size N] [--iterations N] [--output local-file]" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
HOST="$1"
shift

MODE="read"
REMOTE_PATH=""
READ_SIZE="65536"
ITERATIONS=""
OUTPUT=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      MODE="$2"
      shift 2
      ;;
    --path)
      REMOTE_PATH="$2"
      shift 2
      ;;
    --read-size)
      READ_SIZE="$2"
      shift 2
      ;;
    --iterations)
      ITERATIONS="$2"
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

if [[ -z "$REMOTE_PATH" ]]; then
  echo "ERROR: --path is required" >&2
  exit 1
fi

REMOTE_PATH="$(${SCRIPT_DIR}/wos_resolve.py path "$REMOTE_PATH")"

REMOTE_ARGS=(/mnt/disk/testprog)

case "$MODE" in
  read)
    REMOTE_ARGS+=(vfsbench-read --path "$REMOTE_PATH" --read-size "$READ_SIZE")
    if [[ -n "$ITERATIONS" ]]; then
      REMOTE_ARGS+=(--iterations "$ITERATIONS")
    fi
    ;;
  stat)
    REMOTE_ARGS+=(vfsbench-stat --path "$REMOTE_PATH")
    if [[ -n "$ITERATIONS" ]]; then
      REMOTE_ARGS+=(--iterations "$ITERATIONS")
    fi
    ;;
  *)
    echo "ERROR: --mode must be 'read' or 'stat'" >&2
    exit 1
    ;;
esac

printf -v REMOTE_CMD '%q ' "${REMOTE_ARGS[@]}"
RAW_RESULT="$(${SCRIPT_DIR}/wos_ssh.sh "$HOST" sh -lc "$REMOTE_CMD")"
RESULT="$(printf '%s\n' "$RAW_RESULT" | awk '/^\{.*\}$/ { line = $0 } END { print line }')"

if [[ -z "$RESULT" ]]; then
  echo "ERROR: WOS fs benchmark produced no JSON output" >&2
  if [[ -n "$RAW_RESULT" ]]; then
    printf '%s\n' "$RAW_RESULT" >&2
  fi
  exit 1
fi

if [[ -z "$OUTPUT" ]]; then
  TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
  SAFE_HOST="${HOST//[^A-Za-z0-9._-]/_}"
  OUTPUT="benchmarks/results/wos/${SAFE_HOST}-vfsbench-${MODE}-${TIMESTAMP}.json"
fi

mkdir -p "$(dirname "$OUTPUT")"
printf '%s\n' "$RESULT" > "$OUTPUT"
printf '%s\n' "$RESULT"
echo "Saved result to $OUTPUT"
