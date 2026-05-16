#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

usage() {
  echo "Usage: $0 <hostname-or-ip> [--scale quick|full|stress] [--remote-script PATH] [--output local-log] [--env KEY=VALUE] [-- extra remote args...]" >&2
}

if [[ $# -lt 1 ]]; then
  usage
  exit 1
fi

HOST="$1"
shift

REMOTE_SCRIPT="/usr/bin/wos-userland-suite"
SCALE=""
OUTPUT=""
ENV_ARGS=()
REMOTE_EXTRA=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --scale)
      SCALE="$2"
      shift 2
      ;;
    --remote-script)
      REMOTE_SCRIPT="$2"
      shift 2
      ;;
    --output)
      OUTPUT="$2"
      shift 2
      ;;
    --env)
      ENV_ARGS+=("$2")
      shift 2
      ;;
    --)
      shift
      REMOTE_EXTRA+=("$@")
      break
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -n "$SCALE" ]]; then
  ENV_ARGS+=("WOS_SUITE_SCALE=$SCALE")
fi

if [[ -z "$OUTPUT" ]]; then
  TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
  SAFE_HOST="${HOST//[^A-Za-z0-9._-]/_}"
  OUTPUT="${ROOT_DIR}/benchmarks/results/wos/${SAFE_HOST}-userland-suite-${TIMESTAMP}.log"
fi

mkdir -p "$(dirname "$OUTPUT")"

REMOTE_CMD_WORDS=("${ENV_ARGS[@]}" "$REMOTE_SCRIPT" "${REMOTE_EXTRA[@]}")
printf -v REMOTE_CMD '%q ' "${REMOTE_CMD_WORDS[@]}"

set +e
RAW_RESULT="$("${SCRIPT_DIR}/wos_ssh.sh" "$HOST" sh -lc "$REMOTE_CMD" 2>&1)"
STATUS="$?"
set -e

printf '%s\n' "$RAW_RESULT" | tee "$OUTPUT"

REMOTE_RESULT_DIR="$(printf '%s\n' "$RAW_RESULT" | awk -F= '/^RESULT_DIR=/ { value = $2 } END { print value }')"
echo "Saved suite log to $OUTPUT"
if [[ -n "$REMOTE_RESULT_DIR" ]]; then
  echo "Remote artifacts: $REMOTE_RESULT_DIR"
fi

exit "$STATUS"
