#!/bin/bash
set -euo pipefail

WOS_ROOT="${WOS_WORKSPACE_ROOT:-$(git -C "$(dirname "$0")" rev-parse --show-toplevel)}"
REMOTE_SCRIPTS="$WOS_ROOT/scripts/remote"
cd "$WOS_ROOT"

DEFAULT_NETBENCH_CASE_TIMEOUT_SECONDS=120
DEFAULT_PROBE_TIMEOUT=30
DEFAULT_REMOTE_TIMEOUT=7200
LOCAL_SUITE_SCRIPT="$WOS_ROOT/configs/drive/srv/wos_userland_suite.sh"

usage() {
  echo "Usage: $0 <hostname-or-ip> [--scale quick|full|stress] [--shutdown [poweroff|halt|reboot]] [--sync-rootfs|--no-sync] [--config PATH] [--sync-timeout SECONDS] [--probe-timeout SECONDS] [--timeout SECONDS] [--remote-script PATH] [--output local-log] [--env KEY=VALUE] [-- extra remote args...]" >&2
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
SHUTDOWN=""
SYNC_ROOTFS="auto"
CLUSTER_CONFIG="configs/cluster.json"
SYNC_TIMEOUT="300"
PROBE_TIMEOUT="$DEFAULT_PROBE_TIMEOUT"
REMOTE_TIMEOUT="$DEFAULT_REMOTE_TIMEOUT"
ENV_ARGS=()
REMOTE_EXTRA=()

parse_nonnegative_seconds() {
  local name="$1"
  local value="$2"
  if [[ ! "$value" =~ ^[0-9]+$ ]]; then
    echo "$name must be a nonnegative integer number of seconds: $value" >&2
    exit 1
  fi
  printf '%s\n' "$value"
}

run_with_timeout() {
  local seconds="$1"
  shift
  if [[ "$seconds" == "0" ]]; then
    "$@"
    return
  fi
  if ! command -v timeout >/dev/null 2>&1; then
    echo "timeout command is required for bounded userland suite runs" >&2
    exit 127
  fi
  timeout --kill-after=5s "${seconds}s" "$@"
}

has_env_assignment() {
  local key="$1"
  local value
  for value in "${ENV_ARGS[@]}"; do
    if [[ "$value" == "$key="* ]]; then
      return 0
    fi
  done
  return 1
}

host_is_vm_alias() {
  [[ "$HOST" =~ ^vm[0-9]+$ ]]
}

should_sync_rootfs() {
  case "$SYNC_ROOTFS" in
    yes)
      return 0
      ;;
    no)
      return 1
      ;;
  esac
  [[ "$REMOTE_SCRIPT" == "/usr/bin/wos-userland-suite" ]] && host_is_vm_alias
}

sync_suite_script() {
  echo "Syncing /usr/bin/wos-userland-suite before remote run..."
  "$WOS_ROOT/bin/wos-cluster" \
    --sync \
    --config "$CLUSTER_CONFIG" \
    --sync-timeout "$SYNC_TIMEOUT" \
    --filter configs/drive/srv/wos_userland_suite.sh
}

local_suite_revision() {
  awk -F= '/^SUITE_REVISION=/ {
    value = $2
    gsub(/^"/, "", value)
    gsub(/"$/, "", value)
    print value
    exit
  }' "$LOCAL_SUITE_SCRIPT"
}

verify_remote_suite_revision() {
  [[ "$REMOTE_SCRIPT" == "/usr/bin/wos-userland-suite" ]] || return 0

  local expected
  expected="$(local_suite_revision)"
  if [[ -z "$expected" ]]; then
    echo "Unable to read local suite revision from $LOCAL_SUITE_SCRIPT" >&2
    exit 1
  fi

  local remote_script_quoted
  local remote_cmd
  local actual_line
  local actual
  printf -v remote_script_quoted '%q' "$REMOTE_SCRIPT"
  remote_cmd="grep '^SUITE_REVISION=' $remote_script_quoted"

  set +e
  actual_line="$(run_with_timeout "$PROBE_TIMEOUT" "${REMOTE_SCRIPTS}/wos_ssh.sh" "$HOST" "$remote_cmd" 2>/dev/null | awk '/^SUITE_REVISION=/ { print; exit }')"
  set -e
  actual="${actual_line#SUITE_REVISION=}"
  actual="${actual#\"}"
  actual="${actual%\"}"

  if [[ "$actual" != "$expected" ]]; then
    echo "Remote $REMOTE_SCRIPT is stale or missing SUITE_REVISION; expected '$expected', got '${actual:-<none>}'." >&2
    echo "Run with --sync-rootfs, omit --no-sync, or rebuild/relaunch the rootfs before starting the suite." >&2
    exit 1
  fi
}

suite_requested_shutdown_success() {
  awk '
    /^PASS=[0-9]+ FAIL=0 SKIP=[0-9]+$/ { ok = 1 }
    /^REQUESTED_SHUTDOWN=(poweroff|halt|reboot)$/ { shutdown = 1 }
    END { exit !(ok && shutdown) }
  ' "$OUTPUT"
}

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
    --shutdown)
      if [[ $# -ge 2 && "$2" != --* ]]; then
        SHUTDOWN="$2"
        shift 2
      else
        SHUTDOWN="poweroff"
        shift
      fi
      ;;
    --sync-rootfs)
      SYNC_ROOTFS="yes"
      shift
      ;;
    --no-sync)
      SYNC_ROOTFS="no"
      shift
      ;;
    --config)
      CLUSTER_CONFIG="$2"
      shift 2
      ;;
    --sync-timeout)
      SYNC_TIMEOUT="$2"
      shift 2
      ;;
    --probe-timeout)
      PROBE_TIMEOUT="$(parse_nonnegative_seconds "--probe-timeout" "$2")"
      shift 2
      ;;
    --timeout)
      REMOTE_TIMEOUT="$(parse_nonnegative_seconds "--timeout" "$2")"
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

if [[ -n "$SHUTDOWN" ]]; then
  case "$SHUTDOWN" in
    shutdown|poweroff)
      SHUTDOWN="poweroff"
      ;;
    halt|reboot)
      ;;
    *)
      echo "Invalid --shutdown action: $SHUTDOWN" >&2
      usage
      exit 1
      ;;
  esac
  ENV_ARGS+=("WOS_SUITE_SHUTDOWN=$SHUTDOWN")
fi

if ! has_env_assignment WOS_SUITE_NETBENCH_CASE_TIMEOUT_SECONDS; then
  ENV_ARGS+=("WOS_SUITE_NETBENCH_CASE_TIMEOUT_SECONDS=$DEFAULT_NETBENCH_CASE_TIMEOUT_SECONDS")
fi

if [[ -z "$OUTPUT" ]]; then
  TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
  SAFE_HOST="${HOST//[^A-Za-z0-9._-]/_}"
  OUTPUT="${WOS_ROOT}/benchmarks/results/wos/${SAFE_HOST}-userland-suite-${TIMESTAMP}.log"
fi

mkdir -p "$(dirname "$OUTPUT")"

if should_sync_rootfs; then
  sync_suite_script
fi

verify_remote_suite_revision

REMOTE_CMD_WORDS=("${ENV_ARGS[@]}" "$REMOTE_SCRIPT" "${REMOTE_EXTRA[@]}")
printf -v REMOTE_CMD '%q ' "${REMOTE_CMD_WORDS[@]}"

set +e
run_with_timeout "$REMOTE_TIMEOUT" "${REMOTE_SCRIPTS}/wos_ssh.sh" "$HOST" "$REMOTE_CMD" 2>&1 | tee "$OUTPUT"
STATUS="${PIPESTATUS[0]}"
set -e
if [[ "$REMOTE_TIMEOUT" != "0" && "$STATUS" -eq 124 ]]; then
  echo "TIMEOUT after ${REMOTE_TIMEOUT}s" | tee -a "$OUTPUT"
fi
if [[ "$STATUS" -ne 0 && "$STATUS" -ne 124 ]] && suite_requested_shutdown_success; then
  STATUS=0
fi

REMOTE_RESULT_DIR="$(awk -F= '/^RESULT_DIR=/ { value = $2 } END { print value }' "$OUTPUT")"
echo "Saved suite log to $OUTPUT"
if [[ -n "$REMOTE_RESULT_DIR" ]]; then
  echo "Remote artifacts: $REMOTE_RESULT_DIR"
fi

exit "$STATUS"
