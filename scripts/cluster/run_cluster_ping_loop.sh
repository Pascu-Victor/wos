#!/usr/bin/env bash
set -euo pipefail

WOS_ROOT="${WOS_WORKSPACE_ROOT:-$(git -C "$(dirname "$0")/../.." rev-parse --show-toplevel)}"
RESOLVER="$WOS_ROOT/scripts/remote/wos_resolve.py"

if command -v wos-cluster >/dev/null 2>&1; then
  WOS_CLUSTER="${WOS_CLUSTER:-wos-cluster}"
else
  WOS_CLUSTER="${WOS_CLUSTER:-$WOS_ROOT/bin/wos-cluster}"
fi

RUNS=100
BOOT_SLEEP=20
PING_TIMEOUT=1
TARGETS=(vm0 vm1 vm2 vm3)
STOP_ON_FAILURE=0
LOG_DIR="$WOS_ROOT/test-results/cluster-ping-loop-$(date +%Y%m%d-%H%M%S)"
ACTIVE_CLUSTER_PID=""

usage() {
  cat >&2 <<EOF
Usage: $(basename "$0") [options] [-- extra wos-cluster args...]

Launches 'wos-cluster --launch', sleeps, pings vm0..vm3, and repeats.
Successful runs stop the cluster by sending SIGINT to the wos-cluster process.

Options:
  --runs N             Number of launch/ping cycles (default: 100)
  --sleep SECONDS      Seconds to wait before pinging (default: 7)
  --ping-timeout SEC   Per-ping timeout in seconds (default: 1)
  --targets LIST       Comma-separated ping targets (default: vm0,vm1,vm2,vm3)
  --log-dir DIR        Directory for per-run wos-cluster logs
  --stop-on-failure    Also SIGINT the cluster after a failed ping
  -h, --help           Show this help
EOF
}

cleanup_active_cluster() {
  if [[ -n "$ACTIVE_CLUSTER_PID" ]] && kill -0 "$ACTIVE_CLUSTER_PID" 2>/dev/null; then
    echo "Stopping active wos-cluster pid $ACTIVE_CLUSTER_PID"
    kill -INT "$ACTIVE_CLUSTER_PID" 2>/dev/null || true
    wait "$ACTIVE_CLUSTER_PID" 2>/dev/null || true
  fi
}

trap 'cleanup_active_cluster; exit 130' INT
trap 'cleanup_active_cluster; exit 143' TERM

CLUSTER_ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --runs)
      RUNS="$2"
      shift 2
      ;;
    --sleep)
      BOOT_SLEEP="$2"
      shift 2
      ;;
    --ping-timeout)
      PING_TIMEOUT="$2"
      shift 2
      ;;
    --targets)
      IFS=',' read -r -a TARGETS <<<"$2"
      shift 2
      ;;
    --log-dir)
      LOG_DIR="$2"
      shift 2
      ;;
    --stop-on-failure)
      STOP_ON_FAILURE=1
      shift
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    --)
      shift
      CLUSTER_ARGS=("$@")
      break
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 2
      ;;
  esac
done

mkdir -p "$LOG_DIR"
echo "Writing per-run wos-cluster logs to $LOG_DIR"

refresh_sudo() {
  if command -v sudo >/dev/null 2>&1; then
    sudo -v
  fi
}

resolve_target() {
  local target="$1"
  local resolved
  if resolved="$("$RESOLVER" target "$target" 2>/dev/null)"; then
    printf '%s\n' "$resolved"
  else
    printf '%s\n' "$target"
  fi
}

stop_cluster() {
  local pid="$1"
  if ! kill -0 "$pid" 2>/dev/null; then
    wait "$pid" 2>/dev/null || true
    return
  fi
  kill -INT "$pid"
  wait "$pid" || true
}

for ((run = 1; run <= RUNS; run++)); do
  printf '\n=== run %d/%d ===\n' "$run" "$RUNS"
  run_log="$LOG_DIR/run-$(printf '%03d' "$run").log"

  refresh_sudo
  "$WOS_CLUSTER" --launch "${CLUSTER_ARGS[@]}" > >(tee "$run_log") 2>&1 &
  ACTIVE_CLUSTER_PID="$!"
  echo "Started wos-cluster pid $ACTIVE_CLUSTER_PID"
  echo "Sleeping ${BOOT_SLEEP}s before ping checks"
  sleep "$BOOT_SLEEP"

  if ! kill -0 "$ACTIVE_CLUSTER_PID" 2>/dev/null; then
    echo "wos-cluster exited before ping checks; see $run_log" >&2
    wait "$ACTIVE_CLUSTER_PID" || true
    ACTIVE_CLUSTER_PID=""
    exit 1
  fi

  failed=()
  for target in "${TARGETS[@]}"; do
    resolved="$(resolve_target "$target")"
    printf 'ping %-4s -> %-15s ... ' "$target" "$resolved"
    if ping -n -c 1 -W "$PING_TIMEOUT" "$resolved" >/dev/null 2>&1; then
      echo "ok"
    else
      echo "FAILED"
      failed+=("$target:$resolved")
    fi
  done

  if (( ${#failed[@]} != 0 )); then
    echo "Run $run failed ping target(s): ${failed[*]}" >&2
    echo "wos-cluster log: $run_log" >&2
    if (( STOP_ON_FAILURE )); then
      stop_cluster "$ACTIVE_CLUSTER_PID"
      ACTIVE_CLUSTER_PID=""
    else
      echo "Leaving wos-cluster pid $ACTIVE_CLUSTER_PID running for debugging." >&2
      disown "$ACTIVE_CLUSTER_PID" 2>/dev/null || true
      ACTIVE_CLUSTER_PID=""
    fi
    exit 1
  fi

  echo "All targets responded; sending SIGINT to wos-cluster pid $ACTIVE_CLUSTER_PID"
  stop_cluster "$ACTIVE_CLUSTER_PID"
  ACTIVE_CLUSTER_PID=""
done

echo "Completed $RUNS successful run(s)."
