#!/bin/bash
# Exercise generic kernel-balanced placement independently of any build.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
WOS_SSH="$WORKSPACE_ROOT/scripts/remote/wos_ssh.sh"

host="wos-0"
runs=50
tasks=80
systems="wos-0,wos-1,wos-2,wos-3"
task_timeout=30
run_timeout=180

usage() {
    cat <<'EOF'
Usage: scripts/test/run_wki_anywhere_stress.sh [options]

Options:
  --host NAME          Submitter contacted over SSH (default: wos-0)
  --runs N             Consecutive stress iterations (default: 50)
  --tasks N            Concurrent anywhere launches per iteration (default: 80)
  --systems CSV        Required healthy system hostnames
                       (default: wos-0,wos-1,wos-2,wos-3)
  --task-timeout N     Per-launch timeout in seconds (default: 30)
  --run-timeout N      Whole remote test timeout in seconds (default: 180)
  -h, --help           Show this help

Every task emits a unique token and its selected hostname through a private
result file. The test rejects failed, lost, duplicated, malformed, stranded,
or unexpectedly placed tasks and requires every named healthy system to
receive work in every iteration.
EOF
}

positive_integer() {
    case "$2" in
        ''|*[!0-9]*|0)
            echo "$1 must be a positive integer" >&2
            exit 2
            ;;
    esac
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --host)
            host="${2:-}"
            shift 2
            ;;
        --runs)
            runs="${2:-}"
            shift 2
            ;;
        --tasks)
            tasks="${2:-}"
            shift 2
            ;;
        --systems)
            systems="${2:-}"
            shift 2
            ;;
        --task-timeout)
            task_timeout="${2:-}"
            shift 2
            ;;
        --run-timeout)
            run_timeout="${2:-}"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "unknown option: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

positive_integer "--runs" "$runs"
positive_integer "--tasks" "$tasks"
positive_integer "--task-timeout" "$task_timeout"
positive_integer "--run-timeout" "$run_timeout"
case "$host" in
    ''|*[!A-Za-z0-9._-]*)
        echo "--host contains unsafe characters" >&2
        exit 2
        ;;
esac
case "$systems" in
    ''|*[!A-Za-z0-9._,-]*|,*|*,|*,,*)
        echo "--systems must be a comma-separated hostname list" >&2
        exit 2
        ;;
esac

exec timeout --signal=TERM --kill-after=10s "${run_timeout}s" \
    "$WOS_SSH" "$host" /usr/bin/timeout -s TERM -k 10 "$run_timeout" \
    /usr/bin/bash -s -- "$runs" "$tasks" "$systems" "$task_timeout" <<'WOS_STRESS'
set -euo pipefail

runs="$1"
tasks="$2"
systems_csv="$3"
task_timeout="$4"
systems="${systems_csv//,/ }"
system_count=0
for system in $systems; do
    case "$system" in
        ''|*[!A-Za-z0-9._-]*) exit 64 ;;
    esac
    system_count=$((system_count + 1))
done
[ "$system_count" -ge 2 ]

active_pids=""
cleanup_active() {
    local pid
    for pid in $active_pids; do
        kill "$pid" 2>/dev/null || true
    done
    for pid in $active_pids; do
        wait "$pid" 2>/dev/null || true
    done
    active_pids=""
}
trap cleanup_active EXIT HUP INT TERM

iteration=1
total_completed=0
while [ "$iteration" -le "$runs" ]; do
    run_dir="/tmp/wki-anywhere-stress.$$.$iteration"
    mkdir "$run_dir"
    active_pids=""

    index=0
    while [ "$index" -lt "$tasks" ]; do
        (
            status=0
            /usr/bin/timeout -s TERM -k 5 "$task_timeout" \
                anywhere /usr/bin/hostname \
                > "$run_dir/result.$index" 2> "$run_dir/stderr.$index" || status=$?
            printf '%s\n' "$status" > "$run_dir/status.$index"
        ) &
        active_pids="$active_pids $!"
        index=$((index + 1))
    done

    for pid in $active_pids; do
        wait "$pid"
    done
    active_pids=""

    completed=0
    index=0
    while [ "$index" -lt "$tasks" ]; do
        status_file="$run_dir/status.$index"
        result_file="$run_dir/result.$index"
        [ -s "$status_file" ] || {
            echo "iteration=$iteration task=$index missing_status" >&2
            exit 1
        }
        IFS= read -r status < "$status_file"
        [ "$status" = 0 ] || {
            echo "iteration=$iteration task=$index status=$status" >&2
            sed 's/^/task-stderr: /' "$run_dir/stderr.$index" >&2 || true
            exit 1
        }
        [ -s "$result_file" ] || {
            echo "iteration=$iteration task=$index missing_result" >&2
            exit 1
        }
        result="$(cat "$result_file")"
        set -- $result
        [ "$#" -eq 1 ] || {
            echo "iteration=$iteration task=$index duplicate_or_malformed_result=$result" >&2
            exit 1
        }
        selected_system="$1"
        case ",$systems_csv," in
            *,"$selected_system",*) ;;
            *)
                echo "iteration=$iteration task=$index unexpected_system=$selected_system" >&2
                exit 1
                ;;
        esac
        : > "$run_dir/seen.$selected_system"
        completed=$((completed + 1))
        index=$((index + 1))
    done

    distribution=""
    for system in $systems; do
        [ -f "$run_dir/seen.$system" ] || {
            echo "iteration=$iteration system=$system received_no_work" >&2
            exit 1
        }
        assigned=0
        for result_file in "$run_dir"/result.*; do
            IFS= read -r selected_system < "$result_file"
            [ "$selected_system" = "$system" ] && assigned=$((assigned + 1))
        done
        distribution="$distribution $system=$assigned"
    done

    total_completed=$((total_completed + completed))
    printf 'iteration=%s completed=%s systems=%s distribution="%s" status=pass\n' \
        "$iteration" "$completed" "$system_count" "${distribution# }"
    rm -f "$run_dir"/result.* "$run_dir"/stderr.* "$run_dir"/status.* "$run_dir"/seen.*
    rmdir "$run_dir"
    iteration=$((iteration + 1))
done

printf 'runs=%s tasks_per_run=%s completed=%s systems=%s status=pass\n' \
    "$runs" "$tasks" "$total_completed" "$system_count"
WOS_STRESS
