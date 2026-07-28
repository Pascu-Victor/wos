#!/bin/bash
# Verify that remote pipe writers stop and release IPC state after the reader closes.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
WOS_SSH="$WORKSPACE_ROOT/scripts/remote/wos_ssh.sh"

host="wos-0"
runs=30
systems="wos-1,wos-2,wos-3"
task_timeout=5
converge_timeout=120
run_timeout=300

usage() {
    cat <<'EOF'
Usage: scripts/test/run_wki_pipe_reader_close_stress.sh [options]

Options:
  --host NAME              Submitter contacted over SSH (default: wos-0)
  --runs N                 Iterations per remote system (default: 30)
  --systems CSV            Named remote systems (default: wos-1,wos-2,wos-3)
  --task-timeout N         Per-pipeline timeout in seconds (default: 5)
  --converge-timeout N     Named-system readiness timeout (default: 120)
  --run-timeout N          Whole remote test timeout in seconds (default: 300)
  -h, --help               Show this help

Each iteration runs an unbounded producer on a named remote system and closes
its reader after one line. The writer must observe the closed pipe without
timing out, and vm0 must drain all temporary WKI IPC pipe state afterward.
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
        --systems)
            systems="${2:-}"
            shift 2
            ;;
        --task-timeout)
            task_timeout="${2:-}"
            shift 2
            ;;
        --converge-timeout)
            converge_timeout="${2:-}"
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
positive_integer "--task-timeout" "$task_timeout"
positive_integer "--converge-timeout" "$converge_timeout"
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
    /usr/bin/bash -s -- "$runs" "$systems" "$task_timeout" "$converge_timeout" "$host" <<'WOS_STRESS'
set -euo pipefail

runs="$1"
systems_csv="$2"
task_timeout="$3"
converge_timeout="$4"
submitter="$5"
systems="${systems_csv//,/ }"

for system in $systems; do
    case "$system" in
        ''|*[!A-Za-z0-9._-]*) exit 64 ;;
    esac
done

for system in $systems; do
    ready=0
    waited=0
    while [ "$waited" -lt "$converge_timeout" ]; do
        status=0
        result="$(/usr/bin/timeout -s TERM -k 2 5 on "$system" /usr/bin/hostname </dev/null 2>/dev/null)" || status=$?
        route_status=0
        /usr/bin/timeout -s TERM -k 2 5 on "$system" /usr/bin/test -x "/wki/$submitter/usr/bin/yes" \
            </dev/null >/dev/null 2>&1 || route_status=$?
        mount_status=0
        mount_table="$(/usr/bin/timeout -s TERM -k 2 5 on "$system" /usr/bin/cat /proc/mounts </dev/null 2>/dev/null)" || mount_status=$?
        mounts_ready=1
        for suffix in "" /boot /oldroot /tmp /run; do
            needle="remote /wki/$submitter$suffix remote"
            case "$mount_table" in
                *"$needle"*) ;;
                *) mounts_ready=0 ;;
            esac
        done
        if [ "$status" -eq 0 ] && [ "$result" = "$system" ] && [ "$route_status" -eq 0 ] && \
            [ "$mount_status" -eq 0 ] && [ "$mounts_ready" -eq 1 ]; then
            ready=1
            break
        fi
        sleep 1
        waited=$((waited + 1))
    done
    [ "$ready" -eq 1 ] || {
        echo "system=$system readiness_timeout=${converge_timeout}s" >&2
        exit 1
    }
done

completed=0
iteration=1
while [ "$iteration" -le "$runs" ]; do
    for system in $systems; do
        token="wki-reader-close-$iteration-$system"
        result_file="/tmp/wki-reader-close.$$"
        error_file="$result_file.err"
        status=0
        /usr/bin/timeout -s TERM -k 2 "$task_timeout" \
            /usr/bin/bash -o pipefail -c 'on "$1" /usr/bin/yes "$2" </dev/null | /usr/bin/head -n 1' \
            _ "$system" "$token" > "$result_file" 2> "$error_file" || status=$?

        case "$status" in
            0|1|141) ;;
            *)
                echo "iteration=$iteration system=$system status=$status" >&2
                sed 's/^/pipeline-stderr: /' "$error_file" >&2 || true
                rm -f "$result_file" "$error_file"
                exit 1
                ;;
        esac
        result="$(cat "$result_file")"
        rm -f "$result_file" "$error_file"
        [ "$result" = "$token" ] || {
            echo "iteration=$iteration system=$system malformed_result=$result status=$status" >&2
            exit 1
        }
        completed=$((completed + 1))
    done
    printf 'iteration=%s systems=%s status=pass\n' "$iteration" "$systems_csv"
    iteration=$((iteration + 1))
done

sleep 2
stats="$(cat /proc/kipcstat)"
for field in exports proxies pending_deliveries pending_chunks pending_bytes \
    export_backlogs export_backlog_chunks export_backlog_bytes \
    export_close_pending export_flush_queue proxy_close_queue dev_op_queue \
    dev_op_payload_bytes approx_alloc_bytes; do
    value=""
    for pair in $stats; do
        case "$pair" in
            "$field"=*)
                value="${pair#*=}"
                break
                ;;
        esac
    done
    [ "$value" = 0 ] || {
        echo "undrained_ipc_state field=$field value=${value:-missing}" >&2
        echo "$stats" >&2
        exit 1
    }
done

ownerless="$(awk '
    /^pipe=/ {
        write_refs = -1
        write_fds = -1
        for (i = 1; i <= NF; ++i) {
            if ($i ~ /^write_refs=/) {
                split($i, values, "=")
                write_refs = values[2]
            } else if ($i ~ /^write_fds=/) {
                split($i, values, "=")
                write_fds = values[2]
            }
        }
        if (write_refs > 0 && write_fds == 0) {
            print
        }
    }
' /proc/wki/pipes)"
[ -z "$ownerless" ] || {
    echo "ownerless pipe writer references remain:" >&2
    echo "$ownerless" >&2
    exit 1
}

printf 'runs=%s systems=%s completed=%s status=pass\n' \
    "$runs" "$systems_csv" "$completed"
WOS_STRESS
