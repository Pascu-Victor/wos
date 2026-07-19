#!/bin/sh
# Aggregate WOS userspace tests and benchmarks.
#
# Run in WOS:
#   /usr/bin/wos-userland-suite
#
# Useful knobs:
#   WOS_SUITE_SCALE=quick|full|stress
#   WOS_SUITE_RUN_TESTD=0
#   WOS_SUITE_RUN_TESTPROG_PERF=0
#   WOS_SUITE_RUN_STRACE=0
#   WOS_SUITE_RUN_PERF_TRACE=0
#   WOS_SUITE_KEEP_LARGE_WORK=1       # keep generated large *.bin files after successful runs
#   WOS_SUITE_CASE_TIMEOUT_SECONDS=N   # override per-test timeout (0 disables)
#   WOS_SUITE_TIMEOUT_KILL_GRACE_SECONDS=5
#   WOS_SUITE_SHUTDOWN=poweroff|halt|reboot  # request shutdown after printing the summary

# shellcheck disable=SC2329

PATH=/usr/bin:/usr/sbin:/bin:/sbin
export PATH

PASS=0
FAIL=0
SKIP=0

timestamp() {
    date +%Y%m%d-%H%M%S 2>/dev/null || echo unknown-time
}

RUN_ID="${WOS_SUITE_RUN_ID:-$(timestamp)}"
SUITE_REVISION="case-watchdog-v5"
SCALE="${WOS_SUITE_SCALE:-full}"

case "$SCALE" in
    quick)
        DEFAULT_BEE_ITERATIONS=200
        DEFAULT_DATA_MIB=8
        DEFAULT_MANDEL_WIDTH=640
        DEFAULT_MANDEL_HEIGHT=360
        DEFAULT_MANDEL_MAX_ITER=750
        DEFAULT_MANDEL_REPEAT=1
        DEFAULT_RENDER_WIDTH=640
        DEFAULT_RENDER_HEIGHT=360
        DEFAULT_RENDER_SPP=4
        DEFAULT_RENDER_MAX_DEPTH=4
        DEFAULT_NET_ITERATIONS=250
        DEFAULT_NET_TOTAL_BYTES=8388608
        DEFAULT_RUN_TESTPROG_PERF=0
        DEFAULT_CASE_TIMEOUT_SECONDS=900
        ;;
    stress)
        DEFAULT_BEE_ITERATIONS=10000
        DEFAULT_DATA_MIB=256
        DEFAULT_MANDEL_WIDTH=2048
        DEFAULT_MANDEL_HEIGHT=2048
        DEFAULT_MANDEL_MAX_ITER=5000
        DEFAULT_MANDEL_REPEAT=5
        DEFAULT_RENDER_WIDTH=1920
        DEFAULT_RENDER_HEIGHT=1080
        DEFAULT_RENDER_SPP=32
        DEFAULT_RENDER_MAX_DEPTH=8
        DEFAULT_NET_ITERATIONS=10000
        DEFAULT_NET_TOTAL_BYTES=268435456
        DEFAULT_RUN_TESTPROG_PERF=1
        DEFAULT_CASE_TIMEOUT_SECONDS=900
        ;;
    full|*)
        DEFAULT_BEE_ITERATIONS=1000
        DEFAULT_DATA_MIB=64
        DEFAULT_MANDEL_WIDTH=1600
        DEFAULT_MANDEL_HEIGHT=900
        DEFAULT_MANDEL_MAX_ITER=3000
        DEFAULT_MANDEL_REPEAT=2
        DEFAULT_RENDER_WIDTH=1280
        DEFAULT_RENDER_HEIGHT=720
        DEFAULT_RENDER_SPP=16
        DEFAULT_RENDER_MAX_DEPTH=6
        DEFAULT_NET_ITERATIONS=2000
        DEFAULT_NET_TOTAL_BYTES=67108864
        DEFAULT_RUN_TESTPROG_PERF=1
        DEFAULT_CASE_TIMEOUT_SECONDS=900
        ;;
esac

ARTIFACT_ROOT="${WOS_SUITE_ARTIFACT_ROOT:-/tmp/wos-userland-suite-$RUN_ID}"
LOG_DIR="$ARTIFACT_ROOT/logs"
WORK_DIR="$ARTIFACT_ROOT/work"
SUMMARY_FILE="$ARTIFACT_ROOT/summary.tsv"

BEE_ITERATIONS="${WOS_SUITE_BEE_ITERATIONS:-$DEFAULT_BEE_ITERATIONS}"
DATA_MIB="${WOS_SUITE_DATA_MIB:-$DEFAULT_DATA_MIB}"
if [ "${WOS_SUITE_DATA_FILE+x}" = x ]; then
    DATA_FILE="$WOS_SUITE_DATA_FILE"
    DATA_FILE_OWNED=0
else
    DATA_FILE="$WORK_DIR/large-input.bin"
    DATA_FILE_OWNED=1
fi
VFS_READ_SIZE="${WOS_SUITE_VFS_READ_SIZE:-65536}"
VFS_READ_ITERATIONS="${WOS_SUITE_VFS_READ_ITERATIONS:-4}"
VFS_STAT_ITERATIONS="${WOS_SUITE_VFS_STAT_ITERATIONS:-10000}"

MANDEL_WIDTH="${WOS_SUITE_MANDEL_WIDTH:-$DEFAULT_MANDEL_WIDTH}"
MANDEL_HEIGHT="${WOS_SUITE_MANDEL_HEIGHT:-$DEFAULT_MANDEL_HEIGHT}"
MANDEL_MAX_ITER="${WOS_SUITE_MANDEL_MAX_ITER:-$DEFAULT_MANDEL_MAX_ITER}"
MANDEL_THREADS="${WOS_SUITE_MANDEL_THREADS:-8}"
MANDEL_REPEAT="${WOS_SUITE_MANDEL_REPEAT:-$DEFAULT_MANDEL_REPEAT}"
if [ "${WOS_SUITE_MANDEL_NODES+x}" = x ]; then
    MANDEL_NODES="$WOS_SUITE_MANDEL_NODES"
else
    # Auto-placement deliberately requires a remote peer. Keep the aggregate
    # suite valid on a one-node cluster while allowing distributed runs to
    # provide a comma-separated node list explicitly.
    MANDEL_NODES="$(hostname)"
fi

RENDER_SCENE="${WOS_SUITE_RENDER_SCENE:-/srv/Duck.glb}"
RENDER_WIDTH="${WOS_SUITE_RENDER_WIDTH:-$DEFAULT_RENDER_WIDTH}"
RENDER_HEIGHT="${WOS_SUITE_RENDER_HEIGHT:-$DEFAULT_RENDER_HEIGHT}"
RENDER_SPP="${WOS_SUITE_RENDER_SPP:-$DEFAULT_RENDER_SPP}"
RENDER_MAX_DEPTH="${WOS_SUITE_RENDER_MAX_DEPTH:-$DEFAULT_RENDER_MAX_DEPTH}"
RENDER_TILE_SIZE="${WOS_SUITE_RENDER_TILE_SIZE:-24}"
RENDER_THREADS="${WOS_SUITE_RENDER_THREADS:-0}"

NETBENCH_PORT="${WOS_SUITE_NETBENCH_PORT:-9100}"
NETBENCH_PAYLOAD_SIZE="${WOS_SUITE_NETBENCH_PAYLOAD_SIZE:-4096}"
NETBENCH_ITERATIONS="${WOS_SUITE_NETBENCH_ITERATIONS:-$DEFAULT_NET_ITERATIONS}"
NETBENCH_TOTAL_BYTES="${WOS_SUITE_NETBENCH_TOTAL_BYTES:-$DEFAULT_NET_TOTAL_BYTES}"
NETBENCH_STARTUP_SECONDS="${WOS_SUITE_NETBENCH_STARTUP_SECONDS:-1}"
NETBENCH_TIMEOUT_MS="${WOS_SUITE_NETBENCH_TIMEOUT_MS:-30000}"
NETBENCH_CASE_TIMEOUT_SECONDS="${WOS_SUITE_NETBENCH_CASE_TIMEOUT_SECONDS:-120}"

RUN_TESTD="${WOS_SUITE_RUN_TESTD:-1}"
RUN_TESTPROG_PERF="${WOS_SUITE_RUN_TESTPROG_PERF:-$DEFAULT_RUN_TESTPROG_PERF}"
RUN_STRACE="${WOS_SUITE_RUN_STRACE:-1}"
RUN_PERF_TRACE="${WOS_SUITE_RUN_PERF_TRACE:-1}"
KEEP_LARGE_WORK="${WOS_SUITE_KEEP_LARGE_WORK:-0}"
CASE_TIMEOUT_SECONDS="${WOS_SUITE_CASE_TIMEOUT_SECONDS:-$DEFAULT_CASE_TIMEOUT_SECONDS}"
TIMEOUT_KILL_GRACE_SECONDS="${WOS_SUITE_TIMEOUT_KILL_GRACE_SECONDS:-5}"
SUITE_SHUTDOWN="${WOS_SUITE_SHUTDOWN:-0}"

case "$CASE_TIMEOUT_SECONDS" in
    ""|*[!0-9]*)
        printf 'invalid WOS_SUITE_CASE_TIMEOUT_SECONDS=%s; using %s\n' "$CASE_TIMEOUT_SECONDS" "$DEFAULT_CASE_TIMEOUT_SECONDS"
        CASE_TIMEOUT_SECONDS="$DEFAULT_CASE_TIMEOUT_SECONDS"
        ;;
esac

case "$TIMEOUT_KILL_GRACE_SECONDS" in
    ""|*[!0-9]*)
        printf 'invalid WOS_SUITE_TIMEOUT_KILL_GRACE_SECONDS=%s; using 5\n' "$TIMEOUT_KILL_GRACE_SECONDS"
        TIMEOUT_KILL_GRACE_SECONDS=5
        ;;
esac

case "$NETBENCH_CASE_TIMEOUT_SECONDS" in
    ""|*[!0-9]*)
        printf 'invalid WOS_SUITE_NETBENCH_CASE_TIMEOUT_SECONDS=%s; using 120\n' "$NETBENCH_CASE_TIMEOUT_SECONDS"
        NETBENCH_CASE_TIMEOUT_SECONDS=120
        ;;
esac

case "$SUITE_SHUTDOWN" in
    ""|0|false|False|FALSE|no|No|NO|none|None|NONE)
        SUITE_SHUTDOWN=0
        ;;
    shutdown|poweroff)
        SUITE_SHUTDOWN=poweroff
        ;;
    halt|reboot)
        ;;
    *)
        printf 'invalid WOS_SUITE_SHUTDOWN=%s; shutdown request disabled\n' "$SUITE_SHUTDOWN"
        SUITE_SHUTDOWN=0
        ;;
esac

mkdir -p "$LOG_DIR" "$WORK_DIR"
rm -f "$SUMMARY_FILE"
ln -sfn "$ARTIFACT_ROOT" /tmp/wos-userland-suite-latest 2>/dev/null || true

request_suite_shutdown() {
    case "$SUITE_SHUTDOWN" in
        0)
            return 0
            ;;
        poweroff)
            shutdown_cmd=/sbin/poweroff
            ;;
        halt)
            shutdown_cmd=/sbin/halt
            ;;
        reboot)
            shutdown_cmd=/sbin/reboot
            ;;
        *)
            return 0
            ;;
    esac

    printf 'REQUESTED_SHUTDOWN=%s\n' "$SUITE_SHUTDOWN"
    if [ ! -x "$shutdown_cmd" ]; then
        printf 'shutdown request skipped: missing %s\n' "$shutdown_cmd"
        return 0
    fi
    "$shutdown_cmd" -f || printf 'shutdown request failed: %s -f rc=%s\n' "$shutdown_cmd" "$?"
}

record_summary() {
    printf '%s\t%s\t%s\n' "$1" "$2" "$3" >> "$SUMMARY_FILE"
}

print_command() {
    printf 'command:'
    for arg in "$@"; do
        printf ' %s' "$arg"
    done
    printf '\n'
}

cat_log_file() {
    log_file="$1"
    if [ -s "$log_file" ]; then
        cat "$log_file"
    fi
}

status_file_value() {
    status_file="$1"
    if [ -f "$status_file" ]; then
        read -r status_value < "$status_file" || status_value=124
        case "$status_value" in
            ""|*[!0-9]*)
                printf '124\n'
                ;;
            *)
                printf '%s\n' "$status_value"
                ;;
        esac
    else
        printf '124\n'
    fi
}

run_case() {
    name="$1"
    shift
    log="$LOG_DIR/$name.log"
    timeout_marker="$WORK_DIR/$name.timeout"
    case_status="$WORK_DIR/$name.status"
    start="$(date +%s 2>/dev/null || echo 0)"
    timed_out=0

    rm -f "$timeout_marker" "$case_status"

    printf '\n=== RUN %s ===\n' "$name"
    print_command "$@"
    if [ "$CASE_TIMEOUT_SECONDS" -gt 0 ] 2>/dev/null; then
        printf 'timeout: %ss\n' "$CASE_TIMEOUT_SECONDS"
        case_has_group=0
        monitor_was_on=0
        case "$-" in
            *m*) monitor_was_on=1 ;;
        esac
        set -m 2>/dev/null || true
        case "$-" in
            *m*) case_has_group=1 ;;
        esac
        (
            "$@"
            printf '%s\n' "$?" > "$case_status"
        ) > "$log" 2>&1 &
        case_pid="$!"
        (
            trap 'exit 0' TERM INT HUP

            watchdog_elapsed=0
            while [ "$watchdog_elapsed" -lt "$CASE_TIMEOUT_SECONDS" ]; do
                if [ -f "$case_status" ]; then
                    exit 0
                fi
                sleep 1
                watchdog_elapsed=$((watchdog_elapsed + 1))
            done

            if [ -f "$case_status" ]; then
                exit 0
            fi

            if [ "$case_has_group" -eq 1 ]; then
                kill -TERM "-$case_pid" >/dev/null 2>&1 || kill "$case_pid" >/dev/null 2>&1 || true
            else
                kill "$case_pid" >/dev/null 2>&1 || true
            fi

            if [ ! -f "$case_status" ]; then
                printf 'timeout after %ss\n' "$CASE_TIMEOUT_SECONDS" > "$timeout_marker"

                grace_elapsed=0
                while [ "$grace_elapsed" -lt "$TIMEOUT_KILL_GRACE_SECONDS" ]; do
                    if [ -f "$case_status" ]; then
                        exit 0
                    fi
                    sleep 1
                    grace_elapsed=$((grace_elapsed + 1))
                done

                if [ -f "$case_status" ]; then
                    exit 0
                fi

                if [ "$case_has_group" -eq 1 ]; then
                    kill -KILL "-$case_pid" >/dev/null 2>&1 || kill -KILL "$case_pid" >/dev/null 2>&1 || true
                else
                    kill -KILL "$case_pid" >/dev/null 2>&1 || true
                fi
            fi
        ) &
        watchdog_pid="$!"
        if [ "$monitor_was_on" -eq 0 ]; then
            set +m 2>/dev/null || true
        fi

        rc=124
        while [ ! -f "$case_status" ]; do
            if [ -f "$timeout_marker" ]; then
                timed_out=1
                break
            fi
            sleep 1
        done
        if [ -f "$case_status" ]; then
            rc="$(status_file_value "$case_status")"
            wait "$case_pid" 2>/dev/null || true
        fi
        wait "$watchdog_pid" 2>/dev/null || true
        if [ ! -f "$case_status" ]; then
            wait "$case_pid" 2>/dev/null || true
        fi

        if [ -f "$timeout_marker" ]; then
            timed_out=1
            rc=124
            printf '\nTIMEOUT %s after %ss\n' "$name" "$CASE_TIMEOUT_SECONDS" >> "$log"
        fi
    else
        printf 'timeout: disabled\n'
        "$@" > "$log" 2>&1
        rc="$?"
    fi
    if [ "$timed_out" -eq 1 ]; then
        printf 'case log retained at %s after timeout\n' "$log"
    else
        cat_log_file "$log"
    fi

    end="$(date +%s 2>/dev/null || echo 0)"
    duration=0
    if [ "$start" -gt 0 ] 2>/dev/null && [ "$end" -ge "$start" ] 2>/dev/null; then
        duration=$((end - start))
    fi

    if [ "$rc" -eq 0 ]; then
        PASS=$((PASS + 1))
        printf 'PASS %s (%ss)\n' "$name" "$duration"
        record_summary "$name" "PASS" "${duration}s"
    elif [ "$timed_out" -eq 1 ]; then
        FAIL=$((FAIL + 1))
        printf 'FAIL %s timeout=%ss (%ss)\n' "$name" "$CASE_TIMEOUT_SECONDS" "$duration"
        record_summary "$name" "FAIL" "timeout=${CASE_TIMEOUT_SECONDS}s ${duration}s"
    else
        FAIL=$((FAIL + 1))
        printf 'FAIL %s rc=%s (%ss)\n' "$name" "$rc" "$duration"
        record_summary "$name" "FAIL" "rc=$rc ${duration}s"
    fi
    return 0
}

run_case_with_timeout() {
    name="$1"
    timeout_seconds="$2"
    shift 2

    old_case_timeout="$CASE_TIMEOUT_SECONDS"
    CASE_TIMEOUT_SECONDS="$timeout_seconds"
    run_case "$name" "$@"
    CASE_TIMEOUT_SECONDS="$old_case_timeout"
}

skip_case() {
    name="$1"
    reason="$2"
    SKIP=$((SKIP + 1))
    printf '\n=== SKIP %s ===\n%s\n' "$name" "$reason"
    record_summary "$name" "SKIP" "$reason"
}

skip_case_group() {
    reason="$1"
    shift
    for name in "$@"; do
        skip_case "$name" "$reason"
    done
}

cleanup_large_work() {
    if [ "$KEEP_LARGE_WORK" = "1" ]; then
        printf 'keeping large work files in %s\n' "$WORK_DIR"
        return
    fi

    rm -f "$WORK_DIR/cp-output.bin" "$WORK_DIR/dd-output.bin"
    if [ "$DATA_FILE_OWNED" = "1" ]; then
        rm -f "$DATA_FILE"
    fi
    printf 'removed large work files from %s\n' "$WORK_DIR"
}

have_exe() {
    command -v "$1" >/dev/null 2>&1 || [ -x "$1" ]
}

require_exe() {
    name="$1"
    path="$2"
    if have_exe "$path"; then
        return 0
    fi
    skip_case "$name" "missing executable: $path"
    return 1
}

case_shell_validate() {
    sh /srv/tests/validate.sh
}

case_testd() {
    /usr/bin/testd
}

case_testprog_smoke() {
    /usr/bin/testprog
}

case_cat_bee_ipc() {
    if [ ! -f /srv/bee.txt ]; then
        printf '/srv/bee.txt missing\n'
        return 1
    fi

    lines_per_cat="$(wc -l < /srv/bee.txt | awk '{print $1}')"
    expected_lines=$((lines_per_cat * BEE_ITERATIONS))
    got_lines="$(
        i=0
        while [ "$i" -lt "$BEE_ITERATIONS" ]; do
            cat /srv/bee.txt || exit 1
            i=$((i + 1))
        done | wc -l | awk '{print $1}'
    )"

    printf 'cat bee ipc: iterations=%s lines_per_cat=%s got=%s expected=%s\n' "$BEE_ITERATIONS" "$lines_per_cat" "$got_lines" "$expected_lines"
    test "$got_lines" = "$expected_lines"
}

case_make_large_file() {
    mkdir -p "$WORK_DIR"
    rm -f "$DATA_FILE"

    if [ -e /dev/zero ]; then
        if dd if=/dev/zero of="$DATA_FILE" bs=1048576 count="$DATA_MIB"; then
            bytes="$(wc -c < "$DATA_FILE")"
            printf 'created %s bytes at %s using /dev/zero\n' "$bytes" "$DATA_FILE"
            test "$bytes" -gt 0
            return
        fi
        printf 'dd from /dev/zero failed; falling back to /srv/bee.txt fanout\n'
    fi

    if [ ! -f /srv/bee.txt ]; then
        printf '/srv/bee.txt missing and /dev/zero unavailable\n'
        return 1
    fi

    loops=$((DATA_MIB * 12))
    i=0
    : > "$DATA_FILE"
    while [ "$i" -lt "$loops" ]; do
        cat /srv/bee.txt >> "$DATA_FILE" || return 1
        i=$((i + 1))
    done
    bytes="$(wc -c < "$DATA_FILE")"
    printf 'created %s bytes at %s using %s copies of /srv/bee.txt\n' "$bytes" "$DATA_FILE" "$loops"
    test "$bytes" -gt 0
}

case_vfsbench_read() {
    /usr/bin/testprog vfsbench-read --path "$DATA_FILE" --read-size "$VFS_READ_SIZE" --iterations "$VFS_READ_ITERATIONS"
}

case_vfsbench_stat() {
    /usr/bin/testprog vfsbench-stat --path "$DATA_FILE" --iterations "$VFS_STAT_ITERATIONS"
}

case_cp_large() {
    out="$WORK_DIR/cp-output.bin"
    rm -f "$out"
    cp "$DATA_FILE" "$out" || return 1
    in_bytes="$(wc -c < "$DATA_FILE")"
    out_bytes="$(wc -c < "$out")"
    printf 'cp bytes: input=%s output=%s\n' "$in_bytes" "$out_bytes"
    test "$in_bytes" = "$out_bytes"
}

case_dd_large() {
    out="$WORK_DIR/dd-output.bin"
    rm -f "$out"
    dd if="$DATA_FILE" of="$out" bs=65536 || return 1
    in_bytes="$(wc -c < "$DATA_FILE")"
    out_bytes="$(wc -c < "$out")"
    printf 'dd bytes: input=%s output=%s\n' "$in_bytes" "$out_bytes"
    test "$in_bytes" = "$out_bytes"
}

case_testprog_perf() {
    /usr/bin/testprog perf
}

case_mandelbench() {
    out="$ARTIFACT_ROOT/mandelbench"
    mkdir -p "$out"
    (
        cd "$out" || exit 1
        /usr/bin/testprog mandelbench \
            --width "$MANDEL_WIDTH" \
            --height "$MANDEL_HEIGHT" \
            --max-iter "$MANDEL_MAX_ITER" \
            --threads "$MANDEL_THREADS" \
            --repeat "$MANDEL_REPEAT" \
            --nodes "$MANDEL_NODES"
    )
}

case_renderbench_duck() {
    test -f "$RENDER_SCENE" || {
        printf 'render scene missing: %s\n' "$RENDER_SCENE"
        return 1
    }
    /usr/bin/renderbench \
        --scene "$RENDER_SCENE" \
        --backend ipc \
        --placement node-threads \
        --width "$RENDER_WIDTH" \
        --height "$RENDER_HEIGHT" \
        --spp "$RENDER_SPP" \
        --max-depth "$RENDER_MAX_DEPTH" \
        --tile-size "$RENDER_TILE_SIZE" \
        --output-root "$ARTIFACT_ROOT/renderbench" \
        --run-id "duck-$RUN_ID" \
        --threads "$RENDER_THREADS"
}

netbench_stop_pid() {
    pid="$1"
    if [ -n "$pid" ]; then
        kill "$pid" >/dev/null 2>&1 || true
    fi
}

netbench_kill_pid() {
    pid="$1"
    if [ -n "$pid" ]; then
        kill -KILL "$pid" >/dev/null 2>&1 || true
    fi
}

netbench_wait_pid() {
    pid="$1"
    if [ -n "$pid" ]; then
        wait "$pid" 2>/dev/null || true
    fi
}

netbench_status() {
    status_file_value "$1"
}

print_netbench_log() {
    label="$1"
    log_file="$2"
    printf '\n--- %s ---\n' "$label"
    if [ -s "$log_file" ]; then
        cat "$log_file"
    else
        printf '(empty log)\n'
    fi
}

NETBENCH_CHILD_PIDS=

cleanup_netbench_children() {
    for pid in $NETBENCH_CHILD_PIDS; do
        netbench_stop_pid "$pid"
    done
    sleep "$TIMEOUT_KILL_GRACE_SECONDS"
    for pid in $NETBENCH_CHILD_PIDS; do
        netbench_kill_pid "$pid"
    done
    for pid in $NETBENCH_CHILD_PIDS; do
        netbench_wait_pid "$pid"
    done
    NETBENCH_CHILD_PIDS=
}

abort_netbench_case() {
    cleanup_netbench_children
    trap - TERM INT HUP
    exit 124
}

run_netbench_one() {
    mode="$1"
    server_log="$WORK_DIR/netbench-$mode-server.log"
    client_log="$WORK_DIR/netbench-$mode-client.log"
    server_status="$WORK_DIR/netbench-$mode-server.status"
    client_status="$WORK_DIR/netbench-$mode-client.status"
    rm -f "$server_log" "$client_log" "$server_status" "$client_status"
    (
        /usr/bin/testprog netbench-server --port "$NETBENCH_PORT" --sessions 1 --timeout-ms "$NETBENCH_TIMEOUT_MS"
        printf '%s\n' "$?" > "$server_status"
    ) > "$server_log" 2>&1 &
    server_pid="$!"

    sleep "$NETBENCH_STARTUP_SECONDS"

    (
        if [ "$mode" = "pingpong" ]; then
            /usr/bin/testprog netbench-client \
                --host 127.0.0.1 \
                --port "$NETBENCH_PORT" \
                --mode pingpong \
                --payload-size "$NETBENCH_PAYLOAD_SIZE" \
                --iterations "$NETBENCH_ITERATIONS" \
                --timeout-ms "$NETBENCH_TIMEOUT_MS"
        else
            /usr/bin/testprog netbench-client \
                --host 127.0.0.1 \
                --port "$NETBENCH_PORT" \
                --mode stream \
                --payload-size "$NETBENCH_PAYLOAD_SIZE" \
                --total-bytes "$NETBENCH_TOTAL_BYTES" \
                --timeout-ms "$NETBENCH_TIMEOUT_MS"
        fi
        printf '%s\n' "$?" > "$client_status"
    ) > "$client_log" 2>&1 &
    client_pid="$!"
    NETBENCH_CHILD_PIDS="$server_pid $client_pid"

    elapsed=0
    timed_out=0
    while [ ! -f "$client_status" ] || [ ! -f "$server_status" ]; do
        if [ -f "$server_status" ] && [ ! -f "$client_status" ]; then
            server_rc_now="$(netbench_status "$server_status")"
            if [ "$server_rc_now" != "0" ]; then
                cleanup_netbench_children
                break
            fi
        fi
        if [ -f "$client_status" ] && [ ! -f "$server_status" ]; then
            client_rc_now="$(netbench_status "$client_status")"
            if [ "$client_rc_now" != "0" ]; then
                cleanup_netbench_children
                break
            fi
        fi

        if [ "$NETBENCH_CASE_TIMEOUT_SECONDS" -gt 0 ] && [ "$elapsed" -ge "$NETBENCH_CASE_TIMEOUT_SECONDS" ]; then
            timed_out=1
            cleanup_netbench_children
            break
        fi

        sleep 1
        elapsed=$((elapsed + 1))
    done

    if [ -f "$client_status" ]; then
        netbench_wait_pid "$client_pid"
    fi
    if [ -f "$server_status" ]; then
        netbench_wait_pid "$server_pid"
    fi
    NETBENCH_CHILD_PIDS=

    client_rc="$(netbench_status "$client_status")"
    server_rc="$(netbench_status "$server_status")"

    if [ "$timed_out" -eq 1 ]; then
        printf 'netbench %s timed out after %ss\n' "$mode" "$NETBENCH_CASE_TIMEOUT_SECONDS"
        printf 'netbench %s client log retained at %s\n' "$mode" "$client_log"
        printf 'netbench %s server log retained at %s\n' "$mode" "$server_log"
        return 1
    fi

    print_netbench_log "netbench $mode client log" "$client_log"
    print_netbench_log "netbench $mode server log" "$server_log"

    if [ "$client_rc" -ne 0 ] || [ "$server_rc" -ne 0 ]; then
        return 1
    fi
    return 0
}

case_netbench_loopback() {
    trap abort_netbench_case TERM INT HUP
    rc=0
    if run_netbench_one pingpong; then
        run_netbench_one stream || rc=1
    else
        rc=1
    fi
    cleanup_netbench_children
    trap - TERM INT HUP
    return "$rc"
}

case_strace_vfsbench() {
    /usr/bin/strace /usr/bin/testprog vfsbench-stat --path "$DATA_FILE" --iterations 8
}

case_perf_stat() {
    rc=0
    /usr/bin/perf stat 1000 || rc=1
    /usr/bin/perf cpustat || rc=1
    /usr/bin/perf contstat || rc=1
    return "$rc"
}

case_perf_trace_vfs() {
    out="$ARTIFACT_ROOT/perf"
    mkdir -p "$out"
    (
        rc=0
        cd "$out" || exit 1
        /usr/bin/perf run --filter=local,xfs,vmem,loader,irq,switch,wake,sleep,sample \
            /usr/bin/testprog vfsbench-read --path "$DATA_FILE" --read-size "$VFS_READ_SIZE" --iterations 2 || rc=1
        /usr/bin/perf report 200 --time=boot || rc=1
        /usr/bin/perf show-map || rc=1
        /usr/bin/perf local-report || rc=1
        /usr/bin/perf vmem-report || rc=1
        /usr/bin/perf all-report || rc=1
        exit "$rc"
    )
}

printf 'WOS userland suite\n'
printf 'RUN_ID=%s\n' "$RUN_ID"
printf 'SUITE_REVISION=%s\n' "$SUITE_REVISION"
printf 'SCALE=%s\n' "$SCALE"
printf 'RESULT_DIR=%s\n' "$ARTIFACT_ROOT"
printf 'CASE_TIMEOUT_SECONDS=%s\n' "$CASE_TIMEOUT_SECONDS"
printf 'CAT_BEE_ITERATIONS=%s\n' "$BEE_ITERATIONS"
printf 'DATA_MIB=%s\n' "$DATA_MIB"
printf 'MANDELBENCH=%sx%s iter=%s repeat=%s workers=%s nodes=%s\n' "$MANDEL_WIDTH" "$MANDEL_HEIGHT" "$MANDEL_MAX_ITER" "$MANDEL_REPEAT" "$MANDEL_THREADS" "$MANDEL_NODES"
printf 'RENDERBENCH=%sx%s spp=%s depth=%s scene=%s\n' "$RENDER_WIDTH" "$RENDER_HEIGHT" "$RENDER_SPP" "$RENDER_MAX_DEPTH" "$RENDER_SCENE"
printf 'NETBENCH_CASE_TIMEOUT_SECONDS=%s\n' "$NETBENCH_CASE_TIMEOUT_SECONDS"

if [ -f /srv/tests/validate.sh ]; then
    run_case shell_validate case_shell_validate
else
    skip_case shell_validate "missing /srv/tests/validate.sh"
fi

if [ "$RUN_TESTD" = "1" ] && require_exe testd /usr/bin/testd; then
    run_case testd case_testd
elif [ "$RUN_TESTD" != "1" ]; then
    skip_case testd "disabled by WOS_SUITE_RUN_TESTD=$RUN_TESTD"
fi

run_case cat_bee_ipc case_cat_bee_ipc

if require_exe testprog_smoke /usr/bin/testprog; then
    run_case testprog_smoke case_testprog_smoke
    run_case make_large_file case_make_large_file
    run_case vfsbench_read case_vfsbench_read
    run_case vfsbench_stat case_vfsbench_stat
    run_case cp_large_file case_cp_large
    run_case dd_large_file case_dd_large
    if [ "$RUN_TESTPROG_PERF" = "1" ]; then
        run_case testprog_perf_suite case_testprog_perf
    else
        skip_case testprog_perf_suite "disabled by WOS_SUITE_RUN_TESTPROG_PERF=$RUN_TESTPROG_PERF"
    fi
    run_case mandelbench_large case_mandelbench
    run_case_with_timeout netbench_loopback "$NETBENCH_CASE_TIMEOUT_SECONDS" case_netbench_loopback
else
    skip_case_group "missing executable: /usr/bin/testprog" \
        make_large_file \
        vfsbench_read \
        vfsbench_stat \
        cp_large_file \
        dd_large_file \
        testprog_perf_suite \
        mandelbench_large \
        netbench_loopback
fi

if require_exe renderbench_duck /usr/bin/renderbench; then
    run_case renderbench_duck case_renderbench_duck
fi

if [ "$RUN_STRACE" != "1" ]; then
    skip_case strace_vfsbench "disabled by WOS_SUITE_RUN_STRACE=$RUN_STRACE"
elif ! require_exe strace_vfsbench /usr/bin/strace; then
    :
elif ! have_exe /usr/bin/testprog; then
    skip_case strace_vfsbench "missing executable: /usr/bin/testprog"
else
    run_case strace_vfsbench case_strace_vfsbench
fi

if require_exe perf_stat /usr/bin/perf; then
    run_case perf_stat case_perf_stat
    if [ "$RUN_PERF_TRACE" != "1" ]; then
        skip_case perf_trace_vfs "disabled by WOS_SUITE_RUN_PERF_TRACE=$RUN_PERF_TRACE"
    elif ! have_exe /usr/bin/testprog; then
        skip_case perf_trace_vfs "missing executable: /usr/bin/testprog"
    else
        run_case perf_trace_vfs case_perf_trace_vfs
    fi
else
    skip_case perf_trace_vfs "missing executable: /usr/bin/perf"
fi

if [ "$FAIL" -eq 0 ]; then
    cleanup_large_work
elif [ "$KEEP_LARGE_WORK" != "1" ]; then
    printf 'keeping large work files after failed run for debugging: %s\n' "$WORK_DIR"
fi

printf '\n=== WOS USERLAND SUITE SUMMARY ===\n'
cat "$SUMMARY_FILE"
printf '\nPASS=%s FAIL=%s SKIP=%s\n' "$PASS" "$FAIL" "$SKIP"
printf 'RESULT_DIR=%s\n' "$ARTIFACT_ROOT"

suite_rc=0
if [ "$FAIL" -eq 0 ]; then
    suite_rc=0
else
    suite_rc=1
fi
request_suite_shutdown
exit "$suite_rc"
