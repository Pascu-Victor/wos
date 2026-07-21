#!/bin/bash
# Run the WOS self-hosting clone/configure/build benchmark locally or in WOS.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
WOS_SSH="$WORKSPACE_ROOT/scripts/remote/wos_ssh.sh"

DEFAULT_REPO="https://github.com/Pascu-Victor/wos.git"
DEFAULT_LINUX_WORKDIR="${HOME:-/tmp}/wos-selfhost-bench"
DEFAULT_WOS_WORKDIR="/root/wos-selfhost-bench"
DEFAULT_BUILD_DIR="build-selfhost"
DEFAULT_TARGET="wos_full"
DEFAULT_HOST="wos-0"
DEFAULT_JOBS="32"

usage() {
    cat <<EOF
Usage:
  scripts/bench/run_wos_selfhost_build.sh wos [options]
  scripts/bench/run_wos_selfhost_build.sh wos-local [options]
  scripts/bench/run_wos_selfhost_build.sh linux [options]

Modes:
  wos      Run inside an already launched WOS VM over scripts/remote/wos_ssh.sh.
  wos-local
           Run directly in the current WOS shell; useful when this script is
           piped over SSH and detached inside WOS.
  linux    Run the same clone/bootstrap/configure/build flow on this host.

Options:
  --host NAME             WOS node alias/IP for wos mode (default: $DEFAULT_HOST)
  --repo URL              Repository URL to clone (default: $DEFAULT_REPO)
  --workdir PATH          Scratch directory
                         (defaults: wos=$DEFAULT_WOS_WORKDIR, linux=$DEFAULT_LINUX_WORKDIR)
  --build-dir NAME        Build directory inside the checkout (default: $DEFAULT_BUILD_DIR)
  --target NAME           CMake target to build (default: $DEFAULT_TARGET)
  --jobs N                Parallel build jobs for clone, bootstrap, and build
                          (default: $DEFAULT_JOBS)
  --distributed           Run the WOS payload with REMOTE placement enabled.
                          The submitting node owns the checkout and reports;
                          build processes may execute on connected WOS peers.
  --skip-bootstrap        Skip ./tools/bootstrap.sh, useful only for iteration
  --keep-workdir          Refuse to replace an existing checkout in workdir
  --resume-checkout       Reuse the existing depth-1 checkout in workdir after
                         validating its repository and submodules; iteration-only
  --history-file PATH     Append detailed timing rows here
                         (default: <workdir>-history.tsv)
  --log-dir PATH          Store phase command logs here
                         (defaults: wos=/tmp/wos-selfhost-logs-<run>,
                          linux=<workdir>/logs)
  --source-cache PATH     Reuse a pre-cloned depth-1 checkout at this path
                         instead of cloning from the network;
                         iteration-only, path must be visible inside the
                         selected execution environment
  --distdir PATH          Reuse source tarballs from this directory before
                         downloading; iteration-only, path must be visible
                         inside the selected execution environment
  --heartbeat-interval N  Emit phase log progress every N seconds; 0 disables
                         heartbeat output (default: 30)
  --heartbeat-tail N      Include the last N log lines in each heartbeat
                         (default: 4)
  --heartbeat-stall-snapshots N
                         Emit an in-band process snapshot after N unchanged
                         heartbeat intervals; 0 disables snapshots
                         (default: 6)
  --heartbeat-sync        Run sync after each phase completion; useful before
                         forced VM teardown, not benchmark timing
  --full-history          Clone full Git history instead of the default shallow
                         source checkout used for build timing
  --mirror-file PATH      Rewrite https://github.com/ to file://PATH/
  --mirror-local-path PATH
                         Rewrite https://github.com/ to PATH/ directly;
                         iteration-only for WOS file:// upload-pack debugging
  --mirror-http-prefix U  Rewrite https://github.com/ to an HTTP mirror prefix
  --checkout-workers N    Set Git checkout.workers during clone/submodule timing
                         for controlled parallel-checkout experiments
  --host-toolchain PATH   Reuse an existing Linux host toolchain for a valid
                         --skip-bootstrap configure of a fresh checkout
  --host-sysroot PATH     Matching populated WOS sysroot for --host-toolchain
                         (default: <host-toolchain>/../sysroot)
  -h, --help              Show this help

Direct GitHub cloning is the default and is the acceptance-path check. Mirror
options are for controlled WOS-vs-Linux timing comparisons after using
scripts/dev/git_mirror_for_wos.sh. --source-cache is only for faster debugging
with a depth-1 checkout already staged in the guest or host environment. The
default clone is shallow because the benchmark validates source availability
and buildability, not Git history traversal throughput.

For rootless WOS runtime setup, launch first with:
  bin/wos-cluster --config configs/cluster_selfhost.json --launch --no-setup
or:
  bin/wos-ktest --no-setup
EOF
}

die() {
    echo "error: $*" >&2
    exit 1
}

reject_whitespace() {
    local label="$1"
    local value="$2"

    case "$value" in
        *[[:space:]]*)
            die "$label must not contain whitespace: $value"
            ;;
    esac
}

shell_quote() {
    printf "'%s'" "$(printf '%s' "$1" | sed "s/'/'\\\\''/g")"
}

selfhost_payload() {
    cat <<'EOF'
set -euo pipefail

repo="${WOS_SELFHOST_REPO:?}"
workdir="${WOS_SELFHOST_WORKDIR:?}"
build_dir="${WOS_SELFHOST_BUILD_DIR:?}"
target="${WOS_SELFHOST_TARGET:?}"
mode="${WOS_SELFHOST_MODE:?}"
jobs="${WOS_SELFHOST_JOBS:-32}"
skip_bootstrap="${WOS_SELFHOST_SKIP_BOOTSTRAP:-0}"
keep_workdir="${WOS_SELFHOST_KEEP_WORKDIR:-0}"
resume_checkout="${WOS_SELFHOST_RESUME_CHECKOUT:-0}"
full_history="${WOS_SELFHOST_FULL_HISTORY:-0}"
mirror_file="${WOS_SELFHOST_MIRROR_FILE:-}"
mirror_local_path="${WOS_SELFHOST_MIRROR_LOCAL_PATH:-}"
mirror_http_prefix="${WOS_SELFHOST_MIRROR_HTTP_PREFIX:-}"
checkout_workers="${WOS_SELFHOST_CHECKOUT_WORKERS:-}"
host_toolchain="${WOS_SELFHOST_HOST_TOOLCHAIN:-}"
host_sysroot="${WOS_SELFHOST_HOST_SYSROOT:-}"
source_cache="${WOS_SELFHOST_SOURCE_CACHE:-}"
distdir="${WOS_SELFHOST_DISTDIR:-}"
distributed="${WOS_SELFHOST_DISTRIBUTED:-0}"
clean_path="${WOS_SELFHOST_CLEAN_PATH:-}"
priority_reset="${WOS_SELFHOST_PRIORITY_RESET:-}"
priority_nice_delta="${WOS_SELFHOST_PRIORITY_NICE_DELTA:-}"
priority_reset_helper="${WOS_SELFHOST_PRIORITY_RESET_HELPER:-0}"
git_http_low_speed_limit="${WOS_SELFHOST_GIT_HTTP_LOW_SPEED_LIMIT:-1}"
git_http_low_speed_time="${WOS_SELFHOST_GIT_HTTP_LOW_SPEED_TIME:-120}"
heartbeat_interval="${WOS_SELFHOST_HEARTBEAT_INTERVAL:-30}"
heartbeat_tail="${WOS_SELFHOST_HEARTBEAT_TAIL:-4}"
heartbeat_sync="${WOS_SELFHOST_HEARTBEAT_SYNC:-0}"
heartbeat_stall_snapshots="${WOS_SELFHOST_HEARTBEAT_STALL_SNAPSHOTS:-6}"
checkout="$workdir/wos"
report="$workdir/selfhost-report.tsv"
detail_report="$workdir/selfhost-detail.tsv"
bootstrap_detail_report="$workdir/bootstrap-detail.tsv"
cache_diag_report="$workdir/selfhost-cache-deltas.tsv"
history_file="${WOS_SELFHOST_HISTORY_FILE:-}"
log_dir="${WOS_SELFHOST_LOG_DIR:-}"
total_elapsed=0
commit="unknown"
heartbeat_pid=""
workdir_lock=""

sanitize_selfhost_environment() {
    unset CC CXX CPP LD AR AS NM OBJCOPY OBJDUMP RANLIB READELF STRIP
    unset HOSTCC HOSTCXX
    unset CPPFLAGS CFLAGS CXXFLAGS LDFLAGS
    unset CPATH C_INCLUDE_PATH CPLUS_INCLUDE_PATH LIBRARY_PATH LD_LIBRARY_PATH
    unset PKG_CONFIG PKG_CONFIG_PATH PKG_CONFIG_LIBDIR PKG_CONFIG_SYSROOT_DIR
    unset CMAKE_PREFIX_PATH CMAKE_TOOLCHAIN_FILE CMAKE_GENERATOR CMAKE_BUILD_TYPE
    unset MAKEFLAGS NINJA_STATUS
    unset WOS_BIN WOS_CLANG_LIB_DIR WOS_CLANG_RESOURCE_DIR WOS_CLANG_VERSION
    unset WOS_CMAKE WOS_CPACK WOS_CTEST WOS_ENV_LOADED
    unset WOS_HOST_CMAKE WOS_HOST_CPACK WOS_HOST_CTEST
    unset WOS_HOST_TOOLCHAIN_BIN WOS_HOST_TOOLCHAIN_LIB WOS_NATIVE_HOST
    unset WOS_ORIGINAL_LD_LIBRARY_PATH WOS_ORIGINAL_PATH
    unset WOS_SYSROOT WOS_TARGET_ARCH WOS_TOOLCHAIN_MODE WOS_TOOLCHAIN_ROOT
    unset WOS_UNAME
    unset WOS_WORKSPACE_ROOT
    unset WOS_BUILD_JOBS WOS_MAKE_JOBS WOS_NINJA_JOBS CMAKE_BUILD_PARALLEL_LEVEL

    if [ -n "$clean_path" ]; then
        export PATH="$clean_path"
    fi
}

sanitize_selfhost_environment
if [ "$mode" = "wos" ]; then
    export WOS_NATIVE_HOST=1
fi

reset_selfhost_priority_with_helper() {
    if ! command -v clang >/dev/null 2>&1; then
        echo "[selfhost] error: clang not found; cannot build priority reset helper" >&2
        return 1
    fi

    local helper="${TMPDIR:-/tmp}/wos-priority-reset.$$"
    local source="$helper.c"
    cat > "$source" <<'C'
#include <stdio.h>
#include <stdlib.h>
#include <sys/resource.h>
#include <unistd.h>

int main(int argc, char** argv) {
    if (argc < 4) {
        return 64;
    }

    int const prio = atoi(argv[1]);
    if (setpriority(PRIO_PROCESS, 0, prio) != 0) {
        perror("setpriority");
        return 1;
    }

    unlink(argv[0]);
    execv(argv[2], &argv[2]);
    perror("execv");
    return 127;
}
C

    if ! clang -O2 -o "$helper" "$source"; then
        rm -f -- "$source" "$helper"
        echo "[selfhost] error: failed to build priority reset helper" >&2
        return 1
    fi
    rm -f -- "$source"
    exec "$helper" "$priority_reset" /bin/bash "$0" "$@"
}

reset_selfhost_priority() {
    if [ -z "$priority_reset" ] || [ "${WOS_SELFHOST_PRIORITY_RESET_DONE:-0}" = "1" ]; then
        return 0
    fi

    export WOS_SELFHOST_PRIORITY_RESET_DONE=1
    if [ "$priority_reset_helper" = "1" ]; then
        reset_selfhost_priority_with_helper "$@"
        exit 1
    fi

    if [ -n "$priority_nice_delta" ]; then
        if command -v nice >/dev/null 2>&1; then
            exec nice -n "$priority_nice_delta" /bin/bash "$0" "$@"
        fi
        echo "[selfhost] warning: nice not found; continuing without priority reset" >&2
        exec /bin/bash "$0" "$@"
    fi

    exec python3 - "$0" "$@" <<'PY'
import os
import sys

target = int(os.environ["WOS_SELFHOST_PRIORITY_RESET"])
argv = ["bash", sys.argv[1], *sys.argv[2:]]
try:
    current = os.getpriority(os.PRIO_PROCESS, 0)
except (AttributeError, OSError) as exc:
    print(f"[selfhost] warning: failed to read priority before reset to {target}: {exc}", file=sys.stderr)
    os.execv("/bin/bash", argv)

delta = target - current
if delta == 0:
    os.execv("/bin/bash", argv)

try:
    os.execvp("nice", ["nice", "-n", str(delta), "/bin/bash", sys.argv[1], *sys.argv[2:]])
except OSError as exc:
    print(f"[selfhost] warning: failed to reset priority to {target}: {exc}", file=sys.stderr)
    os.execv("/bin/bash", argv)
PY
}

reset_selfhost_priority "$@"

run_id="$(
    python3 - <<'PY'
from datetime import datetime, timezone
import os

stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
print(f"{stamp}-{os.getpid()}")
PY
)"

now_ms() {
    local epoch="${EPOCHREALTIME:-}"

    if [ -n "$epoch" ]; then
        local seconds="${epoch%.*}"
        local fraction="${epoch#*.}000"

        printf '%s\n' "$((10#$seconds * 1000 + 10#${fraction:0:3}))"
        return 0
    fi

    python3 - <<'PY'
import time
print(time.monotonic_ns() // 1_000_000)
PY
}

log() {
    printf '[selfhost] %s\n' "$*"
}

log_path_for() {
    local phase="$1"
    local label="$2"
    local safe_label

    safe_label="$(printf '%s' "$label" | sed 's#[^A-Za-z0-9._-]#_#g')"
    printf '%s/%s-%s.log\n' "$log_dir" "$phase" "$safe_label"
}

default_log_dir() {
    case "$mode" in
        wos)
            printf '/tmp/wos-selfhost-logs-%s\n' "$run_id"
            ;;
        *)
            printf '%s/logs\n' "$workdir"
            ;;
    esac
}

print_log_tail() {
    local output="$1"

    [ -f "$output" ] || return 0
    log "last lines from $output"
    tail -n 80 "$output" || true
}

heartbeat_progress_file() {
    printf '%s/heartbeat-progress.log\n' "$log_dir"
}

heartbeat_snapshot_file() {
    printf '%s/heartbeat-snapshots.log\n' "$log_dir"
}

heartbeat_append_progress() {
    printf '[selfhost] %s\n' "$*" >> "$(heartbeat_progress_file)" 2>/dev/null || true
}

heartbeat_file_bytes() {
    local output="$1"
    local bytes

    bytes="$(stat -c %s "$output" 2>/dev/null || true)"
    if [ -n "$bytes" ]; then
        printf '%s\n' "$bytes"
        return 0
    fi

    wc -c < "$output" 2>/dev/null || printf '0\n'
}

heartbeat_file_lines() {
    local output="$1"

    if [ "$(uname -s 2>/dev/null || printf unknown)" = "WOS" ]; then
        printf 'skipped\n'
        return 0
    fi

    wc -l < "$output" 2>/dev/null || printf '0\n'
}

heartbeat_process_snapshot() {
    local phase="$1"
    local label="$2"
    local snapshot_output
    local is_wos

    snapshot_output="$(heartbeat_snapshot_file)"
    is_wos="$(uname -s 2>/dev/null || printf unknown)"

    {
        log "process snapshot $phase $label"
        if command -v ps >/dev/null 2>&1; then
            if [ "$is_wos" = "WOS" ]; then
                ps w 2>/dev/null | tail -n 120 | sed 's/^/[selfhost-ps] /' || true
            else
                ps -eo pid,ppid,stat,pcpu,pmem,comm,args 2>/dev/null | tail -n 120 | sed 's/^/[selfhost-ps] /' || true
            fi
        fi

        if [ "$is_wos" = "WOS" ]; then
            heartbeat_wos_procfs_snapshot
        fi
    } >> "$snapshot_output" 2>&1 || true

    log "snapshot saved $snapshot_output"
    tail -n 260 "$snapshot_output" 2>/dev/null || true
}

heartbeat_wos_procfs_snapshot() {
    local file status

    for file in /proc/kcpustate /proc/memacc/alloc_totals /proc/memacc/dead; do
        if [ -r "$file" ]; then
            log "procfs snapshot $file"
            sed -n '1,160p' "$file" 2>/dev/null | sed "s#^#[selfhost-proc] $file #g" || true
        fi
    done

    for status in /proc/[0-9]*/status; do
        [ -r "$status" ] || continue
        if ! grep -Eq 'Wchan:[[:space:]]*(waitpid|dirty_bcache|futex|pipe|poll|sock)|Name:.*(bash|make|ninja|clang|ld|conf|busybox|python|git)|ExitInProgress:[[:space:]]*1|ExitNotifyReady:[[:space:]]*1' "$status" 2>/dev/null; then
            continue
        fi
        printf '[selfhost-status] %s\n' "$status"
        grep -E '^(Name|Tgid|Pid|PPid|State|Cpu|SchedQueue|Wchan|DeferredSwitch|VoluntaryBlock|WaitingForPid|WaitpidCompletionClaimed|WaitpidLastRepairUs|ExitInProgress|ExitNotifyReady|WaitedOn|WakeupPending|SigPnd|SigBlk|UserTime|SysTime):' "$status" 2>/dev/null |
            sed 's/^/[selfhost-status] /' || true
    done
}

start_log_heartbeat() {
    local phase="$1"
    local label="$2"
    local output="$3"

    heartbeat_pid=""
    if [ "$heartbeat_interval" = "0" ]; then
        return 0
    fi

    (
        local previous_bytes=""
        local stalled_ticks=0
        while :; do
            sleep "$heartbeat_interval" || exit 0

            local bytes lines
            bytes="$(heartbeat_file_bytes "$output")"
            lines="$(heartbeat_file_lines "$output")"
            local progress
            progress="progress $phase $label log=$output bytes=$bytes lines=$lines"

            if [ "$bytes" = "$previous_bytes" ]; then
                stalled_ticks=$((stalled_ticks + 1))
            else
                stalled_ticks=0
                previous_bytes="$bytes"
            fi

            heartbeat_append_progress "$progress"

            if [ "$heartbeat_stall_snapshots" != "0" ] && [ "$stalled_ticks" -ge "$heartbeat_stall_snapshots" ]; then
                local stall
                stall="no log growth for $stalled_ticks heartbeat intervals during $phase $label"
                heartbeat_append_progress "$stall"
                heartbeat_process_snapshot "$phase" "$label"
                stalled_ticks=0
            fi

            log "$progress"

            if [ "$heartbeat_tail" != "0" ] && [ -f "$output" ]; then
                tail -n "$heartbeat_tail" "$output" 2>/dev/null | sed 's/^/[selfhost-log] /' || true
            fi
        done
    ) &
    heartbeat_pid=$!
}

stop_log_heartbeat() {
    local pid="${1:-$heartbeat_pid}"

    if [ -n "$pid" ]; then
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
        if [ "$heartbeat_pid" = "$pid" ]; then
            heartbeat_pid=""
        fi
    fi

    if [ "$heartbeat_sync" = "1" ]; then
        sync || true
    fi
}

release_workdir_lock() {
    if [ -n "$workdir_lock" ]; then
        rm -rf -- "$workdir_lock"
        workdir_lock=""
    fi
}

selfhost_cleanup() {
    local status=$?
    stop_log_heartbeat "$heartbeat_pid"
    release_workdir_lock
    exit "$status"
}

timestamp_utc() {
    local epoch="${EPOCHREALTIME:-}"

    if [ -n "$epoch" ]; then
        local seconds="${epoch%.*}"
        local fraction="${epoch#*.}000"
        local prefix

        TZ=UTC printf -v prefix '%(%Y-%m-%dT%H:%M:%S)T' "$seconds"
        printf '%s.%sZ\n' "$prefix" "${fraction:0:3}"
        return 0
    fi

    python3 - <<'PY'
from datetime import datetime, timezone

print(datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"))
PY
}

ensure_parent_dir() {
    case "$1" in
        */*) mkdir -p "${1%/*}" ;;
    esac
}

write_timing_header() {
    local output="$1"

    if [ ! -s "$output" ]; then
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
            "run_id" "timestamp_utc" "mode" "phase" "label" "elapsed_ms" \
            "status" "repo" "commit" "target" "jobs" "full_history" \
            "workdir" "build_dir" >> "$output"
    fi
}

write_cache_diag_header() {
    local output="$1"

    if [ ! -s "$output" ]; then
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
            "run_id" "timestamp_utc" "mode" "phase" "label" \
            "record" "key" "before" "after" "delta" >> "$output"
    fi
}

capture_wos_cache_snapshot() {
    local phase="$1"
    local label="$2"
    local marker="$3"

    if [ "$mode" != "wos" ] || [ ! -e /proc/memacc/alloc_totals ]; then
        return 0
    fi

    local output
    output="$(log_path_for diag "${phase}-${label}-${marker}")"
    sed -n '/^buffer_cache /p;/^vfs_cache /p;/^xfs_dentry_cache /p' /proc/memacc/alloc_totals > "$output" 2>/dev/null || true
    printf '%s\n' "$output"
}

record_wos_cache_delta() {
    local phase="$1"
    local label="$2"
    local before="$3"
    local after="$4"

    if [ "$mode" != "wos" ] || [ -z "$before" ] || [ -z "$after" ] || [ ! -s "$before" ] || [ ! -s "$after" ]; then
        return 0
    fi

    python3 - "$cache_diag_report" "$run_id" "$(timestamp_utc)" "$mode" "$phase" "$label" "$before" "$after" <<'PY' || true
import sys

output, run_id, timestamp, mode, phase, label, before_path, after_path = sys.argv[1:]
tracked = {
    "buffer_cache": (
        "hits",
        "misses",
        "dirty_buffers",
        "dirty_bytes",
        "dirty_waiters",
        "disk_read_calls",
        "disk_read_bytes",
        "metadata_disk_read_calls",
        "metadata_disk_read_bytes",
        "data_disk_read_calls",
        "data_disk_read_bytes",
        "range_copy_attempts",
        "range_copy_cover_hits",
        "range_copy_overlap_hits",
        "range_copy_no_state",
        "range_copy_no_overlap",
        "range_copy_incomplete",
        "range_copy_overflow",
        "range_copy_degraded",
    ),
    "vfs_cache": (
        "metadata_hits",
        "metadata_misses",
        "metadata_stores",
        "metadata_miss_empty",
        "metadata_miss_invalidated",
        "metadata_miss_stale_generation",
        "metadata_miss_conflict",
        "metadata_path_invalidations",
        "metadata_generation_resets",
        "existence_hits",
        "existence_misses",
        "existence_stores",
        "symlink_hits",
        "symlink_misses",
        "symlink_stores",
        "fstat_snapshot_hits",
        "fstat_snapshot_misses",
        "fstat_snapshot_stores",
        "fstat_snapshot_miss_uncacheable",
        "fstat_snapshot_miss_empty",
        "fstat_snapshot_miss_generation",
        "fstat_snapshot_miss_invalidated",
    ),
    "xfs_dentry_cache": (
        "hits",
        "misses",
        "stores",
        "invalidations",
    ),
}


def parse(path):
    rows = {}
    with open(path, "r", encoding="utf-8") as file:
        for line in file:
            fields = line.strip().split()
            if not fields:
                continue
            record = fields[0]
            values = {}
            for field in fields[1:]:
                if "=" not in field:
                    continue
                key, value = field.split("=", 1)
                try:
                    values[key] = int(value, 0)
                except ValueError:
                    continue
            rows[record] = values
    return rows


before = parse(before_path)
after = parse(after_path)
with open(output, "a", encoding="utf-8") as file:
    for record, keys in tracked.items():
        before_row = before.get(record, {})
        after_row = after.get(record, {})
        for key in keys:
            if key not in before_row or key not in after_row:
                continue
            old = before_row[key]
            new = after_row[key]
            print(run_id, timestamp, mode, phase, label, record, key, old, new, new - old, sep="\t", file=file)
PY
}

record_timing() {
    local phase="$1"
    local label="$2"
    local elapsed="$3"
    local status="$4"
    local timestamp
    timestamp="$(timestamp_utc)"

    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$run_id" "$timestamp" "$mode" "$phase" "$label" "$elapsed" \
        "$status" "$repo" "$commit" "$target" "${jobs:-auto}" \
        "$full_history" "$workdir" "$build_dir" >> "$detail_report"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$run_id" "$timestamp" "$mode" "$phase" "$label" "$elapsed" \
        "$status" "$repo" "$commit" "$target" "${jobs:-auto}" \
        "$full_history" "$workdir" "$build_dir" >> "$history_file"
}

run_timed_event() {
    local phase="$1"
    local label="$2"
    shift 2

    local output
    output="$(log_path_for "$phase" "$label")"
    log "start $phase $label log=$output"
    local start end elapsed status
    local phase_heartbeat_pid
    local cache_before cache_after
    cache_before="$(capture_wos_cache_snapshot "$phase" "$label" "before")"
    start_log_heartbeat "$phase" "$label" "$output"
    phase_heartbeat_pid="$heartbeat_pid"
    start="$(now_ms)"
    set +e
    "$@" >"$output" 2>&1
    status=$?
    set -e
    end="$(now_ms)"
    stop_log_heartbeat "$phase_heartbeat_pid"
    elapsed=$((end - start))
    cache_after="$(capture_wos_cache_snapshot "$phase" "$label" "after")"
    record_wos_cache_delta "$phase" "$label" "$cache_before" "$cache_after"

    if [ "$status" -eq 0 ]; then
        record_timing "$phase" "$label" "$elapsed" "ok"
        log "done $phase $label ${elapsed}ms"
    else
        record_timing "$phase" "$label" "$elapsed" "fail:$status"
        log "failed $phase $label ${elapsed}ms status=$status"
        print_log_tail "$output"
    fi
    return "$status"
}

require_tool() {
    command -v "$1" >/dev/null 2>&1 || {
        echo "missing required tool: $1" >&2
        exit 1
    }
}

validate_runtime_settings() {
    if [ -n "$host_toolchain" ]; then
        if [ ! -x "$host_toolchain/bin/clang" ] || [ ! -x "$host_toolchain/bin/cmake" ]; then
            echo "ERROR: WOS_SELFHOST_HOST_TOOLCHAIN must contain bin/clang and bin/cmake: $host_toolchain" >&2
            exit 1
        fi
        if [ -z "$host_sysroot" ] || [ ! -f "$host_sysroot/lib/Scrt1.o" ] || [ ! -f "$host_sysroot/lib/libc.a" ]; then
            echo "ERROR: WOS_SELFHOST_HOST_SYSROOT must contain lib/Scrt1.o and lib/libc.a: $host_sysroot" >&2
            exit 1
        fi
    fi

    if [ -n "$distdir" ] && [ ! -d "$distdir" ]; then
        echo "ERROR: WOS_SELFHOST_DISTDIR must name an existing directory: $distdir" >&2
        exit 1
    fi

    case "$git_http_low_speed_limit" in
        ''|*[!0-9]*)
            echo "ERROR: WOS_SELFHOST_GIT_HTTP_LOW_SPEED_LIMIT must be a non-negative integer, got '$git_http_low_speed_limit'" >&2
            exit 1
            ;;
    esac
    case "$git_http_low_speed_time" in
        ''|*[!0-9]*|0)
            echo "ERROR: WOS_SELFHOST_GIT_HTTP_LOW_SPEED_TIME must be a positive integer, got '$git_http_low_speed_time'" >&2
            exit 1
            ;;
    esac

    case "$heartbeat_interval" in
        ''|*[!0-9]*)
            echo "ERROR: WOS_SELFHOST_HEARTBEAT_INTERVAL must be a non-negative integer, got '$heartbeat_interval'" >&2
            exit 1
            ;;
    esac
    case "$heartbeat_tail" in
        ''|*[!0-9]*)
            echo "ERROR: WOS_SELFHOST_HEARTBEAT_TAIL must be a non-negative integer, got '$heartbeat_tail'" >&2
            exit 1
            ;;
    esac
    case "$heartbeat_sync" in
        0|1)
            ;;
        *)
            echo "ERROR: WOS_SELFHOST_HEARTBEAT_SYNC must be 0 or 1, got '$heartbeat_sync'" >&2
            exit 1
            ;;
    esac
    case "$heartbeat_stall_snapshots" in
        ''|*[!0-9]*)
            echo "ERROR: WOS_SELFHOST_HEARTBEAT_STALL_SNAPSHOTS must be a non-negative integer, got '$heartbeat_stall_snapshots'" >&2
            exit 1
            ;;
    esac
}

require_wos_selfhost_tools() {
    local tool
    for tool in \
        sh env make tar sed grep mktemp sha256sum xz yes sleep tail wc stat \
        ld.lld lld llvm-ar llvm-ranlib llvm-nm llvm-objcopy llvm-strip \
        llvm-readelf llvm-objdump llvm-symbolizer llvm-tblgen clang-tblgen \
        llvm-as llvm-dis llvm-link llc opt; do
        require_tool "$tool"
    done
}

require_file() {
    if [ ! -e "$checkout/$1" ]; then
        echo "missing expected artifact: $checkout/$1" >&2
        exit 1
    fi
}

require_any() {
    local label="$1"
    shift

    local candidate
    for candidate in "$@"; do
        if compgen -G "$checkout/$candidate" >/dev/null; then
            return 0
        fi
    done

    echo "missing expected artifact: $label" >&2
    exit 1
}

run_with_jobs_env() {
    if [ "$mode" = "wos" ] && [ "$distributed" = "1" ]; then
        WOS_BUILD_JOBS="$jobs" \
            WOS_NINJA_JOBS="$jobs" \
            WOS_MAKE_JOBS="$jobs" \
            CMAKE_BUILD_PARALLEL_LEVEL="$jobs" \
            WOS_DISTRIBUTED_COMPILER=1 \
            "$@"
    else
        WOS_BUILD_JOBS="$jobs" \
            WOS_NINJA_JOBS="$jobs" \
            WOS_MAKE_JOBS="$jobs" \
            CMAKE_BUILD_PARALLEL_LEVEL="$jobs" \
            "$@"
    fi
}

run_git_http() {
    if [ "${1:-}" = "git" ]; then
        shift
        GIT_TERMINAL_PROMPT=0 \
            GIT_HTTP_LOW_SPEED_LIMIT="$git_http_low_speed_limit" \
            GIT_HTTP_LOW_SPEED_TIME="$git_http_low_speed_time" \
            git \
            -c http.lowSpeedLimit="$git_http_low_speed_limit" \
            -c http.lowSpeedTime="$git_http_low_speed_time" \
            "$@"
        return
    fi

    GIT_TERMINAL_PROMPT=0 \
        GIT_HTTP_LOW_SPEED_LIMIT="$git_http_low_speed_limit" \
        GIT_HTTP_LOW_SPEED_TIME="$git_http_low_speed_time" \
        "$@"
}

time_step() {
    local name="$1"
    shift

    local output
    output="$(log_path_for step "$name")"
    log "start $name log=$output"
    local start end elapsed status
    local step_heartbeat_pid
    local cache_before cache_after
    cache_before="$(capture_wos_cache_snapshot step "$name" "before")"
    start_log_heartbeat "step" "$name" "$output"
    step_heartbeat_pid="$heartbeat_pid"
    start="$(now_ms)"
    set +e
    "$@" >"$output" 2>&1
    status=$?
    set -e
    end="$(now_ms)"
    stop_log_heartbeat "$step_heartbeat_pid"
    elapsed=$((end - start))
    cache_after="$(capture_wos_cache_snapshot step "$name" "after")"
    record_wos_cache_delta step "$name" "$cache_before" "$cache_after"
    total_elapsed=$((total_elapsed + elapsed))
    printf '%s\t%s\n' "$name" "$elapsed" >> "$report"
    if [ "$status" -eq 0 ]; then
        record_timing "step" "$name" "$elapsed" "ok"
        log "done $name ${elapsed}ms"
    else
        record_timing "step" "$name" "$elapsed" "fail:$status"
        log "failed $name ${elapsed}ms status=$status"
        print_log_tail "$output"
    fi
    return "$status"
}

acquire_workdir_lock() {
    workdir_lock="${workdir%/}.lock"
    if ! mkdir "$workdir_lock" 2>/dev/null; then
        echo "self-host benchmark workdir is already in use: $workdir" >&2
        echo "lock directory: $workdir_lock" >&2
        echo "remove the lock only after confirming no benchmark process is using that workdir" >&2
        exit 1
    fi

    printf '%s\n' "$run_id" > "$workdir_lock/run_id" 2>/dev/null || true
    printf '%s\n' "$$" > "$workdir_lock/pid" 2>/dev/null || true
    trap selfhost_cleanup EXIT
}

canonical_path() {
    python3 - "$1" <<'PY'
import os
import sys

print(os.path.realpath(sys.argv[1]))
PY
}

safe_prepare_workdir() {
    local canonical_workdir
    canonical_workdir="$(canonical_path "$workdir")"
    if [ "$canonical_workdir" != "${workdir%/}" ]; then
        echo "scratch workdir must be an absolute normalized path without symlink aliases: $workdir" >&2
        exit 1
    fi

    if [ -n "$source_cache" ]; then
        local canonical_source_cache
        canonical_source_cache="$(canonical_path "$source_cache")"
        case "$canonical_source_cache" in
            "$canonical_workdir"|"$canonical_workdir"/*)
                echo "source cache must be outside the scratch workdir: $source_cache" >&2
                exit 1
                ;;
        esac
    fi

    if [ -n "$distdir" ]; then
        local canonical_distdir
        canonical_distdir="$(canonical_path "$distdir")"
        case "$canonical_distdir" in
            "$canonical_workdir"|"$canonical_workdir"/*)
                echo "source distdir must be outside the scratch workdir: $distdir" >&2
                exit 1
                ;;
        esac
    fi

    case "$canonical_workdir" in
        /tmp|/tmp/|/var/tmp|/var/tmp/)
            echo "refusing to replace temporary directory root: $workdir" >&2
            exit 1
            ;;
        /tmp/*|/var/tmp/*|/root/wos-selfhost-*|/home/*/wos-selfhost-*)
            ;;
        *)
            if [ "$keep_workdir" != "1" ]; then
                echo "refusing to replace non-temporary workdir without --keep-workdir: $workdir" >&2
                exit 1
            fi
            ;;
    esac

    if [ "$keep_workdir" = "1" ] && [ "$resume_checkout" = "1" ]; then
        echo "--keep-workdir and --resume-checkout are mutually exclusive" >&2
        exit 1
    fi

    if [ "$resume_checkout" = "1" ] && [ ! -d "$checkout/.git" ]; then
        echo "--resume-checkout requires an existing Git checkout: $checkout" >&2
        exit 1
    fi

    if [ "$keep_workdir" = "1" ] && [ -e "$checkout" ]; then
        echo "checkout already exists and --keep-workdir was requested: $checkout" >&2
        exit 1
    fi

    acquire_workdir_lock

    if [ "$keep_workdir" != "1" ] && [ "$resume_checkout" != "1" ]; then
        rm -rf -- "$workdir"
    fi

    mkdir -p "$workdir/home" "$workdir/tmp"
    export HOME="$workdir/home"
    export TMPDIR="$workdir/tmp"
    if [ -z "$history_file" ]; then
        history_file="${workdir%/}-history.tsv"
    fi
    if [ -z "$log_dir" ]; then
        log_dir="$(default_log_dir)"
    fi
    ensure_parent_dir "$history_file"
    mkdir -p "$log_dir"
    : > "$report"
    : > "$detail_report"
    : > "$cache_diag_report"
    write_timing_header "$detail_report"
    write_timing_header "$history_file"
    write_cache_diag_header "$cache_diag_report"
}

configure_git_mirror() {
    if [ -n "$checkout_workers" ]; then
        git config --global checkout.workers "$checkout_workers"
    fi

    if [ -n "$mirror_file" ]; then
        git config --global protocol.file.allow always
        git config --global "url.file://$mirror_file/".insteadOf https://github.com/
    fi

    if [ -n "$mirror_local_path" ]; then
        git config --global protocol.file.allow always
        git config --global "url.$mirror_local_path/".insteadOf https://github.com/
    fi

    if [ -n "$mirror_http_prefix" ]; then
        case "$mirror_http_prefix" in
            */) ;;
            *) mirror_http_prefix="$mirror_http_prefix/" ;;
        esac
        git config --global "url.$mirror_http_prefix".insteadOf https://github.com/
    fi
}

write_submodule_status() {
    run_git_http git -C "$checkout" submodule status --recursive > "$workdir/submodules.txt"
}

validate_depth1_git_repo() {
    local path="$1"
    local label="$2"
    local shallow count

    shallow="$(git -C "$path" rev-parse --is-shallow-repository 2>/dev/null || printf false)"
    if [ "$shallow" != "true" ]; then
        echo "$label must be a shallow depth-1 Git checkout: $path" >&2
        return 1
    fi

    count="$(git -C "$path" rev-list --count HEAD 2>/dev/null || printf 0)"
    if [ "$count" != "1" ]; then
        echo "$label must expose exactly one commit, got $count: $path" >&2
        return 1
    fi
}

validate_git_worktree_clean() {
    local path="$1"
    local label="$2"
    local status

    status="$(git -C "$path" status --porcelain --untracked-files=no 2>/dev/null || true)"
    if [ -n "$status" ]; then
        echo "$label must have a populated clean Git worktree: $path" >&2
        printf '%s\n' "$status" | sed -n '1,20p' >&2
        return 1
    fi
}

validate_source_cache_checkout() {
    git -C "$checkout" rev-parse --is-inside-work-tree >/dev/null
    validate_depth1_git_repo "$checkout" "source cache"
    validate_git_worktree_clean "$checkout" "source cache"
}

validate_source_cache_submodules() {
    git -C "$checkout" submodule foreach --recursive '
        shallow="$(git rev-parse --is-shallow-repository 2>/dev/null || printf false)"
        count="$(git rev-list --count HEAD 2>/dev/null || printf 0)"
        if [ "$shallow" != "true" ] || [ "$count" != "1" ]; then
            echo "source cache submodule must be a shallow depth-1 Git checkout: $displaypath" >&2
            exit 1
        fi
        status="$(git status --porcelain --untracked-files=no 2>/dev/null || true)"
        if [ -n "$status" ]; then
            echo "source cache submodule must have a populated clean Git worktree: $displaypath" >&2
            printf "%s\n" "$status" | sed -n "1,20p" >&2
            exit 1
        fi
    ' >/dev/null
}

refresh_resume_checkout_worktrees() {
    git -C "$checkout" checkout -f HEAD
    git -C "$checkout" submodule foreach --recursive 'git checkout -f HEAD'
}

seed_sources_from_cache() {
    source_cache="${source_cache%/}"
    checkout="$source_cache"

    if [ ! -f "$source_cache/.gitmodules" ]; then
        echo "source cache is missing .gitmodules: $source_cache" >&2
        exit 1
    fi

    if ! git -C "$checkout" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
        echo "source cache must be a depth-1 Git checkout, not an exported source tree: $source_cache" >&2
        exit 1
    fi

    run_timed_event "source_cache" "depth1_checkout" validate_source_cache_checkout
    commit="$(git -C "$checkout" rev-parse HEAD)"
    record_timing "metadata" "checkout_commit" "0" "ok"
    run_timed_event "source_cache" "submodule_status" write_submodule_status

    if grep -q '^-' "$workdir/submodules.txt"; then
        echo "source cache has uninitialized submodules:" >&2
        cat "$workdir/submodules.txt" >&2
        exit 1
    fi

    run_timed_event "source_cache" "depth1_submodules" validate_source_cache_submodules
}

submodule_paths() {
    git -C "$checkout" config -f .gitmodules --get-regexp '^submodule\..*\.path$' |
        awk '{ print $2 }'
}

clone_submodules() {
    local path
    local paths_file="$workdir/submodule-paths.txt"
    local have_paths=0
    local submodule_cmd=(git -C "$checkout" submodule update --init --recursive)
    if [ "$full_history" != "1" ]; then
        submodule_cmd+=(--depth 1)
    fi
    submodule_cmd+=(--jobs "$jobs" --)

    submodule_paths > "$paths_file"
    while IFS= read -r path; do
        [ -n "$path" ] || continue
        submodule_cmd+=("$path")
        have_paths=1
    done < "$paths_file"

    if [ "$have_paths" != "1" ]; then
        log "no submodules to clone"
        return 0
    fi

    run_timed_event "clone" "submodules" run_git_http "${submodule_cmd[@]}"
}

resume_existing_checkout() {
    log "resume_checkout=$checkout"
    git -C "$checkout" rev-parse --is-inside-work-tree >/dev/null
    run_timed_event "resume_checkout" "refresh_worktrees" refresh_resume_checkout_worktrees
    run_timed_event "resume_checkout" "depth1_checkout" validate_source_cache_checkout
    commit="$(git -C "$checkout" rev-parse HEAD)"
    record_timing "metadata" "checkout_commit" "0" "ok"
    run_timed_event "resume_checkout" "submodule_status" write_submodule_status

    if grep -q '^-' "$workdir/submodules.txt"; then
        echo "resume checkout has uninitialized submodules:" >&2
        cat "$workdir/submodules.txt" >&2
        exit 1
    fi

    run_timed_event "resume_checkout" "depth1_submodules" validate_source_cache_submodules
}

clone_sources() {
    if [ "$resume_checkout" = "1" ]; then
        resume_existing_checkout
        return 0
    fi

    if [ -n "$source_cache" ]; then
        log "source_cache=$source_cache"
        seed_sources_from_cache
        return 0
    fi

    configure_git_mirror

    local clone_cmd=(git clone)
    if [ "$full_history" != "1" ]; then
        clone_cmd+=(--depth 1)
    fi
    clone_cmd+=("$repo" "$checkout")
    run_timed_event "clone" "wos_repo" run_git_http "${clone_cmd[@]}"
    commit="$(git -C "$checkout" rev-parse HEAD)"
    record_timing "metadata" "checkout_commit" "0" "ok"

    run_timed_event "clone" "submodule_init" run_git_http git -C "$checkout" submodule init

    clone_submodules

    run_timed_event "clone" "submodule_status" write_submodule_status
}

bootstrap_toolchain() {
    if [ "$skip_bootstrap" = "1" ]; then
        log "skip bootstrap_toolchain"
        return 0
    fi

    (
        cd "$checkout"
        WOS_BOOTSTRAP_DETAIL_TSV="$bootstrap_detail_report" \
            WOS_SOURCE_DISTDIR="$distdir" \
            WOS_HOST_CLANG_TIDY_CACHE=0 \
            WOS_USE_CCACHE=0 \
            run_with_jobs_env ./tools/bootstrap.sh
    )
}

configure_cmake_command() {
    if [ "$mode" = "linux" ] && [ -n "$host_toolchain" ]; then
        printf '%s\n' "$host_toolchain/bin/cmake"
        return
    fi
    command -v cmake
}

warm_configure_tools() {
    local cmake_command
    cmake_command="$(configure_cmake_command)"

    "$cmake_command" --version >/dev/null
    ninja --version >/dev/null
    log "warmed configure tools: cmake=$cmake_command ninja=$(command -v ninja)"
}

configure_wos() {
    local cmake_command
    cmake_command="$(configure_cmake_command)"
    local cmake_args=("$cmake_command" -GNinja
        -S "$checkout" \
        -B "$checkout/$build_dir" \
        -DWOS_BUILD_WOSDBG=OFF \
        -DWOS_BUILD_HOST_TOOLS=OFF \
        -DWOS_BUILD_CMAKE_FOR_HOST=OFF \
        -DWOS_USE_CCACHE=OFF \
        -DWOS_BUILD_DISK_IMAGES=OFF \
        -DWOS_ASSUME_BOOTSTRAPPED_TOOLCHAIN=ON)
    if [ -n "$host_toolchain" ]; then
        cmake_args+=("-DWOS_HOST_TOOLCHAIN_PATH=$host_toolchain")
        cmake_args+=("-DWOS_SYSROOT_PATH=$host_sysroot")
    fi
    run_with_jobs_env "${cmake_args[@]}"
}

build_wos() {
    local cmd=(cmake --build "$checkout/$build_dir" --target "$target" --parallel "$jobs")
    run_with_jobs_env "${cmd[@]}"
}

cmake_cache_value() {
    local key="$1"
    local cache="$checkout/$build_dir/CMakeCache.txt"

    [ -f "$cache" ] || return 1
    sed -n "s/^${key}:[^=]*=//p" "$cache" | tail -n 1
}

verify_disk_artifacts() {
    local build_disk_images

    build_disk_images="$(cmake_cache_value WOS_BUILD_DISK_IMAGES || printf 'ON')"
    if [ "$build_disk_images" != "ON" ]; then
        log "skip disk image artifact verification: WOS_BUILD_DISK_IMAGES=$build_disk_images"
        return 0
    fi

    require_file "disk.qcow2"
    require_file "mountfs.qcow2"
}

verify_artifacts() {
    require_file "toolchain/sysroot/bin/clang"
    require_file "toolchain/sysroot/bin/ld.lld"
    require_file "toolchain/sysroot/bin/lld"
    require_file "toolchain/sysroot/bin/llvm-ar"
    require_file "toolchain/sysroot/bin/llvm-ranlib"
    require_file "toolchain/sysroot/bin/llvm-nm"
    require_file "toolchain/sysroot/bin/llvm-objcopy"
    require_file "toolchain/sysroot/bin/llvm-strip"
    require_file "toolchain/sysroot/bin/llvm-readelf"
    require_file "toolchain/sysroot/bin/llvm-objdump"
    require_file "toolchain/sysroot/bin/llvm-symbolizer"
    require_file "toolchain/sysroot/bin/llvm-as"
    require_file "toolchain/sysroot/bin/llvm-dis"
    require_file "toolchain/sysroot/bin/llvm-link"
    require_file "toolchain/sysroot/bin/llvm-size"
    require_file "toolchain/sysroot/bin/llvm-strings"
    require_file "toolchain/sysroot/bin/llvm-dwarfdump"
    require_file "toolchain/sysroot/bin/llc"
    require_file "toolchain/sysroot/bin/opt"
    require_file "toolchain/sysroot/bin/obj2yaml"
    require_file "toolchain/sysroot/bin/yaml2obj"
    require_file "toolchain/sysroot/bin/llvm-tblgen"
    require_file "toolchain/sysroot/bin/clang-tblgen"
    require_file "toolchain/sysroot/bin/x86_64-pc-wos.cfg"
    require_file "toolchain/sysroot/lib/clang"
    require_file "toolchain/sysroot/bin/ninja"
    require_file "toolchain/sysroot/bin/cmake"
    require_file "toolchain/sysroot/bin/ctest"
    require_file "toolchain/sysroot/bin/cpack"
    require_file "toolchain/sysroot/bin/make"
    require_file "toolchain/sysroot/bin/git"
    require_file "toolchain/sysroot/bin/git-shell"
    require_file "toolchain/sysroot/bin/scalar"
    require_file "toolchain/sysroot/libexec/git-core/git-remote-https"
    require_file "toolchain/sysroot/bin/curl"
    require_file "toolchain/sysroot/etc/ssl/certs/ca-certificates.crt"
    require_file "toolchain/sysroot/bin/bash"
    require_file "toolchain/sysroot/bin/dropbearmulti"
    require_file "toolchain/busybox-install/bin/busybox"
    require_file "toolchain/sysroot/bin/meson"
    require_file "toolchain/sysroot/bin/nasm"
    require_file "toolchain/sysroot/bin/ndisasm"
    require_any "python" "toolchain/sysroot/bin/python" "toolchain/sysroot/bin/python*"
    require_any "python3" "toolchain/sysroot/bin/python3" "toolchain/sysroot/bin/python3.*"
    require_file "$build_dir/modules/kern/wos"
    require_file "$build_dir/modules/init/init"
    require_file "$build_dir/modules/testprog/testprog"
    require_file "$build_dir/modules/testd/testd"
    require_file "$build_dir/modules/netd/netd"
    require_file "$build_dir/modules/httpd/httpd"
    require_file "$build_dir/modules/debugserver/debugserver"
    require_file "$build_dir/modules/perf/perf"
    require_file "$build_dir/modules/top/top"
    require_file "$build_dir/modules/memacc/memacc"
    require_file "$build_dir/modules/journal/journal"
    require_file "$build_dir/modules/journal/libjournal.so"
    require_file "$build_dir/modules/wkictl/wkictl"
    require_file "$build_dir/modules/powerctl/powerctl"
    require_file "$build_dir/modules/renderbench/renderbench"
    require_file "$build_dir/modules/strace/strace"
    require_file "$build_dir/modules/sftpserver/sftp-server"
    verify_disk_artifacts
}

for tool in bash git cmake ninja clang clang++ python3; do
    require_tool "$tool"
done

case "$mode" in
    linux)
        ;;
    wos)
        require_wos_selfhost_tools
        ;;
    *)
        echo "unknown self-host mode: $mode" >&2
        exit 1
        ;;
esac

safe_prepare_workdir
validate_runtime_settings
log "mode=$mode"
log "repo=$repo"
log "workdir=$workdir"
log "detail_report=$detail_report"
log "bootstrap_detail_report=$bootstrap_detail_report"
log "cache_diag_report=$cache_diag_report"
log "host_toolchain=$host_toolchain"
log "host_sysroot=$host_sysroot"
log "distdir=$distdir"
log "bootstrap_ccache=disabled"
log "history_file=$history_file"
log "log_dir=$log_dir"
log "target=$target"
log "git_http_low_speed=${git_http_low_speed_limit}Bps/${git_http_low_speed_time}s"
log "heartbeat=${heartbeat_interval}s tail=${heartbeat_tail} stall_snapshots=${heartbeat_stall_snapshots} sync=${heartbeat_sync}"
time_step clone_sources clone_sources
time_step bootstrap_toolchain bootstrap_toolchain
warm_configure_tools
time_step configure_wos configure_wos
time_step build_wos build_wos
if [ "$skip_bootstrap" = "1" ]; then
    log "skip toolchain artifact verification: WOS_SELFHOST_SKIP_BOOTSTRAP=1"
else
    verify_artifacts
fi
printf '%s\t%s\n' "total" "$total_elapsed" >> "$report"
record_timing "step" "total" "$total_elapsed" "ok"
log "report=$report"
log "detail_report=$detail_report"
log "history_file=$history_file"
cat "$report"
EOF
}

run_local() {
    selfhost_payload | env \
        WOS_SELFHOST_MODE="linux" \
        WOS_SELFHOST_REPO="$repo" \
        WOS_SELFHOST_WORKDIR="$workdir" \
        WOS_SELFHOST_BUILD_DIR="$build_dir" \
        WOS_SELFHOST_TARGET="$target" \
        WOS_SELFHOST_JOBS="$jobs" \
        WOS_SELFHOST_SKIP_BOOTSTRAP="$skip_bootstrap" \
        WOS_SELFHOST_KEEP_WORKDIR="$keep_workdir" \
        WOS_SELFHOST_RESUME_CHECKOUT="$resume_checkout" \
        WOS_SELFHOST_HISTORY_FILE="$history_file" \
        WOS_SELFHOST_LOG_DIR="$log_dir" \
        WOS_SELFHOST_FULL_HISTORY="$full_history" \
        WOS_SELFHOST_MIRROR_FILE="$mirror_file" \
        WOS_SELFHOST_MIRROR_LOCAL_PATH="$mirror_local_path" \
        WOS_SELFHOST_MIRROR_HTTP_PREFIX="$mirror_http_prefix" \
        WOS_SELFHOST_CHECKOUT_WORKERS="$checkout_workers" \
        WOS_SELFHOST_HOST_TOOLCHAIN="$host_toolchain" \
        WOS_SELFHOST_HOST_SYSROOT="$host_sysroot" \
        WOS_SELFHOST_SOURCE_CACHE="$source_cache" \
        WOS_SELFHOST_DISTDIR="$distdir" \
        WOS_SELFHOST_HEARTBEAT_INTERVAL="$heartbeat_interval" \
        WOS_SELFHOST_HEARTBEAT_TAIL="$heartbeat_tail" \
        WOS_SELFHOST_HEARTBEAT_STALL_SNAPSHOTS="$heartbeat_stall_snapshots" \
        WOS_SELFHOST_HEARTBEAT_SYNC="$heartbeat_sync" \
        WOS_SELFHOST_CLEAN_PATH="${WOS_ORIGINAL_PATH:-$PATH}" \
        bash -s
}

run_wos_local() {
    selfhost_payload | env \
        WOS_SELFHOST_MODE="wos" \
        WOS_SELFHOST_REPO="$repo" \
        WOS_SELFHOST_WORKDIR="$workdir" \
        WOS_SELFHOST_BUILD_DIR="$build_dir" \
        WOS_SELFHOST_TARGET="$target" \
        WOS_SELFHOST_JOBS="$jobs" \
        WOS_SELFHOST_SKIP_BOOTSTRAP="$skip_bootstrap" \
        WOS_SELFHOST_KEEP_WORKDIR="$keep_workdir" \
        WOS_SELFHOST_RESUME_CHECKOUT="$resume_checkout" \
        WOS_SELFHOST_HISTORY_FILE="$history_file" \
        WOS_SELFHOST_LOG_DIR="$log_dir" \
        WOS_SELFHOST_FULL_HISTORY="$full_history" \
        WOS_SELFHOST_MIRROR_FILE="$mirror_file" \
        WOS_SELFHOST_MIRROR_LOCAL_PATH="$mirror_local_path" \
        WOS_SELFHOST_MIRROR_HTTP_PREFIX="$mirror_http_prefix" \
        WOS_SELFHOST_CHECKOUT_WORKERS="$checkout_workers" \
        WOS_SELFHOST_HOST_TOOLCHAIN="$host_toolchain" \
        WOS_SELFHOST_HOST_SYSROOT="$host_sysroot" \
        WOS_SELFHOST_SOURCE_CACHE="$source_cache" \
        WOS_SELFHOST_DISTDIR="$distdir" \
        WOS_SELFHOST_HEARTBEAT_INTERVAL="$heartbeat_interval" \
        WOS_SELFHOST_HEARTBEAT_TAIL="$heartbeat_tail" \
        WOS_SELFHOST_HEARTBEAT_STALL_SNAPSHOTS="$heartbeat_stall_snapshots" \
        WOS_SELFHOST_HEARTBEAT_SYNC="$heartbeat_sync" \
        WOS_SELFHOST_CLEAN_PATH="/usr/bin:/bin:/usr/sbin:/sbin" \
        bash -s
}

run_wos() {
    [ -x "$WOS_SSH" ] || die "missing WOS SSH helper: $WOS_SSH"

    local remote_env
    remote_env="env"
    remote_env+=" WOS_SELFHOST_MODE=$(shell_quote wos)"
    remote_env+=" WOS_SELFHOST_REPO=$(shell_quote "$repo")"
    remote_env+=" WOS_SELFHOST_WORKDIR=$(shell_quote "$workdir")"
    remote_env+=" WOS_SELFHOST_BUILD_DIR=$(shell_quote "$build_dir")"
    remote_env+=" WOS_SELFHOST_TARGET=$(shell_quote "$target")"
    remote_env+=" WOS_SELFHOST_JOBS=$(shell_quote "$jobs")"
    remote_env+=" WOS_SELFHOST_SKIP_BOOTSTRAP=$(shell_quote "$skip_bootstrap")"
    remote_env+=" WOS_SELFHOST_KEEP_WORKDIR=$(shell_quote "$keep_workdir")"
    remote_env+=" WOS_SELFHOST_RESUME_CHECKOUT=$(shell_quote "$resume_checkout")"
    remote_env+=" WOS_SELFHOST_HISTORY_FILE=$(shell_quote "$history_file")"
    remote_env+=" WOS_SELFHOST_LOG_DIR=$(shell_quote "$log_dir")"
    remote_env+=" WOS_SELFHOST_FULL_HISTORY=$(shell_quote "$full_history")"
    remote_env+=" WOS_SELFHOST_MIRROR_FILE=$(shell_quote "$mirror_file")"
    remote_env+=" WOS_SELFHOST_MIRROR_LOCAL_PATH=$(shell_quote "$mirror_local_path")"
    remote_env+=" WOS_SELFHOST_MIRROR_HTTP_PREFIX=$(shell_quote "$mirror_http_prefix")"
    remote_env+=" WOS_SELFHOST_CHECKOUT_WORKERS=$(shell_quote "$checkout_workers")"
    remote_env+=" WOS_SELFHOST_HOST_TOOLCHAIN=$(shell_quote "$host_toolchain")"
    remote_env+=" WOS_SELFHOST_HOST_SYSROOT=$(shell_quote "$host_sysroot")"
    remote_env+=" WOS_SELFHOST_SOURCE_CACHE=$(shell_quote "$source_cache")"
    remote_env+=" WOS_SELFHOST_DISTDIR=$(shell_quote "$distdir")"
    remote_env+=" WOS_SELFHOST_DISTRIBUTED=$(shell_quote "$distributed")"
    remote_env+=" WOS_SELFHOST_HEARTBEAT_INTERVAL=$(shell_quote "$heartbeat_interval")"
    remote_env+=" WOS_SELFHOST_HEARTBEAT_TAIL=$(shell_quote "$heartbeat_tail")"
    remote_env+=" WOS_SELFHOST_HEARTBEAT_STALL_SNAPSHOTS=$(shell_quote "$heartbeat_stall_snapshots")"
    remote_env+=" WOS_SELFHOST_HEARTBEAT_SYNC=$(shell_quote "$heartbeat_sync")"
    remote_env+=" WOS_SELFHOST_CLEAN_PATH=/usr/bin:/bin:/usr/sbin:/sbin"
    remote_env+=" WOS_SELFHOST_PRIORITY_RESET=0"
    remote_env+=" WOS_SELFHOST_PRIORITY_NICE_DELTA=5"
    remote_env+=" WOS_SELFHOST_PRIORITY_RESET_HELPER=1"

    local remote_command
    remote_command="payload=\${TMPDIR:-/tmp}/wos-selfhost-build.\$\$.sh"
    remote_command+="; cat > \"\$payload\" || exit \$?"
    if [ "$distributed" -eq 1 ]; then
        local distributed_command="locally forward"
        local routed_path
        for routed_path in "$workdir" "$history_file" "$log_dir" "$mirror_file" "$mirror_local_path" \
            "$source_cache" "$distdir" "$host_toolchain" "$host_sysroot"; do
            if [ -n "$routed_path" ]; then
                distributed_command+=" $(shell_quote "+${routed_path%/}")"
            fi
        done
        distributed_command+=" \"+\$payload\""
        for routed_path in /root /usr /bin /lib /lib64 /libexec /share /etc /proc /dev /run /tmp; do
            distributed_command+=" $(shell_quote "-$routed_path")"
        done
        distributed_command+=" -- $remote_env bash \"\$payload\""
        remote_command+="; $distributed_command"
    else
        remote_command+="; $remote_env bash \"\$payload\""
    fi
    remote_command+="; status=\$?"
    remote_command+="; rm -f \"\$payload\""
    remote_command+="; exit \$status"

    selfhost_payload | "$WOS_SSH" "$host" "$remote_command"
}

mode="${1:-}"
if [ -n "$mode" ]; then
    shift
fi

host="$DEFAULT_HOST"
repo="$DEFAULT_REPO"
workdir=""
build_dir="$DEFAULT_BUILD_DIR"
target="$DEFAULT_TARGET"
jobs="$DEFAULT_JOBS"
skip_bootstrap=0
keep_workdir=0
resume_checkout=0
history_file=""
log_dir=""
full_history=0
mirror_file=""
mirror_local_path=""
mirror_http_prefix=""
checkout_workers=""
host_toolchain=""
host_sysroot=""
source_cache=""
distdir=""
heartbeat_interval="30"
heartbeat_tail="4"
heartbeat_stall_snapshots="6"
heartbeat_sync="0"
distributed=0

while (($# > 0)); do
    case "$1" in
        --host)
            host="${2:-}"
            [ -n "$host" ] || die "--host requires a value"
            shift
            ;;
        --repo)
            repo="${2:-}"
            [ -n "$repo" ] || die "--repo requires a value"
            shift
            ;;
        --workdir)
            workdir="${2:-}"
            [ -n "$workdir" ] || die "--workdir requires a value"
            shift
            ;;
        --build-dir)
            build_dir="${2:-}"
            [ -n "$build_dir" ] || die "--build-dir requires a value"
            shift
            ;;
        --target)
            target="${2:-}"
            [ -n "$target" ] || die "--target requires a value"
            shift
            ;;
        --jobs)
            jobs="${2:-}"
            [[ "$jobs" =~ ^[1-9][0-9]*$ ]] || die "--jobs requires a positive integer"
            shift
            ;;
        --skip-bootstrap)
            skip_bootstrap=1
            ;;
        --keep-workdir)
            keep_workdir=1
            ;;
        --resume-checkout)
            resume_checkout=1
            ;;
        --history-file)
            history_file="${2:-}"
            [ -n "$history_file" ] || die "--history-file requires a value"
            shift
            ;;
        --log-dir)
            log_dir="${2:-}"
            [ -n "$log_dir" ] || die "--log-dir requires a value"
            shift
            ;;
        --full-history)
            full_history=1
            ;;
        --mirror-file)
            mirror_file="${2:-}"
            [ -n "$mirror_file" ] || die "--mirror-file requires a value"
            shift
            ;;
        --mirror-local-path)
            mirror_local_path="${2:-}"
            [ -n "$mirror_local_path" ] || die "--mirror-local-path requires a value"
            shift
            ;;
        --mirror-http-prefix)
            mirror_http_prefix="${2:-}"
            [ -n "$mirror_http_prefix" ] || die "--mirror-http-prefix requires a value"
            shift
            ;;
        --checkout-workers)
            checkout_workers="${2:-}"
            [[ "$checkout_workers" =~ ^[1-9][0-9]*$ ]] || die "--checkout-workers requires a positive integer"
            shift
            ;;
        --host-toolchain)
            host_toolchain="${2:-}"
            [ -n "$host_toolchain" ] || die "--host-toolchain requires a value"
            shift
            ;;
        --host-sysroot)
            host_sysroot="${2:-}"
            [ -n "$host_sysroot" ] || die "--host-sysroot requires a value"
            shift
            ;;
        --source-cache)
            source_cache="${2:-}"
            [ -n "$source_cache" ] || die "--source-cache requires a value"
            shift
            ;;
        --distdir)
            distdir="${2:-}"
            [ -n "$distdir" ] || die "--distdir requires a value"
            shift
            ;;
        --heartbeat-interval)
            heartbeat_interval="${2:-}"
            [[ "$heartbeat_interval" =~ ^[0-9]+$ ]] || die "--heartbeat-interval requires a non-negative integer"
            shift
            ;;
        --heartbeat-tail)
            heartbeat_tail="${2:-}"
            [[ "$heartbeat_tail" =~ ^[0-9]+$ ]] || die "--heartbeat-tail requires a non-negative integer"
            shift
            ;;
        --heartbeat-stall-snapshots)
            heartbeat_stall_snapshots="${2:-}"
            [[ "$heartbeat_stall_snapshots" =~ ^[0-9]+$ ]] || die "--heartbeat-stall-snapshots requires a non-negative integer"
            shift
            ;;
        --heartbeat-sync)
            heartbeat_sync="1"
            ;;
        --distributed)
            distributed=1
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            die "unknown option: $1"
            ;;
    esac
    shift
done

case "$mode" in
    wos|wos-local|linux)
        ;;
    -h|--help|"")
        usage
        exit 0
        ;;
    *)
        die "unknown mode: $mode"
        ;;
esac

if [ "$distributed" -eq 1 ] && [ "$mode" != "wos" ]; then
    die "--distributed is only supported with wos mode"
fi

if [ -z "$workdir" ]; then
    case "$mode" in
        wos|wos-local)
            workdir="$DEFAULT_WOS_WORKDIR"
            ;;
        linux)
            workdir="$DEFAULT_LINUX_WORKDIR"
            ;;
    esac
fi

if [ "$mode" = "linux" ] && [ -n "$host_toolchain" ] && [ -z "$host_sysroot" ]; then
    host_sysroot="$(dirname "$host_toolchain")/sysroot"
fi

reject_whitespace "--host" "$host"
reject_whitespace "--repo" "$repo"
reject_whitespace "--workdir" "$workdir"
reject_whitespace "--build-dir" "$build_dir"
reject_whitespace "--target" "$target"
reject_whitespace "--history-file" "$history_file"
reject_whitespace "--log-dir" "$log_dir"
reject_whitespace "--mirror-file" "$mirror_file"
reject_whitespace "--mirror-local-path" "$mirror_local_path"
reject_whitespace "--mirror-http-prefix" "$mirror_http_prefix"
reject_whitespace "--host-toolchain" "$host_toolchain"
reject_whitespace "--host-sysroot" "$host_sysroot"
reject_whitespace "--source-cache" "$source_cache"
reject_whitespace "--distdir" "$distdir"

case "$mode" in
    linux)
        run_local
        ;;
    wos-local)
        run_wos_local
        ;;
    wos)
        run_wos
        ;;
esac
