#!/bin/sh
set -eu

DIR="$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)"
# shellcheck disable=SC1091
. "$DIR/showcase-common.sh"

CXX="${CXX:-clang++}"
SRC="${WOS_LIVE_DEMO_SRC:-$DIR/live-cpp-demo.cpp}"
BIN="${WOS_LIVE_DEMO_BIN:-/tmp/wos-live-demo}"
case "${WOS_SHOWCASE_SCALE:-quick}" in
    quick) DEFAULT_BYTES=65536 ;;
    full) DEFAULT_BYTES=1048576 ;;
    stress) DEFAULT_BYTES=4194304 ;;
    *) DEFAULT_BYTES=65536 ;;
esac
BYTES="${WOS_LIVE_DEMO_BYTES:-$DEFAULT_BYTES}"
LOCAL_FILE="${WOS_LIVE_DEMO_LOCAL_FILE:-/tmp/wos-live-demo-local.dat}"
REMOTE_FILE="${WOS_LIVE_DEMO_REMOTE_FILE:-/tmp/wos-live-demo-remote.dat}"
COMPILE_ONLY="${WOS_LIVE_DISTRIBUTED_COMPILE_ONLY:-0}"
COMPILE_TOTAL_UNITS=32

compile_error() {
    printf 'distributed compile: %s\n' "$*" >&2
    return 1
}

compile_safe_hostname() {
    case "$1" in
        ""|*[!A-Za-z0-9._-]*) return 1 ;;
        *) return 0 ;;
    esac
}

compile_normalize_hostname() {
    case "$1" in
        *.wos) printf '%s\n' "${1%.wos}" ;;
        *) printf '%s\n' "$1" ;;
    esac
}

compile_wosid_spawner() {
    printf '%s\n' "$1" | sed -n 's/^spawner=\([^ ]*\) host=[^ ]* pid=[^ ]* remote_pid=[^ ]*$/\1/p'
}

compile_wosid_runner() {
    printf '%s\n' "$1" | sed -n 's/^spawner=[^ ]* host=\([^ ]*\) pid=[^ ]* remote_pid=[^ ]*$/\1/p'
}

compile_validate_hosts() {
    compile_raw_hosts="$1"
    case "$compile_raw_hosts" in
        ""|,*|*,|*,,*)
            compile_error "WOS_SHOWCASE_HOSTS must contain one to four comma-separated hostnames"
            return 1
            ;;
    esac

    compile_old_ifs="$IFS"
    IFS=,
    # shellcheck disable=SC2086  # Intentional CSV field splitting after validation.
    set -- $compile_raw_hosts
    IFS="$compile_old_ifs"
    COMPILE_HOST_COUNT=$#
    case "$COMPILE_HOST_COUNT" in
        1|2|3|4) ;;
        *)
            compile_error "expected one to four WOS_SHOWCASE_HOSTS, got $COMPILE_HOST_COUNT"
            return 1
            ;;
    esac

    compile_seen=,
    for compile_host in "$@"; do
        if ! compile_safe_hostname "$compile_host"; then
            compile_error "invalid hostname '$compile_host'"
            return 1
        fi
        compile_normalized_host="$(compile_normalize_hostname "$compile_host")"
        case "$compile_seen" in
            *,"$compile_normalized_host",*)
                compile_error "duplicate hostname '$compile_host'"
                return 1
                ;;
        esac
        compile_seen="${compile_seen}${compile_normalized_host},"
    done

    compile_launcher_identity="$(locally wosid)" || {
        compile_error "cannot read the launcher WKI identity"
        return 1
    }
    COMPILE_LAUNCHER_HOST="$(compile_wosid_runner "$compile_launcher_identity")"
    compile_launcher_spawner="$(compile_wosid_spawner "$compile_launcher_identity")"
    if ! compile_safe_hostname "$COMPILE_LAUNCHER_HOST" || ! compile_safe_hostname "$compile_launcher_spawner"; then
        compile_error "invalid launcher wosid record: $compile_launcher_identity"
        return 1
    fi
    COMPILE_LAUNCHER_NORMALIZED="$(compile_normalize_hostname "$COMPILE_LAUNCHER_HOST")"
    if [ "$(compile_normalize_hostname "$compile_launcher_spawner")" != "$COMPILE_LAUNCHER_NORMALIZED" ]; then
        compile_error "launcher wosid spawner/runner mismatch: $compile_launcher_identity"
        return 1
    fi

    compile_launcher_declared=0
    for compile_host in "$@"; do
        if [ "$(compile_normalize_hostname "$compile_host")" = "$COMPILE_LAUNCHER_NORMALIZED" ]; then
            compile_launcher_declared=$((compile_launcher_declared + 1))
        fi
    done
    if [ "$compile_launcher_declared" -ne 1 ]; then
        compile_error "declared hosts do not contain launcher $COMPILE_LAUNCHER_HOST exactly once"
        return 1
    fi
}

compile_generate_driver() {
    compile_driver_source="$1"
    {
        printf '#include <cstddef>\n#include <cstdio>\n\n'
        compile_unit=0
        while [ "$compile_unit" -lt "$COMPILE_TOTAL_UNITS" ]; do
            compile_tag="$(printf '%02d' "$compile_unit")"
            printf 'auto wos_compile_unit_%s(int, char**) -> int;\n' "$compile_tag"
            compile_unit=$((compile_unit + 1))
        done
        printf '\nusing unit_fn = int (*)(int, char**);\n\nauto main() -> int {\n'
        printf '    unit_fn units[] = {\n'
        compile_unit=0
        while [ "$compile_unit" -lt "$COMPILE_TOTAL_UNITS" ]; do
            compile_tag="$(printf '%02d' "$compile_unit")"
            printf '        wos_compile_unit_%s,\n' "$compile_tag"
            compile_unit=$((compile_unit + 1))
        done
        printf '    };\n'
        printf '    std::size_t linked = 0;\n'
        printf '    for (auto unit : units) {\n'
        printf '        linked += unit != nullptr ? 1U : 0U;\n'
        printf '    }\n'
        printf '    std::printf("wos-distributed-compile-ok units=%%zu\\n", linked);\n'
        printf '    return linked == %s ? 0 : 1;\n' "$COMPILE_TOTAL_UNITS"
        printf '}\n'
    } > "$compile_driver_source"
}

compile_cancel_jobs() {
    if [ "${compile_jobs_active:-0}" -ne 1 ]; then
        return 0
    fi
    compile_jobs_active=0
    if [ ! -f "$compile_pids" ]; then
        return 0
    fi

    while read -r compile_pid _compile_tag _compile_host; do
        kill "$compile_pid" 2>/dev/null || true
    done < "$compile_pids"
    while read -r compile_pid _compile_tag _compile_host; do
        wait "$compile_pid" 2>/dev/null || true
    done < "$compile_pids"
}

run_distributed_compile() {
    compile_workspace_root="${WOS_LIVE_COMPILE_WORKSPACE_ROOT:-${WOS_SHOWCASE_OUTPUT_ROOT:-/tmp/wos-showcase-live}}"
    case "$compile_workspace_root" in
        /*) ;;
        *)
            compile_error "workspace root must be absolute: $compile_workspace_root"
            return 1
            ;;
    esac
    compile_workspace="${compile_workspace_root%/}/distributed-compile"
    compile_source="$compile_workspace/source.cpp"
    compile_driver_source="$compile_workspace/driver.cpp"
    compile_driver_object="$compile_workspace/driver.o"
    compile_artifact="$compile_workspace/wos-distributed-compile"
    compile_host_plan="$compile_workspace/hosts.plan"
    compile_pids="$compile_workspace/pids"
    compile_participants="$compile_workspace/participants"

    mkdir -p "$compile_workspace"
    rm -f "$compile_source" "$compile_driver_source" "$compile_driver_object" "$compile_artifact" \
        "$compile_host_plan" "$compile_pids" "$compile_participants" \
        "$compile_workspace"/unit-*.o "$compile_workspace"/runner-*.txt "$compile_workspace"/compile-*.log
    cp "$SRC" "$compile_source"
    compile_generate_driver "$compile_driver_source"

    compile_old_ifs="$IFS"
    IFS=,
    # shellcheck disable=SC2086  # Intentional CSV field splitting after validation.
    set -- $hosts
    IFS="$compile_old_ifs"

    for compile_host in "$@"; do
        # shellcheck disable=SC2016  # The targeted shell expands its positional parameter.
        if ! on "$compile_host" sh -c 'command -v "$1" >/dev/null 2>&1 || [ -x "$1" ]' sh "$CXX"; then
            compile_error "$CXX is not available on $compile_host"
            return 1
        fi
    done

    showcase_section "fixed-total distributed c++ compilation"
    showcase_cmd locally "$CXX" -std=c++23 -O2 -fno-ident -c "$compile_driver_source" -o "$compile_driver_object"

    : > "$compile_host_plan"
    compile_base_units=$((COMPILE_TOTAL_UNITS / COMPILE_HOST_COUNT))
    compile_extra_units=$((COMPILE_TOTAL_UNITS % COMPILE_HOST_COUNT))
    compile_host_index=0
    compile_first_unit=0
    for compile_host in "$@"; do
        compile_host_units=$compile_base_units
        if [ "$compile_host_index" -lt "$compile_extra_units" ]; then
            compile_host_units=$((compile_host_units + 1))
        fi
        printf '%s %s %s\n' "$compile_host" "$compile_host_units" "$compile_first_unit" >> "$compile_host_plan"
        compile_first_unit=$((compile_first_unit + compile_host_units))
        compile_host_index=$((compile_host_index + 1))
    done
    if [ "$compile_first_unit" -ne "$COMPILE_TOTAL_UNITS" ]; then
        compile_error "internal work-plan error: assigned $compile_first_unit units"
        return 1
    fi

    compile_started="$(locally "$BIN" monotonic-ns)" || {
        compile_error "monotonic start timestamp failed"
        return 1
    }
    case "$compile_started" in
        ""|*[!0-9]*)
            compile_error "invalid monotonic start timestamp '$compile_started'"
            return 1
            ;;
    esac

    : > "$compile_pids"
    compile_jobs_active=1
    trap 'compile_cancel_jobs' 0
    trap 'compile_cancel_jobs; exit 129' HUP
    trap 'compile_cancel_jobs; exit 130' INT
    trap 'compile_cancel_jobs; exit 143' TERM
    while read -r compile_host compile_host_units compile_first_unit; do
        compile_offset=0
        while [ "$compile_offset" -lt "$compile_host_units" ]; do
            compile_unit=$((compile_first_unit + compile_offset))
            compile_tag="$(printf '%02d' "$compile_unit")"
            compile_marker="$compile_workspace/runner-$compile_tag.txt"
            compile_object="$compile_workspace/unit-$compile_tag.o"
            compile_log="$compile_workspace/compile-$compile_tag.log"
            compile_symbol="wos_compile_unit_$compile_tag"
            # shellcheck disable=SC2016  # The targeted shell expands the submitted script.
            on "$compile_host" forward "+$compile_workspace" -- sh -c '
set -eu
marker=$1
cxx=$2
source_file=$3
object_file=$4
symbol=$5
wosid > "$marker"
exec "$cxx" -std=c++23 -O2 -fno-ident -c "$source_file" "-Dmain=$symbol" -o "$object_file"
' sh "$compile_marker" "$CXX" "$compile_source" "$compile_object" "$compile_symbol" > "$compile_log" 2>&1 &
            compile_pid=$!
            printf '%s %s %s\n' "$compile_pid" "$compile_tag" "$compile_host" >> "$compile_pids"
            compile_offset=$((compile_offset + 1))
        done
    done < "$compile_host_plan"

    compile_failed=0
    while read -r compile_pid compile_tag compile_host; do
        if wait "$compile_pid"; then
            :
        else
            compile_rc=$?
            printf 'distributed compile: unit %s on %s failed with rc=%s\n' "$compile_tag" "$compile_host" "$compile_rc" >&2
            cat "$compile_workspace/compile-$compile_tag.log" >&2 || true
            compile_failed=1
        fi
    done < "$compile_pids"
    compile_jobs_active=0
    trap - 0 HUP INT TERM
    if [ "$compile_failed" -ne 0 ]; then
        return 1
    fi

    showcase_cmd locally "$CXX" -std=c++23 -O2 "$compile_driver_object" "$compile_workspace"/unit-*.o \
        -Wl,--build-id=none -o "$compile_artifact"
    compile_finished="$(locally "$BIN" monotonic-ns)" || {
        compile_error "monotonic finish timestamp failed"
        return 1
    }
    case "$compile_finished" in
        ""|*[!0-9]*)
            compile_error "invalid monotonic finish timestamp '$compile_finished'"
            return 1
            ;;
    esac
    if [ "$compile_finished" -le "$compile_started" ]; then
        compile_error "non-positive monotonic interval"
        return 1
    fi
    compile_elapsed_ns=$((compile_finished - compile_started))
    compile_elapsed_seconds="$(LC_ALL=C awk -v ns="$compile_elapsed_ns" 'BEGIN { printf "%.9f", ns / 1000000000 }')"

    compile_unit=0
    while [ "$compile_unit" -lt "$COMPILE_TOTAL_UNITS" ]; do
        compile_tag="$(printf '%02d' "$compile_unit")"
        if [ ! -s "$compile_workspace/unit-$compile_tag.o" ]; then
            compile_error "missing object for unit $compile_tag"
            return 1
        fi
        compile_unit=$((compile_unit + 1))
    done

    compile_program_output="$(locally "$compile_artifact")" || {
        compile_error "linked artifact failed verification"
        return 1
    }
    if [ "$compile_program_output" != "wos-distributed-compile-ok units=$COMPILE_TOTAL_UNITS" ]; then
        compile_error "unexpected linked artifact output: $compile_program_output"
        return 1
    fi

    compile_digest_line="$(sha256sum -b "$compile_artifact")" || {
        compile_error "sha256sum failed"
        return 1
    }
    compile_digest="${compile_digest_line%% *}"
    case "$compile_digest" in
        ""|*[!0-9a-f]*)
            compile_error "invalid lowercase SHA-256 digest '$compile_digest'"
            return 1
            ;;
    esac
    if [ "${#compile_digest}" -ne 64 ]; then
        compile_error "invalid SHA-256 digest length"
        return 1
    fi

    : > "$compile_participants"
    while read -r compile_host compile_host_units compile_first_unit; do
        compile_expected_host="$(compile_normalize_hostname "$compile_host")"
        compile_actual_runner=
        compile_offset=0
        while [ "$compile_offset" -lt "$compile_host_units" ]; do
            compile_unit=$((compile_first_unit + compile_offset))
            compile_tag="$(printf '%02d' "$compile_unit")"
            compile_identity="$(cat "$compile_workspace/runner-$compile_tag.txt")" || {
                compile_error "missing wosid marker for unit $compile_tag"
                return 1
            }
            compile_spawner="$(compile_wosid_spawner "$compile_identity")"
            compile_runner="$(compile_wosid_runner "$compile_identity")"
            if ! compile_safe_hostname "$compile_spawner" || ! compile_safe_hostname "$compile_runner"; then
                compile_error "invalid wosid marker for unit $compile_tag: $compile_identity"
                return 1
            fi
            if [ "$(compile_normalize_hostname "$compile_spawner")" != "$COMPILE_LAUNCHER_NORMALIZED" ]; then
                compile_error "unit $compile_tag spawner $compile_spawner is not launcher $COMPILE_LAUNCHER_HOST"
                return 1
            fi
            if [ "$(compile_normalize_hostname "$compile_runner")" != "$compile_expected_host" ]; then
                compile_error "unit $compile_tag ran on $compile_runner instead of $compile_host"
                return 1
            fi
            if [ -z "$compile_actual_runner" ]; then
                compile_actual_runner="$compile_runner"
            elif [ "$compile_actual_runner" != "$compile_runner" ]; then
                compile_error "host $compile_host reported inconsistent runner identities"
                return 1
            fi
            compile_offset=$((compile_offset + 1))
        done

        if [ "$(compile_normalize_hostname "$compile_actual_runner")" = "$COMPILE_LAUNCHER_NORMALIZED" ]; then
            compile_transport=local
        else
            compile_transport=wki
        fi
        printf '%s %s %s %s\n' "$compile_host" "$compile_actual_runner" "$compile_transport" "$compile_host_units" \
            >> "$compile_participants"
    done < "$compile_host_plan"

    if [ "$COMPILE_HOST_COUNT" -eq 1 ]; then
        compile_placement=local-baseline
    else
        compile_placement=strict-on
    fi

    printf '{"benchmark":"wos_distributed_compile","units":%s,"total_workers":%s,' \
        "$COMPILE_TOTAL_UNITS" "$COMPILE_TOTAL_UNITS"
    printf '"artifact_digest":"%s","elapsed_seconds":%s,' "$compile_digest" "$compile_elapsed_seconds"
    printf '"placement":"%s","wki_route":"host-workspace","launcher_host":"%s",' \
        "$compile_placement" "$COMPILE_LAUNCHER_HOST"
    printf '"total_work_units":%s,"participants":[' "$COMPILE_TOTAL_UNITS"
    compile_first_participant=1
    while read -r compile_host compile_runner compile_transport compile_host_units; do
        if [ "$compile_first_participant" -eq 0 ]; then
            printf ','
        fi
        printf '{"host":"%s","runner_host":"%s","transport":"%s","work_units":%s}' \
            "$compile_host" "$compile_runner" "$compile_transport" "$compile_host_units"
        compile_first_participant=0
    done < "$compile_participants"
    printf ']}\n'
}

case "$COMPILE_ONLY" in
    0|1) ;;
    *)
        printf 'invalid WOS_LIVE_DISTRIBUTED_COMPILE_ONLY: %s\n' "$COMPILE_ONLY" >&2
        exit 1
        ;;
esac

if ! showcase_have "$CXX"; then
    printf 'skip: %s is not installed\n' "$CXX"
    exit 0
fi

if [ ! -f "$SRC" ]; then
    printf 'skip: demo source not found: %s\n' "$SRC"
    exit 0
fi

showcase_section "live compile c++ on WOS"
showcase_cmd "$CXX" -std=c++23 -O2 "$SRC" -o "$BIN"

if [ "$COMPILE_ONLY" -eq 0 ]; then
    showcase_section "one process: pipe plus vfs"
    showcase_cmd "$BIN" pipevfs "$LOCAL_FILE" "$BYTES" direct
    showcase_cmd locally "$BIN" pipevfs "$LOCAL_FILE" "$BYTES" locally
    showcase_cmd remotely forward +/tmp -- "$BIN" pipevfs "$REMOTE_FILE" "$BYTES" remotely

    showcase_section "shell pipe: producer into vfs sink"
    showcase_shell "\"$BIN\" emit \"$BYTES\" shell-local | \"$BIN\" sink \"$LOCAL_FILE.pipe\" shell-local"
    showcase_shell "remotely forward +/tmp -- sh -c '\"\$1\" emit \"\$2\" remote-pipe | \"\$1\" sink \"\$3\" remote-pipe' sh \"$BIN\" \"$BYTES\" \"$REMOTE_FILE.pipe\""
fi

hosts="${WOS_SHOWCASE_HOSTS:-}"
if [ -z "$hosts" ]; then
    printf 'skip: set WOS_SHOWCASE_HOSTS=host0,host1 or pass run-all --hosts host0,host1\n'
    exit 0
fi
compile_validate_hosts "$hosts"

if [ "$COMPILE_ONLY" -eq 0 ]; then
    showcase_section "run once on each requested WOS host"
    old_ifs="$IFS"
    IFS=,
    # shellcheck disable=SC2086  # Intentional CSV field splitting after validation.
    set -- $hosts
    IFS="$old_ifs"

    for host in "$@"; do
        showcase_cmd on "$host" forward +/tmp -- "$BIN" pipevfs "$REMOTE_FILE.$host" "$BYTES" "on:$host"
    done
fi

run_distributed_compile
