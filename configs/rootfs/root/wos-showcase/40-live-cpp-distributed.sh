#!/bin/sh
set -eu

DIR="$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)"
# shellcheck disable=SC1091
. "$DIR/showcase-common.sh"

CXX="${CXX:-clang++}"
COMPILE_CXX_REQUEST="$CXX"
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
COMPILE_WORKLOAD_ID=wos-live-cpp-32-tu-v1
COMPILE_EXPECTED_SOURCE_SHA256=aa52bc6a7f7f5b58904b6c1d06fb7f813c8567c97470fbe4161a4e691a60c726
COMPILE_FLAGS='-std=c++23 -O2 -fno-ident'
COMPILE_LINK_FLAGS='-std=c++23 -O2 -Wl,--build-id=none'
COMPILE_CACHE_POLICY=prewarmed-compiler-source-headers-all-hosts
COMPILE_LAUNCH_POLICY=one-controller-per-host-local-tu-workers
COMPILE_LOCAL_ROUTE_OPERANDS='-/root/wos-showcase -/usr -/bin -/lib -/lib64 -/libexec -/share -/tmp'
COMPILE_RUNTIME_PATHS_JSON='["/root/wos-showcase","/usr","/bin","/lib","/lib64","/libexec","/share","/tmp"]'
# shellcheck disable=SC2016  # The targeted shell expands the compiler probe.
COMPILE_COMPILER_PROBE='set -eu
workspace=$2
cd "$workspace"
wkictl_path=$(command -v wkictl) || exit 1
routes=$(locally "$wkictl_path" vfs list)
for runtime_path in /root/wos-showcase /usr /bin /lib /lib64 /libexec /share /tmp; do
    runtime_route=$(printf "%s\n" "$routes" | awk -v path="$runtime_path" '\''$1 ~ /^vfs-task\[[0-9]+\]:$/ && $2 == path && $3 == "->" { print $4 }'\'')
    [ "$runtime_route" = local ] || exit 1
done
workspace_route=$(printf "%s\n" "$routes" | awk -v path="$workspace" '\''$1 ~ /^vfs-task\[[0-9]+\]:$/ && $2 == path && $3 == "->" { print $4 }'\'')
[ "$workspace_route" = host ] || exit 1
compiler=$(command -v "$1") || exit 1
version_output=$("$compiler" --version) || exit 1
[ -n "$version_output" ] || exit 1
digest_line=$(printf "%s\n" "$version_output" | sha256sum -b) || exit 1
digest=${digest_line%% *}
compiler_digest_line=$(sha256sum -b "$compiler") || exit 1
compiler_digest=${compiler_digest_line%% *}
wkictl_digest_line=$(sha256sum -b "$wkictl_path") || exit 1
wkictl_digest=${wkictl_digest_line%% *}
printf "%s %s %s %s\n" "$compiler" "$digest" "$compiler_digest" "$wkictl_digest"'

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

compile_safe_path() {
    case "$1" in
        /*) ;;
        *) return 1 ;;
    esac
    case "$1" in
        *[!A-Za-z0-9_./+-]*) return 1 ;;
        *) return 0 ;;
    esac
}

compile_validate_sha256() {
    compile_sha256_value="$1"
    case "$compile_sha256_value" in
        ""|*[!0-9a-f]*) return 1 ;;
    esac
    [ "${#compile_sha256_value}" -eq 64 ]
}

compile_validate_compiler_identity() {
    compile_identity_record="$1"
    compile_identity_label="$2"
    compile_identity_path="${compile_identity_record%% *}"
    compile_identity_rest="${compile_identity_record#* }"
    compile_identity_digest="${compile_identity_rest%% *}"
    compile_identity_binary_rest="${compile_identity_rest#* }"
    compile_identity_binary_digest="${compile_identity_binary_rest%% *}"
    compile_identity_wkictl_digest="${compile_identity_binary_rest#* }"
    if [ "$compile_identity_record" != "$compile_identity_path $compile_identity_digest $compile_identity_binary_digest $compile_identity_wkictl_digest" ] || \
        ! compile_safe_path "$compile_identity_path" || \
        ! compile_validate_sha256 "$compile_identity_digest" || \
        ! compile_validate_sha256 "$compile_identity_binary_digest" || \
        ! compile_validate_sha256 "$compile_identity_wkictl_digest"; then
        compile_error "invalid compiler identity from $compile_identity_label: $compile_identity_record"
        return 1
    fi
}

compile_resolve_launcher_compiler() {
    compile_resolve_workspace="$1"
    # shellcheck disable=SC2086  # Fixed, whitespace-separated route operands.
    compile_launcher_compiler_identity="$(
        on "$COMPILE_LAUNCHER_TARGET" forward "+$compile_resolve_workspace" $COMPILE_LOCAL_ROUTE_OPERANDS -- \
            sh -c "$COMPILE_COMPILER_PROBE" sh "$COMPILE_CXX_REQUEST" "$compile_resolve_workspace"
    )" || {
        compile_error "cannot resolve or fingerprint compiler $COMPILE_CXX_REQUEST on the launcher"
        return 1
    }
    compile_validate_compiler_identity "$compile_launcher_compiler_identity" launcher || return 1
    COMPILE_COMPILER_PATH="${compile_launcher_compiler_identity%% *}"
    compile_launcher_identity_rest="${compile_launcher_compiler_identity#* }"
    COMPILE_COMPILER_VERSION_SHA256="${compile_launcher_identity_rest%% *}"
    compile_launcher_binary_rest="${compile_launcher_identity_rest#* }"
    COMPILE_COMPILER_SHA256="${compile_launcher_binary_rest%% *}"
    COMPILE_WKICTL_SHA256="${compile_launcher_binary_rest#* }"
    CXX="$COMPILE_COMPILER_PATH"
}

compile_local_profile() {
    # shellcheck disable=SC2086  # Fixed, whitespace-separated route operands.
    # shellcheck disable=SC2016  # The profiled shell expands its command arguments.
    locally forward "+$compile_workspace" $COMPILE_LOCAL_ROUTE_OPERANDS -- sh -c '
set -eu
workspace=$1
shift
cd "$workspace"
exec "$@"
' sh "$compile_workspace" "$@"
}

compile_file_sha256() {
    compile_digest_line="$(sha256sum -b "$1")" || return 1
    compile_file_digest="${compile_digest_line%% *}"
    compile_validate_sha256 "$compile_file_digest" || return 1
    printf '%s\n' "$compile_file_digest"
}

compile_create_private_root() {
    compile_private_attempt=0
    while [ "$compile_private_attempt" -lt 100 ]; do
        COMPILE_WORK_ROOT_OWNER="$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')" || {
            compile_error "cannot generate private workspace ownership token"
            return 1
        }
        case "$COMPILE_WORK_ROOT_OWNER" in
            *[!0-9a-f]*) compile_error "invalid private workspace ownership token"; return 1 ;;
        esac
        if [ "${#COMPILE_WORK_ROOT_OWNER}" -ne 32 ]; then
            compile_error "invalid private workspace ownership token"
            return 1
        fi
        compile_work_suffix="$(printf '%.16s' "$COMPILE_WORK_ROOT_OWNER")"
        COMPILE_WORK_ROOT="/tmp/wos-showcase-fixed-$compile_work_suffix"
        if (umask 077 && mkdir "$COMPILE_WORK_ROOT"); then
            if ! printf '%s' "$COMPILE_WORK_ROOT_OWNER" > "$COMPILE_WORK_ROOT/.wos-showcase-owner"; then
                rm -f -- "$COMPILE_WORK_ROOT/.wos-showcase-owner"
                rmdir "$COMPILE_WORK_ROOT" 2>/dev/null || true
                compile_error "cannot record private workspace ownership"
                return 1
            fi
            return 0
        fi
        compile_private_attempt=$((compile_private_attempt + 1))
    done
    compile_error "cannot allocate a private distributed compile workspace"
    return 1
}

compile_cleanup_private_root() {
    if [ -z "${COMPILE_WORK_ROOT:-}" ]; then
        return 0
    fi
    compile_cleanup_root="$COMPILE_WORK_ROOT"
    compile_cleanup_owner="$COMPILE_WORK_ROOT_OWNER"
    compile_cleanup_suffix="${compile_cleanup_root#/tmp/wos-showcase-fixed-}"
    case "$compile_cleanup_root" in
        /tmp/wos-showcase-fixed-*) ;;
        *) compile_error "refusing unsafe compile workspace cleanup: $compile_cleanup_root"; return 1 ;;
    esac
    if [ "${#compile_cleanup_suffix}" -ne 16 ] || \
        [ "${#compile_cleanup_owner}" -ne 32 ] || \
        [ "$(printf '%.16s' "$compile_cleanup_owner")" != "$compile_cleanup_suffix" ]; then
        compile_error "refusing malformed compile workspace cleanup: $compile_cleanup_root"
        return 1
    fi
    case "$compile_cleanup_suffix$compile_cleanup_owner" in
        *[!0-9a-f]*) compile_error "refusing non-hex compile workspace cleanup"; return 1 ;;
    esac
    if [ ! -d "$compile_cleanup_root" ] || [ -L "$compile_cleanup_root" ] || \
        [ ! -f "$compile_cleanup_root/.wos-showcase-owner" ] || \
        [ -L "$compile_cleanup_root/.wos-showcase-owner" ] || \
        [ "$(wc -c < "$compile_cleanup_root/.wos-showcase-owner")" -ne 32 ] || \
        [ "$(cat "$compile_cleanup_root/.wos-showcase-owner")" != "$compile_cleanup_owner" ]; then
        compile_error "refusing compile workspace with changed ownership: $compile_cleanup_root"
        return 1
    fi
    cd /
    rm -rf -- "$compile_cleanup_root"
    COMPILE_WORK_ROOT=
}

compile_exit_cleanup() {
    compile_cancel_jobs
    compile_cleanup_private_root
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
    COMPILE_LAUNCHER_TARGET=
    for compile_host in "$@"; do
        if [ "$(compile_normalize_hostname "$compile_host")" = "$COMPILE_LAUNCHER_NORMALIZED" ]; then
            compile_launcher_declared=$((compile_launcher_declared + 1))
            COMPILE_LAUNCHER_TARGET="$compile_host"
        fi
    done
    if [ "$compile_launcher_declared" -ne 1 ]; then
        compile_error "declared hosts do not contain launcher $COMPILE_LAUNCHER_HOST exactly once"
        return 1
    fi

    COMPILE_CANONICAL_HOSTS="$(
        for compile_host in "$@"; do
            printf '%s\t%s\n' "$(compile_normalize_hostname "$compile_host")" "$compile_host"
        done | LC_ALL=C sort -k1,1 | awk 'BEGIN { separator = "" } { printf "%s%s", separator, $2; separator = "," } END { print "" }'
    )"
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

    while read -r compile_pid compile_controller_tag _compile_host; do
        if [ ! -f "$compile_workspace/controller-$compile_controller_tag.status" ]; then
            kill "$compile_pid" 2>/dev/null || true
        fi
    done < "$compile_pids"
    while read -r compile_pid _compile_controller_tag _compile_host; do
        wait "$compile_pid" 2>/dev/null || true
    done < "$compile_pids"
}

compile_prewarm_hosts() {
    compile_prewarm_index=0
    while read -r compile_prewarm_host _compile_prewarm_units _compile_prewarm_first; do
        compile_prewarm_tag="$(printf '%02d' "$compile_prewarm_index")"
        compile_prewarm_object="$compile_workspace/prewarm-$compile_prewarm_tag.o"
        compile_prewarm_log="$compile_workspace/prewarm-$compile_prewarm_tag.log"
        compile_prewarm_symbol="wos_compile_prewarm_$compile_prewarm_tag"
        # shellcheck disable=SC2016  # The targeted shell expands the submitted script.
        # shellcheck disable=SC2086  # Fixed, whitespace-separated route operands.
        if ! on "$compile_prewarm_host" forward "+$compile_workspace" $COMPILE_LOCAL_ROUTE_OPERANDS -- sh -c '
set -eu
workspace=$1
cxx=$2
source_file=$3
object_file=$4
symbol=$5
cd "$workspace"
exec "$cxx" -std=c++23 -O2 -fno-ident -c "$source_file" "-Dmain=$symbol" -o "$object_file"
' sh "$compile_workspace" "$CXX" "$compile_source" "$compile_prewarm_object" "$compile_prewarm_symbol" \
            > "$compile_prewarm_log" 2>&1; then
            compile_error "compiler prewarm failed on $compile_prewarm_host"
            cat "$compile_prewarm_log" >&2 || true
            return 1
        fi
        if [ ! -s "$compile_prewarm_object" ]; then
            compile_error "compiler prewarm produced no object on $compile_prewarm_host"
            return 1
        fi
        compile_prewarm_index=$((compile_prewarm_index + 1))
    done < "$compile_host_plan"
    if [ "$compile_prewarm_index" -ne "$COMPILE_HOST_COUNT" ]; then
        compile_error "compiler prewarm did not cover every host"
        return 1
    fi
    rm -f "$compile_workspace"/prewarm-*.o "$compile_workspace"/prewarm-*.log
}

run_distributed_compile() {
    compile_workspace="$COMPILE_WORK_ROOT/distributed-compile"
    compile_source="$compile_workspace/source.cpp"
    compile_driver_source="$compile_workspace/driver.cpp"
    compile_driver_object="$compile_workspace/driver.o"
    compile_artifact="$compile_workspace/wos-distributed-compile"
    compile_host_plan="$compile_workspace/hosts.plan"
    compile_pids="$compile_workspace/pids"
    compile_participants="$compile_workspace/participants"

    mkdir "$compile_workspace"
    cd "$compile_workspace"
    cp "$SRC" "$compile_source"
    compile_generate_driver "$compile_driver_source"

    COMPILE_SOURCE_SHA256="$(compile_file_sha256 "$compile_source")" || {
        compile_error "cannot fingerprint source $SRC"
        return 1
    }
    if [ "$COMPILE_SOURCE_SHA256" != "$COMPILE_EXPECTED_SOURCE_SHA256" ]; then
        compile_error "source fingerprint differs from $COMPILE_WORKLOAD_ID"
        return 1
    fi

    compile_old_ifs="$IFS"
    IFS=,
    # shellcheck disable=SC2086  # Intentional CSV field splitting after validation.
    set -- $COMPILE_CANONICAL_HOSTS
    IFS="$compile_old_ifs"

    compile_resolve_launcher_compiler "$compile_workspace" || return 1

    for compile_host in "$@"; do
        # shellcheck disable=SC2086  # Fixed, whitespace-separated route operands.
        compile_host_compiler_identity="$(
            on "$compile_host" forward "+$compile_workspace" $COMPILE_LOCAL_ROUTE_OPERANDS -- \
                sh -c "$COMPILE_COMPILER_PROBE" sh "$COMPILE_CXX_REQUEST" "$compile_workspace"
        )" || {
            compile_error "cannot resolve or fingerprint compiler $COMPILE_CXX_REQUEST on $compile_host"
            return 1
        }
        compile_validate_compiler_identity "$compile_host_compiler_identity" "$compile_host" || return 1
        if [ "$compile_host_compiler_identity" != "$compile_launcher_compiler_identity" ]; then
            compile_error "compiler identity on $compile_host differs from launcher: $compile_host_compiler_identity"
            return 1
        fi
    done

    showcase_section "fixed-total distributed c++ compilation"
    showcase_cmd compile_local_profile "$CXX" -std=c++23 -O2 -fno-ident -c \
        "$compile_driver_source" -o "$compile_driver_object"

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

    compile_prewarm_hosts || return 1

    compile_started="$(compile_local_profile "$BIN" monotonic-ns)" || {
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
    compile_controller_index=0
    while read -r compile_host compile_host_units compile_first_unit; do
        compile_controller_tag="$(printf '%02d' "$compile_controller_index")"
        compile_controller_units="$compile_workspace/controller-$compile_controller_tag.units"
        compile_controller_log="$compile_workspace/controller-$compile_controller_tag.log"
        # One strict WKI launch owns this host's fixed range. The controller
        # creates the same compiler-process count locally, avoiding a complete
        # WKI submit/proxy/ELF lifecycle for every translation unit.
        # shellcheck disable=SC2016  # The targeted shell expands the submitted script.
        # shellcheck disable=SC2086  # Fixed, whitespace-separated route operands.
        on "$compile_host" forward "+$compile_workspace" $COMPILE_LOCAL_ROUTE_OPERANDS -- sh -c '
set -eu
workspace=$1
cxx=$2
source_file=$3
unit_count=$4
first_unit=$5
cd "$workspace"

cancel_children() {
    if [ "${children_active:-0}" -ne 1 ]; then
        return 0
    fi
    children_active=0
    # shellcheck disable=SC2086  # Intentional split of numeric pid:tag records.
    for child_record in $child_records; do
        child_pid=${child_record%%:*}
        child_tag=${child_record#*:}
        case "$completed_tags" in
            *" $child_tag "*) ;;
            *) kill "$child_pid" 2>/dev/null || true ;;
        esac
    done
    # shellcheck disable=SC2086  # Intentional split of numeric pid:tag records.
    for child_record in $child_records; do
        child_pid=${child_record%%:*}
        child_tag=${child_record#*:}
        case "$completed_tags" in
            *" $child_tag "*) ;;
            *) wait "$child_pid" 2>/dev/null || true ;;
        esac
    done
}

run_compile_unit() {
    marker=$1
    object_file=$2
    symbol=$3
    wosid > "$marker"
    exec "$cxx" -std=c++23 -O2 -fno-ident -c "$source_file" "-Dmain=$symbol" -o "$object_file"
}

child_records=
completed_tags=" "
children_active=1
trap "cancel_children" 0
trap "cancel_children; exit 129" HUP
trap "cancel_children; exit 130" INT
trap "cancel_children; exit 143" TERM

offset=0
while [ "$offset" -lt "$unit_count" ]; do
    unit=$((first_unit + offset))
    tag=$(printf "%02d" "$unit")
    marker="$workspace/runner-$tag.txt"
    object_file="$workspace/unit-$tag.o"
    compile_log="$workspace/compile-$tag.log"
    symbol="wos_compile_unit_$tag"
    run_compile_unit "$marker" "$object_file" "$symbol" > "$compile_log" 2>&1 &
    child_pid=$!
    child_records="$child_records $child_pid:$tag"
    offset=$((offset + 1))
done

failed=0
status_records=
# shellcheck disable=SC2086  # Intentional split of numeric pid:tag records.
for child_record in $child_records; do
    child_pid=${child_record%%:*}
    tag=${child_record#*:}
    if wait "$child_pid"; then
        child_rc=0
    else
        child_rc=$?
        failed=1
    fi
    completed_tags="$completed_tags$tag "
    status_records="$status_records $tag:$child_rc"
done
children_active=0

# Return the compact per-unit status table over the controller stdout pipe.
# The launcher captures it into a local file, so teardown diagnostics do not
# depend on the forwarded workspace remaining reachable.
# shellcheck disable=SC2086  # Intentional split of numeric tag:status records.
for status_record in $status_records; do
    status_tag=${status_record%%:*}
    status_rc=${status_record#*:}
    printf "%s %s\n" "$status_tag" "$status_rc"
done

if [ "$failed" -ne 0 ]; then
    exit 1
fi
' wos-compile-controller "$compile_workspace" "$CXX" "$compile_source" "$compile_host_units" "$compile_first_unit" \
            > "$compile_controller_units" 2> "$compile_controller_log" &
        compile_pid=$!
        printf '%s %s %s\n' "$compile_pid" "$compile_controller_tag" "$compile_host" >> "$compile_pids"
        compile_controller_index=$((compile_controller_index + 1))
    done < "$compile_host_plan"

    while read -r compile_pid compile_controller_tag compile_host; do
        if wait "$compile_pid"; then
            compile_controller_rc=0
        else
            compile_controller_rc=$?
        fi
        printf '%s\n' "$compile_controller_rc" > "$compile_workspace/controller-$compile_controller_tag.status"
    done < "$compile_pids"
    compile_jobs_active=0

    compile_failed=0
    compile_controller_index=0
    while read -r compile_host compile_host_units compile_first_unit; do
        compile_controller_tag="$(printf '%02d' "$compile_controller_index")"
        compile_controller_units="$compile_workspace/controller-$compile_controller_tag.units"
        compile_controller_status_file="$compile_workspace/controller-$compile_controller_tag.status"
        compile_controller_log="$compile_workspace/controller-$compile_controller_tag.log"
        compile_controller_status=
        compile_controller_status_invalid=0
        if ! read -r compile_controller_status < "$compile_controller_status_file"; then
            printf 'distributed compile: controller on %s produced no status\n' "$compile_host" >&2
            compile_controller_status_invalid=1
            compile_failed=1
        else
            case "$compile_controller_status" in
                ''|*[!0-9]*)
                    printf 'distributed compile: controller on %s produced invalid status %s\n' \
                        "$compile_host" "$compile_controller_status" >&2
                    compile_controller_status_invalid=1
                    compile_failed=1
                    ;;
            esac
        fi

        compile_unit_status_invalid=0
        compile_unit_failure_seen=0
        compile_status_count=0
        if [ ! -f "$compile_controller_units" ]; then
            printf 'distributed compile: controller on %s produced no unit statuses\n' "$compile_host" >&2
            compile_unit_status_invalid=1
            compile_failed=1
        else
            while read -r compile_status_tag compile_rc compile_status_extra; do
                if [ "$compile_status_count" -ge "$compile_host_units" ]; then
                    printf 'distributed compile: controller on %s produced extra unit status %s\n' \
                        "$compile_host" "$compile_status_tag" >&2
                    compile_unit_status_invalid=1
                    compile_failed=1
                    continue
                fi
                compile_unit=$((compile_first_unit + compile_status_count))
                compile_tag="$(printf '%02d' "$compile_unit")"
                if [ "$compile_status_tag" != "$compile_tag" ] || [ -n "$compile_status_extra" ]; then
                    printf 'distributed compile: controller on %s produced invalid unit status for %s\n' \
                        "$compile_host" "$compile_status_tag" >&2
                    compile_unit_status_invalid=1
                    compile_failed=1
                fi
                case "$compile_rc" in
                    0) ;;
                    ''|*[!0-9]*)
                        printf 'distributed compile: unit %s on %s produced invalid status %s\n' \
                            "$compile_tag" "$compile_host" "$compile_rc" >&2
                        compile_unit_status_invalid=1
                        compile_failed=1
                        ;;
                    *)
                        printf 'distributed compile: unit %s on %s failed with rc=%s\n' \
                            "$compile_tag" "$compile_host" "$compile_rc" >&2
                        cat "$compile_workspace/compile-$compile_tag.log" >&2 || true
                        compile_unit_failure_seen=1
                        compile_failed=1
                        ;;
                esac
                compile_status_count=$((compile_status_count + 1))
            done < "$compile_controller_units"
        fi
        if [ "$compile_status_count" -ne "$compile_host_units" ]; then
            printf 'distributed compile: controller on %s reported %s of %s unit statuses\n' \
                "$compile_host" "$compile_status_count" "$compile_host_units" >&2
            compile_unit_status_invalid=1
            compile_failed=1
        fi

        compile_report_controller=0
        if [ "$compile_controller_status_invalid" -ne 0 ] || [ "$compile_unit_status_invalid" -ne 0 ]; then
            compile_report_controller=1
        elif [ "$compile_controller_status" -eq 0 ]; then
            if [ "$compile_unit_failure_seen" -ne 0 ]; then
                printf 'distributed compile: controller on %s succeeded despite unit failures\n' "$compile_host" >&2
                compile_report_controller=1
                compile_failed=1
            fi
        elif [ "$compile_controller_status" -ne 1 ] || [ "$compile_unit_failure_seen" -eq 0 ]; then
            compile_report_controller=1
            compile_failed=1
        fi
        if [ "$compile_report_controller" -ne 0 ]; then
            printf 'distributed compile: controller on %s failed with rc=%s\n' \
                "$compile_host" "${compile_controller_status:-unknown}" >&2
            cat "$compile_controller_log" >&2 || true
        fi

        compile_controller_index=$((compile_controller_index + 1))
    done < "$compile_host_plan"

    if [ "$compile_controller_index" -ne "$COMPILE_HOST_COUNT" ]; then
        compile_error "controller status coverage does not match host count"
        compile_failed=1
    fi
    if [ "$compile_failed" -ne 0 ]; then
        return 1
    fi

    showcase_cmd compile_local_profile "$CXX" -std=c++23 -O2 -Wl,--build-id=none \
        "$compile_driver_object" "$compile_workspace"/unit-*.o -o "$compile_artifact"
    compile_finished="$(compile_local_profile "$BIN" monotonic-ns)" || {
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

    compile_program_output="$(compile_local_profile "$compile_artifact")" || {
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
    printf '"workload_id":"%s","source_sha256":"%s",' \
        "$COMPILE_WORKLOAD_ID" "$COMPILE_SOURCE_SHA256"
    printf '"compiler_path":"%s","compiler_version_sha256":"%s","compiler_sha256":"%s",' \
        "$COMPILE_COMPILER_PATH" "$COMPILE_COMPILER_VERSION_SHA256" "$COMPILE_COMPILER_SHA256"
    printf '"wkictl_sha256":"%s",' "$COMPILE_WKICTL_SHA256"
    printf '"compile_flags":"%s","link_flags":"%s",' "$COMPILE_FLAGS" "$COMPILE_LINK_FLAGS"
    printf '"cache_policy":"%s","launch_policy":"%s","controller_count":%s,' \
        "$COMPILE_CACHE_POLICY" "$COMPILE_LAUNCH_POLICY" "$COMPILE_HOST_COUNT"
    printf '"artifact_digest":"%s","elapsed_seconds":%s,' "$compile_digest" "$compile_elapsed_seconds"
    printf '"placement":"%s","wki_route":"host-workspace","launcher_host":"%s",' \
        "$compile_placement" "$COMPILE_LAUNCHER_HOST"
    printf '"runtime_route":"local","runtime_paths":%s,' "$COMPILE_RUNTIME_PATHS_JSON"
    printf '"workspace_route":"host","workspace_path":"%s",' "$compile_workspace"
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

compile_create_private_root
trap 'compile_exit_cleanup' 0
trap 'compile_exit_cleanup; exit 129' HUP
trap 'compile_exit_cleanup; exit 130' INT
trap 'compile_exit_cleanup; exit 143' TERM
cd "$COMPILE_WORK_ROOT"

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
hosts="$COMPILE_CANONICAL_HOSTS"

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
