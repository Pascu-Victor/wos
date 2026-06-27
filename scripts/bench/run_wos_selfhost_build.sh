#!/bin/bash
# Run the WOS self-hosting clone/configure/build benchmark locally or in WOS.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
WOS_SSH="$WORKSPACE_ROOT/scripts/remote/wos_ssh.sh"

DEFAULT_REPO="https://github.com/Pascu-Victor/wos.git"
DEFAULT_LINUX_WORKDIR="/tmp/wos-selfhost-bench"
DEFAULT_WOS_WORKDIR="/root/wos-selfhost-bench"
DEFAULT_BUILD_DIR="build-selfhost"
DEFAULT_TARGET="wos_full"
DEFAULT_HOST="wos-0"

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
  --jobs N                Parallel build jobs passed to cmake --build
  --skip-bootstrap        Skip ./tools/bootstrap.sh, useful only for iteration
  --keep-workdir          Refuse to replace an existing checkout in workdir
  --history-file PATH     Append detailed timing rows here
                         (default: <workdir>-history.tsv)
  --full-history          Clone full Git history instead of the default shallow
                         source checkout used for build timing
  --mirror-file PATH      Rewrite https://github.com/ to file://PATH/
  --mirror-http-prefix U  Rewrite https://github.com/ to an HTTP mirror prefix
  -h, --help              Show this help

Direct GitHub cloning is the default and is the acceptance-path check. Mirror
options are for controlled WOS-vs-Linux timing comparisons after using
scripts/dev/git_mirror_for_wos.sh. The default clone is shallow because the
benchmark validates source availability and buildability, not Git history
traversal throughput.

For rootless WOS runtime setup, launch first with:
  bin/wos-cluster --launch --no-setup
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
jobs="${WOS_SELFHOST_JOBS:-}"
skip_bootstrap="${WOS_SELFHOST_SKIP_BOOTSTRAP:-0}"
keep_workdir="${WOS_SELFHOST_KEEP_WORKDIR:-0}"
full_history="${WOS_SELFHOST_FULL_HISTORY:-0}"
mirror_file="${WOS_SELFHOST_MIRROR_FILE:-}"
mirror_http_prefix="${WOS_SELFHOST_MIRROR_HTTP_PREFIX:-}"
clean_path="${WOS_SELFHOST_CLEAN_PATH:-}"
checkout="$workdir/wos"
report="$workdir/selfhost-report.tsv"
detail_report="$workdir/selfhost-detail.tsv"
history_file="${WOS_SELFHOST_HISTORY_FILE:-}"
total_elapsed=0
commit="unknown"

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

    if [ -n "$clean_path" ]; then
        export PATH="$clean_path"
    fi
}

sanitize_selfhost_environment

run_id="$(
    python3 - <<'PY'
from datetime import datetime, timezone
import os

stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
print(f"{stamp}-{os.getpid()}")
PY
)"

now_ms() {
    python3 - <<'PY'
import time
print(time.monotonic_ns() // 1_000_000)
PY
}

log() {
    printf '[selfhost] %s\n' "$*"
}

timestamp_utc() {
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

    log "start $phase $label"
    local start end elapsed status
    start="$(now_ms)"
    set +e
    "$@"
    status=$?
    set -e
    end="$(now_ms)"
    elapsed=$((end - start))

    if [ "$status" -eq 0 ]; then
        record_timing "$phase" "$label" "$elapsed" "ok"
        log "done $phase $label ${elapsed}ms"
    else
        record_timing "$phase" "$label" "$elapsed" "fail:$status"
        log "failed $phase $label ${elapsed}ms status=$status"
    fi
    return "$status"
}

require_tool() {
    command -v "$1" >/dev/null 2>&1 || {
        echo "missing required tool: $1" >&2
        exit 1
    }
}

require_wos_selfhost_tools() {
    local tool
    for tool in \
        sh env make tar sed grep mktemp sha256sum xz yes \
        ld.lld lld llvm-ar llvm-ranlib llvm-nm llvm-objcopy llvm-strip \
        llvm-readelf llvm-objdump llvm-tblgen clang-tblgen; do
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

time_step() {
    local name="$1"
    shift

    log "start $name"
    local start end elapsed status
    start="$(now_ms)"
    set +e
    "$@"
    status=$?
    set -e
    end="$(now_ms)"
    elapsed=$((end - start))
    total_elapsed=$((total_elapsed + elapsed))
    printf '%s\t%s\n' "$name" "$elapsed" >> "$report"
    if [ "$status" -eq 0 ]; then
        record_timing "step" "$name" "$elapsed" "ok"
        log "done $name ${elapsed}ms"
    else
        record_timing "step" "$name" "$elapsed" "fail:$status"
        log "failed $name ${elapsed}ms status=$status"
    fi
    return "$status"
}

safe_prepare_workdir() {
    case "$workdir" in
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

    if [ "$keep_workdir" = "1" ] && [ -e "$checkout" ]; then
        echo "checkout already exists and --keep-workdir was requested: $checkout" >&2
        exit 1
    fi

    if [ "$keep_workdir" != "1" ]; then
        rm -rf -- "$workdir"
    fi

    mkdir -p "$workdir/home" "$workdir/tmp"
    export HOME="$workdir/home"
    export TMPDIR="$workdir/tmp"
    if [ -z "$history_file" ]; then
        history_file="${workdir%/}-history.tsv"
    fi
    ensure_parent_dir "$history_file"
    : > "$report"
    : > "$detail_report"
    write_timing_header "$detail_report"
    write_timing_header "$history_file"
}

configure_git_mirror() {
    if [ -n "$mirror_file" ]; then
        git config --global protocol.file.allow always
        git config --global "url.file://$mirror_file/".insteadOf https://github.com/
    fi

    if [ -n "$mirror_http_prefix" ]; then
        case "$mirror_http_prefix" in
            */) ;;
            *) mirror_http_prefix="$mirror_http_prefix/" ;;
        esac
        git config --global "url.$mirror_http_prefix".insteadOf https://github.com/
    fi
}

clone_sources() {
    configure_git_mirror

    local clone_cmd=(git clone)
    if [ "$full_history" != "1" ]; then
        clone_cmd+=(--depth 1)
    fi
    clone_cmd+=("$repo" "$checkout")
    run_timed_event "clone" "wos_repo" "${clone_cmd[@]}"
    commit="$(git -C "$checkout" rev-parse HEAD)"
    record_timing "metadata" "checkout_commit" "0" "ok"

    run_timed_event "clone" "submodule_init" git -C "$checkout" submodule init

    local submodule_list="$workdir/submodule-paths.tsv"
    local submodule_paths=()
    local key path
    git -C "$checkout" config --file .gitmodules --get-regexp '^submodule\..*\.path$' > "$submodule_list"
    while read -r key path; do
        if [ -n "$path" ]; then
            submodule_paths+=("$path")
        fi
    done < "$submodule_list"

    for path in "${submodule_paths[@]}"; do
        local submodule_cmd=(git -C "$checkout" submodule update --init --recursive)
        if [ "$full_history" != "1" ]; then
            submodule_cmd+=(--depth 1)
        fi
        if [ -n "$jobs" ]; then
            submodule_cmd+=(--jobs "$jobs")
        fi
        submodule_cmd+=(-- "$path")
        run_timed_event "clone_submodule" "$path" "${submodule_cmd[@]}"
    done

    write_submodule_status() {
        git -C "$checkout" submodule status --recursive > "$workdir/submodules.txt"
    }
    run_timed_event "clone" "submodule_status" write_submodule_status
}

bootstrap_toolchain() {
    if [ "$skip_bootstrap" = "1" ]; then
        log "skip bootstrap_toolchain"
        return 0
    fi

    (
        cd "$checkout"
        ./tools/bootstrap.sh
    )
}

configure_wos() {
    cmake -GNinja \
        -S "$checkout" \
        -B "$checkout/$build_dir" \
        -DWOS_BUILD_WOSDBG=OFF \
        -DWOS_BUILD_CMAKE_FOR_HOST=OFF
}

build_wos() {
    local cmd=(cmake --build "$checkout/$build_dir" --target "$target")
    if [ -n "$jobs" ]; then
        cmd+=(--parallel "$jobs")
    fi
    "${cmd[@]}"
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
    require_file "disk.qcow2"
    require_file "mountfs.qcow2"
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
log "mode=$mode"
log "repo=$repo"
log "workdir=$workdir"
log "detail_report=$detail_report"
log "history_file=$history_file"
log "target=$target"
time_step clone_sources clone_sources
time_step bootstrap_toolchain bootstrap_toolchain
time_step configure_wos configure_wos
time_step build_wos build_wos
verify_artifacts
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
        WOS_SELFHOST_HISTORY_FILE="$history_file" \
        WOS_SELFHOST_FULL_HISTORY="$full_history" \
        WOS_SELFHOST_MIRROR_FILE="$mirror_file" \
        WOS_SELFHOST_MIRROR_HTTP_PREFIX="$mirror_http_prefix" \
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
        WOS_SELFHOST_HISTORY_FILE="$history_file" \
        WOS_SELFHOST_FULL_HISTORY="$full_history" \
        WOS_SELFHOST_MIRROR_FILE="$mirror_file" \
        WOS_SELFHOST_MIRROR_HTTP_PREFIX="$mirror_http_prefix" \
        WOS_SELFHOST_CLEAN_PATH="/usr/bin:/bin:/usr/sbin:/sbin" \
        bash -s
}

run_wos() {
    [ -x "$WOS_SSH" ] || die "missing WOS SSH helper: $WOS_SSH"

    local remote_command
    remote_command="env"
    remote_command+=" WOS_SELFHOST_MODE=$(shell_quote wos)"
    remote_command+=" WOS_SELFHOST_REPO=$(shell_quote "$repo")"
    remote_command+=" WOS_SELFHOST_WORKDIR=$(shell_quote "$workdir")"
    remote_command+=" WOS_SELFHOST_BUILD_DIR=$(shell_quote "$build_dir")"
    remote_command+=" WOS_SELFHOST_TARGET=$(shell_quote "$target")"
    remote_command+=" WOS_SELFHOST_JOBS=$(shell_quote "$jobs")"
    remote_command+=" WOS_SELFHOST_SKIP_BOOTSTRAP=$(shell_quote "$skip_bootstrap")"
    remote_command+=" WOS_SELFHOST_KEEP_WORKDIR=$(shell_quote "$keep_workdir")"
    remote_command+=" WOS_SELFHOST_HISTORY_FILE=$(shell_quote "$history_file")"
    remote_command+=" WOS_SELFHOST_FULL_HISTORY=$(shell_quote "$full_history")"
    remote_command+=" WOS_SELFHOST_MIRROR_FILE=$(shell_quote "$mirror_file")"
    remote_command+=" WOS_SELFHOST_MIRROR_HTTP_PREFIX=$(shell_quote "$mirror_http_prefix")"
    remote_command+=" WOS_SELFHOST_CLEAN_PATH=/usr/bin:/bin:/usr/sbin:/sbin"
    remote_command+=" bash -s"

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
jobs=""
skip_bootstrap=0
keep_workdir=0
history_file=""
full_history=0
mirror_file=""
mirror_http_prefix=""

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
        --history-file)
            history_file="${2:-}"
            [ -n "$history_file" ] || die "--history-file requires a value"
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
        --mirror-http-prefix)
            mirror_http_prefix="${2:-}"
            [ -n "$mirror_http_prefix" ] || die "--mirror-http-prefix requires a value"
            shift
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

reject_whitespace "--host" "$host"
reject_whitespace "--repo" "$repo"
reject_whitespace "--workdir" "$workdir"
reject_whitespace "--build-dir" "$build_dir"
reject_whitespace "--target" "$target"
reject_whitespace "--history-file" "$history_file"
reject_whitespace "--mirror-file" "$mirror_file"
reject_whitespace "--mirror-http-prefix" "$mirror_http_prefix"

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
