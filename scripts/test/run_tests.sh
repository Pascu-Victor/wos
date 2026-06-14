#!/bin/bash
# ---------------------------------------------------------------------------
# WOS Host Unit Tests & Fuzz Targets — Build and Run
# ---------------------------------------------------------------------------
#
# The clang in our toolchain defaults to x86_64-pc-wos. For host Linux tests
# we must explicitly set --target and strip the WOS sysroot so that the
# compiler produces native Linux binaries.
#
# Usage:
#   wos-test                                # build & run all unit tests
#   wos-test build                          # build only (unit tests + fuzz targets)
#   wos-test test                           # build & run unit tests (ctest)
#   wos-test fuzz [target] [minutes]
#                                           # build & run a fuzz target
#                                           #   target:  wki_wire_fuzz (default)
#                                           #            data_struct_fuzz
#                                           #   minutes: duration (default: 5)
#   wos-test fuzz all [min]                 # run every fuzz target sequentially
#   wos-test clean                          # remove build directory
#   wos-test list                           # list available test & fuzz targets
#   wos-test report                         # show summary of last results
#
# Environment:
#   WOS_TEST_BUILD_DIR   — override build directory  (default: /tmp/wos-tests)
#   WOS_TEST_JOBS        — parallel build jobs       (default: nproc)
#   WOS_FUZZ_CORPUS_DIR  — persistent corpus storage (default: /tmp/wos-fuzz-corpus)
#
set -euo pipefail

# ---------------------------------------------------------------------------
# Resolve paths
# ---------------------------------------------------------------------------
WOS_ROOT="${WOS_WORKSPACE_ROOT:-$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)}"

BUILD_DIR="${WOS_TEST_BUILD_DIR:-/tmp/wos-tests}"
JOBS="${WOS_TEST_JOBS:-$(nproc)}"
CORPUS_DIR="${WOS_FUZZ_CORPUS_DIR:-/tmp/wos-fuzz-corpus}"
RESULTS_DIR="$WOS_ROOT/test-results"
TEST_SRC="$WOS_ROOT/tests"

# ---------------------------------------------------------------------------
# Toolchain: find a host-Linux clang
# ---------------------------------------------------------------------------
# The WOS toolchain clang has default target x86_64-pc-wos and a built-in
# sysroot pointing at mlibc, which conflicts with Linux libc/libstdc++
# headers. For host tests we need a clang that targets Linux natively.
#
# Priority:
#   1. System clang (/usr/bin/clang++) — always targets Linux, no sysroot issues
#   2. WOS toolchain clang with explicit --target and --sysroot=/ override
# ---------------------------------------------------------------------------
find_host_compiler() {
    # Check for a system clang that natively targets Linux.
    for candidate in /usr/bin/clang clang; do
        local path
        path="$(command -v "$candidate" 2>/dev/null)" || continue
        # Skip if this resolves to the WOS toolchain clang.
        case "$path" in
            "$WOS_ROOT"/toolchain/*) continue ;;
        esac
        if [ -x "$path" ]; then
            echo "$path"
            return 0
        fi
    done
    return 1
}

HOST_CLANG="$(find_host_compiler)"
if [ -n "$HOST_CLANG" ]; then
    HOST_CLANGXX="${HOST_CLANG}++"
    HOST_CMAKE_EXTRA_FLAGS=""
else
    # Fall back to toolchain clang with explicit Linux target & sysroot.
    HOST_CLANG="$WOS_ROOT/toolchain/host/bin/clang"
    HOST_CLANGXX="$WOS_ROOT/toolchain/host/bin/clang++"
    HOST_CMAKE_EXTRA_FLAGS="--target=x86_64-pc-linux-gnu --sysroot=/"
fi

if [ ! -x "$HOST_CLANG" ]; then
    echo "error: no suitable host clang found"
    echo "       install clang or run tools/bootstrap.sh"
    exit 1
fi

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
info()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
ok()    { printf '\033[1;32m==>\033[0m %s\n' "$*"; }
warn()  { printf '\033[1;33m==>\033[0m %s\n' "$*"; }
err()   { printf '\033[1;31m==>\033[0m %s\n' "$*" >&2; }

timestamp() { date '+%Y-%m-%d_%H%M%S'; }

FUZZ_TARGETS=(wki_wire_fuzz data_struct_fuzz tcp_fuzz xfs_format_fuzz wki_routing_fuzz slab_fuzz)
UNIT_TESTS=(
    host_test_manifest_test
    ktest_cov_test
    ktest_manifest_test
    runtime_test_audit_test
    testd_manifest_test
    testprog_source_test
    httpd_source_test
    top_source_test
    time_source_test
    init_source_test
    strace_source_test
    journal_source_test
    wkictl_source_test
    perf_source_test
    memacc_source_test
    renderbench_source_test
    debugserver_source_test
    sftp_timeout_source_test
    netpoll_backlog_source_test
    virtio_net_source_test
    tcp_deadline_source_test
    poll_deadline_source_test
    futex_source_test
    waitpid_source_test
    scheduler_source_test
    phys_source_test
    wki_timer_source_test
    wki_wait_source_test
    wki_ivshmem_source_test
    wki_dev_server_source_test
    wki_dev_proxy_source_test
    wki_remote_ipc_source_test
    wki_remote_net_source_test
    wki_roce_source_test
    wki_remotable_source_test
    wki_zone_source_test
    wki_peer_source_test
    wki_remote_compute_source_test
    wki_remote_vfs_source_test
    vfs_mount_lifetime_source_test
    wki_wire_test
    wki_routing_test
    data_struct_test
    hashtable_limit_test
    tcp_test
    xfs_format_test
    wki_channel_test
    wki_peer_liveness_test
    wki_event_test
    renderbench_options_test
    bitmap_test
    net_checksum_test
    util_list_test
    crc32c_test
    buffer_cache_test
    vmem_source_test
)

list_targets() {
    echo "Unit tests:"
    for t in "${UNIT_TESTS[@]}"; do echo "  $t"; done
    echo ""
    echo "Fuzz targets:"
    for t in "${FUZZ_TARGETS[@]}"; do echo "  $t"; done
}

# ---------------------------------------------------------------------------
# Configure & Build
# ---------------------------------------------------------------------------
do_configure() {
    info "Configuring tests in $BUILD_DIR (compiler: $HOST_CLANG)"

    # Clear WOS-targeted environment variables so CMake does not inherit
    # the WOS sysroot / target from wos-env.sh.
    CC="$HOST_CLANG"       \
    CXX="$HOST_CLANGXX"    \
    CFLAGS=""               \
    CXXFLAGS=""             \
    LDFLAGS=""              \
    cmake -B "$BUILD_DIR" -S "$TEST_SRC" \
        -DCMAKE_C_COMPILER="$HOST_CLANG" \
        -DCMAKE_CXX_COMPILER="$HOST_CLANGXX" \
        -DCMAKE_C_FLAGS="$HOST_CMAKE_EXTRA_FLAGS" \
        -DCMAKE_CXX_FLAGS="$HOST_CMAKE_EXTRA_FLAGS" \
        -DCMAKE_EXE_LINKER_FLAGS="$HOST_CMAKE_EXTRA_FLAGS" \
        -DCMAKE_EXPORT_COMPILE_COMMANDS=ON

    # Keep stable repo-local entry points for clangd regardless of where the
    # actual host-test build directory lives.
    mkdir -p "$WOS_ROOT/build_tests"
    ln -sfT "$BUILD_DIR/compile_commands.json" \
           "$WOS_ROOT/build_tests/compile_commands.json" 2>/dev/null || true
    ln -sfT "$BUILD_DIR/compile_commands.json" \
           "$WOS_ROOT/compile_commands_tests.json" 2>/dev/null || true
}

do_build() {
    if [ ! -f "$BUILD_DIR/build.ninja" ] && [ ! -f "$BUILD_DIR/Makefile" ]; then
        do_configure
    fi

    info "Building tests (jobs=$JOBS)"
    cmake --build "$BUILD_DIR" -j "$JOBS"
}

# ---------------------------------------------------------------------------
# Run unit tests
# ---------------------------------------------------------------------------
do_test() {
    do_build

    mkdir -p "$RESULTS_DIR"
    local junit_file="$RESULTS_DIR/unit-tests.xml"
    local log_file="$RESULTS_DIR/unit-tests.log"

    info "Running unit tests"
    info "Results: $RESULTS_DIR/"

    local rc=0
    ctest --test-dir "$BUILD_DIR" --output-on-failure -j "$JOBS" \
        --output-junit "$junit_file" 2>&1 | tee "$log_file" || rc=$?

    if [ $rc -eq 0 ]; then
        ok "All unit tests passed"
        ok "JUnit XML: $junit_file"
    else
        err "Unit tests FAILED (exit code $rc)"
        err "Full log:   $log_file"
        err "JUnit XML:  $junit_file"
        # Print the failures summary from the log
        echo ""
        grep -E 'FAILED|TIMEOUT|Error' "$log_file" 2>/dev/null || true
        exit $rc
    fi
}

# ---------------------------------------------------------------------------
# Run fuzz targets
# ---------------------------------------------------------------------------
do_fuzz() {
    local target="${1:-wki_wire_fuzz}"
    local minutes="${2:-5}"

    do_build

    if [ "$target" = "all" ]; then
        for ft in "${FUZZ_TARGETS[@]}"; do
            do_fuzz_single "$ft" "$minutes"
        done
        return
    fi

    do_fuzz_single "$target" "$minutes"
}

do_fuzz_single() {
    local target="$1"
    local minutes="$2"

    local binary="$BUILD_DIR/host/$target"
    if [ ! -x "$binary" ]; then
        binary="$BUILD_DIR/$target"
    fi
    if [ ! -x "$binary" ]; then
        err "Fuzz target not found: $target"
        err "Available: ${FUZZ_TARGETS[*]}"
        exit 1
    fi

    # Support fractional minutes (e.g. 0.5 for 30s).
    local seconds
    seconds=$(awk "BEGIN { printf \"%d\", $minutes * 60 }")

    local corpus="$CORPUS_DIR/$target"
    local artifacts="$RESULTS_DIR/fuzz/$target"
    local log_file="$RESULTS_DIR/fuzz/${target}.log"
    mkdir -p "$corpus" "$artifacts"

    info "Fuzzing $target for ${minutes}m (${seconds}s)"
    info "  corpus:    $corpus"
    info "  artifacts: $artifacts"
    info "  log:       $log_file"
    echo ""

    # LibFuzzer writes crash-*, leak-*, timeout-* files when it finds bugs.
    # We point -artifact_prefix= at our results dir so they're collected.
    #
    # With -jobs=N, libfuzzer spawns N workers and redirects each worker's
    # stderr to fuzz-<N>.log in cwd. We run from the results dir so those
    # per-worker logs are collected alongside the artifacts.
    local rc=0
    (cd "$artifacts" && "$binary" "$corpus" \
        -max_total_time="$seconds" \
        -max_len=65536 \
        -print_final_stats=1 \
        -artifact_prefix="$artifacts/" \
        -jobs="$JOBS" \
        -workers="$JOBS" \
    ) 2>&1 | tee "$log_file" || rc=$?

    # Merge per-worker logs into the main log for stats extraction, then remove them.
    for wlog in "$artifacts"/fuzz-*.log; do
        [ -f "$wlog" ] || continue
        cat "$wlog" >> "$log_file"
        rm -f "$wlog"
    done

    echo ""
    fuzz_report "$target" "$artifacts" "$log_file" "$rc"
}

# ---------------------------------------------------------------------------
# Fuzz results analysis
# ---------------------------------------------------------------------------
fuzz_report() {
    local target="$1"
    local artifacts="$2"
    local log_file="$3"
    local exit_code="$4"

    # Count crash/leak/timeout artifacts
    local crashes=0 leaks=0 timeouts=0 ooms=0
    if [ -d "$artifacts" ]; then
        crashes=$(find "$artifacts" -name 'crash-*' 2>/dev/null | wc -l)
        leaks=$(find "$artifacts" -name 'leak-*' 2>/dev/null | wc -l)
        timeouts=$(find "$artifacts" -name 'timeout-*' 2>/dev/null | wc -l)
        ooms=$(find "$artifacts" -name 'oom-*' 2>/dev/null | wc -l)
    fi

    local total=$((crashes + leaks + timeouts + ooms))

    # Extract stats from libfuzzer output
    local total_runs
    total_runs=$(grep -oP 'stat::number_of_executed_units:\s*\K[0-9]+' "$log_file" 2>/dev/null | tail -1) || true
    local peak_rss
    peak_rss=$(grep -oP 'stat::peak_rss_mb:\s*\K[0-9]+' "$log_file" 2>/dev/null | tail -1) || true
    local corpus_size
    corpus_size=$(grep -oP 'stat::corpus_num_features:\s*\K[0-9]+' "$log_file" 2>/dev/null | tail -1) || true

    echo "────────────────────────────────────────────────"
    echo "  FUZZ REPORT: $target"
    echo "────────────────────────────────────────────────"
    [ -n "$total_runs" ]  && echo "  Executions:  $total_runs"
    [ -n "$peak_rss" ]    && echo "  Peak RSS:    ${peak_rss} MB"
    [ -n "$corpus_size" ] && echo "  Features:    $corpus_size"
    echo ""

    if [ "$total" -eq 0 ] && [ "$exit_code" -eq 0 ]; then
        ok "No bugs found — clean run"
    else
        if [ "$crashes" -gt 0 ]; then
            err "CRASHES: $crashes"
            find "$artifacts" -name 'crash-*' -printf '    %f\n' 2>/dev/null
        fi
        if [ "$leaks" -gt 0 ]; then
            err "MEMORY LEAKS: $leaks"
            find "$artifacts" -name 'leak-*' -printf '    %f\n' 2>/dev/null
        fi
        if [ "$timeouts" -gt 0 ]; then
            warn "TIMEOUTS: $timeouts"
            find "$artifacts" -name 'timeout-*' -printf '    %f\n' 2>/dev/null
        fi
        if [ "$ooms" -gt 0 ]; then
            warn "OOM: $ooms"
            find "$artifacts" -name 'oom-*' -printf '    %f\n' 2>/dev/null
        fi
        echo ""
        echo "  Reproduce a crash:"
        echo "    $binary <artifact-file>"
        echo ""
        echo "  Artifacts saved in:"
        echo "    $artifacts/"
    fi
    echo "────────────────────────────────────────────────"
}

# ---------------------------------------------------------------------------
# Report — summarise last run's results
# ---------------------------------------------------------------------------
do_report() {
    if [ ! -d "$RESULTS_DIR" ]; then
        err "No results found in $RESULTS_DIR"
        exit 1
    fi

    echo ""
    echo "╔══════════════════════════════════════════════╗"
    echo "║           WOS TEST RESULTS SUMMARY           ║"
    echo "╚══════════════════════════════════════════════╝"
    echo ""

    # Unit tests
    if [ -f "$RESULTS_DIR/unit-tests.xml" ]; then
        info "Unit Tests (JUnit XML: $RESULTS_DIR/unit-tests.xml)"
        local tests failures errors
        tests=$(grep -oP 'tests="\K[0-9]+' "$RESULTS_DIR/unit-tests.xml" | head -1) || true
        failures=$(grep -oP 'failures="\K[0-9]+' "$RESULTS_DIR/unit-tests.xml" | head -1) || true
        errors=$(grep -oP 'errors="\K[0-9]+' "$RESULTS_DIR/unit-tests.xml" | head -1) || true
        if [ "${failures:-0}" -eq 0 ] && [ "${errors:-0}" -eq 0 ]; then
            ok "  ${tests:-?} tests passed"
        else
            err "  ${tests:-?} tests, ${failures:-0} failures, ${errors:-0} errors"
            # Show failed test names
            grep -oP 'name="\K[^"]+' "$RESULTS_DIR/unit-tests.xml" | while read -r name; do
                echo "    FAIL: $name"
            done 2>/dev/null || true
        fi
        echo ""
    fi

    # Fuzz results
    if [ -d "$RESULTS_DIR/fuzz" ]; then
        for target_dir in "$RESULTS_DIR"/fuzz/*/; do
            [ -d "$target_dir" ] || continue
            local target
            target=$(basename "$target_dir")
            local log="$RESULTS_DIR/fuzz/${target}.log"
            [ -f "$log" ] || continue
            fuzz_report "$target" "$target_dir" "$log" 0
            echo ""
        done
    fi
}

# ---------------------------------------------------------------------------
# Clean
# ---------------------------------------------------------------------------
do_clean() {
    info "Removing $BUILD_DIR"
    rm -rf "$BUILD_DIR"
    rm -f "$WOS_ROOT/build_tests/compile_commands.json"
    rmdir "$WOS_ROOT/build_tests" 2>/dev/null || true
    ok "Clean"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
cmd="${1:-test}"
shift || true

case "$cmd" in
    build)      do_build ;;
    configure)  do_configure ;;
    test)       do_test ;;
    fuzz)       do_fuzz "$@" ;;
    report)     do_report ;;
    clean)      do_clean ;;
    list)       list_targets ;;
    -h|--help|help)
        sed -n '2,/^set /{ /^#/s/^# \?//p }' "$0"
        ;;
    *)
        err "Unknown command: $cmd"
        err "Try: $0 help"
        exit 1
        ;;
esac
