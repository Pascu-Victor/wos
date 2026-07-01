#!/bin/bash
# Build and install the preconfigured WOS libc++ runtime tree.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$WORKSPACE_ROOT/tools/ccache_env.sh"

WOS_BUILD_JOBS="$(wos_build_jobs)"
WOS_NINJA_JOBS="$(wos_ninja_jobs)"

B="$WORKSPACE_ROOT/toolchain"
LIBCXX_BUILD="${WOS_LIBCXX_BUILD_DIR:-$B/libcxx-build}"

require_file() {
    local path="$1"
    local hint="$2"

    if [ ! -e "$path" ]; then
        echo "ERROR: missing $path" >&2
        echo "$hint" >&2
        exit 1
    fi
}

run_libcxx_ninja() {
    ninja -j"$WOS_NINJA_JOBS" -C "$LIBCXX_BUILD" "$@"
}

reset_libcxx_ninja_state() {
    echo "warning: cleaning stale libc++ Ninja outputs before retry" >&2
    run_libcxx_ninja -t clean
    rm -f "$LIBCXX_BUILD/.ninja_log" "$LIBCXX_BUILD/.ninja_deps"
}

require_file "$LIBCXX_BUILD/build.ninja" "Run tools/wos-toolchain.sh to configure libc++ first."

if ! wos_timed_step "build" "libcxx_runtime" run_libcxx_ninja; then
    reset_libcxx_ninja_state
    wos_timed_step "build" "libcxx_runtime_retry" run_libcxx_ninja
fi

wos_timed_step "install" "libcxx_runtime" run_libcxx_ninja install
