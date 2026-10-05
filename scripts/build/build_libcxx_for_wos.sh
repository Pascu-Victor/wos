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

# mlibc supplies pthread/dl/rt APIs in libc; its compatibility DSOs contain only
# dummy symbols. Keep them installed for clients, but avoid loading them for
# every C++ process when the runtime does not reference any of their symbols.
# Apply this to existing build trees as well as newly bootstrapped toolchains.
require_file "$LIBCXX_BUILD/CMakeCache.txt" "Run tools/wos-toolchain.sh to configure libc++ first."
runtime_link_flags="$(sed -n 's/^CMAKE_SHARED_LINKER_FLAGS:[^=]*=//p' "$LIBCXX_BUILD/CMakeCache.txt")"
case " $runtime_link_flags " in
    *" -Wl,--as-needed "*) ;;
    *)
        runtime_cmake="${WOS_HOST_TOOLCHAIN_ROOT:-$B/host}/bin/cmake"
        require_file "$runtime_cmake" "Run tools/host-toolchain.sh first."
        wos_timed_step "configure" "libcxx_runtime_dependencies" \
            "$runtime_cmake" -S "$B/src/llvm-project/runtimes" -B "$LIBCXX_BUILD" \
            "-DCMAKE_SHARED_LINKER_FLAGS=$runtime_link_flags -Wl,--as-needed"
        ;;
esac

if ! wos_timed_step "build" "libcxx_runtime" run_libcxx_ninja; then
    reset_libcxx_ninja_state
    wos_timed_step "build" "libcxx_runtime_retry" run_libcxx_ninja
fi

wos_timed_step "install" "libcxx_runtime" run_libcxx_ninja install
