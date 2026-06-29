#!/bin/bash
# Configure the default WOS build tree once, then let Ninja/CMake dependency
# checks decide when regeneration is actually needed.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

BUILD_DIR="${WOS_BUILD_DIR:-build}"
GENERATOR="${WOS_CMAKE_GENERATOR:-Ninja}"
TARGET="${WOS_BUILD_TARGET:-wos_full}"
CMAKE_BIN="${WOS_CMAKE:-}"
if [ -z "$CMAKE_BIN" ] && [ -x "$WORKSPACE_ROOT/toolchain/host/bin/cmake" ]; then
    CMAKE_BIN="$WORKSPACE_ROOT/toolchain/host/bin/cmake"
fi
if [ -z "$CMAKE_BIN" ]; then
    CMAKE_BIN="cmake"
fi
EXTRA_CMAKE_ARGS=()
if [[ -n "${WOS_CMAKE_ARGS:-}" ]]; then
    # shellcheck disable=SC2206
    EXTRA_CMAKE_ARGS=($WOS_CMAKE_ARGS)
fi

source "$WORKSPACE_ROOT/tools/ccache_env.sh"

BUILD_JOBS="${WOS_BUILD_JOBS:-${WOS_NINJA_JOBS:-${CMAKE_BUILD_PARALLEL_LEVEL:-}}}"
if [ -z "$BUILD_JOBS" ]; then
    BUILD_JOBS="$(wos_build_jobs)"
fi
case "$BUILD_JOBS" in
    ''|*[!0-9]*|0)
        echo "ERROR: build jobs must be a positive integer, got '$BUILD_JOBS'" >&2
        exit 1
        ;;
esac
export WOS_BUILD_JOBS="$BUILD_JOBS"
export WOS_NINJA_JOBS="${WOS_NINJA_JOBS:-$BUILD_JOBS}"
export WOS_MAKE_JOBS="${WOS_MAKE_JOBS:-$BUILD_JOBS}"
export CMAKE_BUILD_PARALLEL_LEVEL="$BUILD_JOBS"

needs_configure=0
if [ ! -f "$BUILD_DIR/CMakeCache.txt" ]; then
    needs_configure=1
elif [ "$GENERATOR" = "Ninja" ] && [ ! -f "$BUILD_DIR/build.ninja" ]; then
    needs_configure=1
fi

if [ "$needs_configure" -eq 1 ] || [ "${#EXTRA_CMAKE_ARGS[@]}" -gt 0 ]; then
    "$CMAKE_BIN" -B "$BUILD_DIR" -G"$GENERATOR" "${EXTRA_CMAKE_ARGS[@]}" .
fi

"$CMAKE_BIN" --build "$BUILD_DIR" --target "$TARGET" --parallel "$BUILD_JOBS"
