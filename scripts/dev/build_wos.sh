#!/bin/bash
# Configure the default WOS build tree once, then let Ninja/CMake dependency
# checks decide when regeneration is actually needed.
set -euo pipefail

BUILD_DIR="${WOS_BUILD_DIR:-build}"
GENERATOR="${WOS_CMAKE_GENERATOR:-Ninja}"
TARGET="${WOS_BUILD_TARGET:-wos_full}"
EXTRA_CMAKE_ARGS=()
if [[ -n "${WOS_CMAKE_ARGS:-}" ]]; then
    # shellcheck disable=SC2206
    EXTRA_CMAKE_ARGS=($WOS_CMAKE_ARGS)
fi

needs_configure=0
if [ ! -f "$BUILD_DIR/CMakeCache.txt" ]; then
    needs_configure=1
elif [ "$GENERATOR" = "Ninja" ] && [ ! -f "$BUILD_DIR/build.ninja" ]; then
    needs_configure=1
fi

if [ "$needs_configure" -eq 1 ] || [ "${#EXTRA_CMAKE_ARGS[@]}" -gt 0 ]; then
    cmake -B "$BUILD_DIR" -G"$GENERATOR" "${EXTRA_CMAKE_ARGS[@]}" .
fi

cmake --build "$BUILD_DIR" --target "$TARGET"
