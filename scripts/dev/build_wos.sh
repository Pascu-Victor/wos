#!/bin/bash
# Configure the default WOS build tree once, then let Ninja/CMake dependency
# checks decide when regeneration is actually needed.
set -euo pipefail

BUILD_DIR="${WOS_BUILD_DIR:-build}"
GENERATOR="${WOS_CMAKE_GENERATOR:-Ninja}"
TARGET="${WOS_BUILD_TARGET:-wos_full}"

needs_configure=0
if [ ! -f "$BUILD_DIR/CMakeCache.txt" ]; then
    needs_configure=1
elif [ "$GENERATOR" = "Ninja" ] && [ ! -f "$BUILD_DIR/build.ninja" ]; then
    needs_configure=1
fi

if [ "$needs_configure" -eq 1 ]; then
    cmake -B "$BUILD_DIR" -G"$GENERATOR" .
fi

cmake --build "$BUILD_DIR" --target "$TARGET"
