#!/bin/bash
# Incrementally rebuild mlibc for WOS and install into the sysroot.
# Expects the toolchain to already be bootstrapped (tools/bootstrap.sh).
#
# Usage: build_mlibc.sh [CMAKE_BUILD_TYPE]
#   CMAKE_BUILD_TYPE is mapped to meson --buildtype:
#     Debug          -> debug
#     Release        -> release
#     RelWithDebInfo -> debugoptimized
#     MinSizeRel     -> minsize
#   Defaults to "release" if not specified.
set -e

B=$(pwd)/toolchain
HOST="$B/host"
TARGET_SYSROOT="$B/sysroot"
MLIBC_BUILD="$B/mlibc-build"
MLIBC_SRC="$B/src/mlibc"
CROSS_FILE="$(pwd)/tools/x86_64-pc-wos-mlibc.txt"

if [ ! -d "$MLIBC_SRC" ]; then
    echo "ERROR: mlibc source directory not found at $MLIBC_SRC"
    echo "Run tools/bootstrap.sh first to bootstrap the toolchain."
    exit 1
fi

# Map CMAKE_BUILD_TYPE to meson buildtype
CMAKE_BUILD_TYPE="${1:-Release}"
case "$CMAKE_BUILD_TYPE" in
    Debug)            MESON_BUILDTYPE="debug" ;;
    Release)          MESON_BUILDTYPE="release" ;;
    RelWithDebInfo)   MESON_BUILDTYPE="debugoptimized" ;;
    MinSizeRel)       MESON_BUILDTYPE="minsize" ;;
    *)                MESON_BUILDTYPE="release" ;;
esac

echo "Building mlibc with buildtype=$MESON_BUILDTYPE (CMAKE_BUILD_TYPE=$CMAKE_BUILD_TYPE)"

# Export flags expected by the meson cross-file
export CFLAGS="--sysroot=$TARGET_SYSROOT -std=c23 -fno-sanitize=safe-stack"
export CXXFLAGS="--sysroot=$TARGET_SYSROOT -std=c++23 -fno-sanitize=safe-stack"
export LDFLAGS="--sysroot=$TARGET_SYSROOT"
export PATH="$HOST/bin:$PATH"
export LD_LIBRARY_PATH="$HOST/lib"
export NINJA_STATUS="[%f/%t %e] "

if [ -f "$MLIBC_BUILD/build.ninja" ]; then
    # Reconfigure existing build with the requested buildtype
    meson configure "$MLIBC_BUILD" --buildtype="$MESON_BUILDTYPE"
else
    # Fresh setup
    mkdir -p "$MLIBC_BUILD"
    meson setup --prefix="$TARGET_SYSROOT" \
        --sysconfdir=etc \
        --buildtype="$MESON_BUILDTYPE" \
        --cross-file="$CROSS_FILE" \
        -Dheaders_only=false \
        -Dwos_option=enabled \
        -Dlinux_option=disabled \
        -Dglibc_option=enabled \
        -Ddefault_library=both \
        -Duse_freestnd_hdrs=enabled \
        -Dposix_option=enabled \
        -Dbsd_option=enabled \
        -Db_sanitize=none \
        "$MLIBC_BUILD" "$MLIBC_SRC"
fi

ninja -C "$MLIBC_BUILD"
ninja -C "$MLIBC_BUILD" install
