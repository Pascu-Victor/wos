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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$WORKSPACE_ROOT/tools/ccache_env.sh"
wos_setup_ccache
WOS_MESON_COMPILER_PREFIX="$(wos_meson_compiler_prefix)"

B=$(pwd)/toolchain
HOST="$B/host"
TARGET_SYSROOT="${WOS_SYSROOT_PATH:-$B/sysroot}"
MLIBC_BUILD="${WOS_MLIBC_BUILD_DIR:-$B/mlibc-build}"
MLIBC_SRC="$B/src/mlibc"
CROSS_FILE="$MLIBC_BUILD/x86_64-pc-wos-mlibc.txt"

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
echo "  sysroot: $TARGET_SYSROOT"
echo "  build:   $MLIBC_BUILD"

mkdir -p "$TARGET_SYSROOT" "$MLIBC_BUILD"
cat > "$CROSS_FILE" <<EOF
[binaries]
c = [$WOS_MESON_COMPILER_PREFIX'clang', '--target=x86_64-pc-wos', '--sysroot=$TARGET_SYSROOT', '-isystem', '$HOST/lib/clang/22/include', '-isystem', '$TARGET_SYSROOT/include', '-mcmodel=small']
cpp = [$WOS_MESON_COMPILER_PREFIX'clang++', '--target=x86_64-pc-wos', '--sysroot=$TARGET_SYSROOT', '-isystem', '$TARGET_SYSROOT/include/c++/v1', '-isystem', '$HOST/lib/clang/22/include', '-isystem', '$TARGET_SYSROOT/include', '-mcmodel=small']
ar = 'llvm-ar'
strip = 'llvm-strip'

[host_machine]
system = 'wos'
cpu_family = 'x86_64'
cpu = 'x86_64'
endian = 'little'
EOF

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
sync
