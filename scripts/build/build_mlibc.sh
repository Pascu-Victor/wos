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
WOS_BUILD_JOBS="$(wos_build_jobs)"
WOS_NINJA_JOBS="$(wos_ninja_jobs)"

normalize_bool() {
    case "$1" in
        1|ON|on|TRUE|true|YES|yes) echo true ;;
        0|OFF|off|FALSE|false|NO|no) echo false ;;
        *)
            echo "ERROR: expected a boolean value, got '$1'" >&2
            exit 2
            ;;
    esac
}

B="$WORKSPACE_ROOT/toolchain"
HOST="${WOS_HOST_TOOLCHAIN_ROOT:-$B/host}"
TARGET_SYSROOT="${WOS_SYSROOT_PATH:-$B/sysroot}"
MLIBC_BUILD="${WOS_MLIBC_BUILD_DIR:-$B/mlibc-build}"
MLIBC_SRC="$B/src/mlibc"
CROSS_FILE="$MLIBC_BUILD/x86_64-pc-wos-mlibc.txt"
MLIBC_BUILD_TESTS="$(normalize_bool "${WOS_MLIBC_BUILD_TESTS:-0}")"
MLIBC_BUILD_HOST_TESTS="$(normalize_bool "${WOS_MLIBC_BUILD_HOST_TESTS:-0}")"

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
echo "  tests:   $MLIBC_BUILD_TESTS (host-libc tests: $MLIBC_BUILD_HOST_TESTS)"

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
export CFLAGS="--sysroot=$TARGET_SYSROOT -std=gnu23 -fno-sanitize=safe-stack"
export CXXFLAGS="--sysroot=$TARGET_SYSROOT -std=c++23 -fno-sanitize=safe-stack"
export LDFLAGS="--sysroot=$TARGET_SYSROOT"
if [ -z "${CC_FOR_BUILD:-}" ]; then
    if [ -x /usr/bin/clang ]; then
        export CC_FOR_BUILD=/usr/bin/clang
    else
        export CC_FOR_BUILD=/usr/bin/cc
    fi
fi
if [ -z "${CXX_FOR_BUILD:-}" ]; then
    if [ -x /usr/bin/clang++ ]; then
        export CXX_FOR_BUILD=/usr/bin/clang++
    else
        export CXX_FOR_BUILD=/usr/bin/c++
    fi
fi
export CFLAGS_FOR_BUILD="${CFLAGS_FOR_BUILD:-}"
export CXXFLAGS_FOR_BUILD="${CXXFLAGS_FOR_BUILD:-}"
export LDFLAGS_FOR_BUILD="${LDFLAGS_FOR_BUILD:-}"
export PATH="$HOST/bin:$PATH"
export LD_LIBRARY_PATH="$HOST/lib"
export NINJA_STATUS="[%f/%t %e] "

wos_prefetch_meson_subprojects "$MLIBC_SRC" freestnd-c-hdrs freestnd-cxx-hdrs frigg libsmarter

if [ -f "$MLIBC_BUILD/build.ninja" ]; then
    # Reconfigure existing build with the requested buildtype and source list.
    meson setup --reconfigure \
        --buildtype="$MESON_BUILDTYPE" \
        -Dc_args="$CFLAGS" \
        -Dcpp_args="$CXXFLAGS" \
        -Dbuild_tests="$MLIBC_BUILD_TESTS" \
        -Dbuild_tests_host_libc="$MLIBC_BUILD_HOST_TESTS" \
        "$MLIBC_BUILD" "$MLIBC_SRC"
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
        -Dbuild_tests="$MLIBC_BUILD_TESTS" \
        -Dbuild_tests_host_libc="$MLIBC_BUILD_HOST_TESTS" \
        -Db_sanitize=none \
        "$MLIBC_BUILD" "$MLIBC_SRC"
fi

ninja -C "$MLIBC_BUILD" -j"$WOS_NINJA_JOBS"
ninja -C "$MLIBC_BUILD" -j"$WOS_NINJA_JOBS" install
sync
