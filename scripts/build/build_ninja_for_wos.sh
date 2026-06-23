#!/bin/bash
# Cross-build Ninja so it can run inside WOS and install it into the sysroot.
# Expects the host toolchain, mlibc, and libc++ to already be available.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$WORKSPACE_ROOT/tools/ccache_env.sh"
if [ -z "${CCACHE_DIR:-}" ]; then
    export CCACHE_DIR="${TMPDIR:-/tmp}/wos-ninja-ccache"
    mkdir -p "$CCACHE_DIR"
fi
wos_setup_ccache
wos_setup_ccache_cmake_args

B="$WORKSPACE_ROOT/toolchain"
HOST="${WOS_HOST_TOOLCHAIN_ROOT:-$B/host}"
TARGET_SYSROOT="${WOS_SYSROOT_PATH:-$B/sysroot}"
NINJA_SRC="$B/src/ninja"
NINJA_BUILD="${WOS_NINJA_BUILD_DIR:-$B/ninja-build}"
TARGET_ARCH="${WOS_TARGET_ARCH:-x86_64-pc-wos}"
WOS_NINJA_STRIP="${WOS_NINJA_STRIP:-0}"

export PATH="$HOST/bin:$PATH"
export LD_LIBRARY_PATH="$HOST/lib"
export NINJA_STATUS="[%f/%t %e] "

require_file() {
    local path="$1"
    local hint="$2"

    if [ ! -e "$path" ]; then
        echo "ERROR: missing $path" >&2
        echo "$hint" >&2
        exit 1
    fi
}

require_file "$HOST/bin/clang++" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/ld.lld" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/llvm-ar" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/llvm-ranlib" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/llvm-strip" "Run tools/host-toolchain.sh first."
require_file "$NINJA_SRC/CMakeLists.txt" "Initialize toolchain/src/ninja first."
require_file "$TARGET_SYSROOT/lib/libc.so" "Build mlibc before building Ninja."
require_file "$TARGET_SYSROOT/lib/libc++.so" "Build libc++ before building Ninja."
require_file "$TARGET_SYSROOT/lib/Scrt1.o" "Build mlibc startup objects before building Ninja."

mkdir -p "$NINJA_BUILD" "$TARGET_SYSROOT/bin"

WOS_NINJA_COMPAT_INCLUDE="$NINJA_BUILD/wos-compat/include"
mkdir -p "$WOS_NINJA_COMPAT_INCLUDE"
cat > "$WOS_NINJA_COMPAT_INCLUDE/sched.h" <<'EOF'
#ifndef WOS_NINJA_COMPAT_SCHED_H
#define WOS_NINJA_COMPAT_SCHED_H

#include_next <sched.h>

/* Ninja treats CPU_COUNT as a signal that sched_getaffinity() is a complete
   processor-count path. WOS exposes the GNU-style cpuset macros, but its
   sysroot may not export every helper symbol in that path yet, so use Ninja's
   portable sysconf(_SC_NPROCESSORS_ONLN) fallback instead. */
#undef CPU_COUNT
#undef CPU_COUNT_S

#endif
EOF

TARGET_CXX_FLAGS="--sysroot=$TARGET_SYSROOT -I$WOS_NINJA_COMPAT_INCLUDE -fPIC -fPIE -fno-sanitize=safe-stack -fno-stack-protector -fdiagnostics-color=always -isystem $TARGET_SYSROOT/include/c++/v1"
TARGET_LINK_FLAGS="--sysroot=$TARGET_SYSROOT -fuse-ld=lld -L$TARGET_SYSROOT/lib -Wl,--dynamic-linker=/lib/ld.so -Wl,-rpath,/usr/lib -fno-sanitize=safe-stack"
TARGET_CXX_STANDARD_LIBRARIES="-lc++ -lc++abi -lunwind -lm -lpthread -ldl -lrt -lc"

cmake -S "$NINJA_SRC" -B "$NINJA_BUILD" -G Ninja \
    "${WOS_CCACHE_CMAKE_ARGS[@]}" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="$TARGET_SYSROOT" \
    -DCMAKE_INSTALL_RPATH="/usr/lib" \
    -DCMAKE_BUILD_WITH_INSTALL_RPATH=ON \
    -DCMAKE_CXX_COMPILER="$HOST/bin/clang++" \
    -DCMAKE_LINKER="$HOST/bin/ld.lld" \
    -DCMAKE_AR="$HOST/bin/llvm-ar" \
    -DCMAKE_RANLIB="$HOST/bin/llvm-ranlib" \
    -DCMAKE_STRIP="$HOST/bin/llvm-strip" \
    -DCMAKE_CXX_COMPILER_TARGET="$TARGET_ARCH" \
    -DCMAKE_SYSTEM_NAME=WOS \
    -DCMAKE_SYSTEM_PROCESSOR=x86_64 \
    -DCMAKE_SYSROOT="$TARGET_SYSROOT" \
    -DCMAKE_FIND_ROOT_PATH="$TARGET_SYSROOT" \
    -DCMAKE_FIND_ROOT_PATH_MODE_PROGRAM=NEVER \
    -DCMAKE_FIND_ROOT_PATH_MODE_LIBRARY=ONLY \
    -DCMAKE_FIND_ROOT_PATH_MODE_INCLUDE=ONLY \
    -DCMAKE_CXX_COMPILER_WORKS=ON \
    -DCMAKE_TRY_COMPILE_TARGET_TYPE=STATIC_LIBRARY \
    -DCMAKE_CXX_FLAGS="$TARGET_CXX_FLAGS" \
    -DCMAKE_EXE_LINKER_FLAGS="$TARGET_LINK_FLAGS" \
    -DCMAKE_CXX_STANDARD_LIBRARIES="$TARGET_CXX_STANDARD_LIBRARIES" \
    -DBUILD_TESTING=OFF \
    -DNINJA_BUILD_BINARY=ON \
    -DNINJA_FORCE_PSELECT=ON \
    -DHAVE_FORK=0 \
    -DHAVE_PIPE=0

# CMake does not know when sysroot libraries change underneath this external
# build, so force the final link if libc/libc++ were rebuilt.
if [ -f "$NINJA_BUILD/ninja" ]; then
    for lib in "$TARGET_SYSROOT"/lib/libc.so "$TARGET_SYSROOT"/lib/libc++.so \
               "$TARGET_SYSROOT"/lib/libc++abi.so "$TARGET_SYSROOT"/lib/libm.so; do
        if [ -f "$lib" ] && [ "$lib" -nt "$NINJA_BUILD/ninja" ]; then
            echo "Sysroot library $(basename "$lib") changed - forcing relink"
            rm -f "$NINJA_BUILD/ninja"
            break
        fi
    done
fi

cmake --build "$NINJA_BUILD" --parallel "$(nproc)" --target ninja
cmake --install "$NINJA_BUILD"

require_file "$TARGET_SYSROOT/bin/ninja" "Ninja install did not produce $TARGET_SYSROOT/bin/ninja."
if [ "$WOS_NINJA_STRIP" != "0" ]; then
    "$HOST/bin/llvm-strip" "$TARGET_SYSROOT/bin/ninja"
fi

echo "Native WOS Ninja installed to $TARGET_SYSROOT/bin/ninja"
