#!/bin/bash
# Cross-build native CMake tools that can run inside WOS.
# Installs cmake/ctest/cpack and CMake's module data into the target sysroot.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$WORKSPACE_ROOT/tools/ccache_env.sh"
if [ -z "${CCACHE_DIR:-}" ]; then
    export CCACHE_DIR="${TMPDIR:-/tmp}/wos-cmake-for-wos-ccache"
    mkdir -p "$CCACHE_DIR"
fi
wos_setup_ccache
wos_setup_ccache_cmake_args
WOS_BUILD_JOBS="$(wos_build_jobs)"
WOS_NINJA_JOBS="$(wos_ninja_jobs)"

B="$WORKSPACE_ROOT/toolchain"
HOST="${WOS_HOST_TOOLCHAIN_ROOT:-$B/host}"
TARGET_SYSROOT="${WOS_SYSROOT_PATH:-$B/sysroot}"
CMAKE_SRC="$B/src/cmake"
CMAKE_BUILD="${WOS_CMAKE_FOR_WOS_BUILD_DIR:-$B/cmake-wos-build}"
TARGET_ARCH="${WOS_TARGET_ARCH:-x86_64-pc-wos}"
WOS_CMAKE_FOR_WOS_LAZY_BINDING="${WOS_CMAKE_FOR_WOS_LAZY_BINDING:-0}"
WOS_CMAKE_FOR_WOS_STRIP="${WOS_CMAKE_FOR_WOS_STRIP:-0}"

export NINJA_STATUS="[%f/%t %e] "
export PATH="$HOST/bin:$PATH"
export LD_LIBRARY_PATH="$HOST/lib"

require_file() {
    local path="$1"
    local hint="$2"

    if [ ! -e "$path" ]; then
        echo "ERROR: missing $path" >&2
        echo "$hint" >&2
        exit 1
    fi
}

require_file "$HOST/bin/clang" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/clang++" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/ld.lld" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/llvm-strip" "Run tools/host-toolchain.sh first."
require_file "$CMAKE_SRC/CMakeLists.txt" "Initialise toolchain/src/cmake."
require_file "$TARGET_SYSROOT/lib/libc.so" "Build mlibc before building native CMake."
require_file "$TARGET_SYSROOT/lib/libc++.so" "Build libc++ before building native CMake."
require_file "$TARGET_SYSROOT/lib/Scrt1.o" "Build mlibc startup objects before building native CMake."

TARGET_COMMON_FLAGS="--sysroot=$TARGET_SYSROOT -fPIC -fPIE -fno-sanitize=safe-stack -fno-stack-protector -fdiagnostics-color=always"
TARGET_C_INCLUDE_FLAGS="-isystem $HOST/lib/clang/22/include -isystem $TARGET_SYSROOT/include"
TARGET_C_FLAGS="$TARGET_COMMON_FLAGS $TARGET_C_INCLUDE_FLAGS"
TARGET_CXX_FLAGS="$TARGET_COMMON_FLAGS -std=c++23 -isystem $TARGET_SYSROOT/include/c++/v1 $TARGET_C_INCLUDE_FLAGS"
TARGET_LINK_FLAGS="--sysroot=$TARGET_SYSROOT -fuse-ld=lld -L$TARGET_SYSROOT/lib -Wl,--dynamic-linker=/lib/ld.so -Wl,-rpath,/usr/lib -fno-sanitize=safe-stack"
TARGET_CXX_STANDARD_LIBRARIES="-lc++ -lc++abi -lunwind -lm -lpthread -ldl -lrt -lc"

if [ "$WOS_CMAKE_FOR_WOS_LAZY_BINDING" != "0" ]; then
    TARGET_LINK_FLAGS="$TARGET_LINK_FLAGS -Wl,-z,lazy"
fi

mkdir -p "$CMAKE_BUILD" "$TARGET_SYSROOT/bin" "$TARGET_SYSROOT/share"

if [ -d "$CMAKE_BUILD/CMakeFiles" ] && [ ! -f "$CMAKE_BUILD/CMakeCache.txt" ]; then
    echo "Native CMake build tree is missing CMakeCache.txt; resetting"
    wos_remove_tree "$CMAKE_BUILD"
    mkdir -p "$CMAKE_BUILD"
fi

if [ -f "$CMAKE_BUILD/CMakeCache.txt" ]; then
    cached_cxx="$(sed -n 's/^CMAKE_CXX_COMPILER:[^=]*=//p' "$CMAKE_BUILD/CMakeCache.txt")"
    cached_cxx_flags="$(sed -n 's/^CMAKE_CXX_FLAGS:[^=]*=//p' "$CMAKE_BUILD/CMakeCache.txt")"
    reset_cmake_cache=0
    if [ -n "$cached_cxx" ] && [ "$cached_cxx" != "$HOST/bin/clang++" ]; then
        echo "Native CMake cache uses $cached_cxx; resetting for $HOST/bin/clang++"
        reset_cmake_cache=1
    fi
    case " $cached_cxx_flags " in
        *" -isystem $TARGET_SYSROOT/include "*)
            ;;
        *)
            echo "Native CMake cache lacks target sysroot C headers; resetting"
            reset_cmake_cache=1
            ;;
    esac
    if [ "$reset_cmake_cache" != "0" ]; then
        wos_remove_tree "$CMAKE_BUILD"
        mkdir -p "$CMAKE_BUILD"
    fi
fi

wos_timed_step "configure" "cmake_for_wos" \
    cmake -S "$CMAKE_SRC" -B "$CMAKE_BUILD" -G Ninja \
    "${WOS_CCACHE_CMAKE_ARGS[@]}" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_MODULE_PATH="$CMAKE_SRC/Modules" \
    -DCMAKE_INSTALL_PREFIX="$TARGET_SYSROOT" \
    -DCMAKE_INSTALL_RPATH="/usr/lib" \
    -DCMAKE_BUILD_WITH_INSTALL_RPATH=ON \
    -DCMAKE_C_COMPILER="$HOST/bin/clang" \
    -DCMAKE_CXX_COMPILER="$HOST/bin/clang++" \
    -DCMAKE_LINKER="$HOST/bin/ld.lld" \
    -DCMAKE_AR="$HOST/bin/llvm-ar" \
    -DCMAKE_RANLIB="$HOST/bin/llvm-ranlib" \
    -DCMAKE_NM="$HOST/bin/llvm-nm" \
    -DCMAKE_OBJCOPY="$HOST/bin/llvm-objcopy" \
    -DCMAKE_STRIP="$HOST/bin/llvm-strip" \
    -DCMAKE_C_COMPILER_TARGET="$TARGET_ARCH" \
    -DCMAKE_CXX_COMPILER_TARGET="$TARGET_ARCH" \
    -DCMAKE_ASM_COMPILER_TARGET="$TARGET_ARCH" \
    -DCMAKE_SYSTEM_NAME=WOS \
    -DCMAKE_SYSTEM_PROCESSOR=x86_64 \
    -DCMAKE_SYSROOT="$TARGET_SYSROOT" \
    -DCMAKE_FIND_ROOT_PATH="$TARGET_SYSROOT" \
    -DCMAKE_FIND_ROOT_PATH_MODE_PROGRAM=NEVER \
    -DCMAKE_FIND_ROOT_PATH_MODE_LIBRARY=ONLY \
    -DCMAKE_FIND_ROOT_PATH_MODE_INCLUDE=ONLY \
    -DCMAKE_C_COMPILER_WORKS=ON \
    -DCMAKE_CXX_COMPILER_WORKS=ON \
    -DCMAKE_TRY_COMPILE_TARGET_TYPE=STATIC_LIBRARY \
    -DCMake_CXX17_WORKS=TRUE \
    -DCMake_HAVE_CXX_MAKE_UNIQUE=TRUE \
    -DCMake_HAVE_CXX_UNIQUE_PTR=1 \
    -DCMake_HAVE_CXX_FILESYSTEM=TRUE \
    -DCMAKE_HAVE_LIBC_PTHREAD=1 \
    -DHAVE_UNSETENV=1 \
    -DHAVE_ENVIRON_NOT_REQUIRE_PROTOTYPE=0 \
    -DCMAKE_C_FLAGS="$TARGET_C_FLAGS" \
    -DCMAKE_CXX_FLAGS="$TARGET_CXX_FLAGS" \
    -DCMAKE_EXE_LINKER_FLAGS="$TARGET_LINK_FLAGS" \
    -DCMAKE_SHARED_LINKER_FLAGS="$TARGET_LINK_FLAGS" \
    -DCMAKE_CXX_STANDARD_LIBRARIES="$TARGET_CXX_STANDARD_LIBRARIES" \
    -DCMAKE_USE_SYSTEM_LIBRARIES=OFF \
    -DCMAKE_USE_OPENSSL=OFF \
    -DCMake_ENABLE_DEBUGGER=OFF \
    -DBUILD_TESTING=OFF \
    -DCMake_TEST_INSTALL=OFF \
    -DBUILD_CursesDialog=OFF \
    -DBUILD_QtDialog=OFF \
    -DCMake_BUILD_DEVELOPER_REFERENCE=OFF

wos_timed_step "build" "cmake_for_wos" \
    cmake --build "$CMAKE_BUILD" --parallel "$WOS_NINJA_JOBS" --target cmake ctest cpack
wos_timed_step "install" "cmake_for_wos" \
    cmake --install "$CMAKE_BUILD" --prefix "$TARGET_SYSROOT"

for tool in cmake ctest cpack; do
    require_file "$TARGET_SYSROOT/bin/$tool" "CMake install did not produce $tool."
    if [ "$WOS_CMAKE_FOR_WOS_STRIP" != "0" ]; then
        "$HOST/bin/llvm-strip" "$TARGET_SYSROOT/bin/$tool"
    fi
done

if ! compgen -G "$TARGET_SYSROOT/share/cmake-*" >/dev/null; then
    echo "ERROR: CMake install did not produce share/cmake-* data." >&2
    exit 1
fi

echo "Native WOS CMake installed to $TARGET_SYSROOT/bin/cmake"
