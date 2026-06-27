#!/bin/bash
# Build the WOS-patched CMake for the Linux host.
# Installs cmake/ctest/cpack and module data into toolchain/host so wos-env.sh
# naturally prefers it over the system CMake.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$WORKSPACE_ROOT/tools/ccache_env.sh"
if [ -z "${CCACHE_DIR:-}" ]; then
    export CCACHE_DIR="${TMPDIR:-/tmp}/wos-cmake-for-host-ccache"
    mkdir -p "$CCACHE_DIR"
fi
wos_setup_ccache
wos_setup_ccache_cmake_args
WOS_BUILD_JOBS="$(wos_build_jobs)"
WOS_NINJA_JOBS="$(wos_ninja_jobs)"

B="$WORKSPACE_ROOT/toolchain"
HOST="$B/host"
CMAKE_SRC="$B/src/cmake"
CMAKE_BUILD="${WOS_CMAKE_FOR_HOST_BUILD_DIR:-$B/cmake-host-build}"
CMAKE_INSTALL="${WOS_CMAKE_FOR_HOST_INSTALL_DIR:-$HOST}"

export NINJA_STATUS="[%f/%t %e] "
export PATH="$HOST/bin:$PATH"
if [ -n "${LD_LIBRARY_PATH:-}" ]; then
    export LD_LIBRARY_PATH="$HOST/lib:$LD_LIBRARY_PATH"
else
    export LD_LIBRARY_PATH="$HOST/lib"
fi

require_file() {
    local path="$1"
    local hint="$2"

    if [ ! -e "$path" ]; then
        echo "ERROR: missing $path" >&2
        echo "$hint" >&2
        exit 1
    fi
}

ensure_cmake_source() {
    if [ -f "$CMAKE_SRC/CMakeLists.txt" ]; then
        return 0
    fi

    if [ -d "$WORKSPACE_ROOT/.git" ]; then
        git -C "$WORKSPACE_ROOT" submodule update --init toolchain/src/cmake || true
    fi
    if [ -f "$CMAKE_SRC/CMakeLists.txt" ]; then
        return 0
    fi

    mkdir -p "$B/src"
    git clone --branch=wos-support https://github.com/Pascu-Victor/CMake.git "$CMAKE_SRC"
}

host_build_triple() {
    if [ -n "${WOS_HOST_BUILD_TRIPLE:-}" ]; then
        printf '%s\n' "$WOS_HOST_BUILD_TRIPLE"
        return 0
    fi

    local cc_candidate
    for cc_candidate in /usr/bin/cc /usr/bin/gcc gcc cc; do
        if [[ "$cc_candidate" != */* ]] && ! command -v "$cc_candidate" >/dev/null 2>&1; then
            continue
        fi
        if [[ "$cc_candidate" == */* ]] && [ ! -x "$cc_candidate" ]; then
            continue
        fi

        local triple
        triple="$("$cc_candidate" -dumpmachine 2>/dev/null || true)"
        if [ -n "$triple" ] && [[ "$triple" != *wos* ]]; then
            printf '%s\n' "$triple"
            return 0
        fi
    done

    case "$(uname -m)" in
        x86_64)
            printf '%s\n' "x86_64-pc-linux-gnu"
            ;;
        aarch64|arm64)
            printf '%s\n' "aarch64-pc-linux-gnu"
            ;;
        *)
            printf '%s\n' "$(uname -m)-pc-linux-gnu"
            ;;
    esac
}

ensure_cmake_source

require_file "$HOST/bin/clang" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/clang++" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/ld.lld" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/llvm-ar" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/llvm-ranlib" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/llvm-strip" "Run tools/host-toolchain.sh first."
require_file "$CMAKE_SRC/CMakeLists.txt" "Initialise toolchain/src/cmake."

mkdir -p "$CMAKE_BUILD" "$CMAKE_INSTALL/bin" "$CMAKE_INSTALL/share"

HOST_TRIPLE="$(host_build_triple)"
HOST_C_FLAGS="${WOS_CMAKE_FOR_HOST_C_FLAGS:---target=$HOST_TRIPLE -fdiagnostics-color=always}"
HOST_CXX_FLAGS="${WOS_CMAKE_FOR_HOST_CXX_FLAGS:---target=$HOST_TRIPLE -fdiagnostics-color=always}"
HOST_LINK_FLAGS="${WOS_CMAKE_FOR_HOST_LINK_FLAGS:---target=$HOST_TRIPLE -fuse-ld=lld}"

env -u CC -u CXX -u LD -u CFLAGS -u CXXFLAGS -u CPPFLAGS -u LDFLAGS \
    cmake -S "$CMAKE_SRC" -B "$CMAKE_BUILD" -G Ninja \
        "${WOS_CCACHE_CMAKE_ARGS[@]}" \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX="$CMAKE_INSTALL" \
        -DCMAKE_INSTALL_RPATH="\$ORIGIN/../lib" \
        -DCMAKE_C_COMPILER="$HOST/bin/clang" \
        -DCMAKE_CXX_COMPILER="$HOST/bin/clang++" \
        -DCMAKE_LINKER="$HOST/bin/ld.lld" \
        -DCMAKE_AR="$HOST/bin/llvm-ar" \
        -DCMAKE_RANLIB="$HOST/bin/llvm-ranlib" \
        -DCMAKE_STRIP="$HOST/bin/llvm-strip" \
        -DCMAKE_C_COMPILER_TARGET="$HOST_TRIPLE" \
        -DCMAKE_CXX_COMPILER_TARGET="$HOST_TRIPLE" \
        -DCMAKE_C_FLAGS="$HOST_C_FLAGS" \
        -DCMAKE_CXX_FLAGS="$HOST_CXX_FLAGS" \
        -DCMAKE_EXE_LINKER_FLAGS="$HOST_LINK_FLAGS" \
        -DCMAKE_SHARED_LINKER_FLAGS="$HOST_LINK_FLAGS" \
        -DCMAKE_USE_SYSTEM_LIBRARIES=OFF \
        -DCMAKE_USE_OPENSSL=OFF \
        -DCMake_ENABLE_DEBUGGER=OFF \
        -DBUILD_TESTING=OFF \
        -DCMake_TEST_INSTALL=OFF \
        -DBUILD_CursesDialog=OFF \
        -DBUILD_QtDialog=OFF \
        -DCMake_BUILD_DEVELOPER_REFERENCE=OFF

env -u CC -u CXX -u LD -u CFLAGS -u CXXFLAGS -u CPPFLAGS -u LDFLAGS \
    cmake --build "$CMAKE_BUILD" --parallel "$WOS_NINJA_JOBS" --target cmake ctest cpack
env -u CC -u CXX -u LD -u CFLAGS -u CXXFLAGS -u CPPFLAGS -u LDFLAGS \
    cmake --install "$CMAKE_BUILD" --prefix "$CMAKE_INSTALL"

for tool in cmake ctest cpack; do
    require_file "$CMAKE_INSTALL/bin/$tool" "Host CMake install did not produce $tool."
done

if ! compgen -G "$CMAKE_INSTALL/share/cmake-*" >/dev/null; then
    echo "ERROR: host CMake install did not produce share/cmake-* data." >&2
    exit 1
fi

echo "Host WOS CMake installed to $CMAKE_INSTALL/bin/cmake"
