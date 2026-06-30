#!/bin/bash
# Build the WOS target toolchain: sysroot, compiler-rt, mlibc, libc++, busybox,
# dropbear, GNU make, Bash, Ninja, CMake, Python, Meson, NASM, OpenSSL, curl,
# Git, and native WOS clang/lld.
# Requires host-toolchain.sh to have been run first.
#
# Layout:
#   toolchain/host/    - host LLVM binaries (clang, lld, llvm-ar, etc.)
#                       or WOS_HOST_TOOLCHAIN_ROOT when overridden
#   toolchain/sysroot/ - WOS target libraries and headers only
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

source "$WORKSPACE_ROOT/tools/ccache_env.sh"
wos_setup_ccache
wos_setup_ccache_cmake_args
WOS_BUILD_JOBS="$(wos_build_jobs)"
WOS_NINJA_JOBS="$(wos_ninja_jobs)"
WOS_CCACHE_PREFIX="$(wos_ccache_prefix)"
WOS_MESON_COMPILER_PREFIX="$(wos_meson_compiler_prefix)"

cd "$WORKSPACE_ROOT"

B="$WORKSPACE_ROOT/toolchain"
OLD_PATH=$PATH
TARGET_ARCH=x86_64-pc-wos
COMPILER_RT_ARCH=x86_64
HOST="${WOS_HOST_TOOLCHAIN_ROOT:-$B/host}"
SYSROOT=$B/sysroot
HOST_SYSTEM="$(uname -s 2>/dev/null || printf unknown)"
export WOS_HOST_TOOLCHAIN_ROOT="$HOST"
export NINJA_STATUS="[%f/%t %e] "

COMPILER_RT_CMAKE_SYSROOT_ARGS=("-DCMAKE_SYSROOT=$SYSROOT")
COMPILER_RT_NINJA_JOBS="$WOS_NINJA_JOBS"
if [ "$HOST_SYSTEM" = "WOS" ]; then
    COMPILER_RT_CMAKE_SYSROOT_ARGS=("-DCMAKE_SYSROOT_COMPILE=$SYSROOT")
fi

meson_setup_rerunnable() {
    local build_dir="$1"
    shift

    if [ -d "$build_dir/meson-private" ] || [ -f "$build_dir/build.ninja" ]; then
        if meson setup --reconfigure "$build_dir" "$@"; then
            return 0
        fi

        echo "Meson reconfigure failed for $build_dir, retrying with --wipe..."
        meson setup --wipe "$build_dir" "$@"
        return 0
    fi

    meson setup "$build_dir" "$@"
}

install_compiler_rt_resource_dir() {
    local require_sanitizers="$1"
    local resource_dir="$HOST/lib/clang/22"
    local install_dir="$resource_dir/target"
    local runtime_dir="$resource_dir/lib/$TARGET_ARCH"

    if [ ! -d "$install_dir/lib" ]; then
        echo "ERROR: compiler-rt did not install runtime libraries under $install_dir/lib" >&2
        exit 1
    fi

    mkdir -p "$runtime_dir" "$resource_dir/include"
    cp -a "$install_dir/lib"/. "$runtime_dir"/
    if [ -d "$install_dir/include" ]; then
        cp -a "$install_dir/include"/. "$resource_dir/include"/
    fi
    rm -rf "$install_dir"

    ln -fs "$runtime_dir/libclang_rt.builtins-$COMPILER_RT_ARCH.a" "$resource_dir/lib/libclang_rt.builtins.a"
    ln -fs "$runtime_dir/libclang_rt.crtbegin-$COMPILER_RT_ARCH.a" "$resource_dir/lib/libclang_rt.crtbegin.a"
    ln -fs "$runtime_dir/libclang_rt.crtend-$COMPILER_RT_ARCH.a" "$resource_dir/lib/libclang_rt.crtend.a"

    ln -fs "$runtime_dir/libclang_rt.builtins-$COMPILER_RT_ARCH.a" "$runtime_dir/libclang_rt.builtins.a"
    ln -fs "$runtime_dir/libclang_rt.crtbegin-$COMPILER_RT_ARCH.a" "$runtime_dir/libclang_rt.crtbegin.a"
    ln -fs "$runtime_dir/libclang_rt.crtend-$COMPILER_RT_ARCH.a" "$runtime_dir/libclang_rt.crtend.a"

    if [ ! -f "$runtime_dir/libclang_rt.profile-$COMPILER_RT_ARCH.a" ]; then
        echo "ERROR: compiler-rt did not install libclang_rt.profile-$COMPILER_RT_ARCH.a" >&2
        exit 1
    fi
    ln -fs "$runtime_dir/libclang_rt.profile-$COMPILER_RT_ARCH.a" "$runtime_dir/libclang_rt.profile.a"

    if [ "$require_sanitizers" = "ON" ]; then
        local runtime_lib
        for runtime_lib in asan_static asan asan_cxx; do
            if [ ! -f "$runtime_dir/libclang_rt.$runtime_lib-$COMPILER_RT_ARCH.a" ]; then
                echo "ERROR: compiler-rt did not install libclang_rt.$runtime_lib-$COMPILER_RT_ARCH.a" >&2
                exit 1
            fi
            ln -fs "$runtime_dir/libclang_rt.$runtime_lib-$COMPILER_RT_ARCH.a" "$runtime_dir/libclang_rt.$runtime_lib.a"
        done
    fi
}

build_compiler_rt() {
    local build_sanitizers="$1"

    mkdir -p "$B/compiler-rt-build"
    cd "$B/compiler-rt-build"
    env -u LDFLAGS cmake -G Ninja \
     "${WOS_CCACHE_CMAKE_ARGS[@]}" \
     -DCMAKE_BUILD_TYPE=Release \
     -DCMAKE_INSTALL_PREFIX=$HOST/lib/clang/22/target \
     -DCMAKE_C_COMPILER=$CC \
     -DCMAKE_CXX_COMPILER=$CXX \
     "${COMPILER_RT_CMAKE_SYSROOT_ARGS[@]}" \
     -DCMAKE_SYSTEM_NAME=WOS \
     -DCMAKE_C_FLAGS="-fno-sanitize=safe-stack -fdiagnostics-color=always" \
     -DCMAKE_CXX_FLAGS="-fno-sanitize=safe-stack -fdiagnostics-color=always" \
     -DCMAKE_ASM_FLAGS="-fno-sanitize=safe-stack -fdiagnostics-color=always" \
     -DCMAKE_C_COMPILER_TARGET=$TARGET_ARCH \
     -DCMAKE_CXX_COMPILER_TARGET=$TARGET_ARCH \
     -DCMAKE_ASM_COMPILER_TARGET=$TARGET_ARCH \
     -DCMAKE_C_COMPILER_WORKS=ON \
     -DCMAKE_CXX_COMPILER_WORKS=ON \
     -DCMAKE_TRY_COMPILE_TARGET_TYPE=STATIC_LIBRARY \
     -DCOMPILER_RT_BUILD_BUILTINS=ON \
     -DCOMPILER_RT_BUILD_MEMPROF=OFF \
     -DCOMPILER_RT_BUILD_ORC=OFF \
     -DCOMPILER_RT_BUILD_PROFILE=ON \
     -DCOMPILER_RT_BUILD_LIBFUZZER=OFF \
     -DCOMPILER_RT_HAS_SAFESTACK=OFF \
     -DCOMPILER_RT_OS_DIR="" \
     -DCOMPILER_RT_LIBCXXABI_ENABLE_LOCALIZATION=OFF \
     -DCOMPILER_RT_HAS_SCUDO_STANDALONE=OFF \
     -DCOMPILER_RT_BUILD_XRAY=OFF \
     -DCOMPILER_RT_BUILD_SANITIZERS=$build_sanitizers \
     -DCOMPILER_RT_SANITIZERS_TO_BUILD=asan \
     -DCOMPILER_RT_HAS_GCC_S_LIB=OFF \
     -DSANITIZER_CXX_ABI=none \
     -DSANITIZER_TEST_CXX=none \
     -DCOMPILER_RT_STANDALONE_BUILD=ON \
     -DCOMPILER_RT_INTERCEPT_LIBDISPATCH=OFF \
     -DCOMPILER_RT_HAS_PTHREAD_LIB=OFF \
     -DCAN_TARGET_AMD64=ON \
     -DWOS=ON \
     $B/src/llvm-project/compiler-rt

    ninja -j"$COMPILER_RT_NINJA_JOBS" install
    install_compiler_rt_resource_dir "$build_sanitizers"
}

if [ ! -x "$HOST/bin/clang" ]; then
    echo "ERROR: Host toolchain not found at $HOST/bin/clang"
    echo "Run tools/host-toolchain.sh first, or run tools/bootstrap.sh on WOS to create a system-toolchain shim."
    exit 1
fi

# Set up environment - host tools on PATH, sysroot for target libs
export CC="$HOST/bin/clang"
export CXX="$HOST/bin/clang++"
export LD="$HOST/bin/ld.lld"
export PATH=$HOST/bin:$OLD_PATH
export LD_LIBRARY_PATH="$HOST/lib"

if [ "${WOS_BUILD_CMAKE_FOR_HOST:-1}" != "0" ]; then
    "$WORKSPACE_ROOT/scripts/build/build_cmake_for_host.sh"
fi

# 1. Create target directories and empty CRT files
mkdir -p $SYSROOT/bin $SYSROOT/lib $SYSROOT/include/abi-bits
[ ! -e $SYSROOT/usr ] && ln -sf . $SYSROOT/usr
[ ! -e $SYSROOT/lib64 ] && ln -sf lib $SYSROOT/lib64

# headers-only mlibc -- compiler-rt needs these
# Create a basic cross-file for headers-only build
mkdir -p $B/../tools
cat > $B/../tools/x86_64-pc-wos-mlibc.txt << EOF
[binaries]
c = [$WOS_MESON_COMPILER_PREFIX'clang', '--target=x86_64-pc-wos', '-mcmodel=small']
cpp = [$WOS_MESON_COMPILER_PREFIX'clang++', '--target=x86_64-pc-wos', '-mcmodel=small']
ar = 'llvm-ar'
strip = 'llvm-strip'
[host_machine]
system = 'wos'
cpu_family = 'x86_64'
cpu = 'x86_64'
endian = 'little'
EOF

# Build mlibc headers
mkdir -p $B/mlibc-headers
meson_setup_rerunnable "$B/mlibc-headers" --prefix=$SYSROOT \
    --libdir=lib \
    --includedir=include \
    -Dheaders_only=true \
    -Ddefault_library=static \
    --cross-file=$B/../tools/x86_64-pc-wos-mlibc.txt \
    -Dbindir=bin \
    -Dwos_option=enabled \
    -Dglibc_option=enabled \
    -Db_staticpic=disabled \
    $B/src/mlibc
cd $B/mlibc-headers
ninja -j"$WOS_NINJA_JOBS" install

# Create minimal CRT files for compiler-rt build (these will be replaced by mlibc later)
touch empty.c
$CC -O3 -c empty.c       -o $SYSROOT/lib/crtbegin.o
$CC -O3 -c empty.c -fPIC -o $SYSROOT/lib/crtbeginS.o
$CC -O3 -c empty.c       -o $SYSROOT/lib/crtend.o
$CC -O3 -c empty.c -fPIC -o $SYSROOT/lib/crtendS.o

# Create temporary CRT startup files for compiler-rt (will be replaced by mlibc)
$CC -O3 -c empty.c       -o $SYSROOT/lib/Scrt1.o
$CC -O3 -c empty.c       -o $SYSROOT/lib/crt1.o
$CC -O3 -c empty.c       -o $SYSROOT/lib/crti.o
$CC -O3 -c empty.c       -o $SYSROOT/lib/crtn.o

# 2. Build the compiler-rt pieces needed to finish the libc bootstrap.
# Sanitizers are built after mlibc installs real libc/libpthread/libm/etc.
export CFLAGS="--sysroot=$SYSROOT -std=c23 -fno-sanitize=safe-stack "
export CXXFLAGS="--sysroot=$SYSROOT -std=c++23 -fno-sanitize=safe-stack "
unset LDFLAGS
build_compiler_rt OFF

# 3. Bootstrap libcxx (headers only, needed by mlibc)

mkdir -p $B/libcxx-bootstrap
cd $B/libcxx-bootstrap
cmake -G Ninja \
 "${WOS_CCACHE_CMAKE_ARGS[@]}" \
 -DCMAKE_BUILD_TYPE=Release \
 -DCMAKE_INSTALL_PREFIX=$SYSROOT \
 -DCMAKE_C_COMPILER=$CC \
 -DCMAKE_CXX_COMPILER=$CXX \
 -DCMAKE_C_COMPILER_TARGET=$TARGET_ARCH \
 -DCMAKE_CXX_COMPILER_TARGET=$TARGET_ARCH \
 -DCMAKE_ASM_COMPILER_TARGET=$TARGET_ARCH \
 -DCMAKE_C_COMPILER_WORKS=ON \
 -DCMAKE_CXX_COMPILER_WORKS=ON \
 -DCMAKE_TRY_COMPILE_TARGET_TYPE=STATIC_LIBRARY \
 -DCMAKE_SYSROOT=$SYSROOT \
 -DCMAKE_C_FLAGS="--sysroot=$SYSROOT" \
 -DCMAKE_CXX_FLAGS="--sysroot=$SYSROOT" \
 -DCMAKE_CROSSCOMPILING=True \
 -DLLVM_ENABLE_RUNTIMES='libcxx;libcxxabi' \
 -DLIBCXX_CXX_ABI=libcxxabi \
 -DLIBCXX_ENABLE_SHARED=OFF \
 -DLIBCXX_ENABLE_STATIC=OFF \
 -DLIBCXX_ENABLE_ABI_LINKER_SCRIPT=OFF \
 -DLIBCXX_INSTALL_LIBRARY=OFF \
 -DLIBCXX_USE_COMPILER_RT=On \
 -DLIBCXX_HAS_PTHREAD_API=On \
 -DLIBCXX_HAS_PTHREAD_LIB=On \
 -DLIBCXX_INCLUDE_BENCHMARKS=OFF \
 -DLIBCXX_INCLUDE_TESTS=OFF \
 -DLIBCXX_INSTALL_MODULES=OFF \
 -DLIBCXXABI_ENABLE_STATIC=OFF \
 -DLIBCXXABI_ENABLE_SHARED=ON \
 -DLIBCXXABI_INSTALL_LIBRARY=OFF \
 -DLIBCXX_INSTALL_HEADERS=ON \
 -DLIBCXXABI_INCLUDE_TESTS=OFF \
 -DLIBCXXABI_USE_LLVM_UNWINDER=OFF \
 -DLIBCXXABI_HAS_PTHREAD_API=ON \
 -DLIBCXXABI_HAS_PTHREAD_LIB=ON \
 -DLIBCXXABI_USE_COMPILER_RT=ON \
 -DHAVE_LIBPTHREAD=OFF \
 -DLIBCXX_ENABLE_LOCALIZATION=OFF \
 -DLIBCXX_ENABLE_FILESYSTEM=OFF \
 -DLIBCXX_ENABLE_LOCALIZATION=OFF \
 $B/src/llvm-project/runtimes

ninja -j"$WOS_NINJA_JOBS" install-cxx-headers install-cxxabi-headers

# 4. Build mlibc

# Prepare cross-file (always regenerate to ensure correct paths)
mkdir -p $B/../tools
cat > $B/../tools/x86_64-pc-wos-mlibc.txt << EOF
[binaries]
c = [$WOS_MESON_COMPILER_PREFIX'clang', '--target=x86_64-pc-wos', '--sysroot=$SYSROOT', '-isystem', '$HOST/lib/clang/22/include', '-isystem', '$SYSROOT/include', '-mcmodel=small']
cpp = [$WOS_MESON_COMPILER_PREFIX'clang++', '--target=x86_64-pc-wos', '--sysroot=$SYSROOT', '-isystem', '$SYSROOT/include/c++/v1', '-isystem', '$HOST/lib/clang/22/include', '-isystem', '$SYSROOT/include', '-mcmodel=small']
ar = 'llvm-ar'
strip = 'llvm-strip'

[host_machine]
system = 'wos'
cpu_family = 'x86_64'
cpu = 'x86_64'
endian = 'little'
EOF

# Reset flags because the compiler gods want to i guess :D
unset CFLAGS CXXFLAGS LDFLAGS
export CFLAGS="--sysroot=$SYSROOT -std=c23 -fno-sanitize=safe-stack "
export CXXFLAGS="--sysroot=$SYSROOT -std=c++23 -fno-sanitize=safe-stack "
export LDFLAGS="--sysroot=$SYSROOT"

wos_prefetch_meson_subprojects "$B/src/mlibc" freestnd-c-hdrs freestnd-cxx-hdrs frigg

mkdir -p $B/mlibc-build
meson_setup_rerunnable "$B/mlibc-build" --prefix=$SYSROOT \
  --sysconfdir=etc \
  --buildtype=release \
  --cross-file=$B/../tools/x86_64-pc-wos-mlibc.txt \
  -Dheaders_only=false \
  -Dwos_option=enabled \
  -Dlinux_option=disabled \
  -Dglibc_option=enabled \
  -Ddefault_library=both \
  -Duse_freestnd_hdrs=enabled \
  -Dposix_option=enabled \
  -Dbsd_option=enabled \
  -Db_sanitize=none \
  $B/src/mlibc
cd $B/mlibc-build
ninja -j"$WOS_NINJA_JOBS" && ninja -j"$WOS_NINJA_JOBS" install

# 5. Finish compiler-rt now that mlibc installed the libraries ASAN links to.
unset LDFLAGS
build_compiler_rt ON

# 6. Build libcxx, libcxxabi, and libunwind (now that mlibc is available)

mkdir -p $B/libcxx-build
cd $B/libcxx-build
cmake -G Ninja \
 "${WOS_CCACHE_CMAKE_ARGS[@]}" \
 -ULIBCXXABI_HAS_CXA_THREAD_ATEXIT_IMPL \
 -DLLVM_ENABLE_RUNTIMES='libcxx;libcxxabi;libunwind' \
 -DLIBCXXABI_USE_LLVM_UNWINDER=ON \
 -DLIBCXXABI_USE_COMPILER_RT=On \
 -DLIBCXXABI_INCLUDE_TESTS=OFF \
 -DLIBCXXABI_HAS_PTHREAD_LIB=ON \
 -DLIBCXXABI_HAS_PTHREAD_API=ON \
 -DLIBCXXABI_ENABLE_STATIC=ON \
 -DLIBCXXABI_ENABLE_SHARED=ON \
 -DLIBUNWIND_ENABLE_SHARED=ON \
 -DLIBUNWIND_ENABLE_STATIC=ON \
 -DLIBCXX_USE_COMPILER_RT=On \
 -DLIBCXX_INSTALL_HEADERS=ON \
 -DLIBCXX_INCLUDE_TESTS=OFF \
 -DLIBCXX_INCLUDE_BENCHMARKS=OFF \
 -DLIBCXX_HAS_PTHREAD_LIB=On \
 -DLIBCXX_HAS_PTHREAD_API=On \
 -DLIBCXX_ENABLE_LOCALIZATION=ON \
 -DLIBCXX_ENABLE_FILESYSTEM=ON \
 -DLIBCXX_ENABLE_WIDE_CHARACTERS=ON \
 -DLIBCXX_ENABLE_EXPERIMENTAL=ON \
 -DLIBCXX_ENABLE_UNICODE=ON \
 -DLIBCXX_ENABLE_RTTI=ON \
 -DLIBCXX_ENABLE_EXCEPTIONS=ON \
 -DLIBCXX_ENABLE_SHARED_CXX_ABI=ON \
 -DLIBCXX_CXX_ABI=libcxxabi \
 -DLIBCXX_HAS_ATOMIC_LIB=OFF \
 -DLIBCXXABI_HAS_ATOMIC_LIB=OFF \
 -DCMAKE_TRY_COMPILE_TARGET_TYPE=STATIC_LIBRARY \
 -DCMAKE_SYSTEM_NAME=WOS \
 -DCMAKE_SYSROOT=$SYSROOT \
 -DCMAKE_INSTALL_PREFIX=$SYSROOT \
 -DCMAKE_CXX_FLAGS="-I$SYSROOT/include --sysroot=$SYSROOT -fno-sanitize=safe-stack -fdiagnostics-color=always" \
 -DCMAKE_CXX_COMPILER=$CXX \
 -DCMAKE_CXX_COMPILER_WORKS=ON \
 -DCMAKE_CXX_COMPILER_TARGET=$TARGET_ARCH \
 -DCMAKE_CROSSCOMPILING=True \
 -DCMAKE_C_FLAGS="-I$SYSROOT/include --sysroot=$SYSROOT -fno-sanitize=safe-stack -fdiagnostics-color=always" \
 -DCMAKE_C_COMPILER=$CC \
 -DCMAKE_C_COMPILER_WORKS=ON \
 -DCMAKE_C_COMPILER_TARGET=$TARGET_ARCH \
 -DCMAKE_BUILD_TYPE=Release \
 -DCMAKE_ASM_COMPILER_TARGET=$TARGET_ARCH \
 -DCMAKE_SHARED_LINKER_FLAGS="-L$HOST/lib/clang/22/lib/$TARGET_ARCH" \
 -DCMAKE_EXE_LINKER_FLAGS="-L$HOST/lib/clang/22/lib/$TARGET_ARCH" \
 -DWOS=ON \
 $B/src/llvm-project/runtimes

WOS_LIBCXX_BUILD_DIR="$B/libcxx-build" "$B/../scripts/build/build_libcxx_for_wos.sh"

# Generate Clang config file for WOS target triple
# Must be after all library builds (mlibc, libc++) but before userspace binaries
cat > $HOST/bin/x86_64-pc-wos.cfg << 'CFGEOF'
-fPIE
-pie
-Wl,--dynamic-linker=/lib/ld.so
CFGEOF

# 7. Build busybox for WOS userspace
echo "=== Phase 7: BusyBox for WOS userspace ==="
cd $B/src
[ ! -d busybox ] && git clone --depth=1 --branch=wos-support https://github.com/Pascu-Victor/busybox.git

WOS_HOST_TOOLCHAIN_ROOT="$HOST" \
    WOS_SYSROOT_PATH="$SYSROOT" \
    WOS_BUSYBOX_BUILD_DIR="$B/busybox-build" \
    WOS_BUSYBOX_INSTALL_DIR="$B/busybox-install" \
    "$B/../scripts/build/build_busybox.sh"
echo "=== Phase 7 complete: BusyBox ==="

# 8. Build Dropbear SSH for WOS userspace
echo "=== Phase 8: Dropbear SSH for WOS userspace ==="
cd $B/src
[ ! -d dropbear ] && git clone --depth=1 --branch=wos-support https://github.com/Pascu-Victor/dropbear.git

WOS_HOST_TOOLCHAIN_ROOT="$HOST" \
    WOS_SYSROOT_PATH="$SYSROOT" \
    WOS_DROPBEAR_BUILD_DIR="$B/dropbear-build" \
    "$B/../scripts/build/build_dropbear.sh"
echo "=== Phase 8 complete: Dropbear ==="

# 9. Build GNU make for WOS userspace
echo "=== Phase 9: GNU make for WOS userspace ==="
WOS_SYSROOT_PATH="$SYSROOT" \
    WOS_MAKE_BUILD_DIR="$B/make-build" \
    "$B/../scripts/build/build_make.sh"
echo "=== Phase 9 complete: GNU make ==="

# 10. Build Bash for WOS userspace
echo "=== Phase 10: Bash for WOS userspace ==="
WOS_SYSROOT_PATH="$SYSROOT" \
    WOS_BASH_SOURCE_DIR="$B/src/bash" \
    WOS_BASH_BUILD_DIR="$B/bash-build" \
    "$B/../scripts/build/build_bash_for_wos.sh"
echo "=== Phase 10 complete: Bash ==="

# 11. Build Ninja for WOS userspace
echo "=== Phase 11: Ninja for WOS userspace ==="
cd "$B/src"
if [ ! -f ninja/CMakeLists.txt ]; then
    if [ -d "$WORKSPACE_ROOT/.git" ]; then
        git -C "$WORKSPACE_ROOT" submodule update --init --depth=1 toolchain/src/ninja || true
    fi
fi
if [ ! -f ninja/CMakeLists.txt ]; then
    git clone --depth=1 https://github.com/Pascu-Victor/ninja.git ninja
fi

WOS_SYSROOT_PATH="$SYSROOT" \
    WOS_NINJA_BUILD_DIR="$B/ninja-build" \
    "$B/../scripts/build/build_ninja_for_wos.sh"
echo "=== Phase 11 complete: Ninja ==="

# 12. Build CMake for WOS userspace
echo "=== Phase 12: CMake for WOS userspace ==="
cd "$B/src"
if [ ! -f cmake/CMakeLists.txt ]; then
    if [ -d "$WORKSPACE_ROOT/.git" ]; then
        git -C "$WORKSPACE_ROOT" submodule update --init toolchain/src/cmake || true
    fi
fi
if [ ! -f cmake/CMakeLists.txt ]; then
    git clone --branch=wos-support https://github.com/Pascu-Victor/CMake.git cmake
fi

WOS_SYSROOT_PATH="$SYSROOT" \
    WOS_CMAKE_FOR_WOS_BUILD_DIR="$B/cmake-wos-build" \
    "$B/../scripts/build/build_cmake_for_wos.sh"
echo "=== Phase 12 complete: CMake ==="

# 13. Build CPython for WOS userspace
echo "=== Phase 13: CPython for WOS userspace ==="
cd "$B/src"
PYTHON_GIT_BRANCH="${WOS_PYTHON_GIT_BRANCH:-wos-support}"
if [ ! -f python/configure ]; then
    if [ -d "$WORKSPACE_ROOT/.git" ]; then
        git -C "$WORKSPACE_ROOT" submodule update --init --depth=1 toolchain/src/python || true
    fi
fi
if [ ! -f python/configure ]; then
    git clone --depth=1 --branch "$PYTHON_GIT_BRANCH" https://github.com/Pascu-Victor/cpython.git python
fi

WOS_SYSROOT_PATH="$SYSROOT" \
    WOS_PYTHON_SOURCE_DIR="$B/src/python" \
    WOS_PYTHON_BUILD_DIR="$B/python-build" \
    "$B/../scripts/build/build_python_for_wos.sh"
echo "=== Phase 13 complete: CPython ==="

# 14. Stage Meson for WOS userspace
echo "=== Phase 14: Meson for WOS userspace ==="
WOS_SYSROOT_PATH="$SYSROOT" \
    WOS_MESON_SOURCE_DIR="$B/src/meson" \
    WOS_MESON_BUILD_DIR="$B/meson-build" \
    "$B/../scripts/build/build_meson_for_wos.sh"
echo "=== Phase 14 complete: Meson ==="

# 15. Build NASM for WOS userspace
echo "=== Phase 15: NASM for WOS userspace ==="
cd "$B/src"
NASM_GIT_BRANCH="${WOS_NASM_GIT_BRANCH:-wos-support}"
if [ ! -f nasm/configure.ac ]; then
    if [ -d "$WORKSPACE_ROOT/.git" ]; then
        git -C "$WORKSPACE_ROOT" submodule update --init --depth=1 toolchain/src/nasm || true
    fi
fi
if [ ! -f nasm/configure.ac ]; then
    git clone --depth=1 --branch "$NASM_GIT_BRANCH" https://github.com/Pascu-Victor/nasm.git nasm
fi

WOS_SYSROOT_PATH="$SYSROOT" \
    WOS_NASM_SOURCE_DIR="$B/src/nasm" \
    WOS_NASM_BUILD_DIR="$B/nasm-build" \
    "$B/../scripts/build/build_nasm_for_wos.sh"
echo "=== Phase 15 complete: NASM ==="

# 16. Build zlib, OpenSSL, and curl for WOS userspace
echo "=== Phase 16: zlib, OpenSSL, and curl for WOS userspace ==="
WOS_SYSROOT_PATH="$SYSROOT" \
    WOS_ZLIB_SOURCE_DIR="$B/src/zlib" \
    WOS_ZLIB_BUILD_DIR="$B/zlib-build" \
    "$B/../scripts/build/build_zlib_for_wos.sh"

WOS_SYSROOT_PATH="$SYSROOT" \
    WOS_OPENSSL_SOURCE_DIR="$B/src/openssl" \
    WOS_OPENSSL_BUILD_DIR="$B/openssl-build" \
    "$B/../scripts/build/build_openssl_for_wos.sh"

WOS_SYSROOT_PATH="$SYSROOT" \
    WOS_CURL_SOURCE_DIR="$B/src/curl" \
    WOS_CURL_BUILD_DIR="$B/curl-build" \
    "$B/../scripts/build/build_curl_for_wos.sh"
echo "=== Phase 16 complete: zlib, OpenSSL, and curl ==="

# 17. Build Git for WOS userspace
echo "=== Phase 17: Git for WOS userspace ==="
WOS_SYSROOT_PATH="$SYSROOT" \
    WOS_GIT_SOURCE_DIR="$B/src/git" \
    WOS_GIT_BUILD_DIR="$B/git-build" \
    "$B/../scripts/build/build_git_for_wos.sh"
echo "=== Phase 17 complete: Git ==="

# 18. Build clang/lld for WOS userspace
echo "=== Phase 18: clang/lld for WOS userspace ==="
WOS_SYSROOT_PATH="$SYSROOT" \
    WOS_CLANG_FOR_WOS_BUILD_DIR="$B/clang-wos-build" \
    "$B/../scripts/build/build_clang_for_wos.sh"
echo "=== Phase 18 complete: clang/lld ==="
