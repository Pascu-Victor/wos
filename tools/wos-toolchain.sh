#!/bin/bash
# Build the WOS target toolchain: sysroot, compiler-rt, mlibc, libc++, busybox,
# dropbear, GNU make, Ninja, CMake, NASM, OpenSSL, curl, and Git.
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
WOS_CCACHE_PREFIX="$(wos_ccache_prefix)"
WOS_MESON_COMPILER_PREFIX="$(wos_meson_compiler_prefix)"

cd "$WORKSPACE_ROOT"

B="$WORKSPACE_ROOT/toolchain"
OLD_PATH=$PATH
TARGET_ARCH=x86_64-pc-wos
HOST="${WOS_HOST_TOOLCHAIN_ROOT:-$B/host}"
SYSROOT=$B/sysroot
export WOS_HOST_TOOLCHAIN_ROOT="$HOST"
export NINJA_STATUS="[%f/%t %e] "

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
ninja install

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

# 2. Build compiler-rt builtins
# These go into the host compiler's resource dir since clang looks for them there.
export CFLAGS="--sysroot=$SYSROOT -std=c23 -fno-sanitize=safe-stack "
export CXXFLAGS="--sysroot=$SYSROOT -std=c++23 -fno-sanitize=safe-stack "
export LDFLAGS="--sysroot=$SYSROOT"
mkdir -p $B/compiler-rt-build
cd $B/compiler-rt-build
cmake -G Ninja \
 "${WOS_CCACHE_CMAKE_ARGS[@]}" \
 -DCMAKE_BUILD_TYPE=Release \
 -DCMAKE_INSTALL_PREFIX=$HOST/lib/clang/22/target \
 -DCMAKE_C_COMPILER=$CC \
 -DCMAKE_CXX_COMPILER=$CXX \
 -DCMAKE_SYSROOT=$SYSROOT \
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
 -DCOMPILER_RT_BUILD_SANITIZERS=ON \
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

ninja && ninja install

mkdir -p $HOST/lib/clang/22/lib/$TARGET_ARCH
cp -a $HOST/lib/clang/22/target/lib/* $HOST/lib/clang/22/lib/$TARGET_ARCH/
cp -a $HOST/lib/clang/22/target/include/* $HOST/lib/clang/22/include/ 2>/dev/null || \
  cp -a $HOST/lib/clang/22/target/include $HOST/lib/clang/22/include

rm -rf $HOST/lib/clang/22/target

ln -fs $HOST/lib/clang/22/lib/$TARGET_ARCH/libclang_rt.builtins-x86_64.a $HOST/lib/clang/22/lib/libclang_rt.builtins.a
ln -fs $HOST/lib/clang/22/lib/$TARGET_ARCH/libclang_rt.crtbegin-x86_64.a $HOST/lib/clang/22/lib/libclang_rt.crtbegin.a
ln -fs $HOST/lib/clang/22/lib/$TARGET_ARCH/libclang_rt.crtend-x86_64.a $HOST/lib/clang/22/lib/libclang_rt.crtend.a

ln -fs $HOST/lib/clang/22/lib/$TARGET_ARCH/libclang_rt.builtins-x86_64.a $HOST/lib/clang/22/lib/$TARGET_ARCH/libclang_rt.builtins.a
ln -fs $HOST/lib/clang/22/lib/$TARGET_ARCH/libclang_rt.crtbegin-x86_64.a $HOST/lib/clang/22/lib/$TARGET_ARCH/libclang_rt.crtbegin.a
ln -fs $HOST/lib/clang/22/lib/$TARGET_ARCH/libclang_rt.crtend-x86_64.a $HOST/lib/clang/22/lib/$TARGET_ARCH/libclang_rt.crtend.a
ln -fs $HOST/lib/clang/22/lib/$TARGET_ARCH/libclang_rt.asan_static-x86_64.a $HOST/lib/clang/22/lib/$TARGET_ARCH/libclang_rt.asan_static.a
ln -fs $HOST/lib/clang/22/lib/$TARGET_ARCH/libclang_rt.asan-x86_64.a $HOST/lib/clang/22/lib/$TARGET_ARCH/libclang_rt.asan.a
ln -fs $HOST/lib/clang/22/lib/$TARGET_ARCH/libclang_rt.asan_cxx-x86_64.a $HOST/lib/clang/22/lib/$TARGET_ARCH/libclang_rt.asan_cxx.a
if [ ! -f $HOST/lib/clang/22/lib/$TARGET_ARCH/libclang_rt.profile-x86_64.a ]; then
  echo "ERROR: compiler-rt did not install libclang_rt.profile-x86_64.a" >&2
  exit 1
fi
ln -fs $HOST/lib/clang/22/lib/$TARGET_ARCH/libclang_rt.profile-x86_64.a $HOST/lib/clang/22/lib/$TARGET_ARCH/libclang_rt.profile.a

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

ninja install-cxx-headers install-cxxabi-headers

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
ninja && ninja install

# 5. Build libcxx, libcxxabi, and libunwind (now that mlibc is available)

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

ninja && ninja install

# Generate Clang config file for WOS target triple
# Must be after all library builds (mlibc, libc++) but before userspace binaries
cat > $HOST/bin/x86_64-pc-wos.cfg << 'CFGEOF'
-fPIE
-pie
-Wl,--dynamic-linker=/lib/ld.so
CFGEOF

# 6. Build busybox for WOS userspace
cd $B/src
[ ! -d busybox ] && git clone --depth=1 --branch=wos-support https://github.com/Pascu-Victor/busybox.git

mkdir -p $B/busybox-build
cd $B/busybox-build

# Cross-compilation variables
TARGET_SYSROOT="$SYSROOT"
BB_CC="${WOS_CCACHE_PREFIX}$HOST/bin/clang --target=x86_64-pc-wos --sysroot=$TARGET_SYSROOT"
BB_AR="$HOST/bin/llvm-ar"
BB_STRIP="$HOST/bin/llvm-strip"
BB_RANLIB="$HOST/bin/llvm-ranlib"
BB_OBJCOPY="$HOST/bin/llvm-objcopy"
BB_NM="$HOST/bin/llvm-nm"
BB_HOSTCC="${WOS_CCACHE_PREFIX}gcc"
BB_CFLAGS="--sysroot=$TARGET_SYSROOT -fno-sanitize=safe-stack -fno-stack-protector"
BB_LDFLAGS="--sysroot=$TARGET_SYSROOT -fuse-ld=lld"

if [ -f $B/src/busybox/configs/wos_defconfig ]; then
    make -C $B/src/busybox O=$B/busybox-build \
        CROSS_COMPILE="$HOST/bin/" \
        CC="$BB_CC" \
        AR="$BB_AR" \
        STRIP="$BB_STRIP" \
        RANLIB="$BB_RANLIB" \
        OBJCOPY="$BB_OBJCOPY" \
        NM="$BB_NM" \
        HOSTCC="$BB_HOSTCC" \
        CFLAGS="$BB_CFLAGS" \
        LDFLAGS="$BB_LDFLAGS" \
        HOSTCC="${WOS_CCACHE_PREFIX}/usr/bin/clang" \
        KCONFIG_ALLCONFIG=$B/src/busybox/configs/wos_defconfig \
        allnoconfig
else
    make -C $B/src/busybox O=$B/busybox-build \
        CROSS_COMPILE="$HOST/bin/" \
        CC="$BB_CC" \
        AR="$BB_AR" \
        STRIP="$BB_STRIP" \
        RANLIB="$BB_RANLIB" \
        OBJCOPY="$BB_OBJCOPY" \
        NM="$BB_NM" \
        HOSTCC="$BB_HOSTCC" \
        CFLAGS="$BB_CFLAGS" \
        LDFLAGS="$BB_LDFLAGS" \
        HOSTCC="${WOS_CCACHE_PREFIX}/usr/bin/clang" \
        defconfig
fi

make -C $B/busybox-build -j$(nproc) \
    CROSS_COMPILE="$HOST/bin/" \
    CC="$BB_CC" \
    AR="$BB_AR" \
    STRIP="$BB_STRIP" \
    RANLIB="$BB_RANLIB" \
    OBJCOPY="$BB_OBJCOPY" \
    NM="$BB_NM" \
    HOSTCC="$BB_HOSTCC" \
    CFLAGS="$BB_CFLAGS" \
    LDFLAGS="$BB_LDFLAGS" \
    HOSTCC="${WOS_CCACHE_PREFIX}/usr/bin/clang" \
    busybox

cp $B/busybox-build/busybox $SYSROOT/bin/busybox
echo "Busybox installed to $SYSROOT/bin/busybox"

# 7. Build Dropbear SSH for WOS userspace
cd $B/src
[ ! -d dropbear ] && git clone --depth=1 --branch=wos-support https://github.com/Pascu-Victor/dropbear.git

mkdir -p $B/dropbear-build
cd $B/dropbear-build

cat > localoptions.h <<'EOF'
#define DROPBEAR_SFTPSERVER 1
#define SFTPSERVER_PATH "/usr/libexec/sftp-server"
#define DROPBEAR_PATH_SSH_PROGRAM "/usr/bin/dbclient"
#define DROPBEAR_SMALL_CODE 0
#define DEFAULT_RECV_WINDOW (1024 * 1024)
#define RECV_MAX_PAYLOAD_LEN (128 * 1024)
#define TRANS_MAX_PAYLOAD_LEN (64 * 1024)
EOF

TARGET_SYSROOT="$SYSROOT"
DROPBEAR_CFLAGS="--sysroot=$TARGET_SYSROOT -O3 -g -fno-sanitize=safe-stack -fno-stack-protector -I$TARGET_SYSROOT/include"
export CC="${WOS_CCACHE_PREFIX}$HOST/bin/clang --target=x86_64-pc-wos --sysroot=$TARGET_SYSROOT"
export AR="$HOST/bin/llvm-ar"
export RANLIB="$HOST/bin/llvm-ranlib"
export STRIP="$HOST/bin/llvm-strip"
export CFLAGS="$DROPBEAR_CFLAGS"
export LDFLAGS="--sysroot=$TARGET_SYSROOT -fuse-ld=lld -L$TARGET_SYSROOT/lib"

if [ ! -f "$B/src/dropbear/configure" ]; then
    (cd $B/src/dropbear && autoconf && autoheader)
fi

if ! grep -q 'wos\*' "$B/src/dropbear/src/config.sub" 2>/dev/null; then
    sed -i 's/| fiwix\* | mlibc\* | cos\* | mbr\* )/| fiwix* | mlibc* | cos* | mbr* | wos* )/' \
        "$B/src/dropbear/src/config.sub"
fi

$B/src/dropbear/configure \
    --host=x86_64-pc-wos \
    --prefix="$TARGET_SYSROOT" \
    --enable-bundled-libtom \
    --disable-zlib \
    --disable-pam \
    --disable-wtmp \
    --disable-utmp \
    --disable-utmpx \
    --disable-lastlog \
    --disable-syslog \
    --disable-harden

make -j$(nproc) PROGRAMS="dropbear dbclient dropbearkey scp" MULTI=1 dropbearmulti

cp $B/dropbear-build/dropbearmulti $SYSROOT/bin/dropbearmulti
echo "Dropbear installed to $SYSROOT/bin/dropbearmulti"

# 8. Build GNU make for WOS userspace
WOS_SYSROOT_PATH="$SYSROOT" \
    WOS_MAKE_BUILD_DIR="$B/make-build" \
    "$B/../scripts/build/build_make.sh"

# 9. Build Ninja for WOS userspace
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

# 10. Build CMake for WOS userspace
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

# 11. Build CPython for WOS userspace
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

# 12. Build NASM for WOS userspace
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

# 13. Build zlib, OpenSSL, and curl for WOS userspace
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

# 14. Build Git for WOS userspace
WOS_SYSROOT_PATH="$SYSROOT" \
    WOS_GIT_SOURCE_DIR="$B/src/git" \
    WOS_GIT_BUILD_DIR="$B/git-build" \
    "$B/../scripts/build/build_git_for_wos.sh"
