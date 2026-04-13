#!/bin/bash
# Build the WOS target toolchain: sysroot, compiler-rt, mlibc, libc++, busybox, dropbear.
# Requires host-toolchain.sh to have been run first.
#
# Layout:
#   toolchain/host/    - host LLVM binaries (clang, lld, llvm-ar, etc.)
#   toolchain/sysroot/ - WOS target libraries and headers only
set -e

B=$(pwd)/toolchain
OLD_PATH=$PATH
TARGET_ARCH=x86_64-pc-wos
HOST=$B/host
SYSROOT=$B/sysroot
export NINJA_STATUS="[%f/%t %e] "

if [ ! -x "$HOST/bin/clang" ]; then
    echo "ERROR: Host toolchain not found at $HOST/bin/clang"
    echo "Run tools/host-toolchain.sh first."
    exit 1
fi

# Set up environment - host tools on PATH, sysroot for target libs
export CC="$HOST/bin/clang"
export CXX="$HOST/bin/clang++"
export LD="$HOST/bin/ld.lld"
export PATH=$HOST/bin:$OLD_PATH
export LD_LIBRARY_PATH="$HOST/lib"

# 1. Create target directories and empty CRT files
mkdir -p $SYSROOT/bin $SYSROOT/lib $SYSROOT/include/abi-bits
[ ! -e $SYSROOT/usr ] && ln -sf . $SYSROOT/usr
[ ! -e $SYSROOT/lib64 ] && ln -sf lib $SYSROOT/lib64

# headers-only mlibc -- compiler-rt needs these
# Create a basic cross-file for headers-only build
mkdir -p $B/../tools
cat > $B/../tools/x86_64-pc-wos-mlibc.txt << 'EOF'
[binaries]
c = ['clang', '--target=x86_64-pc-wos', '-mcmodel=small']
cpp = ['clang++', '--target=x86_64-pc-wos', '-mcmodel=small']
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
cd $B/mlibc-headers
meson setup --prefix=$SYSROOT \
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
 -DCOMPILER_RT_HAS_SAFESTACK=OFF \
 -DCOMPILER_RT_OS_DIR="" \
 -DCOMPILER_RT_LIBCXXABI_ENABLE_LOCALIZATION=OFF \
 -DCOMPILER_RT_HAS_SCUDO_STANDALONE=OFF \
 -DCOMPILER_RT_BUILD_XRAY=OFF \
 -DCOMPILER_RT_BUILD_SANITIZERS=OFF \
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

# 3. Bootstrap libcxx (headers only, needed by mlibc)

mkdir -p $B/libcxx-bootstrap
cd $B/libcxx-bootstrap
cmake -G Ninja \
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
 -DLIBCXX_USE_COMPILER_RT=On \
 -DLIBCXX_HAS_PTHREAD_API=On \
 -DLIBCXX_HAS_PTHREAD_LIB=On \
 -DLIBCXX_INCLUDE_BENCHMARKS=OFF \
 -DLIBCXX_INCLUDE_TESTS=OFF \
 -DLIBCXXABI_ENABLE_STATIC=OFF \
 -DLIBCXXABI_ENABLE_SHARED=ON \
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

ninja && ninja install

cp -r $B/libcxx-bootstrap/include/* $SYSROOT/include/

# 4. Build mlibc

# Prepare cross-file (always regenerate to ensure correct paths)
mkdir -p $B/../tools
cat > $B/../tools/x86_64-pc-wos-mlibc.txt << EOF
[binaries]
c = ['clang', '--target=x86_64-pc-wos', '--sysroot=$SYSROOT', '-isystem', '$HOST/lib/clang/22/include', '-isystem', '$SYSROOT/include', '-mcmodel=small']
cpp = ['clang++', '--target=x86_64-pc-wos', '--sysroot=$SYSROOT', '-isystem', '$SYSROOT/include/c++/v1', '-isystem', '$HOST/lib/clang/22/include', '-isystem', '$SYSROOT/include', '-mcmodel=small']
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
cd $B/mlibc-build

meson setup --prefix=$SYSROOT \
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

ninja && ninja install

# 5. Build libcxx, libcxxabi, and libunwind (now that mlibc is available)

mkdir -p $B/libcxx-build
cd $B/libcxx-build
cmake -G Ninja \
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
BB_CC="$HOST/bin/clang --target=x86_64-pc-wos --sysroot=$TARGET_SYSROOT"
BB_AR="$HOST/bin/llvm-ar"
BB_STRIP="$HOST/bin/llvm-strip"
BB_RANLIB="$HOST/bin/llvm-ranlib"
BB_OBJCOPY="$HOST/bin/llvm-objcopy"
BB_NM="$HOST/bin/llvm-nm"
BB_HOSTCC="gcc"
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
        HOSTCC="/usr/bin/clang" \
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
        HOSTCC="/usr/bin/clang" \
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
    HOSTCC="/usr/bin/clang" \
    busybox

cp $B/busybox-build/busybox $SYSROOT/bin/busybox
echo "Busybox installed to $SYSROOT/bin/busybox"

# 7. Build Dropbear SSH for WOS userspace
cd $B/src
[ ! -d dropbear ] && git clone --depth=1 --branch=wos-support https://github.com/Pascu-Victor/dropbear.git

mkdir -p $B/dropbear-build
cd $B/dropbear-build

TARGET_SYSROOT="$SYSROOT"
export CC="$HOST/bin/clang --target=x86_64-pc-wos --sysroot=$TARGET_SYSROOT"
export AR="$HOST/bin/llvm-ar"
export RANLIB="$HOST/bin/llvm-ranlib"
export STRIP="$HOST/bin/llvm-strip"
export CFLAGS="--sysroot=$TARGET_SYSROOT -g -O0 -fno-sanitize=safe-stack -fno-stack-protector -I$TARGET_SYSROOT/include"
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
