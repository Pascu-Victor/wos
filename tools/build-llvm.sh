#!/bin/bash
set -e  # Exit on error

mkdir -p toolchain
B=$(pwd)/toolchain
OLD_PATH=$PATH
TARGET_ARCH=x86_64-pc-wos
export NINJA_STATUS="[%f/%t %e] "


# 1. Clone repositories
mkdir -p $B/src
cd $B/src


# Clone required repositories if they don't exist
[ ! -d mlibc ] && git clone --depth=1 --branch=wos-support https://github.com/Pascu-Victor/mlibc.git
[ ! -d llvm-project ] && git clone --depth=1 --branch=wos https://github.com/Pascu-Victor/llvm-project.git

# 2. Build stage1 clang/lld for the host
export CFLAGS="-std=c23"
export CXXFLAGS="-std=c++23"
export CC=clang
export CXX=clang++
mkdir -p $B/stage1-build
cd $B/stage1-build
cmake -G Ninja \
 -DCMAKE_BUILD_TYPE=Release \
 -DCMAKE_INSTALL_PREFIX=$B/target1 \
 -DLLVM_TARGETS_TO_BUILD=X86 \
 -DLLVM_ENABLE_PROJECTS='clang;lld' \
 -DLLVM_ENABLE_LIBXML2=Off \
 -DLLVM_ENABLE_LLD=On \
 -DLLVM_DEFAULT_TARGET_TRIPLE=$TARGET_ARCH \
 -DCLANG_DEFAULT_LINKER=lld \
 -DLLVM_INCLUDE_TESTS=OFF \
 -DLLVM_INCLUDE_EXAMPLES=OFF \
 -DLLVM_INCLUDE_BENCHMARKS=OFF \
 -DLLVM_INCLUDE_DOCS=OFF \
 $B/src/llvm-project/llvm && ninja install

# Create symlinks for easier use
mkdir -p $B/target1/bin
cd $B/target1/bin
ln -sf clang cc
ln -sf clang++ c++
ln -sf llvm-ar ar

# 3. Set up environment
export CC="$B/target1/bin/clang"
export CXX="$B/target1/bin/clang++"
export LD="$B/target1/bin/ld.lld"
export PATH=$B/target1/bin:$OLD_PATH
export LD_LIBRARY_PATH="$B/target1/lib"

# 4. Create target directories and empty CRT files
mkdir -p $B/target1/lib $B/target1/include/abi-bits
ln -sf . $B/target1/usr
ln -sf lib $B/target1/lib64

# headers-only mlibc -- compiler-rt needs these
# Create a basic cross-file for headers-only build
mkdir -p $B/../tools
cat > $B/../tools/x86_64-pc-wos-mlibc.txt << 'EOF'
[binaries]
c = ['clang', '--target=x86_64-pc-wos',  '-fno-PIC', '-mcmodel=small']
cpp = ['clang++', '--target=x86_64-pc-wos',  '-fno-PIC', '-mcmodel=small']
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
meson setup --prefix=$B/target1 \
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
$CC -O3 -c empty.c       -o $B/target1/lib/crtbegin.o
$CC -O3 -c empty.c -fPIC -o $B/target1/lib/crtbeginS.o
$CC -O3 -c empty.c       -o $B/target1/lib/crtend.o
$CC -O3 -c empty.c -fPIC -o $B/target1/lib/crtendS.o

# Create temporary CRT startup files for compiler-rt (will be replaced by mlibc)
$CC -O3 -c empty.c       -o $B/target1/lib/Scrt1.o
$CC -O3 -c empty.c       -o $B/target1/lib/crt1.o
$CC -O3 -c empty.c       -o $B/target1/lib/crti.o
$CC -O3 -c empty.c       -o $B/target1/lib/crtn.o

# 5. Build compiler-rt builtins
export CFLAGS="--sysroot=$B/target1 -std=c23 -fno-sanitize=safe-stack "
export CXXFLAGS="--sysroot=$B/target1 -std=c++23 -fno-sanitize=safe-stack "
export LDFLAGS="--sysroot=$B/target1"
mkdir -p $B/compiler-rt-build
cd $B/compiler-rt-build
cmake -G Ninja \
 -DCMAKE_BUILD_TYPE=Release \
 -DCMAKE_INSTALL_PREFIX=$B/target1/lib/clang/22/target \
 -DCMAKE_C_COMPILER=$CC \
 -DCMAKE_CXX_COMPILER=$CXX \
 -DCMAKE_SYSROOT=$B/target1 \
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
#  -DCOMPILER_RT_BAREMETAL_BUILD=ON \

ninja && ninja install

mkdir -p $B/target1/lib/clang/22/lib/$TARGET_ARCH
mv $B/target1/lib/clang/22/target/lib/* $B/target1/lib/clang/22/lib/$TARGET_ARCH
mv $B/target1/lib/clang/22/target/include $B/target1/lib/clang/22/include

rm -rf $B/target1/lib/clang/22/target

ln -fs $B/target1/lib/clang/22/lib/$TARGET_ARCH/libclang_rt.builtins-x86_64.a $B/target1/lib/clang/22/lib/libclang_rt.builtins.a
ln -fs $B/target1/lib/clang/22/lib/$TARGET_ARCH/libclang_rt.crtbegin-x86_64.a $B/target1/lib/clang/22/lib/libclang_rt.crtbegin.a
ln -fs $B/target1/lib/clang/22/lib/$TARGET_ARCH/libclang_rt.crtend-x86_64.a $B/target1/lib/clang/22/lib/libclang_rt.crtend.a

ln -fs $B/target1/lib/clang/22/lib/$TARGET_ARCH/libclang_rt.builtins-x86_64.a $B/target1/lib/clang/22/lib/$TARGET_ARCH/libclang_rt.builtins.a
ln -fs $B/target1/lib/clang/22/lib/$TARGET_ARCH/libclang_rt.crtbegin-x86_64.a $B/target1/lib/clang/22/lib/$TARGET_ARCH/libclang_rt.crtbegin.a
ln -fs $B/target1/lib/clang/22/lib/$TARGET_ARCH/libclang_rt.crtend-x86_64.a $B/target1/lib/clang/22/lib/$TARGET_ARCH/libclang_rt.crtend.a

# 6.1 fail building libcxx for it's headers see {{cxx-headers}} maybe

mkdir -p $B/libcxx-build
cd $B/libcxx-build
cmake -G Ninja \
 -DCMAKE_BUILD_TYPE=Release \
 -DCMAKE_INSTALL_PREFIX=$B/target1 \
 -DCMAKE_C_COMPILER=$CC \
 -DCMAKE_CXX_COMPILER=$CXX \
 -DCMAKE_C_COMPILER_TARGET=$TARGET_ARCH \
 -DCMAKE_CXX_COMPILER_TARGET=$TARGET_ARCH \
 -DCMAKE_ASM_COMPILER_TARGET=$TARGET_ARCH \
 -DCMAKE_C_COMPILER_WORKS=ON \
 -DCMAKE_CXX_COMPILER_WORKS=ON \
 -DCMAKE_TRY_COMPILE_TARGET_TYPE=STATIC_LIBRARY \
 -DCMAKE_SYSROOT=$B/target1 \
 -DCMAKE_C_FLAGS="--sysroot=$B/target1" \
 -DCMAKE_CXX_FLAGS="--sysroot=$B/target1" \
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

#copy $B/libcxx-build/include to $B/target1/include

cp -r $B/libcxx-build/include/* $B/target1/include/

# 6. Build mlibc

# Prepare cross-file (always regenerate to ensure correct paths)
mkdir -p $B/../tools
cat > $B/../tools/x86_64-pc-wos-mlibc.txt << EOF
[binaries]
c = ['clang', '--target=x86_64-pc-wos', '--sysroot=$B/target1', '-isystem', '$B/target1/lib/clang/22/include', '-isystem', '$B/target1/include', '-mcmodel=small']
cpp = ['clang++', '--target=x86_64-pc-wos', '--sysroot=$B/target1', '-isystem', '$B/target1/include/c++/v1', '-isystem', '$B/target1/lib/clang/22/include', '-isystem', '$B/target1/include', '-fno-PIC', '-mcmodel=small']
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
export CFLAGS="--sysroot=$B/target1 -std=c23 -fno-sanitize=safe-stack "
export CXXFLAGS="--sysroot=$B/target1 -std=c++23 -fno-sanitize=safe-stack "
export LDFLAGS="--sysroot=$B/target1"

mkdir -p $B/mlibc-build
cd $B/mlibc-build

meson setup --prefix=$B/target1 \
  --sysconfdir=etc \
  --buildtype=release \
  --cross-file=$B/../tools/x86_64-pc-wos-mlibc.txt \
  -Dheaders_only=false \
  -Dwos_option=enabled \
  -Dglibc_option=enabled \
  -Ddefault_library=both \
  -Duse_freestnd_hdrs=enabled \
  -Dposix_option=enabled \
  -Dbsd_option=enabled \
  -Db_sanitize=none \
  $B/src/mlibc

ninja && ninja install

# 7. Build libcxx, libcxxabi, and libunwind (now that mlibc is available)

mkdir -p $B/libcxx-build-full
cd $B/libcxx-build-full
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
 -DCMAKE_SYSROOT=$B/target1 \
 -DCMAKE_INSTALL_PREFIX=$B/target1 \
 -DCMAKE_CXX_FLAGS="-I$B/target1/include --sysroot=$B/target1 -fno-sanitize=safe-stack -fdiagnostics-color=always" \
 -DCMAKE_CXX_COMPILER=$CXX \
 -DCMAKE_CXX_COMPILER_WORKS=ON \
 -DCMAKE_CXX_COMPILER_TARGET=$TARGET_ARCH \
 -DCMAKE_CROSSCOMPILING=True \
 -DCMAKE_C_FLAGS="-I$B/target1/include --sysroot=$B/target1 -fno-sanitize=safe-stack -fdiagnostics-color=always" \
 -DCMAKE_C_COMPILER=$CC \
 -DCMAKE_C_COMPILER_WORKS=ON \
 -DCMAKE_C_COMPILER_TARGET=$TARGET_ARCH \
 -DCMAKE_BUILD_TYPE=Release \
 -DCMAKE_ASM_COMPILER_TARGET=$TARGET_ARCH \
 -DCMAKE_SHARED_LINKER_FLAGS="-L$B/target1/lib/clang/22/lib/$TARGET_ARCH" \
 -DCMAKE_EXE_LINKER_FLAGS="-L$B/target1/lib/clang/22/lib/$TARGET_ARCH" \
 -DWOS=ON \
 $B/src/llvm-project/runtimes

ninja && ninja install

# 8. Build busybox for WOS userspace
cd $B/src
[ ! -d busybox ] && git clone --depth=1 --branch=wos-support https://github.com/Pascu-Victor/busybox.git

mkdir -p $B/busybox-build
cd $B/busybox-build

# Cross-compilation variables
TARGET_SYSROOT="$B/target1"
BB_CC="$TARGET_SYSROOT/bin/clang --target=x86_64-pc-wos --sysroot=$TARGET_SYSROOT"
BB_AR="$TARGET_SYSROOT/bin/llvm-ar"
BB_STRIP="$TARGET_SYSROOT/bin/llvm-strip"
BB_RANLIB="$TARGET_SYSROOT/bin/llvm-ranlib"
BB_OBJCOPY="$TARGET_SYSROOT/bin/llvm-objcopy"
BB_NM="$TARGET_SYSROOT/bin/llvm-nm"
BB_HOSTCC="gcc"
BB_CFLAGS="--sysroot=$TARGET_SYSROOT -static -fno-sanitize=safe-stack -fno-stack-protector"
BB_LDFLAGS="--sysroot=$TARGET_SYSROOT -static -fuse-ld=lld"

# Use the WOS defconfig from the fork if available, otherwise use default.
# KCONFIG_ALLCONFIG ensures we start from allnoconfig (everything disabled)
# and only enable what's explicitly listed in the defconfig.
if [ -f $B/src/busybox/configs/wos_defconfig ]; then
    make -C $B/src/busybox O=$B/busybox-build \
        CROSS_COMPILE="$B/target1/bin/" \
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
        CROSS_COMPILE="$B/target1/bin/" \
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

# Build busybox as a static binary targeting WOS
make -C $B/busybox-build -j$(nproc) \
    CROSS_COMPILE="$B/target1/bin/" \
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

# Install busybox into the sysroot
cp $B/busybox-build/busybox $B/target1/bin/busybox
echo "Busybox installed to $B/target1/bin/busybox"

# 9. Build Dropbear SSH for WOS userspace
cd $B/src
[ ! -d dropbear ] && git clone --depth=1 https://github.com/Pascu-Victor/dropbear.git

mkdir -p $B/dropbear-build
cd $B/dropbear-build

# Cross-compilation environment for Dropbear
TARGET_SYSROOT="$B/target1"
export CC="$TARGET_SYSROOT/bin/clang --target=x86_64-pc-wos --sysroot=$TARGET_SYSROOT"
export AR="$TARGET_SYSROOT/bin/llvm-ar"
export RANLIB="$TARGET_SYSROOT/bin/llvm-ranlib"
export STRIP="$TARGET_SYSROOT/bin/llvm-strip"
export CFLAGS="--sysroot=$TARGET_SYSROOT -static -g -O0 -fno-sanitize=safe-stack -fno-stack-protector -I$TARGET_SYSROOT/include"
export LDFLAGS="--sysroot=$TARGET_SYSROOT -static -fuse-ld=lld -L$TARGET_SYSROOT/lib"

# Run autoconf if configure doesn't exist yet
if [ ! -f "$B/src/dropbear/configure" ]; then
    (cd $B/src/dropbear && autoconf && autoheader)
fi

# Patch config.sub to recognise WOS as a valid OS
if ! grep -q 'wos\*' "$B/src/dropbear/src/config.sub" 2>/dev/null; then
    sed -i 's/| fiwix\* | mlibc\* | cos\* | mbr\* )/| fiwix* | mlibc* | cos* | mbr* | wos* )/' \
        "$B/src/dropbear/src/config.sub"
fi

$B/src/dropbear/configure \
    --host=x86_64-pc-wos \
    --prefix="$TARGET_SYSROOT" \
    --enable-static \
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

# Install dropbearmulti into the sysroot
cp $B/dropbear-build/dropbearmulti $B/target1/bin/dropbearmulti
echo "Dropbear installed to $B/target1/bin/dropbearmulti"
