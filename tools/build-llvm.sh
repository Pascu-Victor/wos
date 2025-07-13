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
    $B/src/mlibc
ninja install

# Create empty CRT files
touch empty.c
$CC -O3 -c empty.c       -o $B/target1/lib/crtbegin.o
$CC -O3 -c empty.c -fPIC -o $B/target1/lib/crtbeginS.o
$CC -O3 -c empty.c       -o $B/target1/lib/crtend.o
$CC -O3 -c empty.c -fPIC -o $B/target1/lib/crtendS.o

# 5. Build compiler-rt builtins
mkdir -p $B/compiler-rt-build
cd $B/compiler-rt-build
cmake -G Ninja \
 -DCMAKE_BUILD_TYPE=Release \
 -DCMAKE_INSTALL_PREFIX=$B/target1/lib/clang/21/target \
 -DCMAKE_C_COMPILER=$CC \
 -DCMAKE_CXX_COMPILER=$CXX \
 -DCMAKE_SYSROOT=$B/target1 \
 -DCMAKE_SYSTEM_NAME=WOS \
 -DCMAKE_C_FLAGS="-fdiagnostics-color=always" \
 -DCMAKE_CXX_FLAGS="-fdiagnostics-color=always" \
 -DCMAKE_ASM_FLAGS="-fdiagnostics-color=always" \
 -DCMAKE_C_COMPILER_TARGET=$TARGET_ARCH \
 -DCMAKE_CXX_COMPILER_TARGET=$TARGET_ARCH \
 -DCMAKE_ASM_COMPILER_TARGET=$TARGET_ARCH \
 -DCMAKE_C_COMPILER_WORKS=ON \
 -DCMAKE_CXX_COMPILER_WORKS=ON \
 -DCMAKE_TRY_COMPILE_TARGET_TYPE=STATIC_LIBRARY \
 -DCOMPILER_RT_BUILD_BUILTINS=ON \
 -DCOMPILER_RT_BUILD_MEMPROF=OFF \
 -DCOMPILER_RT_BUILD_ORC=OFF \
 -DCOMPILER_RT_HAS_SAFESTACK=ON \
 -DCOMPILER_RT_OS_DIR="" \
 -DCOMPILER_RT_LIBCXXABI_ENABLE_LOCALIZATION=OFF \
 -DCOMPILER_RT_HAS_SCUDO_STANDALONE=OFF \
 -DCOMPILER_RT_BUILD_XRAY=OFF \
 -DCOMPILER_RT_SANITIZERS_TO_BUILD="safestack" \
 -DCOMPILER_RT_STANDALONE_BUILD=ON \
 -DWOS=ON \
 $B/src/llvm-project/compiler-rt
#  -DCOMPILER_RT_BAREMETAL_BUILD=ON \

ninja && ninja install

mkdir -p $B/target1/lib/clang/21/lib/$TARGET_ARCH
mv $B/target1/lib/clang/21/target/lib/* $B/target1/lib/clang/21/lib/$TARGET_ARCH
rm -rf $B/target1/lib/clang/21/target

ln -s $B/target1/lib/clang/21/lib/$TARGET_ARCH/libclang_rt.builtins-x86_64.a $B/target1/lib/clang/21/lib/libclang_rt.builtins.a
ln -s $B/target1/lib/clang/21/lib/$TARGET_ARCH/clang_rt.crtbegin-x86_64.a $B/target1/lib/clang/21/lib/libclang_rt.crtbegin.a
ln -s $B/target1/lib/clang/21/lib/$TARGET_ARCH/clang_rt.crtend-x86_64.a $B/target1/lib/clang/21/lib/libclang_rt.crtend.a

ln -s $B/target1/lib/clang/21/lib/$TARGET_ARCH/libclang_rt.builtins-x86_64.a $B/target1/lib/clang/21/lib/$TARGET_ARCH/libclang_rt.builtins.a
ln -s $B/target1/lib/clang/21/lib/$TARGET_ARCH/clang_rt.crtbegin-x86_64.a $B/target1/lib/clang/21/lib/$TARGET_ARCH/libclang_rt.crtbegin.a
ln -s $B/target1/lib/clang/21/lib/$TARGET_ARCH/clang_rt.crtend-x86_64.a $B/target1/lib/clang/21/lib/$TARGET_ARCH/libclang_rt.crtend.a

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
 -DLIBCXX_HAS_PTHREAD_API=Off \
 -DLIBCXX_HAS_PTHREAD_LIB=Off \
 -DLIBCXX_INCLUDE_BENCHMARKS=OFF \
 -DLIBCXX_INCLUDE_TESTS=OFF \
 -DLIBCXXABI_ENABLE_STATIC=OFF \
 -DLIBCXXABI_ENABLE_SHARED=ON \
 -DLIBCXX_INSTALL_HEADERS=ON \
 -DLIBCXXABI_INCLUDE_TESTS=OFF \
 -DLIBCXXABI_USE_LLVM_UNWINDER=OFF \
 -DLIBCXXABI_HAS_PTHREAD_API=OFF \
 -DLIBCXXABI_HAS_PTHREAD_LIB=OFF \
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

# Prepare cross-file if it doesn't exist
if [ ! -f $B/../tools/x86_64-pc-wos-mlibc.txt ]; then
  mkdir -p $B/../tools
  cat > $B/../tools/x86_64-pc-wos-mlibc.txt << 'EOF'
[binaries]
c = ['clang', '--target=x86_64-pc-wos']
cpp = ['clang++', '--target=x86_64-pc-wos']
ar = 'llvm-ar'
strip = 'llvm-strip'

[built-in options]
c_args = ['--sysroot=/home/womywomwoo/git/wos/toolchain/target1', '-resource-dir=/home/womywomwoo/git/wos/toolchain/target1/lib/clang/21']
c_link_args = ['--sysroot=/home/womywomwoo/git/wos/toolchain/target1']
cpp_args = ['--sysroot=/home/womywomwoo/git/wos/toolchain/target1', '-resource-dir=/home/womywomwoo/git/wos/toolchain/target1/lib/clang/21']
cpp_link_args = ['--sysroot=/home/womywomwoo/git/wos/toolchain/target1']

[host_machine]
system = 'wos'
cpu_family = 'x86_64'
cpu = 'x86_64'
endian = 'little'
EOF
fi

export CFLAGS="-I$B/target1/include/c++/v1 --sysroot=$B/target1 -fsanitize=safe-stack"
export CXXFLAGS="$CFLAGS"
export LDFLAGS="--sysroot=$B/target1 -fsanitize=safe-stack"

mkdir -p $B/mlibc-build
cd $B/mlibc-build

meson setup --prefix=$B/target1 \
  --sysconfdir=etc \
  --default-library=static \
  --buildtype=release \
  --cross-file=$B/../tools/x86_64-pc-wos-mlibc.txt \
  -Dheaders_only=false \
  -Dwos_option=enabled \
  -Dglibc_option=enabled \
  $B/src/mlibc

ninja && ninja install

# 7. Build libcxx, libcxxabi, and libunwind (now that mlibc is available)
mkdir -p $B/libcxx-build-full
cd $B/libcxx-build-full
cmake -G Ninja \
 -DLLVM_ENABLE_RUNTIMES='libcxx;libcxxabi;libunwind;compiler-rt' \
 -DLIBCXXABI_USE_LLVM_UNWINDER=OFF \
 -DLIBCXXABI_USE_COMPILER_RT=On \
 -DLIBCXXABI_INCLUDE_TESTS=OFF \
 -DLIBCXXABI_HAS_PTHREAD_LIB=OFF \
 -DLIBCXXABI_HAS_PTHREAD_API=OFF \
 -DLIBCXXABI_ENABLE_STATIC=ON \
 -DLIBCXX_USE_COMPILER_RT=On \
 -DLIBCXX_INSTALL_HEADERS=ON \
 -DLIBCXX_INCLUDE_TESTS=OFF \
 -DLIBCXX_INCLUDE_BENCHMARKS=OFF \
 -DLIBCXX_HAS_PTHREAD_LIB=Off \
 -DLIBCXX_HAS_PTHREAD_API=Off \
 -DLIBCXX_ENABLE_LOCALIZATION=OFF \
 -DLIBCXX_ENABLE_FILESYSTEM=OFF \
 -DLIBCXX_ENABLE_WIDE_CHARACTERS=OFF \
 -DLIBCXX_CXX_ABI=libcxxabi \
 -DCMAKE_TRY_COMPILE_TARGET_TYPE=STATIC_LIBRARY \
 -DCMAKE_SYSTEM_NAME=WOS \
 -DCMAKE_SYSROOT=$B/target1 \
 -DCMAKE_INSTALL_PREFIX=$B/target1 \
 -DCMAKE_CXX_FLAGS="-I$B/target1/include -lclang_rt.safestack-x86_64 --sysroot=$B/target1 -fsanitize=safe-stack -fdiagnostics-color=always -L$B/target1/lib/clang/21/target/lib/" \
 -DCMAKE_CXX_COMPILER=$CXX \
 -DCMAKE_CXX_COMPILER_WORKS=ON \
 -DCMAKE_CXX_COMPILER_TARGET=$TARGET_ARCH \
 -DCMAKE_CROSSCOMPILING=True \
 -DCMAKE_C_FLAGS="-I$B/target1/include -lclang_rt.safestack-x86_64 --sysroot=$B/target1 -fsanitize=safe-stack -fdiagnostics-color=always -L$B/target1/lib/clang/21/target/lib/" \
 -DCMAKE_C_COMPILER=$CC \
 -DCMAKE_C_COMPILER_WORKS=ON \
 -DCMAKE_C_COMPILER_TARGET=$TARGET_ARCH \
 -DCMAKE_BUILD_TYPE=Release \
 -DCMAKE_ASM_COMPILER_TARGET=$TARGET_ARCH \
 -DCOMPILER_RT_BUILD_BUILTINS=ON \
 -DCOMPILER_RT_BUILD_MEMPROF=OFF \
 -DCOMPILER_RT_BUILD_ORC=OFF \
 -DCOMPILER_RT_HAS_SAFESTACK=ON \
 -DCOMPILER_RT_OS_DIR="" \
 -DCOMPILER_RT_LIBCXXABI_ENABLE_LOCALIZATION=OFF \
 -DCOMPILER_RT_HAS_SCUDO_STANDALONE=OFF \
 -DCOMPILER_RT_BUILD_XRAY=OFF \
 -DCOMPILER_RT_SANITIZERS_TO_BUILD="safestack" \
 -DCOMPILER_RT_STANDALONE_BUILD=ON \
 -DWOS=ON \
 $B/src/llvm-project/runtimes

ninja && ninja install

# 8. Build full clang toolchain targeting WOS
mkdir -p $B/stage2-build
cd $B/stage2-build
cmake -G Ninja \
 -DCMAKE_BUILD_TYPE=Release \
 -DCMAKE_INSTALL_PREFIX=$B/target1 \
 -DCMAKE_C_COMPILER=$CC \
 -DCMAKE_CXX_COMPILER=$CXX \
 -DCMAKE_CXX_FLAGS="-stdlib=libc++ -I$B/target1/include/c++/v1" \
 -DCMAKE_C_COMPILER_WORKS=ON \
 -DCMAKE_CXX_COMPILER_WORKS=ON \
 -DCMAKE_TRY_COMPILE_TARGET_TYPE=STATIC_LIBRARY \
 -DLLVM_ENABLE_PROJECTS='clang;lld' \
 -DLLVM_TARGETS_TO_BUILD=X86 \
 -DLLVM_DEFAULT_TARGET_TRIPLE=$TARGET_ARCH \
 -DLLVM_ENABLE_LLD=On \
 -DLLVM_ENABLE_LIBXML2=Off \
 -DCLANG_DEFAULT_LINKER=lld \
 -DCLANG_DEFAULT_CXX_STDLIB=libc++ \
 -DCLANG_DEFAULT_RTLIB=compiler-rt \
 -DLLVM_INCLUDE_TESTS=OFF \
 -DLLVM_INCLUDE_EXAMPLES=OFF \
 -DLLVM_INCLUDE_BENCHMARKS=OFF \
 $B/src/llvm-project/llvm

ninja && ninja install

echo "WOS toolchain build complete!"
