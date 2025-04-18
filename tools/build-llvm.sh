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
[ ! -d mlibc ] && git clone --depth=1 --branch=master https://github.com/managarm/mlibc.git
[ ! -d llvm-project ] && git clone --depth=1 --branch=main https://github.com/llvm/llvm-project.git
[ ! -d libcxxrt ] && git clone --depth=1 https://github.com/libcxxrt/libcxxrt.git

# 2. Build stage1 clang/lld for the host
mkdir -p $B/stage1-build
cd $B/stage1-build
cmake -G Ninja \
 -DCMAKE_BUILD_TYPE=Release \
 -DCMAKE_INSTALL_PREFIX=$B/stage1-prefix \
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
mkdir -p $B/stage1-prefix/bin
cd $B/stage1-prefix/bin
ln -sf clang cc
ln -sf clang++ c++
ln -sf llvm-ar ar

# 3. Set up environment
export CC="$B/stage1-prefix/bin/clang"
export CXX="$B/stage1-prefix/bin/clang++"
export LD="$B/stage1-prefix/bin/ld.lld"
export PATH=$B/stage1-prefix/bin:$OLD_PATH
export LD_LIBRARY_PATH="$B/stage1-prefix/lib"

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
    $B/src/mlibc
ninja install

# Copy ABI bits
cp -r $B/src/mlibc/abis/mlibc/* $B/target1/include/abi-bits/

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
 -DCMAKE_INSTALL_PREFIX=$B/stage1-prefix/lib/clang/21/target \
 -DCMAKE_C_COMPILER=$CC \
 -DCMAKE_CXX_COMPILER=$CXX \
 -DCMAKE_SYSTEM_NAME=Linux \
 -DCMAKE_SYSROOT=$B/target1 \
 -DCMAKE_C_FLAGS="--sysroot=$B/target1 -nostdlib" \
 -DCMAKE_CXX_FLAGS="--sysroot=$B/target1 -nostdlib" \
 -DCMAKE_ASM_FLAGS="--sysroot=$B/target1 -nostdlib" \
 -DCMAKE_C_COMPILER_TARGET=$TARGET_ARCH \
 -DCMAKE_CXX_COMPILER_TARGET=$TARGET_ARCH \
 -DCMAKE_ASM_COMPILER_TARGET=$TARGET_ARCH \
 -DCMAKE_C_COMPILER_WORKS=ON \
 -DCMAKE_CXX_COMPILER_WORKS=ON \
 -DCMAKE_TRY_COMPILE_TARGET_TYPE=STATIC_LIBRARY \
 -DCOMPILER_RT_BUILD_BUILTINS=ON \
 -DCOMPILER_RT_BAREMETAL_BUILD=ON \
 -DCOMPILER_RT_OS_DIR="" \
 $B/src/llvm-project/compiler-rt/lib/builtins

ninja && ninja install

mkdir -p $B/stage1-prefix/lib/clang/21/lib/$TARGET_ARCH
mv $B/stage1-prefix/lib/clang/21/target/lib/* $B/stage1-prefix/lib/clang/21/lib/$TARGET_ARCH
rm -rf mv $B/stage1-prefix/lib/clang/21/target

ln -s $B/stage1-prefix/lib/clang/21/lib/$TARGET_ARCH/libclang_rt.builtins-x86_64.a $B/stage1-prefix/lib/clang/21/lib/libclang_rt.builtins.a
ln -s $B/stage1-prefix/lib/clang/21/lib/$TARGET_ARCH/clang_rt.crtbegin-x86_64.a $B/stage1-prefix/lib/clang/21/lib/libclang_rt.crtbegin.a
ln -s $B/stage1-prefix/lib/clang/21/lib/$TARGET_ARCH/clang_rt.crtend-x86_64.a $B/stage1-prefix/lib/clang/21/lib/libclang_rt.crtend.a

# 6. Build mlibc
mkdir -p $B/mlibc-build
cd $B/mlibc-build

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

export CFLAGS="--sysroot=$B/target1"
export CXXFLAGS="$CFLAGS"
export LDFLAGS="--sysroot=$B/target1"

meson setup --prefix=$B/target1 \
  --sysconfdir=etc \
  --default-library=static \
  --buildtype=release \
  -Dc_args="$CFLAGS" \
  -Dcpp_args="$CXXFLAGS" \
  -Dc_link_args="$LDFLAGS" \
  -Dcpp_link_args="$LDFLAGS" \
  --cross-file=$B/../tools/x86_64-pc-wos-mlibc.txt \
  -Dheaders_only=false \
  $B/src/mlibc

ninja && ninja install

# Copy mlibc headers to the target directory
mkdir -p $B/target1/include
cp -r $B/src/mlibc/include/* $B/target1/include

# 7. Build libcxx, libcxxabi, and libunwind
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
 -DCMAKE_SYSTEM_NAME=Linux \
 -DCMAKE_SYSROOT=$B/target1 \
 -DCMAKE_C_FLAGS="--sysroot=$B/target1" \
 -DCMAKE_CXX_FLAGS="--sysroot=$B/target1 -nostdinc++" \
 -DCMAKE_CROSSCOMPILING=True \
 -DLLVM_ENABLE_RUNTIMES='libcxx;libcxxabi;libunwind' \
 -DLIBCXX_CXX_ABI=libcxxabi \
 -DLIBCXX_USE_COMPILER_RT=On \
 -DLIBCXX_HAS_PTHREAD_API=On \
 -DLIBCXXABI_USE_LLVM_UNWINDER=ON \
 -DLIBCXXABI_ENABLE_STATIC=ON \
 -DLIBCXXABI_USE_COMPILER_RT=ON \
 -DLIBUNWIND_USE_COMPILER_RT=ON \
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
 -DCMAKE_SYSTEM_NAME=Linux \
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
