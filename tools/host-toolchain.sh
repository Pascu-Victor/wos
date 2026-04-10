#!/bin/bash
# Build the host toolchain (clang/lld) for cross-compiling to WOS.
# This produces the compiler binaries in toolchain/host/.
# WOS target libraries go into toolchain/sysroot/ (built by wos-toolchain.sh).
set -e

mkdir -p toolchain
B=$(pwd)/toolchain
TARGET_ARCH=x86_64-pc-wos
export NINJA_STATUS="[%f/%t %e] "

# 1. Clone repositories
mkdir -p $B/src
cd $B/src

[ ! -d mlibc ] && git clone --depth=1 --branch=wos-support https://github.com/Pascu-Victor/mlibc.git
[ ! -d llvm-project ] && git clone --depth=1 --branch=wos https://github.com/Pascu-Victor/llvm-project.git

# 2. Build stage1 clang/lld for the host
export CFLAGS="-std=c23"
export CXXFLAGS="-std=c++23"
export CC=clang
export CXX=clang++
mkdir -p $B/host-build
cd $B/host-build
cmake -G Ninja \
 -DCMAKE_BUILD_TYPE=Release \
 -DCMAKE_INSTALL_PREFIX=$B/host \
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

# 3. Create symlinks for easier use
cd $B/host/bin
ln -sf clang cc
ln -sf clang++ c++
ln -sf llvm-ar ar

echo "Host toolchain built successfully at $B/host"
