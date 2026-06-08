#!/bin/bash
# Build the host toolchain (clang/lld) for cross-compiling to WOS.
# This produces the compiler binaries in toolchain/host/.
# WOS target libraries go into toolchain/sysroot/ (built by wos-toolchain.sh).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

source "$WORKSPACE_ROOT/tools/ccache_env.sh"
wos_setup_ccache
wos_setup_ccache_cmake_args

cd "$WORKSPACE_ROOT"

mkdir -p toolchain
B="$WORKSPACE_ROOT/toolchain"
TARGET_ARCH=x86_64-pc-wos
export NINJA_STATUS="[%f/%t %e] "

# 1. Clone repositories
mkdir -p $B/src
cd $B/src

[ ! -d mlibc ] && git clone --depth=1 --branch=wos-support https://github.com/Pascu-Victor/mlibc.git
[ ! -d llvm-project ] && git clone --depth=1 --branch=wos https://github.com/Pascu-Victor/llvm-project.git
[ ! -d clang-tidy-cache ] && git clone --depth=1 https://github.com/williamfligor/clang-tidy-cache.git

# 2. Build stage1 clang/lld for the host
export CFLAGS="-std=c23"
export CXXFLAGS="-std=c++23"
export CC=clang
export CXX=clang++
mkdir -p $B/host-build
cd $B/host-build
cmake -G Ninja \
 "${WOS_CCACHE_CMAKE_ARGS[@]}" \
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
 $B/src/llvm-project/llvm
ninja install

# 3. Create symlinks for easier use
cd $B/host/bin
ln -sf clang cc
ln -sf clang++ c++
ln -sf llvm-ar ar

# 4. Build clang-tidy-cache into the local host tools directory
cd $B/src/clang-tidy-cache
env -u CC -u CXX -u LD -u CFLAGS -u CXXFLAGS -u CPPFLAGS -u LDFLAGS \
    CGO_ENABLED=0 \
    go build -o $B/host/bin/clang-tidy-cache .

echo "Host toolchain built successfully at $B/host"
