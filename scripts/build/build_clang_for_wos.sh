#!/bin/bash
# Cross-build a small native clang/lld toolchain that can run inside WOS.
# Installs the result into the target sysroot so the rootfs manifest can stage
# the compiler, linker, helper tools, and clang resource directory.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$WORKSPACE_ROOT/tools/ccache_env.sh"
if [ -z "${CCACHE_DIR:-}" ]; then
    export CCACHE_DIR="${TMPDIR:-/tmp}/wos-clang-for-wos-ccache"
    mkdir -p "$CCACHE_DIR"
fi
wos_setup_ccache
wos_setup_ccache_cmake_args
WOS_BUILD_JOBS="$(wos_build_jobs)"
WOS_NINJA_JOBS="$(wos_ninja_jobs)"
WOS_LLVM_PARALLEL_LINK_JOBS="${WOS_LLVM_PARALLEL_LINK_JOBS:-$WOS_NINJA_JOBS}"
case "$WOS_LLVM_PARALLEL_LINK_JOBS" in
    ''|*[!0-9]*|0)
        echo "ERROR: WOS_LLVM_PARALLEL_LINK_JOBS must be a positive integer, got '$WOS_LLVM_PARALLEL_LINK_JOBS'" >&2
        exit 1
        ;;
esac

B="$WORKSPACE_ROOT/toolchain"
HOST="${WOS_HOST_TOOLCHAIN_ROOT:-$B/host}"
TARGET_SYSROOT="${WOS_SYSROOT_PATH:-$B/sysroot}"
CLANG_BUILD="${WOS_CLANG_FOR_WOS_BUILD_DIR:-$B/clang-wos-build}"
LLVM_SRC="$B/src/llvm-project/llvm"
TARGET_ARCH="${WOS_TARGET_ARCH:-x86_64-pc-wos}"
CLANG_VERSION="${WOS_CLANG_VERSION:-22}"
HOST_PYTHON="${HOST_PYTHON:-$(command -v python3)}"

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
require_file "$HOST/bin/llvm-tblgen" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/clang-tblgen" "Run tools/host-toolchain.sh first."
require_file "$LLVM_SRC/CMakeLists.txt" "Run tools/bootstrap.sh or initialise toolchain/src/llvm-project."
require_file "$TARGET_SYSROOT/lib/libc.so" "Build mlibc before building native clang."
require_file "$TARGET_SYSROOT/lib/libc++.so" "Build libc++ before building native clang."
require_file "$TARGET_SYSROOT/lib/Scrt1.o" "Build mlibc startup objects before building native clang."

mkdir -p "$CLANG_BUILD" "$TARGET_SYSROOT/bin" "$TARGET_SYSROOT/lib"

TARGET_C_FLAGS="--sysroot=$TARGET_SYSROOT -fPIC -fPIE -fno-sanitize=safe-stack -fno-stack-protector -fdiagnostics-color=always"
TARGET_CXX_FLAGS="$TARGET_C_FLAGS -std=c++23 -isystem $TARGET_SYSROOT/include/c++/v1"
TARGET_LINK_FLAGS="--sysroot=$TARGET_SYSROOT -fuse-ld=lld -L$TARGET_SYSROOT/lib -Wl,--dynamic-linker=/lib/ld.so -Wl,-rpath,/usr/lib -fno-sanitize=safe-stack"
TARGET_CXX_STANDARD_LIBRARIES="-lc++ -lc++abi -lunwind -lm -lpthread -ldl -lrt -lc"

cmake -S "$LLVM_SRC" -B "$CLANG_BUILD" -G Ninja \
    "${WOS_CCACHE_CMAKE_ARGS[@]}" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="$TARGET_SYSROOT/usr" \
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
    -DCMAKE_C_FLAGS="$TARGET_C_FLAGS" \
    -DCMAKE_CXX_FLAGS="$TARGET_CXX_FLAGS" \
    -DCMAKE_EXE_LINKER_FLAGS="$TARGET_LINK_FLAGS" \
    -DCMAKE_SHARED_LINKER_FLAGS="$TARGET_LINK_FLAGS" \
    -DCMAKE_CXX_STANDARD_LIBRARIES="$TARGET_CXX_STANDARD_LIBRARIES" \
    -DPython3_EXECUTABLE="$HOST_PYTHON" \
    -DLLVM_NATIVE_TOOL_DIR="$HOST/bin" \
    -DLLVM_TABLEGEN="$HOST/bin/llvm-tblgen" \
    -DCLANG_TABLEGEN="$HOST/bin/clang-tblgen" \
    -DLLVM_HOST_TRIPLE="$TARGET_ARCH" \
    -DLLVM_DEFAULT_TARGET_TRIPLE="$TARGET_ARCH" \
    -DLLVM_TARGETS_TO_BUILD=X86 \
    -DLLVM_ENABLE_PROJECTS="clang;lld" \
    -DLLVM_BUILD_TOOLS=ON \
    -DLLVM_BUILD_UTILS=OFF \
    -DLLVM_BUILD_TESTS=OFF \
    -DLLVM_ENABLE_THREADS=OFF \
    -DLLVM_INCLUDE_TESTS=OFF \
    -DLLVM_INCLUDE_EXAMPLES=OFF \
    -DLLVM_INCLUDE_BENCHMARKS=OFF \
    -DLLVM_INCLUDE_DOCS=OFF \
    -DLLVM_ENABLE_ASSERTIONS=OFF \
    -DLLVM_ENABLE_BACKTRACES=OFF \
    -DLLVM_ENABLE_TERMINFO=OFF \
    -DLLVM_ENABLE_LIBEDIT=OFF \
    -DLLVM_ENABLE_LIBXML2=OFF \
    -DLLVM_ENABLE_ZLIB=OFF \
    -DLLVM_ENABLE_ZSTD=OFF \
    -DLLVM_ENABLE_CURL=OFF \
    -DLLVM_ENABLE_FFI=OFF \
    -DLLVM_ENABLE_OCAMLDOC=OFF \
    -DLLVM_BUILD_LLVM_DYLIB=OFF \
    -DLLVM_LINK_LLVM_DYLIB=OFF \
    -DLLVM_PARALLEL_LINK_JOBS="$WOS_LLVM_PARALLEL_LINK_JOBS" \
    -DCLANG_LINK_CLANG_DYLIB=OFF \
    -DCLANG_BUILD_EXAMPLES=OFF \
    -DCLANG_INCLUDE_TESTS=OFF \
    -DCLANG_DEFAULT_LINKER=lld \
    -DCLANG_DEFAULT_CXX_STDLIB=libc++ \
    -DCLANG_DEFAULT_RTLIB=compiler-rt \
    -DCLANG_DEFAULT_UNWINDLIB=libunwind \
    -DCLANG_DEFAULT_OBJCOPY=llvm-objcopy \
    -DCLANG_RESOURCE_DIR="/usr/lib/clang/$CLANG_VERSION" \
    -DLLD_ENABLE_COFF=OFF \
    -DLLD_ENABLE_ELF=ON \
    -DLLD_ENABLE_MACHO=OFF \
    -DLLD_ENABLE_MINGW=OFF \
    -DLLD_ENABLE_WASM=OFF \
    -DLLD_SYMLINKS_TO_CREATE=ld.lld \
    -DDEFAULT_SYSROOT=/usr \
    -DC_INCLUDE_DIRS=/usr/include

cmake --build "$CLANG_BUILD" --parallel "$WOS_NINJA_JOBS" --target \
    clang \
    lld \
    llvm-ar \
    llvm-ranlib \
    llvm-nm \
    llvm-objcopy \
    llvm-strip \
    llvm-readelf \
    llvm-objdump \
    llvm-tblgen \
    clang-tblgen

install_tool() {
    local tool="$1"
    local required="${2:-1}"
    local source="$CLANG_BUILD/bin/$tool"

    if [ ! -e "$source" ]; then
        if [ "$required" -eq 0 ]; then
            return 0
        fi
        echo "ERROR: expected native WOS tool was not built: $source" >&2
        exit 1
    fi

    install -m 755 "$source" "$TARGET_SYSROOT/bin/$tool"
}

if [ -e "$CLANG_BUILD/bin/clang" ]; then
    install_tool clang
elif [ -e "$CLANG_BUILD/bin/clang-$CLANG_VERSION" ]; then
    install -m 755 "$CLANG_BUILD/bin/clang-$CLANG_VERSION" "$TARGET_SYSROOT/bin/clang"
else
    echo "ERROR: expected native WOS clang was not built under $CLANG_BUILD/bin" >&2
    exit 1
fi

install_tool lld
install_tool ld.lld
install_tool llvm-ar
install_tool llvm-ranlib
install_tool llvm-nm
install_tool llvm-objcopy
install_tool llvm-strip
install_tool llvm-readelf
install_tool llvm-objdump
install_tool llvm-tblgen
install_tool clang-tblgen

ln -sfn clang "$TARGET_SYSROOT/bin/clang++"
ln -sfn clang "$TARGET_SYSROOT/bin/clang-$CLANG_VERSION"
ln -sfn clang "$TARGET_SYSROOT/bin/cc"
ln -sfn clang++ "$TARGET_SYSROOT/bin/c++"
ln -sfn ld.lld "$TARGET_SYSROOT/bin/ld"
ln -sfn llvm-ar "$TARGET_SYSROOT/bin/ar"
ln -sfn llvm-ranlib "$TARGET_SYSROOT/bin/ranlib"
ln -sfn llvm-nm "$TARGET_SYSROOT/bin/nm"
ln -sfn llvm-objcopy "$TARGET_SYSROOT/bin/objcopy"
ln -sfn llvm-strip "$TARGET_SYSROOT/bin/strip"

cat > "$TARGET_SYSROOT/bin/$TARGET_ARCH.cfg" <<EOF
--target=$TARGET_ARCH
--sysroot=/usr
-fPIE
-pie
-fno-sanitize=safe-stack
-Wl,--dynamic-linker=/lib/ld.so
EOF

RESOURCE_SOURCE="${WOS_CLANG_RESOURCE_SOURCE_DIR:-}"
if [ -z "$RESOURCE_SOURCE" ]; then
    RESOURCE_SOURCE="$("$HOST/bin/clang" -print-resource-dir)"
fi
require_file "$RESOURCE_SOURCE/include/stddef.h" "Host clang resource directory is incomplete."
require_file "$RESOURCE_SOURCE/lib/$TARGET_ARCH/libclang_rt.builtins.a" "Build compiler-rt before building native clang."

RESOURCE_TARGET="$TARGET_SYSROOT/lib/clang/$CLANG_VERSION"
rm -rf "$RESOURCE_TARGET"
mkdir -p "$RESOURCE_TARGET"
cp -aL "$RESOURCE_SOURCE/include" "$RESOURCE_TARGET/include"

mkdir -p "$RESOURCE_TARGET/lib"
for path in "$RESOURCE_SOURCE/lib"/*; do
    if [ -f "$path" ]; then
        cp -aL "$path" "$RESOURCE_TARGET/lib/"
    fi
done

if [ -d "$RESOURCE_SOURCE/lib/$TARGET_ARCH" ]; then
    mkdir -p "$RESOURCE_TARGET/lib/$TARGET_ARCH"
    for path in "$RESOURCE_SOURCE/lib/$TARGET_ARCH"/*; do
        if [ -f "$path" ]; then
            cp -aL "$path" "$RESOURCE_TARGET/lib/$TARGET_ARCH/"
        fi
    done
fi

if [ -d "$RESOURCE_SOURCE/target" ]; then
    cp -aL "$RESOURCE_SOURCE/target" "$RESOURCE_TARGET/target"
fi

require_file "$RESOURCE_TARGET/lib/$TARGET_ARCH/libclang_rt.builtins.a" \
    "Failed to install clang builtins into native WOS resource directory."

echo "Native WOS clang installed to $TARGET_SYSROOT/bin/clang"
