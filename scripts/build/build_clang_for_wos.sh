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
WOS_CLANG_HOST_SYSTEM="$(uname -s 2>/dev/null || printf unknown)"

# LLVM's cross build launches a nested native-tools Ninja graph.  On WOS that
# child graph cannot inherit the outer Ninja's explicit -j limit, and using all
# vCPUs can exhaust kernel memory while several large clang/lld processes are
# resident.  Keep the self-host defaults conservative while preserving explicit
# overrides and full parallelism on development hosts.
if [ -z "${WOS_LLVM_NINJA_JOBS:-}" ]; then
    WOS_LLVM_NINJA_JOBS="$WOS_NINJA_JOBS"
    if [ "$WOS_CLANG_HOST_SYSTEM" = "WOS" ] && [ "$WOS_LLVM_NINJA_JOBS" -gt 8 ]; then
        WOS_LLVM_NINJA_JOBS=8
    fi
fi
if [ -z "${WOS_LLVM_PARALLEL_LINK_JOBS:-}" ]; then
    WOS_LLVM_PARALLEL_LINK_JOBS="$WOS_LLVM_NINJA_JOBS"
    if [ "$WOS_CLANG_HOST_SYSTEM" = "WOS" ] && [ "$WOS_LLVM_PARALLEL_LINK_JOBS" -gt 2 ]; then
        WOS_LLVM_PARALLEL_LINK_JOBS=2
    fi
fi
for jobs_setting in WOS_LLVM_NINJA_JOBS WOS_LLVM_PARALLEL_LINK_JOBS; do
    jobs_value="${!jobs_setting}"
    case "$jobs_value" in
        ''|*[!0-9]*|0)
            echo "ERROR: $jobs_setting must be a positive integer, got '$jobs_value'" >&2
            exit 1
            ;;
    esac
done

# CMake's nested native-tools build does not see the outer Ninja -j argument.
export CMAKE_BUILD_PARALLEL_LEVEL="$WOS_LLVM_NINJA_JOBS"

B="$WORKSPACE_ROOT/toolchain"
HOST="${WOS_HOST_TOOLCHAIN_ROOT:-$B/host}"
TARGET_SYSROOT="${WOS_SYSROOT_PATH:-$B/sysroot}"
CLANG_BUILD="${WOS_CLANG_FOR_WOS_BUILD_DIR:-$B/clang-wos-build}"
LLVM_SRC="$B/src/llvm-project/llvm"
TARGET_ARCH="${WOS_TARGET_ARCH:-x86_64-pc-wos}"
CLANG_VERSION="${WOS_CLANG_VERSION:-22}"
HOST_PYTHON="${HOST_PYTHON:-$(command -v python3)}"
WOS_CMAKE_COMMAND="${WOS_CMAKE_COMMAND:-cmake}"

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

TARGET_COMMON_FLAGS="--sysroot=$TARGET_SYSROOT -fPIC -fPIE -fno-sanitize=safe-stack -fno-stack-protector -fdiagnostics-color=always"
TARGET_C_INCLUDE_FLAGS="-isystem $HOST/lib/clang/22/include -isystem $TARGET_SYSROOT/include"
TARGET_C_FLAGS="$TARGET_COMMON_FLAGS $TARGET_C_INCLUDE_FLAGS"
TARGET_CXX_FLAGS="$TARGET_COMMON_FLAGS -std=c++23 -isystem $TARGET_SYSROOT/include/c++/v1 $TARGET_C_INCLUDE_FLAGS"
TARGET_LINK_FLAGS="--sysroot=$TARGET_SYSROOT -fuse-ld=lld -L$TARGET_SYSROOT/lib -Wl,--dynamic-linker=/lib/ld.so -Wl,-rpath,/usr/lib -fno-sanitize=safe-stack"
TARGET_CXX_STANDARD_LIBRARIES="-lc++ -lc++abi -lunwind -lm -lpthread -ldl -lrt -lc"

wos_timed_step "configure" "clang_for_wos" \
    "$WOS_CMAKE_COMMAND" -S "$LLVM_SRC" -B "$CLANG_BUILD" -G Ninja \
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
    -DLLVM_INSTALL_TOOLCHAIN_ONLY=OFF \
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

collect_wos_llvm_bin_outputs() {
    local build_ninja="$CLANG_BUILD/build.ninja"

    require_file "$build_ninja" "CMake did not generate a Ninja build file for native WOS clang."

    awk '
        function tool_name(name) {
            return name == "bugpoint" ||
                   name == "c-index-test" ||
                   name == "clang" ||
                   name == "clang++" ||
                   name ~ /^clang-/ ||
                   name == "diagtool" ||
                   name == "dsymutil" ||
                   name == "ld.lld" ||
                   name == "ld64.lld" ||
                   name == "lld" ||
                   name ~ /^lld-/ ||
                   name == "llc" ||
                   name == "lli" ||
                   name ~ /^llvm-/ ||
                   name == "obj2yaml" ||
                   name == "offload-arch" ||
                   name == "opt" ||
                   name == "reduce-chunk-list" ||
                   name == "sancov" ||
                   name == "sanstats" ||
                   name == "verify-uselistorder" ||
                   name == "wasm-ld" ||
                   name == "yaml2obj"
        }

        function excluded_tool(name) {
            return name ~ /fuzzer/ ||
                   name == "FileCheck" ||
                   name == "UnicodeNameMappingGenerator" ||
                   name == "apinotes-test" ||
                   name == "clang-import-test" ||
                   name == "count" ||
                   name == "lli-child-target" ||
                   name == "llvm-PerfectShuffle" ||
                   name == "llvm-jitlink-executor" ||
                   name == "llvm-lit" ||
                   name == "llvm-locstats" ||
                   name == "llvm-min-tblgen" ||
                   name == "llvm-test-mustache-spec" ||
                   name == "not" ||
                   name == "split-file" ||
                   name == "yaml-bench"
        }

        /^build / {
            outputs = $0
            sub(/^build /, "", outputs)
            sub(/:.*/, "", outputs)
            from_tool_dir = ($0 ~ /(^|[[:space:]])tools\//)
            from_tablegen = ($0 ~ /(^|[[:space:]])utils\/TableGen\//)
            output_count = split(outputs, output_fields, /[[:space:]]+/)
            for (i = 1; i <= output_count; i++) {
                output = output_fields[i]
                if (output == "|") {
                    break
                }
                gsub(/\$\{cmake_ninja_workdir\}/, "", output)
                if (output !~ /^bin\//) {
                    continue
                }
                name = output
                sub(/^bin\//, "", name)
                if (!from_tool_dir && !from_tablegen && !tool_name(name)) {
                    continue
                }
                if (excluded_tool(name)) {
                    continue
                }
                print output
            }
        }
    ' "$build_ninja" | sort -u
}

WOS_LLVM_BIN_OUTPUTS_LIST="$(mktemp)"
trap 'rm -f "$WOS_LLVM_BIN_OUTPUTS_LIST"' EXIT
collect_wos_llvm_bin_outputs > "$WOS_LLVM_BIN_OUTPUTS_LIST"
mapfile -t WOS_LLVM_BIN_OUTPUTS < "$WOS_LLVM_BIN_OUTPUTS_LIST"
if [ "${#WOS_LLVM_BIN_OUTPUTS[@]}" -eq 0 ]; then
    echo "ERROR: no native WOS LLVM tool outputs were discovered in $CLANG_BUILD/build.ninja" >&2
    exit 1
fi

wos_timed_step "build" "clang_for_wos" \
    ninja -C "$CLANG_BUILD" -j"$WOS_LLVM_NINJA_JOBS" "${WOS_LLVM_BIN_OUTPUTS[@]}"

should_stage_wos_llvm_tool() {
    local name="$1"

    case "$name" in
        *fuzzer*|FileCheck|UnicodeNameMappingGenerator|apinotes-test|clang-import-test|count|lli-child-target|llvm-PerfectShuffle|llvm-jitlink-executor|llvm-lit|llvm-locstats|llvm-min-tblgen|llvm-test-mustache-spec|not|split-file|yaml-bench)
            return 1
            ;;
    esac

    case "$name" in
        bugpoint|c-index-test|clang|clang++|clang-*|diagtool|dsymutil|ld.lld|ld64.lld|lld|lld-*|llc|lli|llvm-*|obj2yaml|offload-arch|opt|reduce-chunk-list|sancov|sanstats|verify-uselistorder|wasm-ld|yaml2obj)
            return 0
            ;;
    esac

    return 1
}

install_tool() {
    local name="$1"
    local source
    local target
    local link_target

    source="$CLANG_BUILD/bin/$name"
    target="$TARGET_SYSROOT/bin/$name"
    if [ -L "$source" ]; then
        link_target="$(readlink "$source")"
        ln -sfn "$link_target" "$target"
    elif [ -f "$source" ]; then
        install -m 755 "$source" "$target"
    else
        echo "ERROR: expected native WOS tool was not built: $source" >&2
        exit 1
    fi
}

install_built_toolset() {
    local source
    local name
    local installed=0

    for source in "$CLANG_BUILD/bin"/*; do
        [ -e "$source" ] || [ -L "$source" ] || continue
        name="$(basename "$source")"
        should_stage_wos_llvm_tool "$name" || continue
        install_tool "$name"
        installed=$((installed + 1))
    done

    if [ "$installed" -eq 0 ]; then
        echo "ERROR: no native WOS LLVM tools were staged from $CLANG_BUILD/bin" >&2
        exit 1
    fi
}

install_built_toolset

[ -e "$TARGET_SYSROOT/bin/clang++" ] || ln -sfn clang "$TARGET_SYSROOT/bin/clang++"
[ -e "$TARGET_SYSROOT/bin/clang-$CLANG_VERSION" ] || ln -sfn clang "$TARGET_SYSROOT/bin/clang-$CLANG_VERSION"
ln -sfn clang "$TARGET_SYSROOT/bin/cc"
ln -sfn clang++ "$TARGET_SYSROOT/bin/c++"
ln -sfn ld.lld "$TARGET_SYSROOT/bin/ld"
ln -sfn llvm-ar "$TARGET_SYSROOT/bin/ar"
ln -sfn llvm-ranlib "$TARGET_SYSROOT/bin/ranlib"
ln -sfn llvm-nm "$TARGET_SYSROOT/bin/nm"
ln -sfn llvm-objcopy "$TARGET_SYSROOT/bin/objcopy"
ln -sfn llvm-strip "$TARGET_SYSROOT/bin/strip"
[ -e "$TARGET_SYSROOT/bin/llvm-objdump" ] && ln -sfn llvm-objdump "$TARGET_SYSROOT/bin/objdump"
[ -e "$TARGET_SYSROOT/bin/llvm-readelf" ] && ln -sfn llvm-readelf "$TARGET_SYSROOT/bin/readelf"
[ -e "$TARGET_SYSROOT/bin/llvm-size" ] && ln -sfn llvm-size "$TARGET_SYSROOT/bin/size"
[ -e "$TARGET_SYSROOT/bin/llvm-strings" ] && ln -sfn llvm-strings "$TARGET_SYSROOT/bin/strings"
[ -e "$TARGET_SYSROOT/bin/llvm-cxxfilt" ] && ln -sfn llvm-cxxfilt "$TARGET_SYSROOT/bin/c++filt"
[ -e "$TARGET_SYSROOT/bin/llvm-addr2line" ] && ln -sfn llvm-addr2line "$TARGET_SYSROOT/bin/addr2line"

for required_tool in \
    clang \
    lld \
    ld.lld \
    llvm-ar \
    llvm-ranlib \
    llvm-nm \
    llvm-objcopy \
    llvm-strip \
    llvm-readelf \
    llvm-objdump \
    llvm-symbolizer \
    llvm-tblgen \
    clang-tblgen; do
    require_file "$TARGET_SYSROOT/bin/$required_tool" "Native WOS clang build did not stage $required_tool."
done

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
