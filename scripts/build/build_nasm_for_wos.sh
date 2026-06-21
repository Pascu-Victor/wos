#!/bin/bash
# Cross-build NASM so it can run inside WOS and install it into the sysroot.
# Expects the WOS host toolchain and mlibc sysroot to already be available.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$WORKSPACE_ROOT/tools/ccache_env.sh"
if [ -z "${CCACHE_DIR:-}" ]; then
    export CCACHE_DIR="${TMPDIR:-/tmp}/wos-nasm-ccache"
    mkdir -p "$CCACHE_DIR"
fi
wos_setup_ccache
WOS_CCACHE_PREFIX="$(wos_ccache_prefix)"

B="$WORKSPACE_ROOT/toolchain"
HOST="$B/host"
TARGET_ARCH="${WOS_TARGET_ARCH:-x86_64-pc-wos}"
TARGET_SYSROOT="${WOS_SYSROOT_PATH:-$B/sysroot}"
NASM_SRC="${WOS_NASM_SOURCE_DIR:-$B/src/nasm}"
NASM_BUILD="${WOS_NASM_BUILD_DIR:-$B/nasm-build}"
WOS_NASM_STRIP="${WOS_NASM_STRIP:-0}"

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

patch_config_sub_for_wos() {
    local config_sub="$1"

    require_file "$config_sub" "NASM source is missing autoconf/helpers/config.sub."
    if grep -q 'wos\*' "$config_sub"; then
        return 0
    fi

    echo "Patching NASM config.sub to recognise WOS..."
    if grep -q '| fiwix\* )' "$config_sub"; then
        sed -i 's/| fiwix\* )/| fiwix* | wos* )/' "$config_sub"
    elif grep -q '| fiwix\* |' "$config_sub"; then
        sed -i 's/| fiwix\* |/| fiwix* | wos* |/' "$config_sub"
    else
        echo "ERROR: do not know how to patch $config_sub for WOS." >&2
        echo "Create a wos-support branch in Pascu-Victor/nasm with config.sub WOS support." >&2
        exit 1
    fi
}

require_file "$HOST/bin/clang" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/ld.lld" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/llvm-ar" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/llvm-ranlib" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/llvm-strip" "Run tools/host-toolchain.sh first."
require_file "$NASM_SRC/configure.ac" "Initialize toolchain/src/nasm from https://github.com/Pascu-Victor/nasm.git."
require_file "$NASM_SRC/autoconf/helpers/config.guess" "NASM source is missing config.guess."
require_file "$TARGET_SYSROOT/lib/libc.so" "Build mlibc before building NASM."
require_file "$TARGET_SYSROOT/lib/Scrt1.o" "Build mlibc startup objects before building NASM."

if [ ! -f "$NASM_SRC/configure" ]; then
    require_file "$NASM_SRC/autogen.sh" "NASM source is missing autogen.sh."
    echo "Generating NASM configure script..."
    (cd "$NASM_SRC" && ./autogen.sh)
fi
patch_config_sub_for_wos "$NASM_SRC/autoconf/helpers/config.sub"

BUILD_TRIPLE="$("$NASM_SRC/autoconf/helpers/config.guess")"

mkdir -p "$NASM_BUILD" "$TARGET_SYSROOT/bin"

NASM_CFLAGS="--sysroot=$TARGET_SYSROOT -O2 -g -m64 -fPIC -fPIE -fno-sanitize=safe-stack -fno-stack-protector"
NASM_LDFLAGS="--sysroot=$TARGET_SYSROOT -fuse-ld=lld -L$TARGET_SYSROOT/lib -Wl,--dynamic-linker=/lib/ld.so -Wl,-rpath,/usr/lib -fno-sanitize=safe-stack"

export CC="${WOS_CCACHE_PREFIX}$HOST/bin/clang --target=$TARGET_ARCH --sysroot=$TARGET_SYSROOT"
export AR="$HOST/bin/llvm-ar"
export RANLIB="$HOST/bin/llvm-ranlib"
export STRIP="$HOST/bin/llvm-strip"
export CFLAGS="$NASM_CFLAGS"
export CPPFLAGS="-I$TARGET_SYSROOT/include"
export LDFLAGS="$NASM_LDFLAGS"

if [ ! -f "$NASM_BUILD/Makefile" ] || [ "$NASM_SRC/configure" -nt "$NASM_BUILD/Makefile" ]; then
    echo "Configuring NASM for WOS..."
    (
        cd "$NASM_BUILD"
        ASCIIDOC=false \
            XMLTO=false \
            XZ=false \
            "$NASM_SRC/configure" \
                --build="$BUILD_TRIPLE" \
                --host="$TARGET_ARCH" \
                --prefix=/usr \
                --with-zlib=no \
                --disable-pdf-compression
    )
fi

if [ -f "$NASM_BUILD/nasm" ]; then
    for lib in "$TARGET_SYSROOT"/lib/libc.so "$TARGET_SYSROOT"/lib/libm.so; do
        if [ -f "$lib" ] && [ "$lib" -nt "$NASM_BUILD/nasm" ]; then
            echo "Sysroot library $(basename "$lib") changed - forcing relink"
            rm -f "$NASM_BUILD/nasm" "$NASM_BUILD/ndisasm"
            break
        fi
    done
fi

make -C "$NASM_BUILD" -j"$(nproc)" nasm ndisasm

install -m 755 "$NASM_BUILD/nasm" "$TARGET_SYSROOT/bin/nasm"
install -m 755 "$NASM_BUILD/ndisasm" "$TARGET_SYSROOT/bin/ndisasm"

if [ "$WOS_NASM_STRIP" != "0" ]; then
    "$HOST/bin/llvm-strip" "$TARGET_SYSROOT/bin/nasm" "$TARGET_SYSROOT/bin/ndisasm"
fi

echo "Native WOS NASM installed to $TARGET_SYSROOT/bin/nasm"
