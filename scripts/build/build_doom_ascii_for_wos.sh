#!/bin/bash
# Cross-build doom-ascii for WOS and install it into the target sysroot.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# shellcheck disable=SC1091
source "$WORKSPACE_ROOT/tools/ccache_env.sh"
if [ -z "${CCACHE_DIR:-}" ]; then
    export CCACHE_DIR="${TMPDIR:-/tmp}/wos-doom-ascii-ccache"
    mkdir -p "$CCACHE_DIR"
fi
wos_setup_ccache
WOS_CCACHE_PREFIX="$(wos_ccache_prefix)"
WOS_MAKE_JOBS="$(wos_make_jobs)"

B="$WORKSPACE_ROOT/toolchain"
HOST="${WOS_HOST_TOOLCHAIN_ROOT:-$B/host}"
TARGET_ARCH="${WOS_TARGET_ARCH:-x86_64-pc-wos}"
TARGET_SYSROOT="${WOS_SYSROOT_PATH:-$B/sysroot}"
DOOM_ASCII_SRC="${WOS_DOOM_ASCII_SOURCE_DIR:-$B/src/doom-ascii}"
DOOM_ASCII_BUILD="${WOS_DOOM_ASCII_BUILD_DIR:-$B/doom-ascii-build}"
DOOM_ASCII_WORK="$DOOM_ASCII_BUILD/work"
DOOM_ASCII_WOS_PATCH="$WORKSPACE_ROOT/toolchain/patches/doom-ascii/wos-clean-exit.patch"
WOS_DOOM_ASCII_STRIP="${WOS_DOOM_ASCII_STRIP:-0}"

require_file() {
    local path="$1"
    local hint="$2"

    if [ ! -e "$path" ]; then
        echo "ERROR: missing $path" >&2
        echo "$hint" >&2
        exit 1
    fi
}

require_file "$DOOM_ASCII_SRC/Makefile" \
    "Initialize the Doom ASCII source with: git submodule update --init toolchain/src/doom-ascii"
require_file "$DOOM_ASCII_SRC/src/doomgeneric_ascii.c" \
    "The Doom ASCII source tree is incomplete."
require_file "$DOOM_ASCII_WOS_PATCH" \
    "The WOS Doom ASCII compatibility patch is missing."
require_file "$TARGET_SYSROOT/lib/libc.so" \
    "Build mlibc before building Doom ASCII."

wos_remove_tree "$DOOM_ASCII_WORK"
mkdir -p "$DOOM_ASCII_WORK"
wos_copy_tree_entries_excluding \
    "$DOOM_ASCII_SRC" "$DOOM_ASCII_WORK" ".git" ".github" "screenshots"
patch --batch --forward --fuzz=0 -d "$DOOM_ASCII_WORK" -p1 < "$DOOM_ASCII_WOS_PATCH"

DOOM_ASCII_CFLAGS=(
    "--target=$TARGET_ARCH"
    "--sysroot=$TARGET_SYSROOT"
    -DNORMALUNIX
    -DLINUX
    -D_DEFAULT_SOURCE
    -DVERSION=0.3.1
    -std=c99
    -O2
    -g
    -m64
    -fPIE
    -fno-sanitize=safe-stack
    -fno-stack-protector
    -Wall
)
if [ "${WOS_USERSPACE_NO_AVX:-0}" = "1" ]; then
    DOOM_ASCII_CFLAGS+=(
        -mno-avx
        -mno-avx2
        -mno-fma
        -mno-f16c
        -fno-vectorize
        -fno-slp-vectorize
    )
fi

DOOM_ASCII_LDFLAGS=(
    "--target=$TARGET_ARCH"
    "--sysroot=$TARGET_SYSROOT"
    -fuse-ld=lld
    "-L$TARGET_SYSROOT/lib"
    "-Wl,--dynamic-linker=/lib/ld.so"
    "-Wl,-rpath,/usr/lib"
    -fno-sanitize=safe-stack
)

printf -v DOOM_ASCII_CFLAGS_STRING "%q " "${DOOM_ASCII_CFLAGS[@]}"
printf -v DOOM_ASCII_LDFLAGS_STRING "%q " "${DOOM_ASCII_LDFLAGS[@]}"

wos_make "$WOS_MAKE_JOBS" \
    -C "$DOOM_ASCII_WORK" \
    PLATFORM=wos \
    "CC=${WOS_CCACHE_PREFIX}$HOST/bin/clang" \
    "CFLAGS=$DOOM_ASCII_CFLAGS_STRING" \
    "LDFLAGS=$DOOM_ASCII_LDFLAGS_STRING"

require_file "$DOOM_ASCII_WORK/_wos/game/doom-ascii" \
    "The Doom ASCII build completed without producing its executable."

mkdir -p "$TARGET_SYSROOT/bin" "$TARGET_SYSROOT/share/doom-ascii"
install -m 755 "$DOOM_ASCII_WORK/_wos/game/doom-ascii" \
    "$TARGET_SYSROOT/bin/doom-ascii"
install -m 644 "$DOOM_ASCII_SRC/src/.default.cfg" \
    "$TARGET_SYSROOT/share/doom-ascii/default.cfg"

if [ "$WOS_DOOM_ASCII_STRIP" != "0" ]; then
    "$HOST/bin/llvm-strip" "$TARGET_SYSROOT/bin/doom-ascii"
fi

echo "Doom ASCII installed to $TARGET_SYSROOT/bin/doom-ascii"
