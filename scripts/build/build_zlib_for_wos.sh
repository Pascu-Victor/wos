#!/bin/bash
# Cross-build static zlib for WOS and install headers/libz.a into the sysroot.
# Git links this statically for object and pack compression support.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$WORKSPACE_ROOT/tools/ccache_env.sh"
if [ -z "${CCACHE_DIR:-}" ]; then
    export CCACHE_DIR="${TMPDIR:-/tmp}/wos-zlib-ccache"
    mkdir -p "$CCACHE_DIR"
fi
wos_setup_ccache
WOS_CCACHE_PREFIX="$(wos_ccache_prefix)"
WOS_BUILD_JOBS="$(wos_build_jobs)"
WOS_MAKE_JOBS="$(wos_make_jobs)"

B="$WORKSPACE_ROOT/toolchain"
HOST="${WOS_HOST_TOOLCHAIN_ROOT:-$B/host}"
TARGET_ARCH="${WOS_TARGET_ARCH:-x86_64-pc-wos}"
TARGET_SYSROOT="${WOS_SYSROOT_PATH:-$B/sysroot}"
ZLIB_BUILD="${WOS_ZLIB_BUILD_DIR:-$B/zlib-build}"
ZLIB_SRC="${WOS_ZLIB_SOURCE_DIR:-$B/src/zlib}"
ZLIB_VERSION="${WOS_ZLIB_VERSION:-1.3.1}"
ZLIB_TARBALL_URL="${WOS_ZLIB_TARBALL_URL:-https://zlib.net/fossils/zlib-$ZLIB_VERSION.tar.gz}"
ZLIB_TARBALL_SHA256="${WOS_ZLIB_TARBALL_SHA256:-9a93b2b7dfdac77ceba5a558a580e74667dd6fede4585b91eefb60f03b72df23}"
ZLIB_TARBALL_URLS="${WOS_ZLIB_TARBALL_URLS:-$ZLIB_TARBALL_URL}"
ZLIB_DOWNLOAD_ATTEMPTS="${WOS_ZLIB_DOWNLOAD_ATTEMPTS:-${WOS_SOURCE_DOWNLOAD_ATTEMPTS:-3}}"

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

download_zlib_source() {
    local dest="$1"
    local archive_dir="$ZLIB_BUILD/src"
    local archive="$archive_dir/zlib-$ZLIB_VERSION.tar.gz"
    local tmp_dest="$dest.tmp"

    mkdir -p "$archive_dir"
    if [ ! -f "$archive" ]; then
        if ! command -v curl >/dev/null 2>&1; then
            echo "ERROR: zlib source not found at $ZLIB_SRC and curl is unavailable." >&2
            echo "Populate $ZLIB_SRC with a zlib release tree or install curl." >&2
            exit 1
        fi
        wos_download_file "zlib $ZLIB_VERSION source" "$archive" "$ZLIB_TARBALL_URLS" "$ZLIB_DOWNLOAD_ATTEMPTS"
    fi

    echo "$ZLIB_TARBALL_SHA256  $archive" | sha256sum -c - >&2
    wos_remove_tree "$tmp_dest"
    wos_remove_tree "$dest"
    mkdir -p "$tmp_dest"
    tar -xzf "$archive" -C "$tmp_dest" --strip-components 1
    mv "$tmp_dest" "$dest"
}

resolve_zlib_source() {
    local fallback_src="$ZLIB_BUILD/src/zlib-$ZLIB_VERSION"

    if [ -f "$ZLIB_SRC/configure" ]; then
        printf '%s\n' "$ZLIB_SRC"
        return 0
    fi

    if [ -f "$fallback_src/configure" ]; then
        printf '%s\n' "$fallback_src"
        return 0
    fi

    if [ -d "$ZLIB_SRC" ] && wos_dir_has_entries "$ZLIB_SRC"; then
        echo "ERROR: zlib source at $ZLIB_SRC does not contain configure." >&2
        echo "Use a zlib release tree or clear the directory so the release tarball can be downloaded." >&2
        exit 1
    fi

    download_zlib_source "$fallback_src"
    printf '%s\n' "$fallback_src"
}

require_file "$HOST/bin/clang" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/llvm-ar" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/llvm-ranlib" "Run tools/host-toolchain.sh first."
require_file "$TARGET_SYSROOT/lib/libc.so" "Build mlibc before building zlib."
require_file "$TARGET_SYSROOT/lib/Scrt1.o" "Build mlibc startup objects before building zlib."

ZLIB_SOURCE_DIR="$(resolve_zlib_source)"
mkdir -p "$ZLIB_BUILD" "$TARGET_SYSROOT/bin" "$TARGET_SYSROOT/lib" "$TARGET_SYSROOT/include"
if [ ! -e "$TARGET_SYSROOT/usr" ]; then
    ln -s . "$TARGET_SYSROOT/usr"
fi

WOS_ZLIB_OPT_FLAGS="${WOS_ZLIB_OPT_FLAGS:--O2}"

ZLIB_CFLAGS="--sysroot=$TARGET_SYSROOT $WOS_ZLIB_OPT_FLAGS -g -fPIC -fno-sanitize=safe-stack -fno-stack-protector"
ZLIB_LDFLAGS="--sysroot=$TARGET_SYSROOT -fuse-ld=lld -L$TARGET_SYSROOT/lib"

export CHOST="$TARGET_ARCH"
export CC="${WOS_CCACHE_PREFIX}$HOST/bin/clang --target=$TARGET_ARCH --sysroot=$TARGET_SYSROOT"
export AR="$HOST/bin/llvm-ar"
export RANLIB="$HOST/bin/llvm-ranlib"
export CFLAGS="$ZLIB_CFLAGS"
export LDFLAGS="$ZLIB_LDFLAGS"

if [ ! -f "$ZLIB_BUILD/Makefile" ] || [ "$ZLIB_SOURCE_DIR/configure" -nt "$ZLIB_BUILD/Makefile" ] || [ "$SCRIPT_DIR/build_zlib_for_wos.sh" -nt "$ZLIB_BUILD/Makefile" ]; then
    echo "Configuring zlib for WOS..."
    wos_timed_step "configure" "zlib" \
        wos_run_in_dir "$ZLIB_BUILD" \
        "$ZLIB_SOURCE_DIR/configure" --prefix= --static
fi

wos_stage_distributed_build_roots \
    "$WORKSPACE_ROOT" "" \
    "$ZLIB_SOURCE_DIR" "$ZLIB_BUILD" "$TARGET_SYSROOT/include"

wos_make "$WOS_MAKE_JOBS" -C "$ZLIB_BUILD" libz.a
wos_make "$WOS_MAKE_JOBS" -C "$ZLIB_BUILD" \
    prefix= \
    exec_prefix= \
    libdir=/lib \
    sharedlibdir=/lib \
    includedir=/include \
    mandir=/share/man \
    man3dir=/share/man/man3 \
    pkgconfigdir=/lib/pkgconfig \
    DESTDIR="$TARGET_SYSROOT" \
    install

require_file "$TARGET_SYSROOT/lib/libz.a" "zlib install did not produce $TARGET_SYSROOT/lib/libz.a."
require_file "$TARGET_SYSROOT/include/zlib.h" "zlib install did not produce $TARGET_SYSROOT/include/zlib.h."

echo "Static WOS zlib installed to $TARGET_SYSROOT/lib/libz.a"
