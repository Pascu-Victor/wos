#!/bin/bash
# Cross-build static OpenSSL libraries for WOS and install them into the sysroot.
# curl links these libraries statically for HTTPS support.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$WORKSPACE_ROOT/tools/ccache_env.sh"
if [ -z "${CCACHE_DIR:-}" ]; then
    export CCACHE_DIR="${TMPDIR:-/tmp}/wos-openssl-ccache"
    mkdir -p "$CCACHE_DIR"
fi
wos_setup_ccache
WOS_CCACHE_PREFIX="$(wos_ccache_prefix)"

B="$WORKSPACE_ROOT/toolchain"
HOST="$B/host"
TARGET_ARCH="${WOS_TARGET_ARCH:-x86_64-pc-wos}"
TARGET_SYSROOT="${WOS_SYSROOT_PATH:-$B/sysroot}"
OPENSSL_BUILD="${WOS_OPENSSL_BUILD_DIR:-$B/openssl-build}"
OPENSSL_SRC="${WOS_OPENSSL_SOURCE_DIR:-$B/src/openssl}"
OPENSSL_WORK="$OPENSSL_BUILD/work"
OPENSSL_VERSION="${WOS_OPENSSL_VERSION:-3.6.3}"
OPENSSL_TARBALL_URL="${WOS_OPENSSL_TARBALL_URL:-https://github.com/openssl/openssl/releases/download/openssl-$OPENSSL_VERSION/openssl-$OPENSSL_VERSION.tar.gz}"
OPENSSL_TARBALL_SHA256="${WOS_OPENSSL_TARBALL_SHA256:-243a86649cf6f23eeb6a2ff2456e09e5d77dd9018a54d3d96b0c6bdd6ba6c7f1}"

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

download_openssl_source() {
    local dest="$1"
    local archive_dir="$OPENSSL_BUILD/src"
    local archive="$archive_dir/openssl-$OPENSSL_VERSION.tar.gz"
    local tmp_dest="$dest.tmp"

    mkdir -p "$archive_dir"
    if [ ! -f "$archive" ]; then
        if ! command -v curl >/dev/null 2>&1; then
            echo "ERROR: OpenSSL source not found at $OPENSSL_SRC and curl is unavailable." >&2
            echo "Populate $OPENSSL_SRC with an OpenSSL release tree or install curl." >&2
            exit 1
        fi
        echo "Downloading OpenSSL $OPENSSL_VERSION source..." >&2
        curl -L "$OPENSSL_TARBALL_URL" -o "$archive.tmp"
        mv "$archive.tmp" "$archive"
    fi

    echo "$OPENSSL_TARBALL_SHA256  $archive" | sha256sum -c - >&2
    rm -rf "$tmp_dest" "$dest"
    mkdir -p "$tmp_dest"
    tar -xzf "$archive" -C "$tmp_dest" --strip-components=1
    mv "$tmp_dest" "$dest"
}

resolve_openssl_source() {
    local fallback_src="$OPENSSL_BUILD/src/openssl-$OPENSSL_VERSION"

    if [ -f "$OPENSSL_SRC/Configure" ]; then
        printf '%s\n' "$OPENSSL_SRC"
        return 0
    fi

    if [ -f "$fallback_src/Configure" ]; then
        printf '%s\n' "$fallback_src"
        return 0
    fi

    if [ -d "$OPENSSL_SRC" ] && [ -n "$(find "$OPENSSL_SRC" -mindepth 1 -maxdepth 1 -print -quit)" ]; then
        echo "ERROR: OpenSSL source at $OPENSSL_SRC does not contain Configure." >&2
        echo "Use an OpenSSL release tree or clear the directory so the release tarball can be downloaded." >&2
        exit 1
    fi

    download_openssl_source "$fallback_src"
    printf '%s\n' "$fallback_src"
}

copy_source_to_workdir() {
    local source_dir="$1"

    rm -rf "$OPENSSL_WORK"
    mkdir -p "$OPENSSL_WORK"
    (
        cd "$source_dir"
        tar --exclude='./.git' --exclude='./.github' -cf - .
    ) | (
        cd "$OPENSSL_WORK"
        tar -xf -
    )
}

require_file "$HOST/bin/clang" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/llvm-ar" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/llvm-ranlib" "Run tools/host-toolchain.sh first."
require_file "$TARGET_SYSROOT/lib/libc.so" "Build mlibc before building OpenSSL."
require_file "$TARGET_SYSROOT/lib/Scrt1.o" "Build mlibc startup objects before building OpenSSL."
if ! command -v perl >/dev/null 2>&1; then
    echo "ERROR: perl is required to configure OpenSSL." >&2
    exit 1
fi

OPENSSL_SOURCE_DIR="$(resolve_openssl_source)"
copy_source_to_workdir "$OPENSSL_SOURCE_DIR"

mkdir -p "$TARGET_SYSROOT/bin" "$TARGET_SYSROOT/lib" "$TARGET_SYSROOT/include"
if [ ! -e "$TARGET_SYSROOT/usr" ]; then
    ln -s . "$TARGET_SYSROOT/usr"
fi

OPENSSL_CFLAGS="--sysroot=$TARGET_SYSROOT -O2 -g -fPIC -fno-sanitize=safe-stack -fno-stack-protector -D__STDC_NO_ATOMICS__"
OPENSSL_LDFLAGS="--sysroot=$TARGET_SYSROOT -fuse-ld=lld -L$TARGET_SYSROOT/lib -Wl,--dynamic-linker=/lib/ld.so -Wl,-rpath,/usr/lib -fno-sanitize=safe-stack"

export CC="${WOS_CCACHE_PREFIX}$HOST/bin/clang --target=$TARGET_ARCH --sysroot=$TARGET_SYSROOT"
export AR="$HOST/bin/llvm-ar"
export RANLIB="$HOST/bin/llvm-ranlib"
export CFLAGS="$OPENSSL_CFLAGS"
export LDFLAGS="$OPENSSL_LDFLAGS"

(
    cd "$OPENSSL_WORK"
    perl Configure linux-x86_64 \
        --prefix=/usr \
        --libdir=lib \
        --openssldir=/etc/ssl \
        no-shared \
        no-tests \
        no-apps \
        no-docs \
        no-module \
        no-async \
        no-engine \
        no-dso \
        no-afalgeng \
        no-devcryptoeng \
        no-asm
)

make -C "$OPENSSL_WORK" -j"$(nproc)" build_libs
make -C "$OPENSSL_WORK" DESTDIR="$TARGET_SYSROOT" install_sw

require_file "$TARGET_SYSROOT/lib/libssl.a" "OpenSSL install did not produce $TARGET_SYSROOT/lib/libssl.a."
require_file "$TARGET_SYSROOT/lib/libcrypto.a" "OpenSSL install did not produce $TARGET_SYSROOT/lib/libcrypto.a."
require_file "$TARGET_SYSROOT/include/openssl/ssl.h" "OpenSSL install did not produce SSL headers."

echo "Static WOS OpenSSL installed to $TARGET_SYSROOT/lib/libssl.a"
