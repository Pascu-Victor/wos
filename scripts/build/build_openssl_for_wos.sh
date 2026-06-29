#!/bin/bash
# Cross-build OpenSSL-compatible static TLS libraries for WOS and install them
# into the sysroot. curl links these libraries statically for HTTPS support.
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
WOS_BUILD_JOBS="$(wos_build_jobs)"
WOS_MAKE_JOBS="$(wos_make_jobs)"

B="$WORKSPACE_ROOT/toolchain"
HOST="${WOS_HOST_TOOLCHAIN_ROOT:-$B/host}"
TARGET_ARCH="${WOS_TARGET_ARCH:-x86_64-pc-wos}"
TARGET_SYSROOT="${WOS_SYSROOT_PATH:-$B/sysroot}"
HOST_SYSTEM="$(uname -s 2>/dev/null || printf unknown)"
TLS_BUILD="${WOS_OPENSSL_BUILD_DIR:-$B/openssl-build}"
TLS_SRC="${WOS_OPENSSL_SOURCE_DIR:-$B/src/openssl}"
TLS_WORK="$TLS_BUILD/work"
LIBRESSL_VERSION="${WOS_LIBRESSL_VERSION:-4.3.2}"
LIBRESSL_TARBALL_URL="${WOS_LIBRESSL_TARBALL_URL:-https://ftp.openbsd.org/pub/OpenBSD/LibreSSL/libressl-$LIBRESSL_VERSION.tar.gz}"
LIBRESSL_TARBALL_SHA256="${WOS_LIBRESSL_TARBALL_SHA256:-edf01aee24c65d69e6a9efcb9d44bcda682ff9d4f3bbbd95e794e1dfa90847b5}"
LIBRESSL_TARBALL_URLS="${WOS_LIBRESSL_TARBALL_URLS:-$LIBRESSL_TARBALL_URL}"
LIBRESSL_DOWNLOAD_ATTEMPTS="${WOS_LIBRESSL_DOWNLOAD_ATTEMPTS:-${WOS_SOURCE_DOWNLOAD_ATTEMPTS:-3}}"

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

download_libressl_source() {
    local dest="$1"
    local archive_dir="$TLS_BUILD/src"
    local archive="$archive_dir/libressl-$LIBRESSL_VERSION.tar.gz"
    local tmp_dest="$dest.tmp"

    mkdir -p "$archive_dir"
    if [ ! -f "$archive" ]; then
        if ! command -v curl >/dev/null 2>&1; then
            echo "ERROR: LibreSSL source not found at $TLS_SRC and curl is unavailable." >&2
            echo "Populate $TLS_SRC with a LibreSSL portable release tree or install curl." >&2
            exit 1
        fi
        wos_download_file "LibreSSL $LIBRESSL_VERSION source" "$archive" "$LIBRESSL_TARBALL_URLS" "$LIBRESSL_DOWNLOAD_ATTEMPTS"
    fi

    echo "$LIBRESSL_TARBALL_SHA256  $archive" | sha256sum -c - >&2
    wos_remove_tree "$tmp_dest"
    wos_remove_tree "$dest"
    mkdir -p "$tmp_dest"
    tar -xzf "$archive" -C "$tmp_dest" --strip-components 1
    mv "$tmp_dest" "$dest"
}

resolve_tls_source() {
    local fallback_src="$TLS_BUILD/src/libressl-$LIBRESSL_VERSION"

    if [ -f "$TLS_SRC/configure" ]; then
        printf '%s\n' "$TLS_SRC"
        return 0
    fi

    if [ -f "$TLS_SRC/Configure" ]; then
        echo "ERROR: $TLS_SRC looks like upstream OpenSSL and needs Perl to configure." >&2
        echo "Use a LibreSSL portable release tree with a generated configure script for self-hosted WOS builds." >&2
        exit 1
    fi

    if [ -f "$fallback_src/configure" ]; then
        printf '%s\n' "$fallback_src"
        return 0
    fi

    if [ -d "$TLS_SRC" ] && wos_dir_has_entries "$TLS_SRC"; then
        echo "ERROR: TLS source at $TLS_SRC does not contain configure." >&2
        echo "Use a LibreSSL portable release tree or clear the directory so the release tarball can be downloaded." >&2
        exit 1
    fi

    download_libressl_source "$fallback_src"
    printf '%s\n' "$fallback_src"
}

copy_source_to_workdir() {
    local source_dir="$1"

    wos_remove_tree "$TLS_WORK"
    mkdir -p "$TLS_WORK"
    wos_copy_tree_entries_excluding "$source_dir" "$TLS_WORK" ".git" ".github"
}

patch_config_sub_for_wos() {
    local config_sub="$1"

    require_file "$config_sub" "LibreSSL source is missing config.sub."
    if grep -q 'wos\*' "$config_sub"; then
        return 0
    fi

    echo "Patching LibreSSL config.sub to recognise WOS..."
    if grep -q '| fiwix\* | mlibc\* )' "$config_sub"; then
        sed -i 's/| fiwix\* | mlibc\* )/| fiwix* | mlibc* | wos* )/' "$config_sub"
    elif grep -q '| fiwix\* | mlibc\* |' "$config_sub"; then
        sed -i 's/| fiwix\* | mlibc\* |/| fiwix* | mlibc* | wos* |/' "$config_sub"
    else
        echo "ERROR: do not know how to patch $config_sub for WOS." >&2
        exit 1
    fi
}

patch_arc4random_for_wos() {
    local arc4random_h="$1"
    local patched="$arc4random_h.tmp"

    require_file "$arc4random_h" "LibreSSL source is missing crypto/compat/arc4random.h."
    if grep -q 'defined(__WOS__)' "$arc4random_h"; then
        return 0
    fi

    echo "Patching LibreSSL arc4random hooks for WOS..."
    if ! grep -q '#elif defined(__NetBSD__)' "$arc4random_h"; then
        echo "ERROR: do not know how to patch $arc4random_h for WOS." >&2
        exit 1
    fi

    while IFS= read -r line; do
        if [ "$line" = "#elif defined(__NetBSD__)" ]; then
            printf '%s\n' '#elif defined(__WOS__)'
            printf '%s\n' '#include "arc4random_netbsd.h"'
            printf '\n'
        fi
        printf '%s\n' "$line"
    done <"$arc4random_h" >"$patched"
    mv "$patched" "$arc4random_h"
}

refresh_libressl_release_generated_files() {
    local generated_files=(
        aclocal.m4
        configure
        Makefile.in
        include/Makefile.in
        include/openssl/Makefile.in
        crypto/Makefile.in
        ssl/Makefile.in
        tls/Makefile.in
        tests/Makefile.in
        apps/Makefile.in
        apps/ocspcheck/Makefile.in
        apps/openssl/Makefile.in
        apps/nc/Makefile.in
        man/Makefile.in
    )
    local file

    sleep 2
    for file in "${generated_files[@]}"; do
        wos_refresh_file_mtime "$TLS_WORK/$file"
    done
}

remove_cross_libtool_archives() {
    local archive

    # These .la files encode target /usr/lib paths that break later host-side
    # cross-links. Consumers use the static archives and pkg-config metadata.
    for archive in \
        "$TARGET_SYSROOT/lib/libcrypto.la" \
        "$TARGET_SYSROOT/lib/libssl.la" \
        "$TARGET_SYSROOT/lib/libtls.la"; do
        [ -e "$archive" ] || continue
        rm -f "$archive"
    done
}

require_file "$HOST/bin/clang" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/llvm-ar" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/llvm-ranlib" "Run tools/host-toolchain.sh first."
require_file "$TARGET_SYSROOT/lib/libc.so" "Build mlibc before building OpenSSL."
require_file "$TARGET_SYSROOT/lib/Scrt1.o" "Build mlibc startup objects before building OpenSSL."

TLS_SOURCE_DIR="$(resolve_tls_source)"
copy_source_to_workdir "$TLS_SOURCE_DIR"
patch_config_sub_for_wos "$TLS_WORK/config.sub"
patch_arc4random_for_wos "$TLS_WORK/crypto/compat/arc4random.h"
refresh_libressl_release_generated_files

mkdir -p "$TARGET_SYSROOT/bin" "$TARGET_SYSROOT/lib" "$TARGET_SYSROOT/include"
if [ ! -e "$TARGET_SYSROOT/usr" ]; then
    ln -s . "$TARGET_SYSROOT/usr"
fi

OPENSSL_CFLAGS="--sysroot=$TARGET_SYSROOT -O2 -g -fPIC -fno-sanitize=safe-stack -fno-stack-protector -D__STDC_NO_ATOMICS__ -D__WOS__=1"
OPENSSL_LDFLAGS="--sysroot=$TARGET_SYSROOT -fuse-ld=lld -L$TARGET_SYSROOT/lib -Wl,--dynamic-linker=/lib/ld.so -Wl,-rpath,/usr/lib -fno-sanitize=safe-stack"

export CC="${WOS_CCACHE_PREFIX}$HOST/bin/clang --target=$TARGET_ARCH --sysroot=$TARGET_SYSROOT"
export AR="$HOST/bin/llvm-ar"
export RANLIB="$HOST/bin/llvm-ranlib"
export STRIP="$HOST/bin/llvm-strip"
export CPPFLAGS="-I$TARGET_SYSROOT/include -D__WOS__=1"
export CFLAGS="$OPENSSL_CFLAGS"
export LDFLAGS="$OPENSSL_LDFLAGS"

TLS_CONFIGURE_BUILD_ARGS=()
TLS_CONFIGURE_CACHE_ARGS=()
if [ "$HOST_SYSTEM" = "WOS" ]; then
    TLS_CONFIGURE_BUILD_ARGS=(--build="$TARGET_ARCH")
    TLS_CONFIGURE_CACHE_ARGS=(
        ac_cv_path_GREP=/usr/bin/grep
        "ac_cv_path_EGREP=/usr/bin/grep -E"
        "ac_cv_path_FGREP=/usr/bin/grep -F"
    )
fi

(
    cd "$TLS_WORK"
    ./configure \
        "${TLS_CONFIGURE_CACHE_ARGS[@]}" \
        "${TLS_CONFIGURE_BUILD_ARGS[@]}" \
        --host="$TARGET_ARCH" \
        --prefix= \
        --libdir=/lib \
        --disable-shared \
        --enable-static \
        --disable-tests \
        --disable-asm
)

wos_make "$WOS_MAKE_JOBS" -C "$TLS_WORK"
wos_make "$WOS_MAKE_JOBS" -C "$TLS_WORK" \
    prefix= \
    exec_prefix= \
    libdir=/lib \
    includedir=/include \
    datarootdir=/share \
    datadir=/share \
    mandir=/share/man \
    pkgconfigdir=/lib/pkgconfig \
    DESTDIR="$TARGET_SYSROOT" \
    install
remove_cross_libtool_archives

require_file "$TARGET_SYSROOT/lib/libssl.a" "OpenSSL install did not produce $TARGET_SYSROOT/lib/libssl.a."
require_file "$TARGET_SYSROOT/lib/libcrypto.a" "OpenSSL install did not produce $TARGET_SYSROOT/lib/libcrypto.a."
require_file "$TARGET_SYSROOT/include/openssl/ssl.h" "OpenSSL install did not produce SSL headers."

echo "Static WOS OpenSSL-compatible TLS libraries installed to $TARGET_SYSROOT/lib/libssl.a"
