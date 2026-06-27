#!/bin/bash
# Cross-build curl/libcurl for WOS and install it into the sysroot.
# Git links libcurl statically to provide git-remote-https.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$WORKSPACE_ROOT/tools/ccache_env.sh"
if [ -z "${CCACHE_DIR:-}" ]; then
    export CCACHE_DIR="${TMPDIR:-/tmp}/wos-curl-ccache"
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
CURL_BUILD="${WOS_CURL_BUILD_DIR:-$B/curl-build}"
CURL_SRC="${WOS_CURL_SOURCE_DIR:-$B/src/curl}"
CURL_WORK="$CURL_BUILD/work"
CURL_VERSION="${WOS_CURL_VERSION:-8.20.0}"
CURL_TARBALL_URL="${WOS_CURL_TARBALL_URL:-https://curl.se/download/curl-$CURL_VERSION.tar.xz}"
CURL_TARBALL_SHA256="${WOS_CURL_TARBALL_SHA256:-63fe2dc148ba0ceae89922ef838f7e5c946272c2e78b7c59fab4b79d3ce2b896}"
WOS_CURL_STRIP="${WOS_CURL_STRIP:-0}"
HOST_SYSTEM="$(uname -s 2>/dev/null || printf unknown)"
CURL_CONFIGURE_BUILD_ARGS=()
CURL_CONFIGURE_CACHE_ARGS=()
if [ "$HOST_SYSTEM" = "WOS" ]; then
    CURL_CONFIGURE_BUILD_ARGS=(--build="$TARGET_ARCH")
    CURL_CONFIGURE_CACHE_ARGS=(
        ac_cv_path_GREP=/usr/bin/grep
        "ac_cv_path_EGREP=/usr/bin/grep -E"
        "ac_cv_path_FGREP=/usr/bin/grep -F"
    )
fi

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

download_curl_source() {
    local dest="$1"
    local archive_dir="$CURL_BUILD/src"
    local archive="$archive_dir/curl-$CURL_VERSION.tar.xz"
    local tmp_dest="$dest.tmp"

    mkdir -p "$archive_dir"
    if [ ! -f "$archive" ]; then
        if ! command -v curl >/dev/null 2>&1; then
            echo "ERROR: curl source not found at $CURL_SRC and curl is unavailable." >&2
            echo "Populate $CURL_SRC with a curl release tree or install curl." >&2
            exit 1
        fi
        echo "Downloading curl $CURL_VERSION source..." >&2
        curl -L "$CURL_TARBALL_URL" -o "$archive.tmp"
        mv "$archive.tmp" "$archive"
    fi

    echo "$CURL_TARBALL_SHA256  $archive" | sha256sum -c - >&2
    wos_remove_tree "$tmp_dest"
    wos_remove_tree "$dest"
    mkdir -p "$tmp_dest"
    tar -xJf "$archive" -C "$tmp_dest" --strip-components 1
    mv "$tmp_dest" "$dest"
}

resolve_curl_source() {
    local fallback_src="$CURL_BUILD/src/curl-$CURL_VERSION"

    if [ -f "$CURL_SRC/configure" ]; then
        printf '%s\n' "$CURL_SRC"
        return 0
    fi

    if [ -f "$fallback_src/configure" ]; then
        printf '%s\n' "$fallback_src"
        return 0
    fi

    if [ -d "$CURL_SRC" ] && wos_dir_has_entries "$CURL_SRC"; then
        echo "ERROR: curl source at $CURL_SRC does not contain configure." >&2
        echo "Use a curl release tree or clear the directory so the release tarball can be downloaded." >&2
        exit 1
    fi

    download_curl_source "$fallback_src"
    printf '%s\n' "$fallback_src"
}

copy_source_to_workdir() {
    local source_dir="$1"

    wos_remove_tree "$CURL_WORK"
    mkdir -p "$CURL_WORK"
    wos_copy_tree_entries_excluding "$source_dir" "$CURL_WORK" ".git" ".github"
}

patch_config_sub_for_wos() {
    local config_sub="$1"

    require_file "$config_sub" "curl source is missing config.sub."
    if grep -q 'wos\*' "$config_sub"; then
        return 0
    fi

    echo "Patching curl config.sub to recognise WOS..."
    if grep -q '| fiwix\* )' "$config_sub"; then
        sed -i 's/| fiwix\* )/| fiwix* | wos* )/' "$config_sub"
    else
        echo "ERROR: do not know how to patch $config_sub for WOS." >&2
        exit 1
    fi
}

stage_ca_bundle() {
    local source="${WOS_CA_CERT_BUNDLE:-}"
    local candidate
    local target="$TARGET_SYSROOT/etc/ssl/certs/ca-certificates.crt"

    if [ -z "$source" ]; then
        for candidate in \
            /etc/ssl/certs/ca-certificates.crt \
            /etc/ssl/cert.pem \
            /etc/ca-certificates/extracted/tls-ca-bundle.pem; do
            if [ -f "$candidate" ]; then
                source="$candidate"
                break
            fi
        done
    fi

    if [ -n "$source" ] && [ -f "$source" ]; then
        mkdir -p "$(dirname "$target")" "$TARGET_SYSROOT/etc/ssl"
        rm -f "$target"
        cp "$source" "$target"
        ln -sfn certs/ca-certificates.crt "$TARGET_SYSROOT/etc/ssl/cert.pem"
        echo "Staged CA bundle from $source"
    else
        echo "WARNING: no host CA bundle found; HTTPS certificate verification will need /etc/ssl/certs/ca-certificates.crt in WOS." >&2
    fi
}

require_file "$HOST/bin/clang" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/llvm-ar" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/llvm-ranlib" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/llvm-strip" "Run tools/host-toolchain.sh first."
require_file "$TARGET_SYSROOT/lib/libc.so" "Build mlibc before building curl."
require_file "$TARGET_SYSROOT/lib/Scrt1.o" "Build mlibc startup objects before building curl."
require_file "$TARGET_SYSROOT/lib/libz.a" "Run scripts/build/build_zlib_for_wos.sh before building curl."
require_file "$TARGET_SYSROOT/include/zlib.h" "Run scripts/build/build_zlib_for_wos.sh before building curl."
require_file "$TARGET_SYSROOT/lib/libssl.a" "Run scripts/build/build_openssl_for_wos.sh before building curl."
require_file "$TARGET_SYSROOT/lib/libcrypto.a" "Run scripts/build/build_openssl_for_wos.sh before building curl."
require_file "$TARGET_SYSROOT/include/openssl/ssl.h" "Run scripts/build/build_openssl_for_wos.sh before building curl."

CURL_SOURCE_DIR="$(resolve_curl_source)"
copy_source_to_workdir "$CURL_SOURCE_DIR"
patch_config_sub_for_wos "$CURL_WORK/config.sub"

mkdir -p "$TARGET_SYSROOT/bin" "$TARGET_SYSROOT/lib" "$TARGET_SYSROOT/include"
if [ ! -e "$TARGET_SYSROOT/usr" ]; then
    ln -s . "$TARGET_SYSROOT/usr"
fi
stage_ca_bundle

CURL_CFLAGS="--sysroot=$TARGET_SYSROOT -O2 -g -fPIC -fPIE -fno-sanitize=safe-stack -fno-stack-protector"
CURL_LDFLAGS="--sysroot=$TARGET_SYSROOT -fuse-ld=lld -L$TARGET_SYSROOT/lib -Wl,--dynamic-linker=/lib/ld.so -Wl,-rpath,/usr/lib -fno-sanitize=safe-stack"

export CC="${WOS_CCACHE_PREFIX}$HOST/bin/clang --target=$TARGET_ARCH --sysroot=$TARGET_SYSROOT"
export AR="$HOST/bin/llvm-ar"
export RANLIB="$HOST/bin/llvm-ranlib"
export STRIP="$HOST/bin/llvm-strip"
export CFLAGS="$CURL_CFLAGS"
export CPPFLAGS="-I$TARGET_SYSROOT/include"
export LDFLAGS="$CURL_LDFLAGS"
export LIBS="-lssl -lcrypto -lz -lpthread -ldl"
export PKG_CONFIG=false
export ac_cv_header_stdatomic_h=no

(
    cd "$CURL_WORK"
    ./configure \
        "${CURL_CONFIGURE_CACHE_ARGS[@]}" \
        "${CURL_CONFIGURE_BUILD_ARGS[@]}" \
        --host="$TARGET_ARCH" \
        --prefix= \
        --bindir=/bin \
        --libdir=/lib \
        --includedir=/include \
        --datarootdir=/share \
        --datadir=/share \
        --mandir=/share/man \
        --disable-shared \
        --enable-static \
        --with-openssl="$TARGET_SYSROOT" \
        --with-zlib="$TARGET_SYSROOT" \
        --with-ca-bundle=/etc/ssl/certs/ca-certificates.crt \
        --disable-ldap \
        --disable-ldaps \
        --without-libpsl \
        --without-brotli \
        --without-zstd \
        --without-nghttp2 \
        --without-ngtcp2 \
        --without-nghttp3 \
        --without-quiche \
        --without-libidn2 \
        --disable-threaded-resolver \
        --disable-manual \
        --disable-docs
)

make -C "$CURL_WORK" -j"$WOS_MAKE_JOBS"
make -C "$CURL_WORK" \
    prefix= \
    exec_prefix= \
    bindir=/bin \
    libdir=/lib \
    includedir=/include \
    datarootdir=/share \
    datadir=/share \
    mandir=/share/man \
    DESTDIR="$TARGET_SYSROOT" \
    install

require_file "$TARGET_SYSROOT/bin/curl" "curl install did not produce $TARGET_SYSROOT/bin/curl."
require_file "$TARGET_SYSROOT/bin/curl-config" "curl install did not produce $TARGET_SYSROOT/bin/curl-config."
require_file "$TARGET_SYSROOT/lib/libcurl.a" "curl install did not produce $TARGET_SYSROOT/lib/libcurl.a."
require_file "$TARGET_SYSROOT/include/curl/curl.h" "curl install did not produce curl headers."

if [ "$WOS_CURL_STRIP" != "0" ]; then
    "$HOST/bin/llvm-strip" "$TARGET_SYSROOT/bin/curl"
fi

echo "WOS curl installed to $TARGET_SYSROOT/bin/curl"
