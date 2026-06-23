#!/bin/bash
# Incrementally rebuild GNU make for WOS and install it into the sysroot.
# Expects the WOS libc/libc++ sysroot and host compiler to already exist.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$WORKSPACE_ROOT/tools/ccache_env.sh"
if [ -z "${CCACHE_DIR:-}" ]; then
    export CCACHE_DIR="${TMPDIR:-/tmp}/wos-make-ccache"
    mkdir -p "$CCACHE_DIR"
fi
wos_setup_ccache
WOS_CCACHE_PREFIX="$(wos_ccache_prefix)"

B="$WORKSPACE_ROOT/toolchain"
HOST="${WOS_HOST_TOOLCHAIN_ROOT:-$B/host}"
TARGET_ARCH="${WOS_TARGET_ARCH:-x86_64-pc-wos}"
TARGET_SYSROOT="${WOS_SYSROOT_PATH:-$B/sysroot}"
MAKE_BUILD="${WOS_MAKE_BUILD_DIR:-$B/make-build}"
MAKE_SRC="${WOS_MAKE_SOURCE_DIR:-$B/src/make}"
MAKE_VERSION="${WOS_GNU_MAKE_VERSION:-4.4.1}"
MAKE_TARBALL_URL="${WOS_GNU_MAKE_TARBALL_URL:-https://ftp.gnu.org/gnu/make/make-$MAKE_VERSION.tar.gz}"
MAKE_TARBALL_SHA256="${WOS_GNU_MAKE_TARBALL_SHA256:-dd16fb1d67bfab79a72f5e8390735c49e3e8e70b4945a15ab1f81ddb78658fb3}"

require_file() {
    local path="$1"
    local hint="$2"

    if [ ! -e "$path" ]; then
        echo "ERROR: missing $path" >&2
        echo "$hint" >&2
        exit 1
    fi
}

download_make_source() {
    local dest="$1"
    local archive_dir="$MAKE_BUILD/src"
    local archive="$archive_dir/make-$MAKE_VERSION.tar.gz"
    local tmp_dest="$dest.tmp"

    mkdir -p "$archive_dir"
    if [ ! -f "$archive" ]; then
        if ! command -v curl >/dev/null 2>&1; then
            echo "ERROR: GNU make source not found at $MAKE_SRC and curl is unavailable." >&2
            echo "Populate $MAKE_SRC with a GNU make release tree or install curl." >&2
            exit 1
        fi
        echo "Downloading GNU make $MAKE_VERSION source..." >&2
        curl -L "$MAKE_TARBALL_URL" -o "$archive.tmp"
        mv "$archive.tmp" "$archive"
    fi

    echo "$MAKE_TARBALL_SHA256  $archive" | sha256sum -c - >&2
    rm -rf "$tmp_dest" "$dest"
    mkdir -p "$tmp_dest"
    tar -xzf "$archive" -C "$tmp_dest" --strip-components=1
    mv "$tmp_dest" "$dest"
}

resolve_make_source() {
    local fallback_src="$MAKE_BUILD/src/make-$MAKE_VERSION"

    if [ -f "$MAKE_SRC/configure" ] || [ -f "$MAKE_SRC/bootstrap" ]; then
        printf '%s\n' "$MAKE_SRC"
        return 0
    fi

    if [ -f "$fallback_src/configure" ] || [ -f "$fallback_src/bootstrap" ]; then
        printf '%s\n' "$fallback_src"
        return 0
    fi

    if [ -d "$MAKE_SRC" ] && [ -n "$(find "$MAKE_SRC" -mindepth 1 -maxdepth 1 -print -quit)" ]; then
        echo "ERROR: GNU make source at $MAKE_SRC does not contain configure or bootstrap." >&2
        echo "Use a GNU make release tree or regenerate the Autotools files there." >&2
        exit 1
    fi

    download_make_source "$fallback_src"
    printf '%s\n' "$fallback_src"
}

ensure_make_configure() {
    local source_dir="$1"

    if [ -f "$source_dir/configure" ]; then
        return 0
    fi

    if [ ! -f "$source_dir/bootstrap" ]; then
        echo "ERROR: GNU make source at $source_dir has neither configure nor bootstrap." >&2
        exit 1
    fi

    echo "Running GNU make bootstrap..."
    (cd "$source_dir" && ./bootstrap --skip-po --no-bootstrap-sync)
}

patch_config_sub_for_wos() {
    local source_dir="$1"
    local config_sub="$source_dir/build-aux/config.sub"

    require_file "$config_sub" "GNU make source is missing build-aux/config.sub."
    if grep -q 'wos\*' "$config_sub"; then
        return 0
    fi

    echo "Patching GNU make config.sub to recognise WOS..."
    if grep -q '| fiwix\* | mlibc\* )' "$config_sub"; then
        sed -i 's/| fiwix\* | mlibc\* )/| fiwix* | mlibc* | wos* )/' "$config_sub"
    elif grep -q '| fiwix\* | mlibc\* |' "$config_sub"; then
        sed -i 's/| fiwix\* | mlibc\* |/| fiwix* | mlibc* | wos* |/' "$config_sub"
    else
        echo "ERROR: do not know how to patch $config_sub for WOS." >&2
        exit 1
    fi
}

require_file "$HOST/bin/clang" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/clang++" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/ld.lld" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/llvm-ar" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/llvm-ranlib" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/llvm-strip" "Run tools/host-toolchain.sh first."
require_file "$TARGET_SYSROOT/lib/libc.so" "Build mlibc before building GNU make."
require_file "$TARGET_SYSROOT/lib/libc++.so" "Build libc++ before building GNU make."
require_file "$TARGET_SYSROOT/lib/Scrt1.o" "Build mlibc startup objects before building GNU make."

MAKE_SOURCE_DIR="$(resolve_make_source)"
ensure_make_configure "$MAKE_SOURCE_DIR"
patch_config_sub_for_wos "$MAKE_SOURCE_DIR"

GNU_MAKE_CFLAGS="--sysroot=$TARGET_SYSROOT -O2 -g -fno-sanitize=safe-stack -fno-stack-protector"
GNU_MAKE_CXXFLAGS="$GNU_MAKE_CFLAGS -std=c++23 -isystem $TARGET_SYSROOT/include/c++/v1"
GNU_MAKE_LDFLAGS="--sysroot=$TARGET_SYSROOT -fuse-ld=lld -L$TARGET_SYSROOT/lib"

export CC="${WOS_CCACHE_PREFIX}$HOST/bin/clang --target=$TARGET_ARCH --sysroot=$TARGET_SYSROOT"
export CXX="${WOS_CCACHE_PREFIX}$HOST/bin/clang++ --target=$TARGET_ARCH --sysroot=$TARGET_SYSROOT"
export AR="$HOST/bin/llvm-ar"
export RANLIB="$HOST/bin/llvm-ranlib"
export STRIP="$HOST/bin/llvm-strip"
export CFLAGS="$GNU_MAKE_CFLAGS"
export CXXFLAGS="$GNU_MAKE_CXXFLAGS"
export LDFLAGS="$GNU_MAKE_LDFLAGS"

mkdir -p "$MAKE_BUILD" "$TARGET_SYSROOT/bin"

if [ ! -f "$MAKE_BUILD/Makefile" ] || [ "$MAKE_SOURCE_DIR/configure" -nt "$MAKE_BUILD/Makefile" ]; then
    echo "Configuring GNU make for WOS..."
    (
        cd "$MAKE_BUILD"
        "$MAKE_SOURCE_DIR/configure" \
            --host="$TARGET_ARCH" \
            --prefix="$TARGET_SYSROOT" \
            --disable-nls \
            --disable-load \
            --without-guile
    )
fi

if [ -f "$MAKE_BUILD/make" ]; then
    for lib in "$TARGET_SYSROOT"/lib/libc.so "$TARGET_SYSROOT"/lib/libc++.so \
               "$TARGET_SYSROOT"/lib/libc++abi.so "$TARGET_SYSROOT"/lib/libm.so; do
        if [ -f "$lib" ] && [ "$lib" -nt "$MAKE_BUILD/make" ]; then
            echo "Sysroot library $(basename "$lib") changed - forcing relink"
            rm -f "$MAKE_BUILD/make"
            break
        fi
    done
fi

make -C "$MAKE_BUILD" -j"$(nproc)"

install -m 755 "$MAKE_BUILD/make" "$TARGET_SYSROOT/bin/make"
ln -sfn make "$TARGET_SYSROOT/bin/gmake"
echo "GNU make installed to $TARGET_SYSROOT/bin/make"
