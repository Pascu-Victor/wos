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
WOS_BUILD_JOBS="$(wos_build_jobs)"
WOS_MAKE_JOBS="$(wos_make_jobs)"

B="$WORKSPACE_ROOT/toolchain"
HOST="${WOS_HOST_TOOLCHAIN_ROOT:-$B/host}"
TARGET_ARCH="${WOS_TARGET_ARCH:-x86_64-pc-wos}"
TARGET_SYSROOT="${WOS_SYSROOT_PATH:-$B/sysroot}"
MAKE_BUILD="${WOS_MAKE_BUILD_DIR:-$B/make-build}"
MAKE_SRC="${WOS_MAKE_SOURCE_DIR:-$B/src/make}"
MAKE_VERSION="${WOS_GNU_MAKE_VERSION:-4.4.1}"
MAKE_TARBALL_URL="${WOS_GNU_MAKE_TARBALL_URL:-https://ftp.gnu.org/gnu/make/make-$MAKE_VERSION.tar.gz}"
MAKE_TARBALL_SHA256="${WOS_GNU_MAKE_TARBALL_SHA256:-dd16fb1d67bfab79a72f5e8390735c49e3e8e70b4945a15ab1f81ddb78658fb3}"
MAKE_TARBALL_URLS="${WOS_GNU_MAKE_TARBALL_URLS:-$MAKE_TARBALL_URL}"
MAKE_DOWNLOAD_ATTEMPTS="${WOS_GNU_MAKE_DOWNLOAD_ATTEMPTS:-${WOS_SOURCE_DOWNLOAD_ATTEMPTS:-3}}"
HOST_SYSTEM="$(uname -s 2>/dev/null || printf unknown)"

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
        wos_download_file "GNU make $MAKE_VERSION source" "$archive" "$MAKE_TARBALL_URLS" "$MAKE_DOWNLOAD_ATTEMPTS"
    fi

    echo "$MAKE_TARBALL_SHA256  $archive" | sha256sum -c - >&2
    wos_remove_tree "$tmp_dest"
    wos_remove_tree "$dest"
    mkdir -p "$tmp_dest"
    tar -xzf "$archive" -C "$tmp_dest" --strip-components 1
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

    if [ -d "$MAKE_SRC" ] && wos_dir_has_entries "$MAKE_SRC"; then
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

patch_default_jobserver_for_wos() {
    local source_dir="$1"
    local posixos="$source_dir/src/posixos.c"
    local marker="default to pipe jobserver on WOS"
    local needle='  if (!style || strcmp (style, "fifo") == 0)'
    local patched="$posixos.wos-jobserver.$$"

    require_file "$posixos" "GNU make source is missing src/posixos.c."
    if grep -q "$marker" "$posixos"; then
        return 0
    fi
    if ! grep -qF "$needle" "$posixos"; then
        echo "ERROR: do not know how to patch GNU make jobserver default in $posixos." >&2
        exit 1
    fi

    echo "Patching GNU make to default to pipe jobserver on WOS..."
    while IFS= read -r line || [ -n "$line" ]; do
        if [ "$line" = "$needle" ]; then
            printf '%s\n' "#if defined(__WOS__) /* $marker */"
            printf '%s\n' '  if (style && strcmp (style, "fifo") == 0)'
            printf '%s\n' "#else"
            printf '%s\n' "$needle"
            printf '%s\n' "#endif"
        else
            printf '%s\n' "$line"
        fi
    done <"$posixos" >"$patched"
    mv "$patched" "$posixos"
}

rewrite_file_for_mtime() {
    local path="$1"
    local tmp="$path.wos-mtime.$$"

    if [ ! -f "$path" ]; then
        return 0
    fi

    if touch "$path" 2>/dev/null; then
        return 0
    fi

    cp "$path" "$tmp"
    mv "$tmp" "$path"
}

refresh_make_release_generated_files() {
    local source_dir="$1"
    local file
    local generated_files=(
        aclocal.m4
        configure
        src/config.h.in
        Makefile.in
        lib/Makefile.in
        doc/Makefile.in
        po/Makefile.in.in
        po/Makefile.in
    )

    for file in "${generated_files[@]}"; do
        rewrite_file_for_mtime "$source_dir/$file"
    done
}

refresh_make_build_generated_files() {
    local build_dir="$1"
    local file
    local generated_files=(
        config.status
        Makefile
        lib/Makefile
        doc/Makefile
        po/Makefile.in
        po/Makefile
        src/config.h
        src/stamp-h1
    )

    for file in "${generated_files[@]}"; do
        rewrite_file_for_mtime "$build_dir/$file"
    done
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
patch_default_jobserver_for_wos "$MAKE_SOURCE_DIR"
if [ "$HOST_SYSTEM" = "WOS" ]; then
    refresh_make_release_generated_files "$MAKE_SOURCE_DIR"
fi

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

if [ "$HOST_SYSTEM" = "WOS" ] && { [ ! -x "$MAKE_BUILD/make" ] || [ ! -f "$MAKE_BUILD/config.status" ]; }; then
    rm -f "$MAKE_BUILD/Makefile"
fi

if [ ! -f "$MAKE_BUILD/Makefile" ] || [ ! -f "$MAKE_BUILD/config.status" ] || [ "$MAKE_SOURCE_DIR/configure" -nt "$MAKE_BUILD/Makefile" ]; then
    echo "Configuring GNU make for WOS..."
    GNU_MAKE_CONFIGURE_BUILD_ARGS=()
    GNU_MAKE_CONFIGURE_CACHE_ARGS=()
    if [ "$HOST_SYSTEM" = "WOS" ]; then
        GNU_MAKE_CONFIGURE_BUILD_ARGS=(--build="$TARGET_ARCH")
        GNU_MAKE_CONFIGURE_CACHE_ARGS=(
            ac_cv_path_GREP=/usr/bin/grep
            "ac_cv_path_EGREP=/usr/bin/grep -E"
            "ac_cv_path_FGREP=/usr/bin/grep -F"
            ac_cv_func_mempcpy=yes
        )
    fi
    (
        cd "$MAKE_BUILD"
        "$MAKE_SOURCE_DIR/configure" \
            "${GNU_MAKE_CONFIGURE_CACHE_ARGS[@]}" \
            "${GNU_MAKE_CONFIGURE_BUILD_ARGS[@]}" \
            --host="$TARGET_ARCH" \
            --prefix="$TARGET_SYSROOT" \
            --disable-nls \
            --disable-load \
            --without-guile
    )
fi

if [ "$HOST_SYSTEM" = "WOS" ]; then
    refresh_make_build_generated_files "$MAKE_BUILD"
fi

GNU_MAKE_BUILD_ARGS=()
if [ "$HOST_SYSTEM" = "WOS" ]; then
    GNU_MAKE_BUILD_ARGS=(MAKEINFO=true)
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

wos_make "$WOS_MAKE_JOBS" -C "$MAKE_BUILD" "${GNU_MAKE_BUILD_ARGS[@]}"

install -m 755 "$MAKE_BUILD/make" "$TARGET_SYSROOT/bin/make"
ln -sfn make "$TARGET_SYSROOT/bin/gmake"
echo "GNU make installed to $TARGET_SYSROOT/bin/make"
