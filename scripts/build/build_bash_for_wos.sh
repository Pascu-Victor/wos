#!/bin/bash
# Cross-build Bash so it can run inside WOS and install it into the sysroot.
# Uses toolchain/src/bash when populated, otherwise downloads a GNU release.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$WORKSPACE_ROOT/tools/ccache_env.sh"
if [ -z "${CCACHE_DIR:-}" ]; then
    export CCACHE_DIR="${TMPDIR:-/tmp}/wos-bash-ccache"
    mkdir -p "$CCACHE_DIR"
fi
wos_setup_ccache
WOS_CCACHE_PREFIX="$(wos_ccache_prefix)"
WOS_BUILD_JOBS="$(wos_build_jobs)"
WOS_MAKE_JOBS="$(wos_make_jobs)"
HOST_SYSTEM="$(uname -s 2>/dev/null || printf unknown)"

B="$WORKSPACE_ROOT/toolchain"
HOST="${WOS_HOST_TOOLCHAIN_ROOT:-$B/host}"
TARGET_ARCH="${WOS_TARGET_ARCH:-x86_64-pc-wos}"
TARGET_SYSROOT="${WOS_SYSROOT_PATH:-$B/sysroot}"
BASH_SRC="${WOS_BASH_SOURCE_DIR:-$B/src/bash}"
BASH_BUILD="${WOS_BASH_BUILD_DIR:-$B/bash-build}"
BASH_WORK="$BASH_BUILD/work"
BASH_VERSION="${WOS_BASH_VERSION:-5.3}"
BASH_TARBALL_URL="${WOS_BASH_TARBALL_URL:-https://ftp.gnu.org/gnu/bash/bash-$BASH_VERSION.tar.gz}"
BASH_TARBALL_SHA256="${WOS_BASH_TARBALL_SHA256:-0d5cd86965f869a26cf64f4b71be7b96f90a3ba8b3d74e27e8e9d9d5550f31ba}"
BASH_TARBALL_URLS="${WOS_BASH_TARBALL_URLS:-$BASH_TARBALL_URL}"
BASH_DOWNLOAD_ATTEMPTS="${WOS_BASH_DOWNLOAD_ATTEMPTS:-${WOS_SOURCE_DOWNLOAD_ATTEMPTS:-3}}"
WOS_BASH_STRIP="${WOS_BASH_STRIP:-0}"

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

find_build_cc() {
    local candidate

    for candidate in "${CC_FOR_BUILD:-}" /usr/bin/gcc /usr/bin/cc gcc cc; do
        [ -n "$candidate" ] || continue
        if command -v "$candidate" >/dev/null 2>&1; then
            command -v "$candidate"
            return 0
        fi
    done

    return 1
}

download_bash_source() {
    local dest="$1"
    local archive_dir="$BASH_BUILD/src"
    local archive="$archive_dir/bash-$BASH_VERSION.tar.gz"
    local tmp_dest="$dest.tmp"

    mkdir -p "$archive_dir"
    if [ ! -f "$archive" ]; then
        if ! command -v curl >/dev/null 2>&1; then
            echo "ERROR: Bash source not found at $BASH_SRC and curl is unavailable." >&2
            echo "Populate $BASH_SRC with a Bash release tree or install curl." >&2
            exit 1
        fi
        wos_download_file "Bash $BASH_VERSION source" "$archive" "$BASH_TARBALL_URLS" "$BASH_DOWNLOAD_ATTEMPTS"
    fi

    echo "$BASH_TARBALL_SHA256  $archive" | sha256sum -c - >&2
    wos_remove_tree "$tmp_dest"
    wos_remove_tree "$dest"
    mkdir -p "$tmp_dest"
    tar -xzf "$archive" -C "$tmp_dest" --strip-components 1
    mv "$tmp_dest" "$dest"
}

resolve_bash_source() {
    local fallback_src="$BASH_BUILD/src/bash-$BASH_VERSION"

    if [ -f "$BASH_SRC/configure" ]; then
        printf '%s\n' "$BASH_SRC"
        return 0
    fi

    if [ -f "$fallback_src/configure" ]; then
        printf '%s\n' "$fallback_src"
        return 0
    fi

    if [ -d "$BASH_SRC" ] && wos_dir_has_entries "$BASH_SRC"; then
        echo "ERROR: Bash source at $BASH_SRC does not contain configure." >&2
        echo "Use a Bash release tree or clear the directory so the release tarball can be downloaded." >&2
        exit 1
    fi

    download_bash_source "$fallback_src"
    printf '%s\n' "$fallback_src"
}

copy_source_to_workdir() {
    local source_dir="$1"

    wos_remove_tree "$BASH_WORK"
    mkdir -p "$BASH_WORK"
    wos_copy_tree_entries_excluding "$source_dir" "$BASH_WORK" ".git" ".github"
}

refresh_bash_release_generated_files() {
    local source_dir="$1"
    local file
    local generated_files=(
        aclocal.m4
        config.h.in
        configure
    )

    # WOS BusyBox touch currently does not advance existing mtimes. Rewrite the
    # release-generated Autoconf outputs so Bash maintainer rules stay idle.
    sleep 1
    for file in "${generated_files[@]}"; do
        wos_refresh_file_mtime "$source_dir/$file"
    done
}

refresh_bash_build_generated_files() {
    local build_dir="$1"
    local file
    local generated_files=(
        config.status
        Makefile
        config.h
        stamp-h
        buildconf.h
        support/bashbug.sh
    )

    sleep 1
    for file in "${generated_files[@]}"; do
        wos_refresh_file_mtime "$build_dir/$file"
    done
}

patch_config_sub_for_wos() {
    local config_sub="$1"

    require_file "$config_sub" "Bash source is missing support/config.sub."
    if grep -q 'wos\*' "$config_sub"; then
        return 0
    fi

    echo "Patching Bash config.sub to recognise WOS..."
    if grep -q '| fiwix\* \\' "$config_sub"; then
        sed -i '/| fiwix\* \\/a\	| wos* \\' "$config_sub"
    elif grep -q '| fiwix\* |' "$config_sub"; then
        sed -i 's/| fiwix\* |/| fiwix* | wos* |/' "$config_sub"
    elif grep -q '| fiwix\* )' "$config_sub"; then
        sed -i 's/| fiwix\* )/| fiwix* | wos* )/' "$config_sub"
    else
        echo "ERROR: do not know how to patch $config_sub for WOS." >&2
        exit 1
    fi
}

patch_shobj_conf_for_wos() {
    local shobj_conf="$1"

    require_file "$shobj_conf" "Bash source is missing support/shobj-conf."
    if grep -q 'wos\*-\*' "$shobj_conf"; then
        return 0
    fi

    echo "Patching Bash shobj-conf to use ELF shared-object flags for WOS..."
    if grep -q 'linux\*-\*|gnu\*-\*' "$shobj_conf"; then
        sed -i 's/linux\*-\*|gnu\*-\*/linux*-*|wos*-*|gnu*-*/' "$shobj_conf"
    else
        echo "ERROR: do not know how to patch $shobj_conf for WOS." >&2
        exit 1
    fi
}

detect_bash_build_triple() {
    local config_guess="$1"
    local build_triple
    local host_system

    if build_triple="$(sh "$config_guess")"; then
        printf '%s\n' "$build_triple"
        return 0
    fi

    host_system="$(uname -s 2>/dev/null || printf unknown)"
    if [ "$host_system" = "WOS" ]; then
        echo "Bash config.guess does not recognise WOS; using $TARGET_ARCH as build triplet." >&2
        printf '%s\n' "$TARGET_ARCH"
        return 0
    fi

    echo "ERROR: Bash config.guess failed for host system $host_system." >&2
    return 1
}

write_config_site() {
    local tmp_config_site

    mkdir -p "$BASH_BUILD"
    tmp_config_site="$(mktemp "$BASH_BUILD/config.site.XXXXXX")"
    cat > "$tmp_config_site" <<'EOF'
bash_cv_dev_fd=absent
bash_cv_dev_stdin=present
bash_cv_func_snprintf=yes
bash_cv_func_vsnprintf=yes
bash_cv_job_control_missing=present
bash_cv_must_reinstall_sighandlers=no
bash_cv_pgrp_pipe=no
bash_cv_printf_a_format=yes
bash_cv_sys_named_pipes=missing
bash_cv_termcap_lib=gnutermcap
bash_cv_unusable_rtsigs=yes
bash_cv_wcontinued_broken=no
EOF

    if [ ! -f "$BASH_BUILD/config.site" ] || ! cmp -s "$tmp_config_site" "$BASH_BUILD/config.site"; then
        mv "$tmp_config_site" "$BASH_BUILD/config.site"
    else
        rm -f "$tmp_config_site"
    fi
}

require_file "$HOST/bin/clang" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/ld.lld" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/llvm-ar" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/llvm-ranlib" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/llvm-strip" "Run tools/host-toolchain.sh first."
require_file "$TARGET_SYSROOT/lib/libc.so" "Build mlibc before building Bash."
require_file "$TARGET_SYSROOT/lib/Scrt1.o" "Build mlibc startup objects before building Bash."

BASH_SOURCE_DIR="$(resolve_bash_source)"
copy_source_to_workdir "$BASH_SOURCE_DIR"
patch_config_sub_for_wos "$BASH_WORK/support/config.sub"
patch_shobj_conf_for_wos "$BASH_WORK/support/shobj-conf"
if [ "$HOST_SYSTEM" = "WOS" ]; then
    refresh_bash_release_generated_files "$BASH_WORK"
fi
write_config_site

BUILD_TRIPLE="$(detect_bash_build_triple "$BASH_WORK/support/config.guess")"

mkdir -p "$TARGET_SYSROOT/bin" "$TARGET_SYSROOT/lib"
if [ ! -e "$TARGET_SYSROOT/usr" ]; then
    ln -s . "$TARGET_SYSROOT/usr"
fi

BASH_CFLAGS="--sysroot=$TARGET_SYSROOT -O2 -g -fPIC -fPIE -fno-sanitize=safe-stack -fno-stack-protector -DNEED_EXTERN_PC"
BASH_LDFLAGS="--sysroot=$TARGET_SYSROOT -fuse-ld=lld -L$TARGET_SYSROOT/lib -Wl,--dynamic-linker=/lib/ld.so -Wl,-rpath,/usr/lib -fno-sanitize=safe-stack"
BUILD_CC="$(find_build_cc)" || {
    echo "ERROR: could not find a host C compiler for Bash build tools." >&2
    exit 1
}

export CC="${WOS_CCACHE_PREFIX}$HOST/bin/clang --target=$TARGET_ARCH --sysroot=$TARGET_SYSROOT"
export CC_FOR_BUILD="$BUILD_CC"
export CFLAGS_FOR_BUILD="${CFLAGS_FOR_BUILD:--g -std=gnu17}"
export AR="$HOST/bin/llvm-ar"
export RANLIB="$HOST/bin/llvm-ranlib"
export STRIP="$HOST/bin/llvm-strip"
export CFLAGS="$BASH_CFLAGS"
export CPPFLAGS="-I$TARGET_SYSROOT/include"
export LDFLAGS="$BASH_LDFLAGS"
export CONFIG_SITE="$BASH_BUILD/config.site"

(
    cd "$BASH_WORK"
    ./configure \
        --build="$BUILD_TRIPLE" \
        --host="$TARGET_ARCH" \
        --prefix=/usr \
        --exec-prefix=/usr \
        --bindir=/usr/bin \
        --disable-nls \
        --disable-rpath \
        --without-bash-malloc \
        --without-installed-readline \
        --with-gnu-ld
)

if [ "$HOST_SYSTEM" = "WOS" ]; then
    refresh_bash_build_generated_files "$BASH_WORK"
fi

if [ -f "$BASH_WORK/bash" ]; then
    for lib in "$TARGET_SYSROOT"/lib/libc.so "$TARGET_SYSROOT"/lib/libm.so "$TARGET_SYSROOT"/lib/libdl.so; do
        if [ -f "$lib" ] && [ "$lib" -nt "$BASH_WORK/bash" ]; then
            echo "Sysroot library $(basename "$lib") changed - forcing relink"
            rm -f "$BASH_WORK/bash"
            break
        fi
    done
fi

wos_make "$WOS_MAKE_JOBS" -C "$BASH_WORK" bash bashbug

# Upstream install also builds optional example loadable builtins. WOS only
# stages the runtime shell, so install the required outputs directly.
install -d "$TARGET_SYSROOT/bin"
install -m 0755 "$BASH_WORK/bash" "$TARGET_SYSROOT/bin/bash"
install -m 0755 "$BASH_WORK/bashbug" "$TARGET_SYSROOT/bin/bashbug"

require_file "$TARGET_SYSROOT/bin/bash" "Bash install did not produce $TARGET_SYSROOT/bin/bash."

if [ "$WOS_BASH_STRIP" != "0" ]; then
    "$HOST/bin/llvm-strip" "$TARGET_SYSROOT/bin/bash"
fi

echo "Native WOS Bash installed to $TARGET_SYSROOT/bin/bash"
