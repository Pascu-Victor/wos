#!/bin/bash
# Cross-build ncursesw for WOS and install it into the sysroot.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$WORKSPACE_ROOT/tools/ccache_env.sh"
if [ -z "${CCACHE_DIR:-}" ]; then
    export CCACHE_DIR="${TMPDIR:-/tmp}/wos-ncurses-ccache"
    mkdir -p "$CCACHE_DIR"
fi
wos_setup_ccache
WOS_CCACHE_PREFIX="$(wos_ccache_prefix)"
WOS_MAKE_JOBS="$(wos_make_jobs)"

B="$WORKSPACE_ROOT/toolchain"
HOST="${WOS_HOST_TOOLCHAIN_ROOT:-$B/host}"
TARGET_ARCH="${WOS_TARGET_ARCH:-x86_64-pc-wos}"
TARGET_SYSROOT="${WOS_SYSROOT_PATH:-$B/sysroot}"
NCURSES_SRC="${WOS_NCURSES_SOURCE_DIR:-$B/src/ncurses}"
NCURSES_BUILD="${WOS_NCURSES_BUILD_DIR:-$B/ncurses-build}"
NCURSES_WORK="$NCURSES_BUILD/work"
NCURSES_VERSION="${WOS_NCURSES_VERSION:-6.6}"
NCURSES_TARBALL_URL="${WOS_NCURSES_TARBALL_URL:-https://ftp.gnu.org/pub/gnu/ncurses/ncurses-$NCURSES_VERSION.tar.gz}"
NCURSES_TARBALL_SHA256="${WOS_NCURSES_TARBALL_SHA256:-355b4cbbed880b0381a04c46617b7656e362585d52e9cf84a67e2009b749ff11}"
NCURSES_TARBALL_URLS="${WOS_NCURSES_TARBALL_URLS:-$NCURSES_TARBALL_URL}"
NCURSES_DOWNLOAD_ATTEMPTS="${WOS_NCURSES_DOWNLOAD_ATTEMPTS:-${WOS_SOURCE_DOWNLOAD_ATTEMPTS:-3}}"
WOS_NCURSES_STRIP="${WOS_NCURSES_STRIP:-0}"
HOST_SYSTEM="$(uname -s 2>/dev/null || printf unknown)"

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

download_ncurses_source() {
    local dest="$1"
    local archive_dir="$NCURSES_BUILD/src"
    local archive="$archive_dir/ncurses-$NCURSES_VERSION.tar.gz"
    local tmp_dest="$dest.tmp"

    mkdir -p "$archive_dir"
    if [ ! -f "$archive" ]; then
        if ! command -v curl >/dev/null 2>&1; then
            echo "ERROR: ncurses source not found at $NCURSES_SRC and curl is unavailable." >&2
            echo "Populate $NCURSES_SRC with an ncurses release tree or install curl." >&2
            exit 1
        fi
        wos_download_file "ncurses $NCURSES_VERSION source" "$archive" "$NCURSES_TARBALL_URLS" "$NCURSES_DOWNLOAD_ATTEMPTS"
    fi

    echo "$NCURSES_TARBALL_SHA256  $archive" | sha256sum -c - >&2
    wos_remove_tree "$tmp_dest"
    wos_remove_tree "$dest"
    mkdir -p "$tmp_dest"
    tar -xzf "$archive" -C "$tmp_dest" --strip-components 1
    mv "$tmp_dest" "$dest"
}

resolve_ncurses_source() {
    local fallback_src="$NCURSES_BUILD/src/ncurses-$NCURSES_VERSION"

    if [ -f "$NCURSES_SRC/configure" ]; then
        printf '%s\n' "$NCURSES_SRC"
        return 0
    fi

    if [ -f "$fallback_src/configure" ]; then
        printf '%s\n' "$fallback_src"
        return 0
    fi

    if [ -d "$NCURSES_SRC" ] && wos_dir_has_entries "$NCURSES_SRC"; then
        echo "ERROR: ncurses source at $NCURSES_SRC does not contain configure." >&2
        echo "Use an ncurses release tree or clear the directory so the release tarball can be downloaded." >&2
        exit 1
    fi

    download_ncurses_source "$fallback_src" >&2 || exit 1
    printf '%s\n' "$fallback_src"
}

copy_source_to_workdir() {
    local source_dir="$1"

    wos_remove_tree "$NCURSES_WORK"
    mkdir -p "$NCURSES_WORK"
    wos_copy_tree_entries_excluding "$source_dir" "$NCURSES_WORK" ".git" ".github"
}

patch_config_sub_for_wos() {
    local config_sub="$1"

    require_file "$config_sub" "ncurses source is missing config.sub."
    if grep -q 'wos\*' "$config_sub"; then
        return 0
    fi

    echo "Patching ncurses config.sub to recognise WOS..."
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

write_ncurses_config_site() {
    local tmp_config_site

    mkdir -p "$NCURSES_WORK"
    tmp_config_site="$(mktemp "$NCURSES_WORK/config.site.XXXXXX")"
    cat > "$tmp_config_site" <<'EOF'
ac_cv_c_bigendian=no
ac_cv_c_const=yes
ac_cv_c_inline=inline
ac_cv_func_access=yes
ac_cv_func_fcntl=yes
ac_cv_func_getcwd=yes
ac_cv_func_getegid=yes
ac_cv_func_geteuid=yes
ac_cv_func_getopt=yes
ac_cv_func_gettimeofday=yes
ac_cv_func_isascii=yes
ac_cv_func_lstat=yes
ac_cv_func_memmove=yes
ac_cv_func_mkstemp=yes
ac_cv_func_openat=yes
ac_cv_func_poll=yes
ac_cv_func_putenv=yes
ac_cv_func_realloc=yes
ac_cv_func_setenv=yes
ac_cv_func_setlocale=yes
ac_cv_func_sigaction=yes
ac_cv_func_sigvec=no
ac_cv_func_strdup=yes
ac_cv_func_strstr=yes
ac_cv_func_symlink=yes
ac_cv_func_tcgetattr=yes
ac_cv_func_times=yes
ac_cv_func_tsearch=yes
ac_cv_func_unsetenv=yes
ac_cv_func_vsnprintf=yes
ac_cv_have_decl_errno=yes
ac_cv_header_dirent_dirent_h=yes
ac_cv_header_fcntl_h=yes
ac_cv_header_inttypes_h=yes
ac_cv_header_limits_h=yes
ac_cv_header_locale_h=yes
ac_cv_header_poll_h=yes
ac_cv_header_stdint_h=yes
ac_cv_header_sys_ioctl_h=yes
ac_cv_header_sys_param_h=yes
ac_cv_header_sys_poll_h=yes
ac_cv_header_sys_select_h=yes
ac_cv_header_sys_time_h=yes
ac_cv_header_sys_times_h=yes
ac_cv_header_termio_h=no
ac_cv_header_termios_h=yes
ac_cv_header_unistd_h=yes
ac_cv_path_LDCONFIG=:
ac_cv_path_NCURSES_CONFIG=none
ac_cv_prog_AWK=awk
ac_cv_prog_cc_c89=
ac_cv_prog_cc_g=yes
ac_cv_prog_cc_stdc=
ac_cv_prog_make_make_set=yes
ac_cv_safe_to_define___extensions__=yes
ac_cv_sys_largefile_opts='none needed'
ac_cv_type_sigaction=yes
ac_cv_type_sig_atomic_t=yes
ac_cv_type_size_t=yes
ac_cv_type_ssize_t=yes
cf_cv_curses_dir=none
cf_cv_fopen_bin_r=yes
cf_cv_fopen_bin_w=yes
cf_cv_func_nanosleep=yes
cf_cv_gettimeofday=yes
cf_cv_have_tcgetattr=yes
cf_cv_link_dataonly=yes
cf_cv_link_funcs=yes
cf_cv_link_libs=yes
cf_cv_mixedcase=unknown
cf_cv_need_xopen_extension=no
cf_cv_prog_CC_c_o=yes
cf_cv_prog_cc_c_o=yes
cf_cv_sig_atomic_t=sig_atomic_t
cf_cv_sizechange=yes
cf_cv_snprintf=yes
cf_cv_sys_time_select=yes
cf_cv_system_name=wos
cf_cv_termios=yes
cf_cv_type_of_bool='unsigned char'
cf_cv_typeof_chtype=uint32_t
cf_cv_typeof_mmask_t=uint32_t
cf_cv_typeof_ospeed=short
cf_cv_unicode_locale=yes
cf_cv_utf8_lib=no
cf_cv_working_poll=yes
cf_cv_working_pollin=yes
cf_cv_working_tdelete=yes
cf_cv_wsizeof_chtype=4
cf_cv_wsizeof_mmask_t=4
EOF

    if [ ! -f "$NCURSES_WORK/config.site" ] || ! cmp -s "$tmp_config_site" "$NCURSES_WORK/config.site"; then
        mv "$tmp_config_site" "$NCURSES_WORK/config.site"
    else
        rm -f "$tmp_config_site"
    fi
}

require_file "$HOST/bin/clang" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/ld.lld" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/llvm-ar" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/llvm-ranlib" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/llvm-strip" "Run tools/host-toolchain.sh first."
require_file "$TARGET_SYSROOT/lib/libc.so" "Build mlibc before building ncurses."
require_file "$TARGET_SYSROOT/lib/Scrt1.o" "Build mlibc startup objects before building ncurses."

BUILD_CC="$(find_build_cc)" || {
    echo "ERROR: host C compiler not found for ncurses build helper tools." >&2
    exit 1
}

NCURSES_SOURCE_DIR="$(resolve_ncurses_source)"
copy_source_to_workdir "$NCURSES_SOURCE_DIR"
patch_config_sub_for_wos "$NCURSES_WORK/config.sub"
write_ncurses_config_site
export CONFIG_SITE="$NCURSES_WORK/config.site"

mkdir -p "$TARGET_SYSROOT/bin" "$TARGET_SYSROOT/lib" "$TARGET_SYSROOT/include" "$TARGET_SYSROOT/share"
if [ ! -e "$TARGET_SYSROOT/usr" ]; then
    ln -s . "$TARGET_SYSROOT/usr"
fi

NCURSES_TARGET_FLAGS="--target=$TARGET_ARCH --sysroot=$TARGET_SYSROOT"
NCURSES_CFLAGS="$NCURSES_TARGET_FLAGS -O2 -g -fPIC -fno-sanitize=safe-stack -fno-stack-protector"
NCURSES_CPPFLAGS="$NCURSES_TARGET_FLAGS -I$TARGET_SYSROOT/include"
NCURSES_LDFLAGS="$NCURSES_TARGET_FLAGS -fuse-ld=lld -L$TARGET_SYSROOT/lib -fno-sanitize=safe-stack"

export CC="${WOS_CCACHE_PREFIX}$HOST/bin/clang"
export AR="$HOST/bin/llvm-ar"
export RANLIB="$HOST/bin/llvm-ranlib"
export STRIP="$HOST/bin/llvm-strip"
export CFLAGS="$NCURSES_CFLAGS"
export CPPFLAGS="$NCURSES_CPPFLAGS"
export LDFLAGS="$NCURSES_LDFLAGS"

NCURSES_CONFIGURE_BUILD_ARGS=()
if [ "$HOST_SYSTEM" = "WOS" ]; then
    NCURSES_CONFIGURE_BUILD_ARGS=(--build="$TARGET_ARCH")
fi

wos_timed_step "configure" "ncurses" \
    wos_run_in_dir "$NCURSES_WORK" \
    ./configure \
    "${NCURSES_CONFIGURE_BUILD_ARGS[@]}" \
    --host="$TARGET_ARCH" \
    --target="$TARGET_ARCH" \
    --prefix= \
    --bindir=/bin \
    --libdir=/lib \
    --includedir=/include \
    --datarootdir=/share \
    --datadir=/share \
    --mandir=/share/man \
    --with-build-cc="$BUILD_CC" \
    --with-build-cpp="$BUILD_CC -E" \
    --with-normal \
    --without-shared \
    --without-debug \
    --without-profile \
    --without-ada \
    --without-cxx \
    --without-cxx-binding \
    --without-tests \
    --without-manpages \
    --without-progs \
    --without-gpm \
    --without-dlsym \
    --disable-db-install \
    --disable-home-terminfo \
    --disable-rpath \
    --disable-stripping \
    --enable-widec \
    --enable-pc-files \
    --with-pkg-config-libdir=/lib/pkgconfig \
    --with-default-terminfo-dir=/usr/share/terminfo \
    --with-terminfo-dirs=/usr/share/terminfo \
    --with-fallbacks=xterm,xterm-256color,screen,screen-256color,tmux,tmux-256color,rxvt,rxvt-256color,linux,vt100,vt220,ansi,dumb

wos_make "$WOS_MAKE_JOBS" -C "$NCURSES_WORK"
# These top-level install goals overlap in ncurses' recursive include target.
# Running them in one parallel make can make two sub-makes create and remove
# the same generated headers.sed temporary files.
for install_target in install.libs install.includes install.data; do
    wos_make "$WOS_MAKE_JOBS" -C "$NCURSES_WORK" \
        DESTDIR="$TARGET_SYSROOT" \
        "$install_target"
done

if [ -f "$TARGET_SYSROOT/bin/ncursesw6-config" ]; then
    ln -sfn ncursesw6-config "$TARGET_SYSROOT/bin/ncursesw-config"
fi
if [ -f "$TARGET_SYSROOT/lib/libncursesw.a" ]; then
    ln -sfn libncursesw.a "$TARGET_SYSROOT/lib/libcursesw.a"
    ln -sfn libncursesw.a "$TARGET_SYSROOT/lib/libcurses.a"
fi
if [ -d "$TARGET_SYSROOT/include/ncursesw" ]; then
    for header in curses.h ncurses.h term.h termcap.h unctrl.h panel.h menu.h form.h eti.h; do
        [ -f "$TARGET_SYSROOT/include/ncursesw/$header" ] || continue
        ln -sfn "ncursesw/$header" "$TARGET_SYSROOT/include/$header"
    done
fi

require_file "$TARGET_SYSROOT/lib/libncursesw.a" "ncurses install did not produce $TARGET_SYSROOT/lib/libncursesw.a."
require_file "$TARGET_SYSROOT/include/curses.h" "ncurses install did not produce curses.h."
require_file "$TARGET_SYSROOT/include/term.h" "ncurses install did not produce term.h."

if [ "$WOS_NCURSES_STRIP" != "0" ] && [ -f "$TARGET_SYSROOT/lib/libncursesw.a" ]; then
    "$HOST/bin/llvm-strip" -g "$TARGET_SYSROOT/lib/libncursesw.a"
fi

echo "WOS ncursesw installed to $TARGET_SYSROOT/lib/libncursesw.a"
