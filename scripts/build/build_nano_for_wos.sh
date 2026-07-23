#!/bin/bash
# Cross-build GNU nano so it can run inside WOS and install it into the sysroot.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$WORKSPACE_ROOT/tools/ccache_env.sh"
if [ -z "${CCACHE_DIR:-}" ]; then
    export CCACHE_DIR="${TMPDIR:-/tmp}/wos-nano-ccache"
    mkdir -p "$CCACHE_DIR"
fi
wos_setup_ccache
WOS_CCACHE_PREFIX="$(wos_ccache_prefix)"
WOS_MAKE_JOBS="$(wos_make_jobs)"

B="$WORKSPACE_ROOT/toolchain"
HOST="${WOS_HOST_TOOLCHAIN_ROOT:-$B/host}"
TARGET_ARCH="${WOS_TARGET_ARCH:-x86_64-pc-wos}"
TARGET_SYSROOT="${WOS_SYSROOT_PATH:-$B/sysroot}"
NANO_SRC="${WOS_NANO_SOURCE_DIR:-$B/src/nano}"
NANO_BUILD="${WOS_NANO_BUILD_DIR:-$B/nano-build}"
NANO_WORK="$NANO_BUILD/work"
NANO_VERSION="${WOS_NANO_VERSION:-9.1}"
NANO_TARBALL_URL="${WOS_NANO_TARBALL_URL:-https://www.nano-editor.org/dist/latest/nano-$NANO_VERSION.tar.xz}"
NANO_TARBALL_SHA256="${WOS_NANO_TARBALL_SHA256:-5f47764274cb7532349ce0aa20ec10f1e8e851a6e9fa3eb66812c43d196db042}"
NANO_TARBALL_URLS="${WOS_NANO_TARBALL_URLS:-$NANO_TARBALL_URL}"
NANO_DOWNLOAD_ATTEMPTS="${WOS_NANO_DOWNLOAD_ATTEMPTS:-${WOS_SOURCE_DOWNLOAD_ATTEMPTS:-3}}"
WOS_NANO_STRIP="${WOS_NANO_STRIP:-0}"
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

download_nano_source() {
    local dest="$1"
    local archive_dir="$NANO_BUILD/src"
    local archive="$archive_dir/nano-$NANO_VERSION.tar.xz"
    local tmp_dest="$dest.tmp"

    mkdir -p "$archive_dir"
    if [ ! -f "$archive" ]; then
        if ! command -v curl >/dev/null 2>&1; then
            echo "ERROR: nano source not found at $NANO_SRC and curl is unavailable." >&2
            echo "Populate $NANO_SRC with a nano release tree or install curl." >&2
            exit 1
        fi
        wos_download_file "nano $NANO_VERSION source" "$archive" "$NANO_TARBALL_URLS" "$NANO_DOWNLOAD_ATTEMPTS"
    fi

    echo "$NANO_TARBALL_SHA256  $archive" | sha256sum -c - >&2
    wos_remove_tree "$tmp_dest"
    wos_remove_tree "$dest"
    mkdir -p "$tmp_dest"
    tar -xJf "$archive" -C "$tmp_dest" --strip-components 1
    mv "$tmp_dest" "$dest"
}

resolve_nano_source() {
    local fallback_src="$NANO_BUILD/src/nano-$NANO_VERSION"

    if [ -f "$NANO_SRC/configure" ]; then
        printf '%s\n' "$NANO_SRC"
        return 0
    fi

    if [ -f "$fallback_src/configure" ]; then
        printf '%s\n' "$fallback_src"
        return 0
    fi

    if [ -d "$NANO_SRC" ] && wos_dir_has_entries "$NANO_SRC"; then
        echo "ERROR: nano source at $NANO_SRC does not contain configure." >&2
        echo "Use a nano release tree or clear the directory so the release tarball can be downloaded." >&2
        exit 1
    fi

    download_nano_source "$fallback_src" >&2 || exit 1
    printf '%s\n' "$fallback_src"
}

copy_source_to_workdir() {
    local source_dir="$1"

    wos_remove_tree "$NANO_WORK"
    mkdir -p "$NANO_WORK"
    wos_copy_tree_entries_excluding "$source_dir" "$NANO_WORK" ".git" ".github"
}

patch_config_sub_for_wos() {
    local config_sub="$1"

    require_file "$config_sub" "nano source is missing config.sub."
    if grep -q 'wos\*' "$config_sub"; then
        return 0
    fi

    echo "Patching nano config.sub to recognise WOS..."
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

write_nano_config_site() {
    local tmp_config_site

    mkdir -p "$NANO_WORK"
    tmp_config_site="$(mktemp "$NANO_WORK/config.site.XXXXXX")"
    cat > "$tmp_config_site" <<'EOF'
ac_cv_c_bigendian=no
ac_cv_c_const=yes
ac_cv_c_inline=inline
ac_cv_c_restrict=__restrict__
ac_cv_c_undeclared_builtin_options='none needed'
ac_cv_func_access=yes
ac_cv_func_canonicalize_file_name=yes
ac_cv_func_chmod=yes
ac_cv_func_chown=yes
ac_cv_func_clock_gettime=yes
ac_cv_func_dup2=yes
ac_cv_func_faccessat=yes
ac_cv_func_fchmod=yes
ac_cv_func_fchown=yes
ac_cv_func_fcntl=yes
ac_cv_func_fsync=yes
ac_cv_func_getcwd=yes
ac_cv_func_getdelim=yes
ac_cv_func_getopt_long=yes
ac_cv_func_gettimeofday=yes
ac_cv_func_isblank=yes
ac_cv_func_iswblank=yes
ac_cv_func_lstat=yes
ac_cv_func_memmove=yes
ac_cv_func_mkstemp=yes
ac_cv_func_nanosleep=yes
ac_cv_func_regcomp=yes
ac_cv_func_regexec=yes
ac_cv_func_rename=yes
ac_cv_func_setlocale=yes
ac_cv_func_sigaction=yes
ac_cv_func_snprintf=yes
ac_cv_func_stat=yes
ac_cv_func_strcasecmp=yes
ac_cv_func_strdup=yes
ac_cv_func_strerror=yes
ac_cv_func_strncasecmp=yes
ac_cv_func_strnlen=yes
ac_cv_func_strstr=yes
ac_cv_func_unlink=yes
ac_cv_func_vsnprintf=yes
ac_cv_func_wcrtomb=yes
ac_cv_func_wctomb=yes
ac_cv_func_wctype=yes
ac_cv_func_wcwidth=yes
ac_cv_have_decl__snprintf=no
ac_cv_have_decl__snwprintf=no
ac_cv_header_curses_h=yes
ac_cv_header_dirent_h=yes
ac_cv_header_fcntl_h=yes
ac_cv_header_getopt_h=yes
ac_cv_header_inttypes_h=yes
ac_cv_header_langinfo_h=yes
ac_cv_header_limits_h=yes
ac_cv_header_locale_h=yes
ac_cv_header_ncursesw_curses_h=no
ac_cv_header_pwd_h=yes
ac_cv_header_regex_h=yes
ac_cv_header_stdbool_h=yes
ac_cv_header_stdint_h=yes
ac_cv_header_stdio_ext_h=no
ac_cv_header_stdio_h=yes
ac_cv_header_stdlib_h=yes
ac_cv_header_string_h=yes
ac_cv_header_strings_h=yes
ac_cv_header_sys_ioctl_h=yes
ac_cv_header_sys_param_h=yes
ac_cv_header_sys_stat_h=yes
ac_cv_header_sys_time_h=yes
ac_cv_header_sys_types_h=yes
ac_cv_header_termios_h=yes
ac_cv_header_unistd_h=yes
ac_cv_header_wchar_h=yes
ac_cv_header_wctype_h=yes
ac_cv_lib_magic_magic_open=no
ac_cv_objext=o
ac_cv_prog_AWK=awk
ac_cv_prog_cc_c99=
ac_cv_prog_cc_g=yes
ac_cv_prog_cc_stdc=
ac_cv_prog_make_make_set=yes
ac_cv_safe_to_define___extensions__=yes
ac_cv_search_clock_gettime='none required'
ac_cv_search_getpwnam='none required'
ac_cv_search_setlocale='none required'
ac_cv_sys_largefile_opts='none needed'
ac_cv_type_mode_t=yes
ac_cv_type_off_t=yes
ac_cv_type_pid_t=yes
ac_cv_type_size_t=yes
ac_cv_type_ssize_t=yes
ac_cv_type_uid_t=yes
am_cv_CC_dependencies_compiler_type=gcc3
am_cv_func_iconv=no
am_cv_make_support_nested_variables=yes
am_cv_prog_cc_c_o=yes
gl_cv_c_bool=no
gl_cv_func_getcwd_null=yes
gl_cv_func_getdelim=yes
gl_cv_func_malloc_0_nonnull=yes
gl_cv_func_mbrtowc_null_arg=yes
gl_cv_func_memchr_works=yes
gl_cv_func_realloc_0_nonnull=yes
gl_cv_func_regex=yes
gl_cv_func_rename_dest_works=yes
gl_cv_func_rename_dir=yes
gl_cv_func_rename_slash_dst_works=yes
gl_cv_func_rename_slash_src_works=yes
gl_cv_func_snprintf_posix=yes
gl_cv_func_stat_dir_slash=yes
gl_cv_func_stat_file_slash=yes
gl_cv_func_strerror_0_works=yes
gl_cv_func_vsnprintf_posix=yes
gl_cv_func_wcrtomb_retval=yes
gl_cv_func_wcwidth_works=yes
gl_cv_header_working_fcntl_h=yes
gl_cv_onwards_func_getcwd=yes
gl_cv_onwards_func_memchr=yes
gl_cv_onwards_func_strnlen=yes
gl_cv_onwards_func_wcwidth=yes
gl_cv_sys_struct_timespec_in_time_h=yes
gl_cv_type_wint_t=yes
gt_cv_func_CFLocaleCopyCurrent=no
gt_cv_func_CFPreferencesCopyAppValue=no
gt_cv_locale_fr=no
gt_cv_locale_ja=no
gt_cv_locale_zh_CN=no
lt_cv_deplibs_check_method=unknown
lt_cv_objdir=.libs
lt_cv_prog_compiler_c_o=yes
lt_cv_prog_compiler_pic='-fPIC -DPIC'
lt_cv_prog_compiler_pic_works=yes
lt_cv_prog_compiler_static_works=yes
lt_cv_sys_max_cmd_len=1572864
EOF

    if [ ! -f "$NANO_WORK/config.site" ] || ! cmp -s "$tmp_config_site" "$NANO_WORK/config.site"; then
        mv "$tmp_config_site" "$NANO_WORK/config.site"
    else
        rm -f "$tmp_config_site"
    fi
}

require_file "$HOST/bin/clang" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/ld.lld" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/llvm-ar" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/llvm-ranlib" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/llvm-strip" "Run tools/host-toolchain.sh first."
require_file "$TARGET_SYSROOT/lib/libc.so" "Build mlibc before building nano."
require_file "$TARGET_SYSROOT/lib/Scrt1.o" "Build mlibc startup objects before building nano."
require_file "$TARGET_SYSROOT/lib/libncursesw.a" "Run scripts/build/build_ncurses_for_wos.sh before building nano."
require_file "$TARGET_SYSROOT/include/curses.h" "Run scripts/build/build_ncurses_for_wos.sh before building nano."
require_file "$TARGET_SYSROOT/include/term.h" "Run scripts/build/build_ncurses_for_wos.sh before building nano."

NANO_SOURCE_DIR="$(resolve_nano_source)"
copy_source_to_workdir "$NANO_SOURCE_DIR"
patch_config_sub_for_wos "$NANO_WORK/config.sub"
write_nano_config_site
export CONFIG_SITE="$NANO_WORK/config.site"

mkdir -p "$TARGET_SYSROOT/bin" "$TARGET_SYSROOT/share"
if [ ! -e "$TARGET_SYSROOT/usr" ]; then
    ln -s . "$TARGET_SYSROOT/usr"
fi

NANO_TARGET_FLAGS="--target=$TARGET_ARCH --sysroot=$TARGET_SYSROOT"
NANO_CFLAGS="$NANO_TARGET_FLAGS -O2 -g -fPIC -fPIE -fno-sanitize=safe-stack -fno-stack-protector"
NANO_CPPFLAGS="$NANO_TARGET_FLAGS -I$TARGET_SYSROOT/include"
NANO_LDFLAGS="$NANO_TARGET_FLAGS -fuse-ld=lld -L$TARGET_SYSROOT/lib -Wl,--dynamic-linker=/lib/ld.so -Wl,-rpath,/usr/lib -fno-sanitize=safe-stack"

export CC="${WOS_CCACHE_PREFIX}$HOST/bin/clang"
export CPP="${WOS_CCACHE_PREFIX}$HOST/bin/clang -E"
export AR="$HOST/bin/llvm-ar"
export RANLIB="$HOST/bin/llvm-ranlib"
export STRIP="$HOST/bin/llvm-strip"
export CFLAGS="$NANO_CFLAGS"
export CPPFLAGS="$NANO_CPPFLAGS"
export LDFLAGS="$NANO_LDFLAGS"
export LIBS="-lncursesw"
export PKG_CONFIG=false
export NCURSESW_CFLAGS="-I$TARGET_SYSROOT/include"
export NCURSESW_LIBS="-L$TARGET_SYSROOT/lib -lncursesw"
export NCURSES_CFLAGS="$NCURSESW_CFLAGS"
export NCURSES_LIBS="$NCURSESW_LIBS"

NANO_CONFIGURE_BUILD_ARGS=()
NANO_CONFIGURE_CACHE_ARGS=()
if [ "$HOST_SYSTEM" = "WOS" ]; then
    NANO_CONFIGURE_BUILD_ARGS=(--build="$TARGET_ARCH")
    NANO_CONFIGURE_CACHE_ARGS=(
        ac_cv_path_GREP=/usr/bin/grep
        "ac_cv_path_EGREP=/usr/bin/grep -E"
        "ac_cv_path_FGREP=/usr/bin/grep -F"
        ac_cv_path_SED=/usr/bin/sed
        "ac_cv_path_install=/usr/bin/install -c"
        ac_cv_prog_AWK=awk
        ac_cv_path_MSGFMT=:
        ac_cv_path_GMSGFMT=:
        ac_cv_path_XGETTEXT=:
        ac_cv_path_MSGMERGE=:
    )
fi

wos_timed_step "configure" "nano" \
    wos_run_in_dir "$NANO_WORK" \
    ./configure \
    "${NANO_CONFIGURE_CACHE_ARGS[@]}" \
    "${NANO_CONFIGURE_BUILD_ARGS[@]}" \
    --host="$TARGET_ARCH" \
    --prefix= \
    --bindir=/bin \
    --sysconfdir=/etc \
    --datarootdir=/share \
    --datadir=/share \
    --mandir=/share/man \
    --disable-nls \
    --disable-libmagic \
    --disable-speller \
    --disable-linter \
    --disable-formatter \
    --disable-browser \
    --disable-mouse \
    --enable-utf8

wos_make "$WOS_MAKE_JOBS" -C "$NANO_WORK/src" revision.h
wos_stage_distributed_build_roots \
    "$WORKSPACE_ROOT" "" \
    "$NANO_WORK" "$TARGET_SYSROOT/include"

wos_make "$WOS_MAKE_JOBS" -C "$NANO_WORK"
wos_make "$WOS_MAKE_JOBS" -C "$NANO_WORK" \
    prefix= \
    bindir=/bin \
    sysconfdir=/etc \
    datarootdir=/share \
    datadir=/share \
    mandir=/share/man \
    DESTDIR="$TARGET_SYSROOT" \
    install

require_file "$TARGET_SYSROOT/bin/nano" "nano install did not produce $TARGET_SYSROOT/bin/nano."

if [ "$WOS_NANO_STRIP" != "0" ]; then
    "$HOST/bin/llvm-strip" "$TARGET_SYSROOT/bin/nano"
fi

echo "WOS nano installed to $TARGET_SYSROOT/bin/nano"
