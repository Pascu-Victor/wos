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
if [ -z "${WOS_TLS_BUILD_JOBS:-}" ]; then
    WOS_TLS_BUILD_JOBS="$WOS_MAKE_JOBS"
    # LibreSSL's recursive Automake graph can enter apps/openssl concurrently
    # under native WOS make, racing the generated .deps/*.Tpo rename. Keep this
    # package serial even when compiler processes themselves are distributed;
    # the rest of the self-host build retains its configured parallelism.
    if [ "$HOST_SYSTEM" = "WOS" ]; then
        WOS_TLS_BUILD_JOBS=1
    fi
fi
case "$WOS_TLS_BUILD_JOBS" in
    ''|*[!0-9]*|0)
        echo "ERROR: WOS_TLS_BUILD_JOBS must be a positive integer, got '$WOS_TLS_BUILD_JOBS'" >&2
        exit 1
        ;;
esac
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

disable_libressl_man_install() {
    local makefile="$TLS_WORK/Makefile"

    require_file "$makefile" "LibreSSL configure did not produce a top-level Makefile."
    if grep -q '^SUBDIRS = include crypto ssl tls apps man ' "$makefile"; then
        sed -i 's/^SUBDIRS = include crypto ssl tls apps man /SUBDIRS = include crypto ssl tls apps /' "$makefile"
    fi

    if grep -q '^SUBDIRS = .* man ' "$makefile"; then
        echo "ERROR: failed to remove LibreSSL man subdir from $makefile." >&2
        exit 1
    fi
}

write_libressl_config_site() {
    local tmp_config_site

    tmp_config_site="$(mktemp "$TLS_WORK/config.site.XXXXXX")"
    cat > "$tmp_config_site" <<'EOF'
ac_cv_c_compiler_gnu=yes
ac_cv_func__mkgmtime=no
ac_cv_func_accept4=yes
ac_cv_func_arc4random=no
ac_cv_func_arc4random_buf=no
ac_cv_func_arc4random_uniform=no
ac_cv_func_asprintf=yes
ac_cv_func_clock_gettime=yes
ac_cv_func_dl_iterate_phdr=yes
ac_cv_func_explicit_bzero=yes
ac_cv_func_freezero=no
ac_cv_func_ftruncate=yes
ac_cv_func_funopen=no
ac_cv_func_getauxval=yes
ac_cv_func_getdelim=yes
ac_cv_func_getentropy=yes
ac_cv_func_getline=yes
ac_cv_func_getopt=yes
ac_cv_func_getpagesize=yes
ac_cv_func_getprogname=no
ac_cv_func_memmem=yes
ac_cv_func_pipe2=yes
ac_cv_func_pledge=no
ac_cv_func_poll=yes
ac_cv_func_readpassphrase=no
ac_cv_func_reallocarray=yes
ac_cv_func_recallocarray=no
ac_cv_func_socketpair=yes
ac_cv_func_strcasecmp=yes
ac_cv_func_strlcat=yes
ac_cv_func_strlcpy=yes
ac_cv_func_strndup=yes
ac_cv_func_strnlen=yes
ac_cv_func_strsep=yes
ac_cv_func_strtonum=no
ac_cv_func_symlink=yes
ac_cv_func_syslog=yes
ac_cv_func_syslog_r=no
ac_cv_func_timegm=yes
ac_cv_func_timespecsub=no
ac_cv_func_timingsafe_bcmp=no
ac_cv_func_timingsafe_memcmp=no
ac_cv_have___va_copy=yes
ac_cv_have_b64_ntop_arg=no
ac_cv_have_va_copy=yes
ac_cv_header_arpa_nameser_h=yes
ac_cv_header_dlfcn_h=yes
ac_cv_header_endian_h=yes
ac_cv_header_err_h=yes
ac_cv_header_inttypes_h=yes
ac_cv_header_machine_endian_h=yes
ac_cv_header_netdb_h=yes
ac_cv_header_netinet_in_h=yes
ac_cv_header_netinet_ip_h=yes
ac_cv_header_readpassphrase_h=no
ac_cv_header_resolv_h=yes
ac_cv_header_stdint_h=yes
ac_cv_header_stdio_h=yes
ac_cv_header_stdlib_h=yes
ac_cv_header_string_h=yes
ac_cv_header_strings_h=yes
ac_cv_header_sys_stat_h=yes
ac_cv_header_sys_types_h=yes
ac_cv_header_unistd_h=yes
ac_cv_objext=o
ac_cv_prog_cc_c11=
ac_cv_prog_cc_g=yes
ac_cv_prog_cc_stdc=
ac_cv_prog_make_make_set=yes
ac_cv_search___b64_ntop=no
ac_cv_search_b64_ntop=no
ac_cv_search_clock_gettime='none required'
ac_cv_search_dl_iterate_phdr='none required'
ac_cv_search_pthread_mutex_lock='none required'
ac_cv_search_pthread_once='none required'
ac_cv_sizeof_time_t=8
am_cv_CCAS_dependencies_compiler_type=gcc3
am_cv_CC_dependencies_compiler_type=gcc3
am_cv_make_support_nested_variables=yes
am_cv_prog_cc_c_o=yes
am_cv_prog_tar_ustar=gnutar
am_cv_sleep_fractional_seconds=yes
am_cv_xargs_n_works=yes
ax_cv_check_cflags___Werror=yes
lt_cv_ar_at_file=@
lt_cv_deplibs_check_method=unknown
lt_cv_ld_reload_flag=-r
lt_cv_nm_interface='BSD nm'
lt_cv_objdir=.libs
lt_cv_prog_compiler_c_o=yes
lt_cv_prog_compiler_pic='-fPIC -DPIC'
lt_cv_prog_compiler_pic_works=yes
lt_cv_prog_compiler_rtti_exceptions=yes
lt_cv_prog_compiler_static_works=yes
lt_cv_prog_gnu_ld=yes
lt_cv_sharedlib_from_linklib_cmd='printf %s\n'
lt_cv_sys_max_cmd_len=1572864
lt_cv_to_host_file_cmd=func_convert_file_noop
lt_cv_to_tool_file_cmd=func_convert_file_noop
EOF

    if [ ! -f "$TLS_WORK/config.site" ] || ! cmp -s "$tmp_config_site" "$TLS_WORK/config.site"; then
        mv "$tmp_config_site" "$TLS_WORK/config.site"
    else
        rm -f "$tmp_config_site"
    fi
}

require_file "$HOST/bin/clang" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/llvm-ar" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/llvm-ranlib" "Run tools/host-toolchain.sh first."
require_file "$TARGET_SYSROOT/lib/libc.so" "Build mlibc before building OpenSSL."
require_file "$TARGET_SYSROOT/lib/Scrt1.o" "Build mlibc startup objects before building OpenSSL."

is_truthy() {
    case "${1:-0}" in
        1 | ON | On | on | TRUE | True | true | YES | Yes | yes)
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

TLS_SOURCE_DIR="$(resolve_tls_source)"
copy_source_to_workdir "$TLS_SOURCE_DIR"
patch_config_sub_for_wos "$TLS_WORK/config.sub"
patch_arc4random_for_wos "$TLS_WORK/crypto/compat/arc4random.h"
refresh_libressl_release_generated_files
write_libressl_config_site
export CONFIG_SITE="$TLS_WORK/config.site"

mkdir -p "$TARGET_SYSROOT/bin" "$TARGET_SYSROOT/lib" "$TARGET_SYSROOT/include"
if [ ! -e "$TARGET_SYSROOT/usr" ]; then
    ln -s . "$TARGET_SYSROOT/usr"
fi

OPENSSL_OPTFLAGS="${WOS_OPENSSL_OPTFLAGS:--O0}"
# Keep LibreSSL crypto conservative for now. Optimized portable ChaCha has
# been observed to silently corrupt one 64-byte block under WOS runtime.
OPENSSL_CFLAGS="--sysroot=$TARGET_SYSROOT $OPENSSL_OPTFLAGS -g -fPIC -fno-sanitize=safe-stack -fno-stack-protector -D__STDC_NO_ATOMICS__ -D__WOS__=1"
OPENSSL_LDFLAGS="--sysroot=$TARGET_SYSROOT -fuse-ld=lld -L$TARGET_SYSROOT/lib -Wl,--dynamic-linker=/lib/ld.so -Wl,-rpath,/usr/lib -fno-sanitize=safe-stack"
if is_truthy "${WOS_USERSPACE_NO_AVX:-0}"; then
    OPENSSL_CFLAGS+=" -mno-avx -mno-avx2 -mno-fma -mno-f16c -fno-vectorize -fno-slp-vectorize"
fi

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
        "ac_cv_path_EGREP_TRADITIONAL=/usr/bin/grep -E"
        "ac_cv_path_FGREP=/usr/bin/grep -F"
        ac_cv_path_SED=/usr/bin/sed
        "ac_cv_path_install=/usr/bin/install -c"
        ac_cv_path_mkdir=/usr/bin/mkdir
    )
fi

wos_timed_step "configure" "libressl" \
    wos_run_in_dir "$TLS_WORK" \
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
disable_libressl_man_install

wos_stage_distributed_build_roots \
    "$WORKSPACE_ROOT" "" \
    "$TLS_WORK" "$TARGET_SYSROOT/include"

wos_make "$WOS_TLS_BUILD_JOBS" -C "$TLS_WORK"
wos_make "$WOS_TLS_BUILD_JOBS" -C "$TLS_WORK" \
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
