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
CURL_TARBALL_URLS="${WOS_CURL_TARBALL_URLS:-$CURL_TARBALL_URL}"
CURL_DOWNLOAD_ATTEMPTS="${WOS_CURL_DOWNLOAD_ATTEMPTS:-${WOS_SOURCE_DOWNLOAD_ATTEMPTS:-3}}"
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
        ac_cv_path_SED=/usr/bin/sed
        "ac_cv_path_install=/usr/bin/install -c"
        ac_cv_path_lt_DD=/usr/bin/dd
        ac_cv_prog_AWK=awk
        ac_cv_prog_ac_ct_OBJDUMP=objdump
        lt_cv_path_LD=/usr/bin/ld
        "lt_cv_path_NM=/usr/bin/nm -B"
        lt_cv_path_mainfest_tool=no
        "lt_cv_truncate_bin=/usr/bin/dd bs=4096 count=1"
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

download_curl_tarball() {
    local archive="$1"
    wos_download_file "curl $CURL_VERSION source" "$archive" "$CURL_TARBALL_URLS" "$CURL_DOWNLOAD_ATTEMPTS"
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
        download_curl_tarball "$archive"
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
    local metadata

    wos_remove_tree "$CURL_WORK"
    mkdir -p "$CURL_WORK"
    wos_copy_tree_entries_excluding "$source_dir" "$CURL_WORK" ".git" ".github" "tests"
    if [ -d "$source_dir/tests" ]; then
        mkdir -p "$CURL_WORK/tests"
        wos_copy_tree_entries_excluding "$source_dir/tests" "$CURL_WORK/tests" "data"
        if [ -d "$source_dir/tests/data" ]; then
            mkdir -p "$CURL_WORK/tests/data"
            for metadata in Makefile.am Makefile.in; do
                if [ -f "$source_dir/tests/data/$metadata" ]; then
                    cp "$source_dir/tests/data/$metadata" "$CURL_WORK/tests/data/"
                fi
            done
        fi
    fi
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

patch_getifaddrs_probe_for_wos() {
    local configure="$1"
    local patched="$configure.tmp"

    require_file "$configure" "curl source is missing configure."
    if grep -q 'WOS skips curl getifaddrs run probe' "$configure"; then
        return 0
    fi

    echo "Patching curl getifaddrs runtime probe for WOS..."
    if ! awk '
        /tst_works_getifaddrs="unknown"/ {
            in_getifaddrs_probe = 1
        }
        in_getifaddrs_probe && $0 ~ /^[[:space:]]*if test "\$cross_compiling" != "yes" &&$/ {
            print "    # WOS skips curl getifaddrs run probe: mlibc exposes the symbol,"
            print "    # but the sysdep currently aborts, and curl must leave it disabled."
            print "    tst_works_getifaddrs=\"no\""
            print "    if false && test \"$cross_compiling\" != \"yes\" &&"
            in_getifaddrs_probe = 0
            patched = 1
            next
        }
        { print }
        END {
            if (!patched)
                exit 42
        }
    ' "$configure" >"$patched"; then
        rm -f "$patched"
        echo "ERROR: do not know how to patch $configure getifaddrs probe for WOS." >&2
        exit 1
    fi

    mv "$patched" "$configure"
    chmod +x "$configure"
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

write_curl_config_site() {
    local tmp_config_site

    tmp_config_site="$(mktemp "$CURL_WORK/config.site.XXXXXX")"
    cat > "$tmp_config_site" <<'EOF'
ac_cv_c_compiler_gnu=yes
ac_cv_c_const=yes
ac_cv_c_undeclared_builtin_options='none needed'
ac_cv_func_SSL_set0_wbio=no
ac_cv_func_SSL_set_quic_use_legacy_codepoint=yes
ac_cv_func_accept4=yes
ac_cv_func_eventfd=no
ac_cv_func_fnmatch=yes
ac_cv_func_fseeko=yes
ac_cv_func_geteuid=yes
ac_cv_func_gethostbyname=yes
ac_cv_func_getpass_r=no
ac_cv_func_getppid=yes
ac_cv_func_getpwuid=yes
ac_cv_func_getpwuid_r=yes
ac_cv_func_getrlimit=yes
ac_cv_func_gettimeofday=yes
ac_cv_func_if_nametoindex=yes
ac_cv_func_mach_absolute_time=no
ac_cv_func_opendir=yes
ac_cv_func_pipe=yes
ac_cv_func_pipe2=yes
ac_cv_func_poll=yes
ac_cv_func_pthread_create=yes
ac_cv_func_realpath=yes
ac_cv_func_sched_yield=yes
ac_cv_func_sendmsg=yes
ac_cv_func_sendmmsg=yes
ac_cv_func_setlocale=yes
ac_cv_func_setrlimit=yes
ac_cv_func_utime=yes
ac_cv_func_utimes=yes
ac_cv_have_decl_fseeko=yes
ac_cv_header_arpa_inet_h=yes
ac_cv_header_dirent_h=yes
ac_cv_header_dlfcn_h=yes
ac_cv_header_fcntl_h=yes
ac_cv_header_ifaddrs_h=yes
ac_cv_header_inttypes_h=yes
ac_cv_header_io_h=no
ac_cv_header_libgen_h=yes
ac_cv_header_linux_tcp_h=no
ac_cv_header_locale_h=yes
ac_cv_header_net_if_h=yes
ac_cv_header_netdb_h=yes
ac_cv_header_netinet_in6_h=no
ac_cv_header_netinet_in_h=yes
ac_cv_header_netinet_tcp_h=yes
ac_cv_header_netinet_udp_h=yes
ac_cv_header_openssl_crypto_h=yes
ac_cv_header_openssl_err_h=yes
ac_cv_header_openssl_pem_h=yes
ac_cv_header_openssl_rsa_h=yes
ac_cv_header_openssl_ssl_h=yes
ac_cv_header_poll_h=yes
ac_cv_header_proto_bsdsocket_h=no
ac_cv_header_pthread_h=yes
ac_cv_header_pwd_h=yes
ac_cv_header_stdatomic_h=no
ac_cv_header_stdbool_h=yes
ac_cv_header_stdint_h=yes
ac_cv_header_stdio_h=yes
ac_cv_header_stdlib_h=yes
ac_cv_header_string_h=yes
ac_cv_header_strings_h=yes
ac_cv_header_stropts_h=no
ac_cv_header_sys_eventfd_h=no
ac_cv_header_sys_filio_h=no
ac_cv_header_sys_ioctl_h=yes
ac_cv_header_sys_param_h=yes
ac_cv_header_sys_poll_h=yes
ac_cv_header_sys_resource_h=yes
ac_cv_header_sys_select_h=yes
ac_cv_header_sys_sockio_h=no
ac_cv_header_sys_stat_h=yes
ac_cv_header_sys_types_h=yes
ac_cv_header_sys_un_h=yes
ac_cv_header_sys_utime_h=no
ac_cv_header_sys_xattr_h=no
ac_cv_header_termio_h=no
ac_cv_header_termios_h=yes
ac_cv_header_unistd_h=yes
ac_cv_header_utime_h=yes
ac_cv_header_zlib_h=yes
ac_cv_lib_crypto_HMAC_Update=yes
ac_cv_lib_ssl_SSL_connect=yes
ac_cv_lib_z_gzread=yes
ac_cv_member_struct_sockaddr_un_sun_path=yes
ac_cv_objext=o
ac_cv_prog_cc_c11=
ac_cv_prog_cc_g=yes
ac_cv_prog_cc_stdc=
ac_cv_prog_make_make_set=yes
ac_cv_sizeof_curl_off_t=8
ac_cv_sizeof_curl_socket_t=4
ac_cv_sizeof_int=4
ac_cv_sizeof_long=8
ac_cv_sizeof_off_t=8
ac_cv_sizeof_size_t=8
ac_cv_sizeof_time_t=8
ac_cv_sys_file_offset_bits=no
ac_cv_sys_largefile_CC=no
ac_cv_type_bool=yes
ac_cv_type_sa_family_t=yes
ac_cv_type_size_t=yes
ac_cv_type_ssize_t=yes
ac_cv_type_struct_sockaddr_storage=yes
ac_cv_type_suseconds_t=yes
am_cv_CC_dependencies_compiler_type=gcc3
am_cv_make_support_nested_variables=yes
am_cv_prog_cc_c_o=yes
curl_cv_native_windows=no
curl_cv_struct_timeval=yes
lt_cv_ar_at_file=@
lt_cv_deplibs_check_method=unknown
lt_cv_ld_reload_flag=-r
lt_cv_nm_interface='BSD nm'
lt_cv_objdir=.libs
lt_cv_prog_compiler_c_o=yes
lt_cv_prog_compiler_c_o_RC=yes
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

    if [ ! -f "$CURL_WORK/config.site" ] || ! cmp -s "$tmp_config_site" "$CURL_WORK/config.site"; then
        mv "$tmp_config_site" "$CURL_WORK/config.site"
    else
        rm -f "$tmp_config_site"
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

CURL_SOURCE_DIR="$(resolve_curl_source)"
copy_source_to_workdir "$CURL_SOURCE_DIR"
patch_config_sub_for_wos "$CURL_WORK/config.sub"
patch_getifaddrs_probe_for_wos "$CURL_WORK/configure"
write_curl_config_site
export CONFIG_SITE="$CURL_WORK/config.site"

mkdir -p "$TARGET_SYSROOT/bin" "$TARGET_SYSROOT/lib" "$TARGET_SYSROOT/include"
if [ ! -e "$TARGET_SYSROOT/usr" ]; then
    ln -s . "$TARGET_SYSROOT/usr"
fi
stage_ca_bundle

CURL_TARGET_FLAGS="--target=$TARGET_ARCH --sysroot=$TARGET_SYSROOT"
CURL_CFLAGS="$CURL_TARGET_FLAGS -O2 -g -fPIC -fPIE -fno-sanitize=safe-stack -fno-stack-protector"
CURL_CPPFLAGS="$CURL_TARGET_FLAGS -I$TARGET_SYSROOT/include"
CURL_LDFLAGS="$CURL_TARGET_FLAGS -fuse-ld=lld -L$TARGET_SYSROOT/lib -Wl,--dynamic-linker=/lib/ld.so -Wl,-rpath,/usr/lib -fno-sanitize=safe-stack"
if is_truthy "${WOS_USERSPACE_NO_AVX:-0}"; then
    CURL_CFLAGS+=" -mno-avx -mno-avx2 -mno-fma -mno-f16c -fno-vectorize -fno-slp-vectorize"
fi

# curl's libtool parses the compile command itself and misclassifies a CC value
# with embedded target/sysroot flags as a library object. Keep CC to one path.
export CC="$HOST/bin/clang"
export AR="$HOST/bin/llvm-ar"
export RANLIB="$HOST/bin/llvm-ranlib"
export STRIP="$HOST/bin/llvm-strip"
export CFLAGS="$CURL_CFLAGS"
export CPPFLAGS="$CURL_CPPFLAGS"
export LDFLAGS="$CURL_LDFLAGS"
export LIBS="-lssl -lcrypto -lz -lpthread -ldl"
export PKG_CONFIG=false
export ac_cv_header_stdatomic_h=no

wos_timed_step "configure" "curl" \
    wos_run_in_dir "$CURL_WORK" \
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

# curl creates this disabled-CA placeholder as a Make prerequisite. Materialize
# it before snapshotting the build tree so distributed compilers never race a
# generated source that exists only on the submitter.
wos_make 1 -C "$CURL_WORK/src" tool_ca_embed.c

wos_stage_distributed_build_roots \
    "$WORKSPACE_ROOT" "" \
    "$CURL_WORK" "$TARGET_SYSROOT/include"

wos_make "$WOS_MAKE_JOBS" -C "$CURL_WORK"
wos_make "$WOS_MAKE_JOBS" -C "$CURL_WORK" \
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
