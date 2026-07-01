#!/bin/bash
# Incrementally rebuild Dropbear SSH for WOS and install into the sysroot.
# Expects the toolchain to already be bootstrapped (tools/bootstrap.sh).
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$WORKSPACE_ROOT/tools/ccache_env.sh"
if [ -z "${CCACHE_DIR:-}" ]; then
    export CCACHE_DIR="${TMPDIR:-/tmp}/wos-dropbear-ccache"
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
DB_SRC="$B/src/dropbear"
DB_BUILD="${WOS_DROPBEAR_BUILD_DIR:-$B/dropbear-build}"
HOST_SYSTEM="$(uname -s 2>/dev/null || printf unknown)"

if [ ! -d "$DB_SRC" ]; then
    echo "ERROR: dropbear source directory not found at $DB_SRC"
    echo "Run tools/bootstrap.sh first to bootstrap the toolchain."
    exit 1
fi

# Cross-compilation environment - host tools, target sysroot
DROPBEAR_CFLAGS="--sysroot=$TARGET_SYSROOT -O3 -g -fno-sanitize=safe-stack -fno-stack-protector -I$TARGET_SYSROOT/include"
export CC="${WOS_CCACHE_PREFIX}$HOST/bin/clang --target=$TARGET_ARCH --sysroot=$TARGET_SYSROOT"
export AR="$HOST/bin/llvm-ar"
export RANLIB="$HOST/bin/llvm-ranlib"
export STRIP="$HOST/bin/llvm-strip"
export CFLAGS="$DROPBEAR_CFLAGS"
export LDFLAGS="--sysroot=$TARGET_SYSROOT -fuse-ld=lld -L$TARGET_SYSROOT/lib"

mkdir -p "$DB_BUILD"

write_dropbear_config_site() {
    local tmp_config_site

    mkdir -p "$DB_BUILD"
    tmp_config_site="$(mktemp "$DB_BUILD/config.site.XXXXXX")"
    cat > "$tmp_config_site" <<'EOF'
ac_cv_c_compiler_gnu=yes
ac_cv_c_const=yes
ac_cv_c_undeclared_builtin_options='none needed'
ac_cv_func__getpty=no
ac_cv_func_basename=yes
ac_cv_func_clearenv=yes
ac_cv_func_clock_gettime=yes
ac_cv_func_crypt=no
ac_cv_func_daemon=yes
ac_cv_func_explicit_bzero=yes
ac_cv_func_fexecve=yes
ac_cv_func_fork=yes
ac_cv_func_freeaddrinfo=yes
ac_cv_func_gai_strerror=yes
ac_cv_func_getaddrinfo=yes
ac_cv_func_getgrouplist=yes
ac_cv_func_getnameinfo=yes
ac_cv_func_getpass=yes
ac_cv_func_getrandom=no
ac_cv_func_getspnam=yes
ac_cv_func_htole64=no
ac_cv_func_logout=no
ac_cv_func_logwtmp=no
ac_cv_func_mach_absolute_time=no
ac_cv_func_memset_s=no
ac_cv_func_putenv=yes
ac_cv_func_select_args='int,fd_set *,struct timeval *'
ac_cv_func_setresgid=yes
ac_cv_func_strlcat=yes
ac_cv_func_strlcpy=yes
ac_cv_func_updwtmp=no
ac_cv_func_writev=yes
ac_cv_have_decl___UCLIBC__=no
ac_cv_have_decl_htole64=yes
ac_cv_have_static_assert=yes
ac_cv_have_struct_addrinfo=yes
ac_cv_have_struct_in6_addr=yes
ac_cv_have_struct_sockaddr_in6=yes
ac_cv_have_struct_sockaddr_storage=yes
ac_cv_have_underscore_static_assert=yes
ac_cv_header_crypt_h=no
ac_cv_header_endian_h=yes
ac_cv_header_inttypes_h=yes
ac_cv_header_lastlog_h=no
ac_cv_header_libgen_h=yes
ac_cv_header_libutil_h=no
ac_cv_header_linux_pkt_sched_h=no
ac_cv_header_mach_mach_time_h=no
ac_cv_header_netdb_h=yes
ac_cv_header_netinet_in_h=yes
ac_cv_header_netinet_in_systm_h=yes
ac_cv_header_netinet_tcp_h=yes
ac_cv_header_pam_pam_appl_h=no
ac_cv_header_paths_h=yes
ac_cv_header_pty_h=yes
ac_cv_header_security_pam_appl_h=no
ac_cv_header_shadow_h=yes
ac_cv_header_stdint_h=yes
ac_cv_header_stdio_h=yes
ac_cv_header_stdlib_h=yes
ac_cv_header_string_h=yes
ac_cv_header_strings_h=yes
ac_cv_header_stropts_h=no
ac_cv_header_sys_endian_h=yes
ac_cv_header_sys_prctl_h=no
ac_cv_header_sys_random_h=no
ac_cv_header_sys_select_h=yes
ac_cv_header_sys_socket_h=yes
ac_cv_header_sys_stat_h=yes
ac_cv_header_sys_types_h=yes
ac_cv_header_sys_uio_h=yes
ac_cv_header_sys_wait_h=yes
ac_cv_header_unistd_h=yes
ac_cv_header_util_h=no
ac_cv_header_utmp_h=yes
ac_cv_header_utmpx_h=no
ac_cv_lib_crypt_crypt=no
ac_cv_member_struct_sockaddr_storage_ss_family=yes
ac_cv_member_struct_utmp_ut_addr=no
ac_cv_member_struct_utmp_ut_addr_v6=no
ac_cv_member_struct_utmp_ut_exit=no
ac_cv_member_struct_utmp_ut_host=no
ac_cv_member_struct_utmp_ut_id=no
ac_cv_member_struct_utmp_ut_pid=no
ac_cv_member_struct_utmp_ut_time=no
ac_cv_member_struct_utmp_ut_tv=no
ac_cv_member_struct_utmp_ut_type=no
ac_cv_member_struct_utmpx_ut_addr=no
ac_cv_member_struct_utmpx_ut_addr_v6=no
ac_cv_member_struct_utmpx_ut_host=no
ac_cv_member_struct_utmpx_ut_id=no
ac_cv_member_struct_utmpx_ut_syslen=no
ac_cv_member_struct_utmpx_ut_time=no
ac_cv_member_struct_utmpx_ut_tv=no
ac_cv_member_struct_utmpx_ut_type=no
ac_cv_objext=o
ac_cv_prog_cc_c11=
ac_cv_prog_cc_g=yes
ac_cv_prog_cc_stdc=
ac_cv_search_basename='none required'
ac_cv_search_login=no
ac_cv_search_openpty='none required'
ac_cv_sys_largefile_opts='none needed'
ac_cv_type_gid_t=yes
ac_cv_type_mode_t=yes
ac_cv_type_pid_t=yes
ac_cv_type_size_t=yes
ac_cv_type_socklen_t=yes
ac_cv_type_struct_sockaddr_storage=no
ac_cv_type_u_int16_t=yes
ac_cv_type_u_int32_t=yes
ac_cv_type_u_int8_t=yes
ac_cv_type_uid_t=yes
ac_cv_type_uint16_t=yes
ac_cv_type_uint32_t=yes
ac_cv_type_uint8_t=yes
dropbear_cv_func_have_openpty=yes
EOF

    if [ "$HOST_SYSTEM" = "WOS" ]; then
        cat >> "$tmp_config_site" <<'EOF'
ac_cv_func_endutent=no
ac_cv_func_endutxent=no
ac_cv_func_getusershell=no
ac_cv_func_getutent=no
ac_cv_func_getutxent=no
ac_cv_func_getutid=no
ac_cv_func_getutxid=no
ac_cv_func_getutline=no
ac_cv_func_getutxline=no
ac_cv_func_memcmp_working=yes
ac_cv_func_pututline=no
ac_cv_func_pututxline=no
ac_cv_func_setutent=no
ac_cv_func_setutxent=no
ac_cv_func_utmpname=no
ac_cv_func_utmpxname=no
EOF
    else
        cat >> "$tmp_config_site" <<'EOF'
ac_cv_func_endutent=no
ac_cv_func_endutxent=yes
ac_cv_func_getusershell=yes
ac_cv_func_getutent=no
ac_cv_func_getutxent=yes
ac_cv_func_getutid=no
ac_cv_func_getutxid=yes
ac_cv_func_getutline=no
ac_cv_func_getutxline=yes
ac_cv_func_memcmp_working=no
ac_cv_func_pututline=no
ac_cv_func_pututxline=yes
ac_cv_func_setutent=no
ac_cv_func_setutxent=yes
ac_cv_func_utmpname=no
ac_cv_func_utmpxname=yes
EOF
    fi

    if [ ! -f "$DB_BUILD/config.site" ] || ! cmp -s "$tmp_config_site" "$DB_BUILD/config.site"; then
        mv "$tmp_config_site" "$DB_BUILD/config.site"
    else
        rm -f "$tmp_config_site"
    fi
}

LOCALOPTIONS="$DB_BUILD/localoptions.h"
LOCALOPTIONS_TMP="$DB_BUILD/localoptions.h.tmp"
cat > "$LOCALOPTIONS_TMP" <<'EOF'
#define DROPBEAR_SFTPSERVER 1
#define SFTPSERVER_PATH "/usr/libexec/sftp-server"
#define DROPBEAR_PATH_SSH_PROGRAM "/usr/bin/dbclient"
#define DROPBEAR_SMALL_CODE 0
#define DEFAULT_RECV_WINDOW (1024 * 1024)
#define RECV_MAX_PAYLOAD_LEN (128 * 1024)
#define TRANS_MAX_PAYLOAD_LEN (64 * 1024)
EOF
if [ ! -f "$LOCALOPTIONS" ] || ! cmp -s "$LOCALOPTIONS_TMP" "$LOCALOPTIONS"; then
    mv "$LOCALOPTIONS_TMP" "$LOCALOPTIONS"
else
    rm "$LOCALOPTIONS_TMP"
fi

# Run autoconf if configure doesn't exist yet
if [ ! -f "$DB_SRC/configure" ]; then
    echo "Running autoconf in dropbear source..."
    (cd "$DB_SRC" && autoconf && autoheader)
fi

# Patch config.sub to recognise WOS as a valid OS
if ! grep -q 'wos\*' "$DB_SRC/src/config.sub" 2>/dev/null; then
    echo "Patching config.sub to recognise WOS..."
    sed -i 's/| fiwix\* | mlibc\* | cos\* | mbr\* )/| fiwix* | mlibc* | cos* | mbr* | wos* )/' \
        "$DB_SRC/src/config.sub"
fi

write_dropbear_config_site
export CONFIG_SITE="$DB_BUILD/config.site"

# Configure if not yet configured
if [ ! -f "$DB_BUILD/Makefile" ] || [ "$DB_BUILD/config.site" -nt "$DB_BUILD/Makefile" ]; then
    echo "Configuring dropbear for WOS..."
    DROPBEAR_CONFIGURE_BUILD_ARGS=()
    DROPBEAR_CONFIGURE_CACHE_ARGS=()
    if [ "$HOST_SYSTEM" = "WOS" ]; then
        DROPBEAR_CONFIGURE_BUILD_ARGS=(--build="$TARGET_ARCH")
        DROPBEAR_CONFIGURE_CACHE_ARGS=(
            "ac_cv_path_install=/usr/bin/install -c"
            ac_cv_func_memcmp_working=yes
            ac_cv_func_endutent=no
            ac_cv_func_endutxent=no
            ac_cv_func_getusershell=no
            ac_cv_func_getutent=no
            ac_cv_func_getutxent=no
            ac_cv_func_getutid=no
            ac_cv_func_getutxid=no
            ac_cv_func_getutline=no
            ac_cv_func_getutxline=no
            ac_cv_func_pututline=no
            ac_cv_func_pututxline=no
            ac_cv_func_setutent=no
            ac_cv_func_setutxent=no
            ac_cv_func_utmpname=no
            ac_cv_func_utmpxname=no
        )
    fi
    wos_timed_step "configure" "dropbear" \
        wos_run_in_dir "$DB_BUILD" \
        "$DB_SRC/configure" \
        "${DROPBEAR_CONFIGURE_CACHE_ARGS[@]}" \
        "${DROPBEAR_CONFIGURE_BUILD_ARGS[@]}" \
        --host="$TARGET_ARCH" \
        --prefix="$TARGET_SYSROOT" \
        --enable-bundled-libtom \
        --disable-zlib \
        --disable-pam \
        --disable-wtmp \
        --disable-utmp \
        --disable-utmpx \
        --disable-lastlog \
        --disable-syslog \
        --disable-harden
fi

# Force relink if any sysroot library is newer than the binary.
# make only tracks dropbear's own .o files and misses external changes
# (e.g. libc.a rebuilt with new sysdeps).
if [ -f "$DB_BUILD/dropbearmulti" ]; then
    for lib in "$TARGET_SYSROOT"/lib/libc.so "$TARGET_SYSROOT"/lib/libc++.so \
               "$TARGET_SYSROOT"/lib/libc++abi.so "$TARGET_SYSROOT"/lib/libm.so; do
        if [ -f "$lib" ] && [ "$lib" -nt "$DB_BUILD/dropbearmulti" ]; then
            echo "Sysroot library $(basename "$lib") changed - forcing relink"
            rm -f "$DB_BUILD/dropbearmulti"
            break
        fi
    done
fi

# Build dropbearmulti (combined binary like busybox). Don't pass CFLAGS on the
# make command line: bundled libtommath appends Dropbear-specific include paths
# via its own Makefile, and command-line CFLAGS suppress those additions.
wos_make "$WOS_MAKE_JOBS" -C "$DB_BUILD" STATIC=0 PROGRAMS="dropbear dbclient dropbearkey scp" MULTI=1 dropbearmulti

# Install into sysroot
cp "$DB_BUILD/dropbearmulti" "$TARGET_SYSROOT/bin/dropbearmulti"
echo "dropbearmulti installed to $TARGET_SYSROOT/bin/dropbearmulti"
