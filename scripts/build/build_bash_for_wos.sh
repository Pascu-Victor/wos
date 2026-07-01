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
bash_cv_dev_fd=standard
bash_cv_dev_stdin=present
bash_cv_decl_under_sys_siglist=no
bash_cv_dup2_broken=no
bash_cv_fionread_in_ioctl=yes
bash_cv_fnmatch_equiv_fallback=no
bash_cv_fnmatch_equiv_value=0
bash_cv_func_sbrk=yes
bash_cv_func_sigsetjmp=present
bash_cv_func_snprintf=yes
bash_cv_func_strchrnul_works=no
bash_cv_func_strcoll_broken=no
bash_cv_func_vsnprintf=yes
bash_cv_getcwd_malloc=no
bash_cv_getenv_redef=yes
bash_cv_getpw_declared=yes
bash_cv_have_strsignal=yes
bash_cv_job_control_missing=present
bash_cv_mail_dir=/var/mail
bash_cv_must_reinstall_sighandlers=no
bash_cv_opendir_not_robust=no
bash_cv_pgrp_pipe=no
bash_cv_posix_signals=yes
bash_cv_printf_a_format=yes
bash_cv_signal_vintage=posix
bash_cv_speed_t_in_sys_types=no
bash_cv_std_putenv=yes
bash_cv_std_unsetenv=yes
bash_cv_strtold_broken=no
bash_cv_struct_timeval=yes
bash_cv_struct_timezone=yes
bash_cv_struct_winsize_header=ioctl_h
bash_cv_struct_winsize_ioctl=yes
bash_cv_struct_winsize_termios=yes
bash_cv_sys_errlist=yes
bash_cv_sys_named_pipes=missing
bash_cv_sys_siglist=no
bash_cv_sys_struct_timespec_in_time_h=yes
bash_cv_termcap_lib=gnutermcap
bash_cv_tiocstat_in_ioctl=no
bash_cv_type_clock_t=yes
bash_cv_type_rlimit=rlim_t
bash_cv_type_sigset_t=yes
bash_cv_type_socklen_t=yes
bash_cv_type_wchar_t=yes
bash_cv_type_wctype_t=yes
bash_cv_type_wint_t=yes
bash_cv_ulimit_maxfds=no
bash_cv_under_sys_siglist=no
bash_cv_unusable_rtsigs=yes
bash_cv_wcontinued_broken=no
bash_cv_wcwidth_broken=no
bash_cv_wexitstatus_offset=0
ac_cv_c_bigendian=no
ac_cv_c_char_unsigned=no
ac_cv_c_const=yes
ac_cv_c_flexmember=yes
ac_cv_c_inline=inline
ac_cv_c_restrict=__restrict__
ac_cv_c_undeclared_builtin_options='none needed'
ac_cv_c_volatile=yes
ac_cv_func___argz_count=no
ac_cv_func___argz_next=no
ac_cv_func___argz_stringify=no
ac_cv_func___fpurge=yes
ac_cv_func___fsetlocking=yes
ac_cv_func___setostype=no
ac_cv_func_alarm=yes
ac_cv_func_alloca_works=yes
ac_cv_func_arc4random=no
ac_cv_func_asprintf=yes
ac_cv_func_bcopy=yes
ac_cv_func_brk=no
ac_cv_func_bzero=yes
ac_cv_func_chown_works=no
ac_cv_func_clock_gettime=yes
ac_cv_func_confstr=yes
ac_cv_func_dcgettext=no
ac_cv_func_dlclose=yes
ac_cv_func_dlopen=yes
ac_cv_func_dlsym=yes
ac_cv_func_dprintf=yes
ac_cv_func_dup2=yes
ac_cv_func_eaccess=no
ac_cv_func_faccessat=yes
ac_cv_func_fcntl=yes
ac_cv_func_fnmatch=yes
ac_cv_func_fpurge=no
ac_cv_func_getaddrinfo=yes
ac_cv_func_getcwd=yes
ac_cv_func_getdtablesize=yes
ac_cv_func_getegid=yes
ac_cv_func_getentropy=yes
ac_cv_func_geteuid=yes
ac_cv_func_getgid=yes
ac_cv_func_getgroups=yes
ac_cv_func_gethostbyname=yes
ac_cv_func_gethostname=yes
ac_cv_func_getlocalename_l=yes
ac_cv_func_getpagesize=yes
ac_cv_func_getpeername=yes
ac_cv_func_getpgrp_void=yes
ac_cv_func_getpwent=yes
ac_cv_func_getpwnam=yes
ac_cv_func_getpwuid=yes
ac_cv_func_getrandom=no
ac_cv_func_getrlimit=yes
ac_cv_func_getrusage=yes
ac_cv_func_getservbyname=yes
ac_cv_func_getservent=yes
ac_cv_func_gettimeofday=yes
ac_cv_func_getuid=yes
ac_cv_func_imaxdiv=yes
ac_cv_func_inet_aton=yes
ac_cv_func_isascii=yes
ac_cv_func_isblank=yes
ac_cv_func_isgraph=yes
ac_cv_func_isprint=yes
ac_cv_func_isspace=yes
ac_cv_func_iswctype=yes
ac_cv_func_iswlower=yes
ac_cv_func_iswupper=yes
ac_cv_func_isxdigit=yes
ac_cv_func_kill=yes
ac_cv_func_killpg=yes
ac_cv_func_locale_charset=no
ac_cv_func_localeconv=yes
ac_cv_func_lstat=yes
ac_cv_func_mbrlen=yes
ac_cv_func_mbrtowc=yes
ac_cv_func_mbscasecmp=no
ac_cv_func_mbschr=no
ac_cv_func_mbscmp=no
ac_cv_func_mbsncmp=no
ac_cv_func_mbsnrtowcs=yes
ac_cv_func_mbsrtowcs=yes
ac_cv_func_memfd_create=yes
ac_cv_func_memmove=yes
ac_cv_func_mempcpy=yes
ac_cv_func_memset=yes
ac_cv_func_mkdtemp=yes
ac_cv_func_mkfifo=yes
ac_cv_func_mkstemp=yes
ac_cv_func_mmap_fixed_mapped=no
ac_cv_func_mremap=yes
ac_cv_func_munmap=yes
ac_cv_func_nanosleep=yes
ac_cv_func_newlocale=yes
ac_cv_func_pathconf=yes
ac_cv_func_pselect=yes
ac_cv_func_putenv=yes
ac_cv_func_raise=yes
ac_cv_func_random=yes
ac_cv_func_readlink=yes
ac_cv_func_reallocarray=yes
ac_cv_func_regcomp=yes
ac_cv_func_regexec=yes
ac_cv_func_rename=yes
ac_cv_func_sbrk=yes
ac_cv_func_select=yes
ac_cv_func_setdtablesize=no
ac_cv_func_setenv=yes
ac_cv_func_setitimer=yes
ac_cv_func_setlinebuf=yes
ac_cv_func_setlocale=yes
ac_cv_func_setresgid=yes
ac_cv_func_setresuid=yes
ac_cv_func_setvbuf=yes
ac_cv_func_shm_mkstemp=no
ac_cv_func_shm_open=yes
ac_cv_func_siginterrupt=yes
ac_cv_func_statfs=yes
ac_cv_func_stpcpy=yes
ac_cv_func_strcasecmp=yes
ac_cv_func_strcasestr=yes
ac_cv_func_strchr=yes
ac_cv_func_strcoll_works=no
ac_cv_func_strcspn=yes
ac_cv_func_strdup=yes
ac_cv_func_strerror=yes
ac_cv_func_strftime=yes
ac_cv_func_strlcat=yes
ac_cv_func_strlcpy=yes
ac_cv_func_strnlen=yes
ac_cv_func_strpbrk=yes
ac_cv_func_strstr=yes
ac_cv_func_strtod=yes
ac_cv_func_strtoimax=yes
ac_cv_func_strtol=yes
ac_cv_func_strtoll=yes
ac_cv_func_strtoul=yes
ac_cv_func_strtoull=yes
ac_cv_func_strtoumax=yes
ac_cv_func_symlink=yes
ac_cv_func_sysconf=yes
ac_cv_func_syslog=yes
ac_cv_func_tcgetattr=yes
ac_cv_func_tcgetpgrp=yes
ac_cv_func_tcgetwinsize=yes
ac_cv_func_tcsetwinsize=yes
ac_cv_func_times=yes
ac_cv_func_towlower=yes
ac_cv_func_towupper=yes
ac_cv_func_tsearch=yes
ac_cv_func_ttyname=yes
ac_cv_func_tzset=yes
ac_cv_func_ulimit=no
ac_cv_func_uname=yes
ac_cv_func_unsetenv=yes
ac_cv_func_uselocale=yes
ac_cv_func_vasprintf=yes
ac_cv_func_vprintf=yes
ac_cv_func_wait3=yes
ac_cv_func_waitpid=yes
ac_cv_func_wcrtomb=yes
ac_cv_func_wcscoll=yes
ac_cv_func_wcsdup=yes
ac_cv_func_wcslen=yes
ac_cv_func_wcsnlen=yes
ac_cv_func_wcsnrtombs=yes
ac_cv_func_wcswidth=yes
ac_cv_func_wctype=yes
ac_cv_func_wcwidth=yes
ac_cv_func_working_mktime=no
ac_cv_func_wprintf=yes
ac_cv_gnu_library_2=no
ac_cv_have_decl_AUDIT_USER_TTY=no
ac_cv_have_decl__snprintf=no
ac_cv_have_decl__snwprintf=no
ac_cv_have_decl_brk=no
ac_cv_have_decl_clearerr_unlocked=yes
ac_cv_have_decl_confstr=yes
ac_cv_have_decl_feof_unlocked=yes
ac_cv_have_decl_ferror_unlocked=yes
ac_cv_have_decl_fflush_unlocked=yes
ac_cv_have_decl_fgets_unlocked=yes
ac_cv_have_decl_fpurge=no
ac_cv_have_decl_fputc_unlocked=yes
ac_cv_have_decl_fputs_unlocked=yes
ac_cv_have_decl_fread_unlocked=yes
ac_cv_have_decl_fwrite_unlocked=yes
ac_cv_have_decl_getc_unlocked=yes
ac_cv_have_decl_getchar_unlocked=yes
ac_cv_have_decl_printf=yes
ac_cv_have_decl_putc_unlocked=yes
ac_cv_have_decl_putchar_unlocked=yes
ac_cv_have_decl_sbrk=yes
ac_cv_have_decl_setregid=yes
ac_cv_have_decl_strcpy=yes
ac_cv_have_decl_strsignal=yes
ac_cv_have_decl_strtoimax=yes
ac_cv_have_decl_strtol=yes
ac_cv_have_decl_strtold=yes
ac_cv_have_decl_strtoll=yes
ac_cv_have_decl_strtoul=yes
ac_cv_have_decl_strtoull=yes
ac_cv_have_decl_strtoumax=yes
ac_cv_have_decl_sys_siglist=no
ac_cv_have_sig_atomic_t=yes
ac_cv_header_argz_h=no
ac_cv_header_arpa_inet_h=yes
ac_cv_header_dirent_dirent_h=yes
ac_cv_header_dlfcn_h=yes
ac_cv_header_errno_h=yes
ac_cv_header_fcntl_h=yes
ac_cv_header_features_h=yes
ac_cv_header_grp_h=yes
ac_cv_header_inttypes_h=yes
ac_cv_header_langinfo_h=yes
ac_cv_header_libaudit_h=no
ac_cv_header_limits_h=yes
ac_cv_header_locale_h=yes
ac_cv_header_malloc_h=yes
ac_cv_header_mbstr_h=no
ac_cv_header_memory_h=yes
ac_cv_header_minix_config_h=no
ac_cv_header_netdb_h=yes
ac_cv_header_netinet_in_h=yes
ac_cv_header_pthread_h=yes
ac_cv_header_pwd_h=yes
ac_cv_header_regex_h=yes
ac_cv_header_stat_broken=no
ac_cv_header_stdbool_h=yes
ac_cv_header_stdckdint_h=yes
ac_cv_header_stddef_h=yes
ac_cv_header_stdint_h=yes
ac_cv_header_stdio_ext_h=yes
ac_cv_header_stdio_h=yes
ac_cv_header_stdlib_h=yes
ac_cv_header_string_h=yes
ac_cv_header_strings_h=yes
ac_cv_header_sys_file_h=yes
ac_cv_header_sys_ioctl_h=yes
ac_cv_header_sys_mkdev_h=no
ac_cv_header_sys_mman_h=yes
ac_cv_header_sys_param_h=yes
ac_cv_header_sys_pte_h=no
ac_cv_header_sys_ptem_h=no
ac_cv_header_sys_random_h=no
ac_cv_header_sys_resource_h=yes
ac_cv_header_sys_select_h=yes
ac_cv_header_sys_socket_h=yes
ac_cv_header_sys_stat_h=yes
ac_cv_header_sys_stream_h=no
ac_cv_header_sys_sysmacros_h=yes
ac_cv_header_sys_time_h=yes
ac_cv_header_sys_times_h=yes
ac_cv_header_sys_types_h=yes
ac_cv_header_sys_wait_h=yes
ac_cv_header_syslog_h=yes
ac_cv_header_termcap_h=no
ac_cv_header_termio_h=no
ac_cv_header_termios_h=yes
ac_cv_header_threads_h=yes
ac_cv_header_ulimit_h=no
ac_cv_header_unistd_h=yes
ac_cv_header_varargs_h=no
ac_cv_header_wchar_h=yes
ac_cv_header_wctype_h=yes
ac_cv_header_xlocale_h=no
ac_cv_lib_dl_dlopen=yes
ac_cv_lib_pthread_pthread_kill=yes
ac_cv_member_struct_dirent_d_fileno=yes
ac_cv_member_struct_dirent_d_ino=yes
ac_cv_member_struct_dirent_d_namlen=no
ac_cv_member_struct_stat_st_atim_tv_nsec=yes
ac_cv_member_struct_stat_st_blocks=yes
ac_cv_member_struct_termio_c_line=no
ac_cv_member_struct_termios_c_line=yes
ac_cv_member_struct_tm_tm_zone=yes
ac_cv_safe_to_define___extensions__=yes
ac_cv_search_opendir='none required'
ac_cv_should_define__xopen_source=no
ac_cv_sizeof_char=1
ac_cv_sizeof_char_p=8
ac_cv_sizeof_double=8
ac_cv_sizeof_int=4
ac_cv_sizeof_intmax_t=8
ac_cv_sizeof_long=8
ac_cv_sizeof_long_long=8
ac_cv_sizeof_short=2
ac_cv_sizeof_size_t=8
ac_cv_sizeof_wchar_t=4
ac_cv_struct_tm=time.h
ac_cv_sys_largefile_opts='none needed'
ac_cv_sys_tiocgwinsz_in_termios_h=yes
ac_cv_type_bits16_t=no
ac_cv_type_bits32_t=no
ac_cv_type_bits64_t=no
ac_cv_type_getgroups=gid_t
ac_cv_type_gid_t=yes
ac_cv_type_intmax_t=yes
ac_cv_type_intptr_t=yes
ac_cv_type_long_double=yes
ac_cv_type_long_long_int=yes
ac_cv_type_mode_t=yes
ac_cv_type_off_t=yes
ac_cv_type_pid_t=yes
ac_cv_type_pthread_rwlock_t=yes
ac_cv_type_ptrdiff_t=yes
ac_cv_type_quad_t=yes
ac_cv_type_size_t=yes
ac_cv_type_ssize_t=yes
ac_cv_type_time_t=yes
ac_cv_type_u_bits16_t=no
ac_cv_type_u_bits32_t=no
ac_cv_type_uid_t=yes
ac_cv_type_uintmax_t=yes
ac_cv_type_uintptr_t=yes
ac_cv_type_unsigned_long_long_int=yes
ac_cv_typeof_struct_stat_st_atim_is_struct_timespec=yes
ac_cv_working_alloca_h=yes
am_cv_func_iconv='no, consider installing GNU libiconv'
am_cv_langinfo_codeset=yes
am_cv_lib_iconv=no
gl_cv_c_bool=no
gl_cv_cc_vis_werror=yes
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

BASH_CONFIGURE_CACHE_ARGS=()
if [ "$HOST_SYSTEM" = "WOS" ]; then
    BASH_CONFIGURE_CACHE_ARGS=(
        "ac_cv_path_install=/usr/bin/install -c"
        ac_cv_path_mkdir=/usr/bin/mkdir
        ac_cv_path_SED=/usr/bin/sed
        "ac_cv_path_EGREP_TRADITIONAL=/usr/bin/grep -E"
        ac_cv_path_MSGFMT=:
        ac_cv_path_GMSGFMT=:
        ac_cv_path_XGETTEXT=:
        ac_cv_path_MSGMERGE=:
    )
fi

wos_timed_step "configure" "bash" \
    wos_run_in_dir "$BASH_WORK" \
    ./configure \
    "${BASH_CONFIGURE_CACHE_ARGS[@]}" \
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
