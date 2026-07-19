#!/bin/bash
# Cross-build CPython so it can run inside WOS and install it into the sysroot.
# Expects the host toolchain, mlibc, and libc++ to already be available.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$WORKSPACE_ROOT/tools/ccache_env.sh"
export CCACHE_DIR="${CCACHE_DIR:-$WORKSPACE_ROOT/.cache/ccache}"
mkdir -p "$CCACHE_DIR"
wos_setup_ccache
WOS_CCACHE_PREFIX="$(wos_ccache_prefix)"
WOS_BUILD_JOBS="$(wos_build_jobs)"
WOS_MAKE_JOBS="$(wos_make_jobs)"
WOS_MAKE_JOBSERVER_ARG="$(wos_make_jobserver_arg "$WOS_MAKE_JOBS")"
WOS_PYTHON_INSTALL_JOBS="${WOS_PYTHON_INSTALL_JOBS:-1}"
case "$WOS_PYTHON_INSTALL_JOBS" in
    ''|*[!0-9]*|0)
        echo "ERROR: WOS_PYTHON_INSTALL_JOBS must be a positive integer, got '$WOS_PYTHON_INSTALL_JOBS'" >&2
        exit 1
        ;;
esac

B="$WORKSPACE_ROOT/toolchain"
HOST="${WOS_HOST_TOOLCHAIN_ROOT:-$B/host}"
TARGET_ARCH="${WOS_TARGET_ARCH:-x86_64-pc-wos}"
TARGET_SYSROOT="${WOS_SYSROOT_PATH:-$B/sysroot}"
PYTHON_SRC="${WOS_PYTHON_SOURCE_DIR:-$B/src/python}"
PYTHON_BUILD="${WOS_PYTHON_BUILD_DIR:-$B/python-build}"
PYTHON_HOST_BUILD="${WOS_PYTHON_HOST_BUILD_DIR:-$PYTHON_BUILD/build-python}"
PYTHON_TARGET_BUILD="$PYTHON_BUILD/target"
PYTHON_CONFIG_SITE="${WOS_PYTHON_CONFIG_SITE:-$PYTHON_BUILD/config.site}"
PYTHON_LIBRESSL_SIGALGS_COMPAT_HEADER="${WOS_PYTHON_LIBRESSL_SIGALGS_COMPAT_HEADER:-$PYTHON_BUILD/libressl_sigalgs_compat.h}"
WOS_PYTHON_STRIP="${WOS_PYTHON_STRIP:-0}"
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

patch_config_sub_for_wos() {
    local config_sub="$1"

    require_file "$config_sub" "CPython source is missing config.sub."
    if grep -q 'wos\*' "$config_sub"; then
        return 0
    fi

    echo "Patching CPython config.sub to recognise WOS..."
    sed -i \
        -e 's/| fiwix\* | mlibc\* )/| fiwix* | mlibc* | wos* )/' \
        -e 's/| fiwix\* | mlibc\* |/| fiwix* | mlibc* | wos* |/' \
        "$config_sub"

    if ! grep -q 'wos\*' "$config_sub"; then
        echo "ERROR: do not know how to patch $config_sub for WOS." >&2
        echo "Create a wos-support branch in Pascu-Victor/cpython with config.sub WOS support." >&2
        exit 1
    fi
}

host_env() {
	env -u CC -u CXX -u AR -u RANLIB -u STRIP \
		-u CFLAGS -u CXXFLAGS -u CPPFLAGS -u LDFLAGS \
		-u CONFIG_SITE -u CPATH -u C_INCLUDE_PATH -u CPLUS_INCLUDE_PATH \
		-u LIBRARY_PATH "$@"
}

python_source_version() {
	local patchlevel="$PYTHON_SRC/Include/patchlevel.h"
	local major
	local minor

	require_file "$patchlevel" "CPython source is missing Include/patchlevel.h."
	major="$(sed -n 's/^#define[[:space:]]\+PY_MAJOR_VERSION[[:space:]]\+\([0-9][0-9]*\).*/\1/p' "$patchlevel" | head -n 1)"
	minor="$(sed -n 's/^#define[[:space:]]\+PY_MINOR_VERSION[[:space:]]\+\([0-9][0-9]*\).*/\1/p' "$patchlevel" | head -n 1)"
	if [ -z "$major" ] || [ -z "$minor" ]; then
		echo "ERROR: could not determine CPython source version from $patchlevel" >&2
		return 1
	fi
	printf '%s.%s\n' "$major" "$minor"
}

python_interpreter_matches_source() {
	local interpreter="$1"
	local expected_version="$2"

	"$interpreter" - "$expected_version" <<'PY' >/dev/null 2>&1
import sys

expected = tuple(int(part) for part in sys.argv[1].split("."))
raise SystemExit(sys.version_info[:2] != expected)
PY
}

find_compatible_build_python() {
	local expected_version
	local explicit="${WOS_PYTHON_BUILD_PYTHON:-}"
	local candidate
	local resolved

	expected_version="$(python_source_version)"
	if [ -n "$explicit" ]; then
		resolved="$(command -v "$explicit" 2>/dev/null || true)"
		if [ -z "$resolved" ]; then
			echo "ERROR: WOS_PYTHON_BUILD_PYTHON=$explicit is not executable" >&2
			return 1
		fi
		if ! python_interpreter_matches_source "$resolved" "$expected_version"; then
			echo "ERROR: WOS_PYTHON_BUILD_PYTHON=$resolved is not Python $expected_version" >&2
			return 1
		fi
		printf '%s\n' "$resolved"
		return 0
	fi

	for candidate in "python$expected_version" python3.16 python3 python; do
		resolved="$(command -v "$candidate" 2>/dev/null || true)"
		[ -n "$resolved" ] || continue
		if python_interpreter_matches_source "$resolved" "$expected_version"; then
			printf '%s\n' "$resolved"
			return 0
		fi
	done

	return 1
}

write_config_site() {
	local tmp_config_site

	mkdir -p "$(dirname "$PYTHON_CONFIG_SITE")"
	tmp_config_site="$(mktemp "$PYTHON_CONFIG_SITE.XXXXXX")"
	cat > "$tmp_config_site" <<'EOF'
ac_cv_file__dev_ptmx=no
ac_cv_file__dev_ptc=no
ac_cv_buggy_getaddrinfo=no
ac_cv_func_close_range=no
ac_cv_func_closefrom=no
ac_cv_func_getentropy=no
ac_cv_func_getrandom=no
ac_cv_getrandom_syscall=no
ac_cv_func_posix_spawn_file_actions_addclosefrom_np=no
ac_cv_func_process_vm_readv=no
ac_cv_header_linux_auxvec_h=no
ac_cv_header_linux_random_h=no
ac_cv_header_sys_pidfd_h=no
ac_cv_header_sys_random_h=no
ac_cv_func_setgroups=no
ac_cv_func_setpriority=no
ac_cv_func_sched_rr_get_interval=no
ac_cv_have_decl_PR_SET_VMA_ANON_NAME=no
ac_cv_alignof_long=8
ac_cv_alignof_max_align_t=16
ac_cv_alignof_size_t=8
ac_cv_aligned_required=yes
ac_cv_broken_mbstowcs=no
ac_cv_broken_nice=no
ac_cv_broken_poll=no
ac_cv_broken_sem_getvalue=yes
ac_cv_computed_gotos=no
ac_cv_cxx_thread=no
ac_cv_have_chflags=no
ac_cv_have_lchflags=no
ac_cv_have_pthread_t=yes
ac_cv_kpthread=no
ac_cv_kthread=no
ac_cv_posix_semaphores_enabled=yes
ac_cv_pthread=no
ac_cv_pthread_is_default=no
ac_cv_pthread_key_t_is_arithmetic_type=no
ac_cv_pthread_system_supported=no
ac_cv_rshift_extends_sign=yes
ac_cv_sizeof__Bool=1
ac_cv_sizeof_double=8
ac_cv_sizeof_float=4
ac_cv_sizeof_fpos_t=8
ac_cv_sizeof_int=4
ac_cv_sizeof_long=8
ac_cv_sizeof_long_double=16
ac_cv_sizeof_long_long=8
ac_cv_sizeof_off_t=8
ac_cv_sizeof_pid_t=8
ac_cv_sizeof_pthread_key_t=8
ac_cv_sizeof_pthread_t=8
ac_cv_sizeof_short=2
ac_cv_sizeof_size_t=8
ac_cv_sizeof_time_t=8
ac_cv_sizeof_uintptr_t=8
ac_cv_sizeof_void_p=8
ac_cv_wchar_t_signed=yes
ac_cv_working_tzset=no
ac_cv_x87_double_rounding=no
py_cv_module__remote_debugging=n/a
ac_cv_c_bigendian=no
ac_cv_c_compiler_gnu=yes
ac_cv_c_const=yes
ac_cv_c_undeclared_builtin_options='none needed'
ac_cv_cc_name=clang
ac_cv_cc_supports_fstrict_overflow=yes
ac_cv_cc_supports_og=yes
ac_cv_compile_o2=yes
ac_cv_builtin_atomic=yes
ac_cv_device_macros=yes
ac_cv_dirent_d_type=yes
ac_cv_disable_int_conversion_warning=yes
ac_cv_disable_missing_field_initializers_warning=yes
ac_cv_disable_unused_parameter_warning=yes
ac_cv_enable_extra_warning=yes
ac_cv_enable_implicit_function_declaration_error=yes
ac_cv_enable_sign_compare_warning=yes
ac_cv_enable_strict_prototypes_warning=yes
ac_cv_enable_unreachable_code_warning=yes
ac_cv_enable_visibility=yes
ac_cv_function_prototypes=yes
ac_cv_header_alloca_h=yes
ac_cv_header_asm_types_h=no
ac_cv_header_bluetooth_bluetooth_h=no
ac_cv_header_bluetooth_h=no
ac_cv_header_bzlib_h=no
ac_cv_header_conio_h=no
ac_cv_header_curses_h=no
ac_cv_header_db_h=no
ac_cv_header_direct_h=no
ac_cv_header_dirent_dirent_h=yes
ac_cv_header_dlfcn_h=yes
ac_cv_header_endian_h=yes
ac_cv_header_errno_h=yes
ac_cv_header_execinfo_h=yes
ac_cv_header_fcntl_h=yes
ac_cv_header_ffi_h=no
ac_cv_header_gdbm_dash_ndbm_h=no
ac_cv_header_gdbm_h=no
ac_cv_header_gdbm_slash_ndbm_h=no
ac_cv_header_grp_h=yes
ac_cv_header_inttypes_h=yes
ac_cv_header_io_h=no
ac_cv_header_langinfo_h=yes
ac_cv_header_libintl_h=no
ac_cv_header_libutil_h=no
ac_cv_header_link_h=yes
ac_cv_header_linux_can_bcm_h=no
ac_cv_header_linux_can_h=no
ac_cv_header_linux_can_isotp_h=no
ac_cv_header_linux_can_j1939_h=no
ac_cv_header_linux_can_raw_h=no
ac_cv_header_linux_fs_h=no
ac_cv_header_linux_limits_h=no
ac_cv_header_linux_memfd_h=no
ac_cv_header_linux_netfilter_ipv4_h=no
ac_cv_header_linux_netlink_h=no
ac_cv_header_linux_qrtr_h=no
ac_cv_header_linux_sched_h=no
ac_cv_header_linux_soundcard_h=no
ac_cv_header_linux_tipc_h=no
ac_cv_header_linux_vm_sockets_h=no
ac_cv_header_linux_wait_h=no
ac_cv_header_lzma_h=no
ac_cv_header_minix_config_h=no
ac_cv_header_mpdecimal_h=no
ac_cv_header_ncurses_curses_h=no
ac_cv_header_ncurses_h=no
ac_cv_header_ncurses_ncurses_h=no
ac_cv_header_ncurses_panel_h=no
ac_cv_header_ncursesw_curses_h=no
ac_cv_header_ncursesw_ncurses_h=no
ac_cv_header_ncursesw_panel_h=no
ac_cv_header_ndbm_h=no
ac_cv_header_net_ethernet_h=yes
ac_cv_header_net_if_h=yes
ac_cv_header_netcan_can_h=no
ac_cv_header_netdb_h=yes
ac_cv_header_netinet_in_h=yes
ac_cv_header_netlink_netlink_h=no
ac_cv_header_netpacket_packet_h=no
ac_cv_header_panel_h=no
ac_cv_header_poll_h=yes
ac_cv_header_process_h=no
ac_cv_header_pthread_h=yes
ac_cv_header_pty_h=yes
ac_cv_header_readline_readline_h=no
ac_cv_header_sched_h=yes
ac_cv_header_setjmp_h=yes
ac_cv_header_shadow_h=yes
ac_cv_header_signal_h=yes
ac_cv_header_spawn_h=yes
ac_cv_header_sqlite3_h=no
ac_cv_header_stdatomic_h=no
ac_cv_header_stdint_h=yes
ac_cv_header_stdio_h=yes
ac_cv_header_stdlib_h=yes
ac_cv_header_string_h=yes
ac_cv_header_strings_h=yes
ac_cv_header_sys_audioio_h=no
ac_cv_header_sys_auxv_h=yes
ac_cv_header_sys_bsdtty_h=no
ac_cv_header_sys_devpoll_h=no
ac_cv_header_sys_endian_h=yes
ac_cv_header_sys_epoll_h=yes
ac_cv_header_sys_event_h=no
ac_cv_header_sys_eventfd_h=no
ac_cv_header_sys_file_h=yes
ac_cv_header_sys_ioctl_h=yes
ac_cv_header_sys_kern_control_h=no
ac_cv_header_sys_loadavg_h=no
ac_cv_header_sys_lock_h=no
ac_cv_header_sys_memfd_h=no
ac_cv_header_sys_mkdev_h=no
ac_cv_header_sys_mman_h=yes
ac_cv_header_sys_modem_h=no
ac_cv_header_sys_param_h=yes
ac_cv_header_sys_poll_h=yes
ac_cv_header_sys_resource_h=yes
ac_cv_header_sys_select_h=yes
ac_cv_header_sys_sendfile_h=yes
ac_cv_header_sys_socket_h=yes
ac_cv_header_sys_soundcard_h=no
ac_cv_header_sys_stat_h=yes
ac_cv_header_sys_statvfs_h=yes
ac_cv_header_sys_sys_domain_h=no
ac_cv_header_sys_syscall_h=yes
ac_cv_header_sys_sysmacros_h=yes
ac_cv_header_sys_termio_h=no
ac_cv_header_sys_time_h=yes
ac_cv_header_sys_timerfd_h=no
ac_cv_header_sys_times_h=yes
ac_cv_header_sys_types_h=yes
ac_cv_header_sys_uio_h=yes
ac_cv_header_sys_un_h=yes
ac_cv_header_sys_utsname_h=yes
ac_cv_header_sys_wait_h=yes
ac_cv_header_sys_xattr_h=no
ac_cv_header_sysexits_h=yes
ac_cv_header_syslog_h=yes
ac_cv_header_term_h=no
ac_cv_header_termios_h=yes
ac_cv_header_time_altzone=no
ac_cv_header_unistd_h=yes
ac_cv_header_util_h=no
ac_cv_header_utime_h=yes
ac_cv_header_utmp_h=yes
ac_cv_header_uuid_h=no
ac_cv_header_uuid_uuid_h=no
ac_cv_header_wchar_h=yes
ac_cv_func___fpu_control=no
ac_cv_func__dyld_shared_cache_contains_path=no
ac_cv_func__getpty=no
ac_cv_func_accept=yes
ac_cv_func_accept4=yes
ac_cv_func_acosh=yes
ac_cv_func_acospi=no
ac_cv_func_alarm=yes
ac_cv_func_asinh=yes
ac_cv_func_asinpi=no
ac_cv_func_atan2pi=no
ac_cv_func_atanh=yes
ac_cv_func_atanpi=no
ac_cv_func_backtrace=yes
ac_cv_func_bind=yes
ac_cv_func_bind_textdomain_codeset=no
ac_cv_func_chmod=yes
ac_cv_func_chown=yes
ac_cv_func_chroot=yes
ac_cv_func_clearenv=yes
ac_cv_func_clock=yes
ac_cv_func_clock_getres=yes
ac_cv_func_clock_gettime=yes
ac_cv_func_clock_nanosleep=yes
ac_cv_func_clock_settime=yes
ac_cv_func_confstr=yes
ac_cv_func_connect=yes
ac_cv_func_copy_file_range=no
ac_cv_func_cospi=no
ac_cv_func_ctermid=yes
ac_cv_func_ctermid_r=no
ac_cv_func_dladdr=yes
ac_cv_func_dladdr1=yes
ac_cv_func_dlopen=yes
ac_cv_func_dup=yes
ac_cv_func_dup2=yes
ac_cv_func_dup3=yes
ac_cv_func_epoll_create=yes
ac_cv_func_epoll_create1=yes
ac_cv_func_erf=yes
ac_cv_func_erfc=yes
ac_cv_func_eventfd=no
ac_cv_func_execv=yes
ac_cv_func_explicit_bzero=yes
ac_cv_func_explicit_memset=no
ac_cv_func_expm1=yes
ac_cv_func_faccessat=yes
ac_cv_func_fchdir=yes
ac_cv_func_fchmod=yes
ac_cv_func_fchmodat=yes
ac_cv_func_fchown=yes
ac_cv_func_fchownat=yes
ac_cv_func_fdatasync=yes
ac_cv_func_fdopendir=yes
ac_cv_func_fdwalk=no
ac_cv_func_fexecve=yes
ac_cv_func_flock=yes
ac_cv_func_fork=yes
ac_cv_func_fork1=no
ac_cv_func_forkpty=yes
ac_cv_func_fpathconf=yes
ac_cv_func_fseek64=no
ac_cv_func_fseeko=yes
ac_cv_func_fstatat=yes
ac_cv_func_fstatvfs=yes
ac_cv_func_fsync=yes
ac_cv_func_ftell64=no
ac_cv_func_ftello=yes
ac_cv_func_ftime=no
ac_cv_func_ftruncate=yes
ac_cv_func_futimens=yes
ac_cv_func_futimes=yes
ac_cv_func_futimesat=no
ac_cv_func_gai_strerror=yes
ac_cv_func_getaddrinfo=yes
ac_cv_func_getegid=yes
ac_cv_func_geteuid=yes
ac_cv_func_getgid=yes
ac_cv_func_getgrent=yes
ac_cv_func_getgrgid=yes
ac_cv_func_getgrgid_r=yes
ac_cv_func_getgrnam_r=yes
ac_cv_func_getgrouplist=yes
ac_cv_func_getgroups=yes
ac_cv_func_gethostbyaddr=yes
ac_cv_func_gethostbyname=yes
ac_cv_func_gethostbyname_r=yes
ac_cv_func_gethostname=yes
ac_cv_func_getitimer=yes
ac_cv_func_getloadavg=yes
ac_cv_func_getlogin=yes
ac_cv_func_getlogin_r=yes
ac_cv_func_getnameinfo=yes
ac_cv_func_getpagesize=yes
ac_cv_func_getpeername=yes
ac_cv_func_getpgid=yes
ac_cv_func_getpgrp=yes
ac_cv_func_getpid=yes
ac_cv_func_getppid=yes
ac_cv_func_getpriority=yes
ac_cv_func_getprotobyname=yes
ac_cv_func_getpwent=yes
ac_cv_func_getpwnam_r=yes
ac_cv_func_getpwuid=yes
ac_cv_func_getpwuid_r=yes
ac_cv_func_getresgid=yes
ac_cv_func_getresuid=yes
ac_cv_func_getrusage=yes
ac_cv_func_getservbyname=yes
ac_cv_func_getservbyport=yes
ac_cv_func_getsid=yes
ac_cv_func_getsockname=yes
ac_cv_func_getspent=no
ac_cv_func_getspnam=yes
ac_cv_func_getuid=yes
ac_cv_func_getwd=no
ac_cv_func_grantpt=yes
ac_cv_func_hstrerror=yes
ac_cv_func_if_nameindex=yes
ac_cv_func_inet_aton=yes
ac_cv_func_inet_ntoa=yes
ac_cv_func_inet_pton=yes
ac_cv_func_initgroups=yes
ac_cv_func_kill=yes
ac_cv_func_killpg=yes
ac_cv_func_kqueue=no
ac_cv_func_lchmod=no
ac_cv_func_lchown=yes
ac_cv_func_le64toh=yes
ac_cv_func_link=yes
ac_cv_func_linkat=yes
ac_cv_func_listen=yes
ac_cv_func_lockf=yes
ac_cv_func_log1p=yes
ac_cv_func_log2=yes
ac_cv_func_lstat=yes
ac_cv_func_lutimes=yes
ac_cv_func_madvise=yes
ac_cv_func_makedev=yes
ac_cv_func_mbrtowc=yes
ac_cv_func_memfd_create=yes
ac_cv_func_memrchr=yes
ac_cv_func_mkdirat=yes
ac_cv_func_mkfifo=yes
ac_cv_func_mkfifoat=yes
ac_cv_func_mknod=yes
ac_cv_func_mknodat=yes
ac_cv_func_mktime=yes
ac_cv_func_mmap=yes
ac_cv_func_mremap=yes
ac_cv_func_nanosleep=yes
ac_cv_func_nice=yes
ac_cv_func_openat=yes
ac_cv_func_opendir=yes
ac_cv_func_openpty=yes
ac_cv_func_pathconf=yes
ac_cv_func_pause=yes
ac_cv_func_pipe=yes
ac_cv_func_pipe2=yes
ac_cv_func_plock=no
ac_cv_func_poll=yes
ac_cv_func_posix_fadvise=yes
ac_cv_func_posix_fallocate=yes
ac_cv_func_posix_openpt=yes
ac_cv_func_posix_spawn=yes
ac_cv_func_posix_spawnp=yes
ac_cv_func_ppoll=yes
ac_cv_func_pread=yes
ac_cv_func_preadv=yes
ac_cv_func_preadv2=no
ac_cv_func_prlimit=yes
ac_cv_func_pthread_cond_timedwait_relative_np=no
ac_cv_func_pthread_condattr_setclock=yes
ac_cv_func_pthread_get_name_np=no
ac_cv_func_pthread_getattr_np=yes
ac_cv_func_pthread_getcpuclockid=yes
ac_cv_func_pthread_getname_np=yes
ac_cv_func_pthread_init=no
ac_cv_func_pthread_kill=yes
ac_cv_func_pthread_set_name_np=no
ac_cv_func_pthread_setname_np=yes
ac_cv_func_pthread_sigmask=yes
ac_cv_func_ptsname=yes
ac_cv_func_ptsname_r=yes
ac_cv_func_pwrite=yes
ac_cv_func_pwritev=yes
ac_cv_func_pwritev2=no
ac_cv_func_readlink=yes
ac_cv_func_readlinkat=yes
ac_cv_func_readv=yes
ac_cv_func_realpath=yes
ac_cv_func_recvfrom=yes
ac_cv_func_renameat=yes
ac_cv_func_rtpSpawn=no
ac_cv_func_sched_get_priority_max=yes
ac_cv_func_sched_setaffinity=yes
ac_cv_func_sched_setparam=yes
ac_cv_func_sched_setscheduler=yes
ac_cv_func_sem_clockwait=yes
ac_cv_func_sem_getvalue=yes
ac_cv_func_sem_open=yes
ac_cv_func_sem_timedwait=yes
ac_cv_func_sem_unlink=yes
ac_cv_func_sendfile=yes
ac_cv_func_sendto=yes
ac_cv_func_setegid=yes
ac_cv_func_seteuid=yes
ac_cv_func_setgid=yes
ac_cv_func_sethostname=yes
ac_cv_func_setitimer=yes
ac_cv_func_setlocale=yes
ac_cv_func_setns=yes
ac_cv_func_setpgid=yes
ac_cv_func_setpgrp=yes
ac_cv_func_setregid=yes
ac_cv_func_setresgid=yes
ac_cv_func_setresuid=yes
ac_cv_func_setreuid=yes
ac_cv_func_setsid=yes
ac_cv_func_setsockopt=yes
ac_cv_func_setuid=yes
ac_cv_func_setvbuf=yes
ac_cv_func_shm_open=yes
ac_cv_func_shm_unlink=yes
ac_cv_func_shutdown=yes
ac_cv_func_sigaction=yes
ac_cv_func_sigaltstack=yes
ac_cv_func_sigfillset=yes
ac_cv_func_siginterrupt=yes
ac_cv_func_sigpending=yes
ac_cv_func_sigrelse=no
ac_cv_func_sigtimedwait=yes
ac_cv_func_sigwait=yes
ac_cv_func_sigwaitinfo=yes
ac_cv_func_sinpi=no
ac_cv_func_snprintf=yes
ac_cv_func_socket=yes
ac_cv_func_socketpair=yes
ac_cv_func_splice=no
ac_cv_func_statvfs=yes
ac_cv_func_strftime=yes
ac_cv_func_strlcpy=yes
ac_cv_func_strsignal=yes
ac_cv_func_symlink=yes
ac_cv_func_symlinkat=yes
ac_cv_func_sync=yes
ac_cv_func_sysconf=yes
ac_cv_func_system=yes
ac_cv_func_tanpi=no
ac_cv_func_tcgetpgrp=yes
ac_cv_func_tcsetpgrp=yes
ac_cv_func_tempnam=yes
ac_cv_func_timegm=yes
ac_cv_func_timerfd_create=no
ac_cv_func_times=yes
ac_cv_func_tmpfile=yes
ac_cv_func_tmpnam=yes
ac_cv_func_tmpnam_r=no
ac_cv_func_truncate=yes
ac_cv_func_ttyname_r=yes
ac_cv_func_umask=yes
ac_cv_func_uname=yes
ac_cv_func_unlinkat=yes
ac_cv_func_unlockpt=yes
ac_cv_func_unshare=yes
ac_cv_func_utimensat=yes
ac_cv_func_utimes=yes
ac_cv_func_vfork=yes
ac_cv_func_wait=yes
ac_cv_func_wait3=yes
ac_cv_func_wait4=yes
ac_cv_func_waitid=yes
ac_cv_func_waitpid=yes
ac_cv_func_wcscoll=yes
ac_cv_func_wcsftime=yes
ac_cv_func_wcsxfrm=yes
ac_cv_func_wmemcmp=yes
ac_cv_func_writev=yes
ac_cv_have_decl_I_PUSH=no
ac_cv_have_decl_MAXLOGNAME=no
ac_cv_have_decl_RTLD_DEEPBIND=yes
ac_cv_have_decl_RTLD_GLOBAL=yes
ac_cv_have_decl_RTLD_LAZY=yes
ac_cv_have_decl_RTLD_LOCAL=yes
ac_cv_have_decl_RTLD_MEMBER=no
ac_cv_have_decl_RTLD_NODELETE=yes
ac_cv_have_decl_RTLD_NOLOAD=yes
ac_cv_have_decl_RTLD_NOW=yes
ac_cv_have_decl_UT_NAMESIZE=no
ac_cv_have_decl_dirfd=yes
ac_cv_have_getc_unlocked=yes
ac_cv_have_libgcc_eh_frame_registration=no
ac_cv_member_struct_passwd_pw_gecos=yes
ac_cv_member_struct_passwd_pw_passwd=yes
ac_cv_member_struct_siginfo_t_si_band=yes
ac_cv_member_struct_sockaddr_sa_len=no
ac_cv_member_struct_stat_st_birthtime=no
ac_cv_member_struct_stat_st_blksize=yes
ac_cv_member_struct_stat_st_blocks=yes
ac_cv_member_struct_stat_st_flags=no
ac_cv_member_struct_stat_st_gen=no
ac_cv_member_struct_stat_st_rdev=yes
ac_cv_member_struct_tm_tm_zone=yes
ac_cv_type___uint128_t=yes
ac_cv_type_addrinfo=yes
ac_cv_type_clock_t=yes
ac_cv_type_gid_t=yes
ac_cv_type_long_double=yes
ac_cv_type_mode_t=yes
ac_cv_type_off_t=yes
ac_cv_type_pid_t=yes
ac_cv_type_size_t=yes
ac_cv_type_sockaddr_alg=no
ac_cv_type_sockaddr_storage=yes
ac_cv_type_socklen_t=yes
ac_cv_type_ssize_t=yes
ac_cv_type_uid_t=yes
ac_cv_wchar_t_usable=no
EOF

	if [ ! -f "$PYTHON_CONFIG_SITE" ] || ! cmp -s "$tmp_config_site" "$PYTHON_CONFIG_SITE"; then
		mv "$tmp_config_site" "$PYTHON_CONFIG_SITE"
	else
		rm -f "$tmp_config_site"
	fi
}

write_libressl_sigalgs_compat_header() {
	local header="$1"
	local opensslv="$TARGET_SYSROOT/include/openssl/opensslv.h"
	local tmp_header

	if [ ! -f "$opensslv" ] || ! grep -q 'LIBRESSL_VERSION_NUMBER' "$opensslv"; then
		return 1
	fi

	mkdir -p "$(dirname "$header")"
	tmp_header="$(mktemp "$header.XXXXXX")"
	cat > "$tmp_header" <<'EOF'
#ifndef WOS_LIBRESSL_SIGALGS_COMPAT_H
#define WOS_LIBRESSL_SIGALGS_COMPAT_H 1

/*
 * LibreSSL exposes enough OpenSSL-compatible TLS APIs for CPython _ssl, but
 * not OpenSSL's signature-algorithm setter helpers.  CPython only uses these
 * in optional methods, so make those calls fail cleanly instead of failing the
 * whole target build on an implicit declaration.
 */
#define SSL_CTX_set1_client_sigalgs_list(ctx, sigalgslist) (0)
#define SSL_CTX_set1_sigalgs_list(ctx, sigalgslist) (0)

#endif
EOF

	if [ ! -f "$header" ] || ! cmp -s "$tmp_header" "$header"; then
		mv "$tmp_header" "$header"
	else
		rm -f "$tmp_header"
	fi
	return 0
}

python_target_config_is_wos() {
	local makefile="$PYTHON_TARGET_BUILD/Makefile"
	local pyconfig="$PYTHON_TARGET_BUILD/pyconfig.h"

	[ -f "$makefile" ] || return 1
	grep -Eq '^HOST_GNU_TYPE[[:space:]]*=[[:space:]]*x86_64-pc-wos([[:space:]]|$)' "$makefile" || return 1
	grep -Eq '^CC[[:space:]]*=.*clang' "$makefile" || return 1
	grep -Eq '^CC[[:space:]]*=.*--target=x86_64-pc-wos' "$makefile" || return 1
	grep -Fq -- "-isystem $HOST/lib/clang/22/include" "$makefile" || return 1
	grep -Fq -- "-isystem $TARGET_SYSROOT/include" "$makefile" || return 1
	if [ -n "${BUILD_PYTHON:-}" ]; then
		grep -Fq "PYTHON_FOR_FREEZE=$BUILD_PYTHON" "$makefile" || return 1
	fi
	! grep -Eq '^CC[[:space:]]*=[[:space:]]*gcc([[:space:]]|$)' "$makefile" || return 1
	! grep -Eq '^LDSHARED[[:space:]]*=[[:space:]]*ld([[:space:]]|$)' "$makefile" || return 1
	[ -f "$pyconfig" ] || return 1
	! grep -Eq '^MODULE__REMOTE_DEBUGGING_STATE[[:space:]]*=[[:space:]]*yes([[:space:]]|$)' "$makefile" || return 1

	local disallowed_define
	for disallowed_define in \
		HAVE_CLOSE_RANGE \
		HAVE_CLOSEFROM \
		HAVE_GETRANDOM \
		HAVE_GETRANDOM_SYSCALL \
		HAVE_LINUX_AUXVEC_H \
		HAVE_LINUX_RANDOM_H \
		HAVE_POSIX_SPAWN_FILE_ACTIONS_ADDCLOSEFROM_NP \
		HAVE_PROCESS_VM_READV \
		HAVE_SYS_PIDFD_H \
		HAVE_SYS_RANDOM_H \
		PY_HAVE_PERF_TRAMPOLINE \
		_Py_HAVE_PR_SET_VMA_ANON_NAME; do
		! grep -Eq "^#define[[:space:]]+$disallowed_define[[:space:]]+1" "$pyconfig" || return 1
	done
}

prune_disabled_extension_artifacts() {
	if [ ! -f "$PYTHON_TARGET_BUILD/Makefile" ]; then
		return 0
	fi

	if grep -Eq '^MODULE__REMOTE_DEBUGGING_STATE[[:space:]]*=[[:space:]]*n/a([[:space:]]|$)' "$PYTHON_TARGET_BUILD/Makefile"; then
		find "$PYTHON_TARGET_BUILD/Modules" -maxdepth 1 -type f -name '_remote_debugging*.so' -delete 2>/dev/null || true
		find "$TARGET_SYSROOT/lib" "$TARGET_SYSROOT/usr/lib" \
			-path '*/python*/lib-dynload/_remote_debugging*.so' \
			-type f -delete 2>/dev/null || true
	fi
}

discard_stale_target_config() {
	if [ ! -f "$PYTHON_TARGET_BUILD/Makefile" ]; then
		return 0
	fi

	if python_target_config_is_wos; then
		return 0
	fi

	echo "Discarding stale CPython target configure output; expected WOS clang, not host gcc/Linux."
	rm -f \
		"$PYTHON_TARGET_BUILD/Makefile" \
		"$PYTHON_TARGET_BUILD/Makefile.pre" \
		"$PYTHON_TARGET_BUILD/config.cache" \
		"$PYTHON_TARGET_BUILD/config.log" \
		"$PYTHON_TARGET_BUILD/config.status" \
		"$PYTHON_TARGET_BUILD/pyconfig.h"
}

require_file "$HOST/bin/clang" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/clang++" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/ld.lld" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/llvm-ar" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/llvm-ranlib" "Run tools/host-toolchain.sh first."
require_file "$HOST/bin/llvm-strip" "Run tools/host-toolchain.sh first."
require_file "$PYTHON_SRC/configure" "Initialize toolchain/src/python from https://github.com/Pascu-Victor/cpython.git."
require_file "$PYTHON_SRC/config.guess" "CPython source is missing config.guess."
require_file "$TARGET_SYSROOT/lib/libc.so" "Build mlibc before building Python."
require_file "$TARGET_SYSROOT/lib/libc++.so" "Build libc++ before building Python."
require_file "$TARGET_SYSROOT/lib/Scrt1.o" "Build mlibc startup objects before building Python."

patch_config_sub_for_wos "$PYTHON_SRC/config.sub"
if [ "$HOST_SYSTEM" = "WOS" ]; then
	BUILD_TRIPLE="$TARGET_ARCH"
else
	BUILD_TRIPLE="$("$PYTHON_SRC/config.guess")"
fi
PYTHON_HOST_CONFIGURE_BUILD_ARGS=()
PYTHON_CONFIGURE_CACHE_ARGS=()
if [ "$HOST_SYSTEM" = "WOS" ]; then
	PYTHON_HOST_CONFIGURE_BUILD_ARGS=(--build="$BUILD_TRIPLE")
	PYTHON_CONFIGURE_CACHE_ARGS=(
		ac_cv_path_GREP=/usr/bin/grep
		"ac_cv_path_EGREP=/usr/bin/grep -E"
		"ac_cv_path_EGREP_TRADITIONAL=/usr/bin/grep -E"
		"ac_cv_path_FGREP=/usr/bin/grep -F"
		ac_cv_path_SED=/usr/bin/sed
		"ac_cv_path_install=/usr/bin/install -c"
		ac_cv_path_mkdir=/usr/bin/mkdir
		ac_cv_prog_HAS_GIT=found
	)
fi

mkdir -p "$PYTHON_HOST_BUILD" "$PYTHON_TARGET_BUILD" "$TARGET_SYSROOT/bin"
if [ ! -e "$TARGET_SYSROOT/usr" ]; then
    ln -s . "$TARGET_SYSROOT/usr"
fi

BUILD_PYTHON=""
EXTERNAL_BUILD_PYTHON=""
if [ "$HOST_SYSTEM" = "WOS" ] || [ -n "${WOS_PYTHON_BUILD_PYTHON:-}" ]; then
	if ! EXTERNAL_BUILD_PYTHON="$(find_compatible_build_python)"; then
		if [ -n "${WOS_PYTHON_BUILD_PYTHON:-}" ]; then
			exit 1
		fi
		EXTERNAL_BUILD_PYTHON=""
	fi
fi

if [ -n "$EXTERNAL_BUILD_PYTHON" ]; then
	echo "Using existing build Python $EXTERNAL_BUILD_PYTHON for CPython build helpers"
	BUILD_PYTHON="$EXTERNAL_BUILD_PYTHON"
else
		if [ ! -f "$PYTHON_HOST_BUILD/Makefile" ] || [ "$PYTHON_SRC/configure" -nt "$PYTHON_HOST_BUILD/Makefile" ] || [ "$SCRIPT_DIR/build_python_for_wos.sh" -nt "$PYTHON_HOST_BUILD/Makefile" ]; then
			echo "Configuring build-host CPython..."
			if [ -f "$PYTHON_HOST_BUILD/Makefile" ]; then
				host_env make ${WOS_MAKE_JOBSERVER_ARG:+"$WOS_MAKE_JOBSERVER_ARG"} -j"$WOS_MAKE_JOBS" -C "$PYTHON_HOST_BUILD" clean >/dev/null 2>&1 || true
			fi
			wos_timed_step "configure" "cpython_host" \
				wos_run_in_dir "$PYTHON_HOST_BUILD" \
				host_env "$PYTHON_SRC/configure" \
				"${PYTHON_CONFIGURE_CACHE_ARGS[@]}" \
				"${PYTHON_HOST_CONFIGURE_BUILD_ARGS[@]}" \
				--prefix="$PYTHON_HOST_BUILD/install" \
				--without-ensurepip \
				--disable-test-modules \
				--disable-ipv6
	fi

	echo "Building build-host CPython with WOS_MAKE_JOBS=$WOS_MAKE_JOBS..."
	wos_timed_step "make" "cpython_host:python" \
		host_env make ${WOS_MAKE_JOBSERVER_ARG:+"$WOS_MAKE_JOBSERVER_ARG"} -C "$PYTHON_HOST_BUILD" -j"$WOS_MAKE_JOBS" python
	BUILD_PYTHON="$PYTHON_HOST_BUILD/python"
	require_file "$BUILD_PYTHON" "Build-host CPython did not produce $BUILD_PYTHON."
fi

write_config_site

TARGET_CC="${WOS_CCACHE_PREFIX}$HOST/bin/clang --target=$TARGET_ARCH --sysroot=$TARGET_SYSROOT"
TARGET_CXX="${WOS_CCACHE_PREFIX}$HOST/bin/clang++ --target=$TARGET_ARCH --sysroot=$TARGET_SYSROOT"
TARGET_AR="$HOST/bin/llvm-ar"
TARGET_RANLIB="$HOST/bin/llvm-ranlib"
TARGET_STRIP="$HOST/bin/llvm-strip"
TARGET_READELF="$HOST/bin/llvm-readelf"
TARGET_PKG_CONFIG_LIBDIR="$TARGET_SYSROOT/lib/pkgconfig:$TARGET_SYSROOT/usr/lib/pkgconfig:$TARGET_SYSROOT/share/pkgconfig:$TARGET_SYSROOT/usr/share/pkgconfig"
PYTHON_CFLAGS="--sysroot=$TARGET_SYSROOT -O2 -g -fno-sanitize=safe-stack -fno-stack-protector -fPIC -fPIE"
PYTHON_CPPFLAGS="-isystem $HOST/lib/clang/22/include -isystem $TARGET_SYSROOT/include"
if write_libressl_sigalgs_compat_header "$PYTHON_LIBRESSL_SIGALGS_COMPAT_HEADER"; then
	PYTHON_CPPFLAGS="$PYTHON_CPPFLAGS -include $PYTHON_LIBRESSL_SIGALGS_COMPAT_HEADER"
fi
PYTHON_LDFLAGS="--sysroot=$TARGET_SYSROOT -fuse-ld=lld -L$TARGET_SYSROOT/lib -Wl,--dynamic-linker=/lib/ld.so -Wl,-rpath,/usr/lib -fno-sanitize=safe-stack"

discard_stale_target_config

if [ ! -f "$PYTHON_TARGET_BUILD/Makefile" ] || [ "$PYTHON_SRC/configure" -nt "$PYTHON_TARGET_BUILD/Makefile" ] || [ "$PYTHON_CONFIG_SITE" -nt "$PYTHON_TARGET_BUILD/Makefile" ] || [ "$SCRIPT_DIR/build_python_for_wos.sh" -nt "$PYTHON_TARGET_BUILD/Makefile" ]; then
	echo "Configuring CPython for WOS..."
	wos_timed_step "configure" "cpython_target" \
		wos_run_env_in_dir "$PYTHON_TARGET_BUILD" \
		CC="$TARGET_CC" \
		CXX="$TARGET_CXX" \
		AR="$TARGET_AR" \
		RANLIB="$TARGET_RANLIB" \
		STRIP="$TARGET_STRIP" \
		READELF="$TARGET_READELF" \
		CFLAGS="$PYTHON_CFLAGS" \
		CPPFLAGS="$PYTHON_CPPFLAGS" \
		LDFLAGS="$PYTHON_LDFLAGS" \
		CONFIG_SITE="$PYTHON_CONFIG_SITE" \
		PKG_CONFIG_LIBDIR="$TARGET_PKG_CONFIG_LIBDIR" \
		PKG_CONFIG_SYSROOT_DIR="$TARGET_SYSROOT" \
		PKG_CONFIG_PATH= \
		"$PYTHON_SRC/configure" \
		"${PYTHON_CONFIGURE_CACHE_ARGS[@]}" \
		--build="$BUILD_TRIPLE" \
		--host="$TARGET_ARCH" \
		--prefix=/usr \
		--exec-prefix=/usr \
		--with-build-python="$BUILD_PYTHON" \
		--without-ensurepip \
		--without-remote-debug \
		--disable-test-modules \
		--disable-ipv6
fi

if ! python_target_config_is_wos; then
    echo "ERROR: CPython target configure did not select WOS clang." >&2
    echo "Inspect $PYTHON_TARGET_BUILD/config.log and $PYTHON_TARGET_BUILD/Makefile." >&2
    exit 1
fi
prune_disabled_extension_artifacts

if [ -f "$PYTHON_TARGET_BUILD/python" ]; then
    for lib in "$TARGET_SYSROOT"/lib/libc.so "$TARGET_SYSROOT"/lib/libc++.so \
               "$TARGET_SYSROOT"/lib/libc++abi.so "$TARGET_SYSROOT"/lib/libm.so; do
        if [ -f "$lib" ] && [ "$lib" -nt "$PYTHON_TARGET_BUILD/python" ]; then
            echo "Sysroot library $(basename "$lib") changed - forcing relink"
            rm -f "$PYTHON_TARGET_BUILD/python"
            break
        fi
    done
fi

echo "Building target CPython with WOS_MAKE_JOBS=$WOS_MAKE_JOBS..."
wos_make "$WOS_MAKE_JOBS" -C "$PYTHON_TARGET_BUILD" \
    CC="$TARGET_CC" \
    CXX="$TARGET_CXX" \
    AR="$TARGET_AR" \
    RANLIB="$TARGET_RANLIB" \
    STRIP="$TARGET_STRIP" \
    READELF="$TARGET_READELF" \
    python
echo "Installing target CPython with WOS_PYTHON_INSTALL_JOBS=$WOS_PYTHON_INSTALL_JOBS..."
wos_make "$WOS_PYTHON_INSTALL_JOBS" -C "$PYTHON_TARGET_BUILD" \
    CC="$TARGET_CC" \
    CXX="$TARGET_CXX" \
    AR="$TARGET_AR" \
    RANLIB="$TARGET_RANLIB" \
    STRIP="$TARGET_STRIP" \
    READELF="$TARGET_READELF" \
    prefix= \
    exec_prefix= \
    DESTDIR="$TARGET_SYSROOT" \
    install

if [ ! -e "$TARGET_SYSROOT/bin/python3" ]; then
    for candidate in "$TARGET_SYSROOT"/bin/python3.*; do
        [ -e "$candidate" ] || continue
        ln -sfn "$(basename "$candidate")" "$TARGET_SYSROOT/bin/python3"
        break
    done
fi
require_file "$TARGET_SYSROOT/bin/python3" "CPython install did not produce $TARGET_SYSROOT/bin/python3."
ln -sfn python3 "$TARGET_SYSROOT/bin/python"

if [ "$WOS_PYTHON_STRIP" != "0" ]; then
    for binary in "$TARGET_SYSROOT"/bin/python "$TARGET_SYSROOT"/bin/python3 "$TARGET_SYSROOT"/bin/python3.*; do
        [ -f "$binary" ] || continue
        "$HOST/bin/llvm-strip" "$binary" || true
    done
    find "$TARGET_SYSROOT"/lib -path '*/lib-dynload/*.so' -type f -exec "$HOST/bin/llvm-strip" {} + 2>/dev/null || true
fi

echo "Native WOS Python installed to $TARGET_SYSROOT/bin/python3"
