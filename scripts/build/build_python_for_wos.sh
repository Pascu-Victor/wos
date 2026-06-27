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
py_cv_module__remote_debugging=n/a
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
		"ac_cv_path_FGREP=/usr/bin/grep -F"
	)
fi

mkdir -p "$PYTHON_HOST_BUILD" "$PYTHON_TARGET_BUILD" "$TARGET_SYSROOT/bin"
if [ ! -e "$TARGET_SYSROOT/usr" ]; then
    ln -s . "$TARGET_SYSROOT/usr"
fi

if [ ! -f "$PYTHON_HOST_BUILD/Makefile" ] || [ "$PYTHON_SRC/configure" -nt "$PYTHON_HOST_BUILD/Makefile" ] || [ "$SCRIPT_DIR/build_python_for_wos.sh" -nt "$PYTHON_HOST_BUILD/Makefile" ]; then
    echo "Configuring build-host CPython..."
    if [ -f "$PYTHON_HOST_BUILD/Makefile" ]; then
        host_env make -C "$PYTHON_HOST_BUILD" clean >/dev/null 2>&1 || true
    fi
    (
        cd "$PYTHON_HOST_BUILD"
        host_env "$PYTHON_SRC/configure" \
            "${PYTHON_CONFIGURE_CACHE_ARGS[@]}" \
            "${PYTHON_HOST_CONFIGURE_BUILD_ARGS[@]}" \
            --prefix="$PYTHON_HOST_BUILD/install" \
            --without-ensurepip \
            --disable-test-modules \
            --disable-ipv6
    )
fi

host_env make -C "$PYTHON_HOST_BUILD" -j"$WOS_MAKE_JOBS" python
BUILD_PYTHON="$PYTHON_HOST_BUILD/python"
require_file "$BUILD_PYTHON" "Build-host CPython did not produce $BUILD_PYTHON."

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
	(
		cd "$PYTHON_TARGET_BUILD"
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
	)
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

make -C "$PYTHON_TARGET_BUILD" -j"$WOS_MAKE_JOBS" \
    CC="$TARGET_CC" \
    CXX="$TARGET_CXX" \
    AR="$TARGET_AR" \
    RANLIB="$TARGET_RANLIB" \
    STRIP="$TARGET_STRIP" \
    READELF="$TARGET_READELF" \
    python
make -C "$PYTHON_TARGET_BUILD" \
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
