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

B="$WORKSPACE_ROOT/toolchain"
HOST="$B/host"
TARGET_ARCH="${WOS_TARGET_ARCH:-x86_64-pc-wos}"
TARGET_SYSROOT="${WOS_SYSROOT_PATH:-$B/sysroot}"
PYTHON_SRC="${WOS_PYTHON_SOURCE_DIR:-$B/src/python}"
PYTHON_BUILD="${WOS_PYTHON_BUILD_DIR:-$B/python-build}"
PYTHON_HOST_BUILD="${WOS_PYTHON_HOST_BUILD_DIR:-$PYTHON_BUILD/build-python}"
PYTHON_TARGET_BUILD="$PYTHON_BUILD/target"
PYTHON_CONFIG_SITE="${WOS_PYTHON_CONFIG_SITE:-$PYTHON_BUILD/config.site}"
WOS_PYTHON_STRIP="${WOS_PYTHON_STRIP:-0}"

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

write_config_site() {
	local tmp_config_site

	mkdir -p "$(dirname "$PYTHON_CONFIG_SITE")"
	tmp_config_site="$(mktemp "$PYTHON_CONFIG_SITE.XXXXXX")"
	cat > "$tmp_config_site" <<'EOF'
ac_cv_file__dev_ptmx=no
ac_cv_file__dev_ptc=no
ac_cv_buggy_getaddrinfo=no
ac_cv_func_getentropy=no
ac_cv_func_getrandom=no
ac_cv_func_setgroups=no
ac_cv_func_setpriority=no
ac_cv_func_sched_rr_get_interval=no
EOF

	if [ ! -f "$PYTHON_CONFIG_SITE" ] || ! cmp -s "$tmp_config_site" "$PYTHON_CONFIG_SITE"; then
		mv "$tmp_config_site" "$PYTHON_CONFIG_SITE"
	else
		rm -f "$tmp_config_site"
	fi
}

python_target_config_is_wos() {
	local makefile="$PYTHON_TARGET_BUILD/Makefile"

	[ -f "$makefile" ] || return 1
	grep -Eq '^HOST_GNU_TYPE[[:space:]]*=[[:space:]]*x86_64-pc-wos([[:space:]]|$)' "$makefile" || return 1
	grep -Eq '^CC[[:space:]]*=.*clang' "$makefile" || return 1
	grep -Eq '^CC[[:space:]]*=.*--target=x86_64-pc-wos' "$makefile" || return 1
	! grep -Eq '^CC[[:space:]]*=[[:space:]]*gcc([[:space:]]|$)' "$makefile" || return 1
	! grep -Eq '^LDSHARED[[:space:]]*=[[:space:]]*ld([[:space:]]|$)' "$makefile" || return 1
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
BUILD_TRIPLE="$("$PYTHON_SRC/config.guess")"

mkdir -p "$PYTHON_HOST_BUILD" "$PYTHON_TARGET_BUILD" "$TARGET_SYSROOT/bin"
if [ ! -e "$TARGET_SYSROOT/usr" ]; then
    ln -s . "$TARGET_SYSROOT/usr"
fi

if [ ! -f "$PYTHON_HOST_BUILD/Makefile" ] || [ "$PYTHON_SRC/configure" -nt "$PYTHON_HOST_BUILD/Makefile" ]; then
    echo "Configuring build-host CPython..."
    (
        cd "$PYTHON_HOST_BUILD"
        env -u CC -u CXX -u AR -u RANLIB -u STRIP -u CFLAGS -u CXXFLAGS -u LDFLAGS \
            "$PYTHON_SRC/configure" \
                --prefix="$PYTHON_HOST_BUILD/install" \
                --without-ensurepip \
                --disable-test-modules
    )
fi

make -C "$PYTHON_HOST_BUILD" -j"$(nproc)" python
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
			CPPFLAGS="-I$TARGET_SYSROOT/include" \
			LDFLAGS="$PYTHON_LDFLAGS" \
			CONFIG_SITE="$PYTHON_CONFIG_SITE" \
			PKG_CONFIG_LIBDIR="$TARGET_PKG_CONFIG_LIBDIR" \
			PKG_CONFIG_SYSROOT_DIR="$TARGET_SYSROOT" \
			PKG_CONFIG_PATH= \
			"$PYTHON_SRC/configure" \
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

make -C "$PYTHON_TARGET_BUILD" -j"$(nproc)" \
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
