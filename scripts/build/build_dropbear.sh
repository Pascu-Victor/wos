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

# Configure if not yet configured
if [ ! -f "$DB_BUILD/Makefile" ]; then
    echo "Configuring dropbear for WOS..."
    DROPBEAR_CONFIGURE_BUILD_ARGS=()
    DROPBEAR_CONFIGURE_CACHE_ARGS=()
    if [ "$(uname -s 2>/dev/null || printf unknown)" = "WOS" ]; then
        DROPBEAR_CONFIGURE_BUILD_ARGS=(--build="$TARGET_ARCH")
        DROPBEAR_CONFIGURE_CACHE_ARGS=(
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
    cd "$DB_BUILD"
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
    cd "$B/.."
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
