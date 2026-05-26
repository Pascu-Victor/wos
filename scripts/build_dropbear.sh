#!/bin/bash
# Incrementally rebuild Dropbear SSH for WOS and install into the sysroot.
# Expects the toolchain to already be bootstrapped (tools/bootstrap.sh).
set -e

B=$(pwd)/toolchain
HOST="$B/host"
TARGET_SYSROOT="$B/sysroot"
DB_SRC="$B/src/dropbear"
DB_BUILD="$B/dropbear-build"

if [ ! -d "$DB_SRC" ]; then
    echo "ERROR: dropbear source directory not found at $DB_SRC"
    echo "Run tools/bootstrap.sh first to bootstrap the toolchain."
    exit 1
fi

# Cross-compilation environment - host tools, target sysroot
DROPBEAR_CFLAGS="--sysroot=$TARGET_SYSROOT -O3 -g -fno-sanitize=safe-stack -fno-stack-protector -I$TARGET_SYSROOT/include"
export CC="$HOST/bin/clang --target=x86_64-pc-wos --sysroot=$TARGET_SYSROOT"
export AR="$HOST/bin/llvm-ar"
export RANLIB="$HOST/bin/llvm-ranlib"
export STRIP="$HOST/bin/llvm-strip"
export CFLAGS="$DROPBEAR_CFLAGS"
export LDFLAGS="--sysroot=$TARGET_SYSROOT -fuse-ld=lld -L$TARGET_SYSROOT/lib"

mkdir -p "$DB_BUILD"

cat > "$DB_BUILD/localoptions.h" <<'EOF'
#define DROPBEAR_SFTPSERVER 1
#define SFTPSERVER_PATH "/usr/libexec/sftp-server"
#define DROPBEAR_PATH_SSH_PROGRAM "/usr/bin/dbclient"
#define DROPBEAR_SMALL_CODE 0
#define DEFAULT_RECV_WINDOW (1024 * 1024)
#define RECV_MAX_PAYLOAD_LEN (128 * 1024)
#define TRANS_MAX_PAYLOAD_LEN (64 * 1024)
EOF

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
    cd "$DB_BUILD"
    "$DB_SRC/configure" \
        --host=x86_64-pc-wos \
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
make -C "$DB_BUILD" -j"$(nproc)" STATIC=0 PROGRAMS="dropbear dbclient dropbearkey scp" MULTI=1 dropbearmulti

# Install into sysroot
cp "$DB_BUILD/dropbearmulti" "$TARGET_SYSROOT/bin/dropbearmulti"
echo "dropbearmulti installed to $TARGET_SYSROOT/bin/dropbearmulti"
