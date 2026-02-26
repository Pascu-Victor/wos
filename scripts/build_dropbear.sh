#!/bin/bash
# Incrementally rebuild Dropbear SSH for WOS and install into the sysroot.
# Expects the toolchain to already be bootstrapped (build-llvm.sh step 9).
set -e

B=$(pwd)/toolchain
TARGET_SYSROOT="$B/target1"
DB_SRC="$B/src/dropbear"
DB_BUILD="$B/dropbear-build"

if [ ! -d "$DB_SRC" ]; then
    echo "ERROR: dropbear source directory not found at $DB_SRC"
    echo "Run tools/build-llvm.sh first to bootstrap the toolchain."
    exit 1
fi

# Cross-compilation environment
export CC="$TARGET_SYSROOT/bin/clang --target=x86_64-pc-wos --sysroot=$TARGET_SYSROOT"
export AR="$TARGET_SYSROOT/bin/llvm-ar"
export RANLIB="$TARGET_SYSROOT/bin/llvm-ranlib"
export STRIP="$TARGET_SYSROOT/bin/llvm-strip"
export CFLAGS="--sysroot=$TARGET_SYSROOT -static -g -O0 -fno-sanitize=safe-stack -fno-stack-protector -I$TARGET_SYSROOT/include"
export LDFLAGS="--sysroot=$TARGET_SYSROOT -static -fuse-ld=lld -L$TARGET_SYSROOT/lib -Wl,--whole-archive,-lc,--no-whole-archive"

mkdir -p "$DB_BUILD"

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
        --enable-static \
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
    for lib in "$TARGET_SYSROOT"/lib/libc.a "$TARGET_SYSROOT"/lib/libc++.a \
               "$TARGET_SYSROOT"/lib/libc++abi.a "$TARGET_SYSROOT"/lib/libm.a; do
        if [ -f "$lib" ] && [ "$lib" -nt "$DB_BUILD/dropbearmulti" ]; then
            echo "Sysroot library $(basename "$lib") changed — forcing relink"
            rm -f "$DB_BUILD/dropbearmulti"
            break
        fi
    done
fi

# Build dropbearmulti (combined binary like busybox)
make -C "$DB_BUILD" -j"$(nproc)" PROGRAMS="dropbear dbclient dropbearkey scp" MULTI=1 dropbearmulti

# Install into sysroot
cp "$DB_BUILD/dropbearmulti" "$TARGET_SYSROOT/bin/dropbearmulti"
echo "dropbearmulti installed to $TARGET_SYSROOT/bin/dropbearmulti"
