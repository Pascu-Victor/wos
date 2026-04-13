#!/bin/bash
# Incrementally rebuild busybox for WOS and install into the sysroot.
# Expects the toolchain to already be bootstrapped (tools/bootstrap.sh).
set -e

B=$(pwd)/toolchain
HOST="$B/host"
TARGET_SYSROOT="$B/sysroot"

if [ ! -d "$B/busybox-build" ]; then
    echo "ERROR: busybox build directory not found at $B/busybox-build"
    echo "Run tools/bootstrap.sh first to bootstrap the toolchain."
    exit 1
fi

# Cross-compilation variables - host tools, target sysroot
BB_CC="$HOST/bin/clang --target=x86_64-pc-wos --sysroot=$TARGET_SYSROOT"
BB_AR="$HOST/bin/llvm-ar"
BB_STRIP="$HOST/bin/llvm-strip"
BB_RANLIB="$HOST/bin/llvm-ranlib"
BB_OBJCOPY="$HOST/bin/llvm-objcopy"
BB_NM="$HOST/bin/llvm-nm"
BB_HOSTCC="gcc"
BB_CFLAGS="--sysroot=$TARGET_SYSROOT -fno-sanitize=safe-stack -fno-stack-protector"
BB_LDFLAGS="--sysroot=$TARGET_SYSROOT -fuse-ld=lld -Wl,--no-dynamic-linker"

# Re-apply config from wos_defconfig to stay in sync.
# 1) Run allnoconfig to set everything to 'n'.
# 2) Merge wos_defconfig on top using a scripts/kconfig merge_config if
#    available, otherwise use sed-based approach.
BB_SRC="$B/src/busybox"
if [ -f "$BB_SRC/configs/wos_defconfig" ]; then
    # Step 1: allnoconfig - everything off
    make -C "$BB_SRC" O="$B/busybox-build" \
        CC="$BB_CC" \
        AR="$BB_AR" \
        STRIP="$BB_STRIP" \
        RANLIB="$BB_RANLIB" \
        OBJCOPY="$BB_OBJCOPY" \
        NM="$BB_NM" \
        HOSTCC="$BB_HOSTCC" \
        CFLAGS="$BB_CFLAGS" \
        LDFLAGS="$BB_LDFLAGS" \
        allnoconfig

    # Step 2: Apply wos_defconfig entries on top of .config
    while IFS= read -r line; do
        # Skip comments and blank lines
        [[ "$line" =~ ^#|^$ ]] && continue
        # Extract CONFIG_FOO=value
        key="${line%%=*}"
        if grep -q "^# $key is not set" "$B/busybox-build/.config" 2>/dev/null; then
            sed -i "s|^# $key is not set|$line|" "$B/busybox-build/.config"
        elif grep -q "^$key=" "$B/busybox-build/.config" 2>/dev/null; then
            sed -i "s|^$key=.*|$line|" "$B/busybox-build/.config"
        else
            echo "$line" >> "$B/busybox-build/.config"
        fi
    done < "$BB_SRC/configs/wos_defconfig"

    # Step 3: silentoldconfig to resolve any new dependencies
    yes "" | make -C "$BB_SRC" O="$B/busybox-build" \
        CC="$BB_CC" \
        AR="$BB_AR" \
        STRIP="$BB_STRIP" \
        RANLIB="$BB_RANLIB" \
        OBJCOPY="$BB_OBJCOPY" \
        NM="$BB_NM" \
        HOSTCC="$BB_HOSTCC" \
        CFLAGS="$BB_CFLAGS" \
        LDFLAGS="$BB_LDFLAGS" \
        oldconfig
fi

# Force relink if any sysroot library is newer than the binary
if [ -f "$B/busybox-build/busybox" ]; then
    for lib in "$TARGET_SYSROOT"/lib/libc.so "$TARGET_SYSROOT"/lib/libc++.so \
               "$TARGET_SYSROOT"/lib/libc++abi.so "$TARGET_SYSROOT"/lib/libm.so; do
        if [ -f "$lib" ] && [ "$lib" -nt "$B/busybox-build/busybox" ]; then
            echo "Sysroot library $(basename "$lib") changed - forcing relink"
            rm -f "$B/busybox-build/busybox"
            break
        fi
    done
fi

# Build busybox
make -C "$B/busybox-build" -j"$(nproc)" \
    CC="$BB_CC" \
    AR="$BB_AR" \
    STRIP="$BB_STRIP" \
    RANLIB="$BB_RANLIB" \
    OBJCOPY="$BB_OBJCOPY" \
    NM="$BB_NM" \
    HOSTCC="$BB_HOSTCC" \
    CFLAGS="$BB_CFLAGS" \
    LDFLAGS="$BB_LDFLAGS" \
    busybox

# Install into sysroot
mkdir -p "$TARGET_SYSROOT/bin"
cp "$B/busybox-build/busybox" "$TARGET_SYSROOT/bin/busybox"
echo "busybox installed to $TARGET_SYSROOT/bin/busybox"
