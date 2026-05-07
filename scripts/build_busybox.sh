#!/bin/bash
# Incrementally rebuild busybox for WOS and install into the sysroot.
# Expects the toolchain to already be bootstrapped (tools/bootstrap.sh).
set -e

B=$(pwd)/toolchain
HOST="$B/host"
TARGET_SYSROOT="$B/sysroot"
BB_INSTALL="$B/busybox-install"
BB_SHARED_DIR="$B/busybox-build/0_lib"

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
BB_CFLAGS="--sysroot=$TARGET_SYSROOT -fPIC -fno-sanitize=safe-stack -fno-stack-protector -Wno-string-plus-int"
BB_LDFLAGS="--sysroot=$TARGET_SYSROOT -fuse-ld=lld"

merge_busybox_config() {
    local config="$1"
    local overlay="$2"
    local line sym val

    while IFS= read -r line || [ -n "$line" ]; do
        case "$line" in
            "")
                continue
                ;;
            \#\ CONFIG_*\ is\ not\ set)
                sym="${line#\# CONFIG_}"
                sym="${sym% is not set}"
                if grep -qE "^CONFIG_${sym}=|^# CONFIG_${sym} is not set$" "$config"; then
                    sed -i -E "s|^CONFIG_${sym}=.*$|# CONFIG_${sym} is not set|; s|^# CONFIG_${sym} is not set$|# CONFIG_${sym} is not set|" "$config"
                else
                    printf '# CONFIG_%s is not set\n' "$sym" >> "$config"
                fi
                ;;
            \#*)
                continue
                ;;
            CONFIG_*=*)
                sym="${line%%=*}"
                val="${line#*=}"
                if grep -qE "^${sym}=|^# ${sym} is not set$" "$config"; then
                    sed -i -E "s|^${sym}=.*$|${sym}=${val}|; s|^# ${sym} is not set$|${sym}=${val}|" "$config"
                else
                    printf '%s=%s\n' "$sym" "$val" >> "$config"
                fi
                ;;
        esac
    done < "$overlay"
}

# Re-apply config from wos_defconfig to stay in sync.
BB_SRC="$B/src/busybox"
if [ -f "$BB_SRC/configs/wos_defconfig" ]; then
    rm -f "$B/busybox-build/.config"
    if ! make -C "$BB_SRC" O="$B/busybox-build" \
        CC="$BB_CC" \
        AR="$BB_AR" \
        STRIP="$BB_STRIP" \
        RANLIB="$BB_RANLIB" \
        OBJCOPY="$BB_OBJCOPY" \
        NM="$BB_NM" \
        HOSTCC="$BB_HOSTCC" \
        CFLAGS="$BB_CFLAGS" \
        LDFLAGS="$BB_LDFLAGS" \
        allnoconfig >/tmp/busybox_allnoconfig.log 2>&1; then
        cat /tmp/busybox_allnoconfig.log
        exit 1
    fi

    merge_busybox_config "$B/busybox-build/.config" "$BB_SRC/configs/wos_defconfig"

    if ! yes "" | make -C "$BB_SRC" O="$B/busybox-build" \
        CC="$BB_CC" \
        AR="$BB_AR" \
        STRIP="$BB_STRIP" \
        RANLIB="$BB_RANLIB" \
        OBJCOPY="$BB_OBJCOPY" \
        NM="$BB_NM" \
        HOSTCC="$BB_HOSTCC" \
        CFLAGS="$BB_CFLAGS" \
        LDFLAGS="$BB_LDFLAGS" \
        oldconfig >/tmp/busybox_oldconfig.log 2>&1; then
        cat /tmp/busybox_oldconfig.log
        exit 1
    fi
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

# Install the generated BusyBox runtime tree for rootfs packaging.
rm -rf "$BB_INSTALL"
mkdir -p "$BB_INSTALL"
if ! make -C "$B/busybox-build" \
    CC="$BB_CC" \
    AR="$BB_AR" \
    STRIP="$BB_STRIP" \
    RANLIB="$BB_RANLIB" \
    OBJCOPY="$BB_OBJCOPY" \
    NM="$BB_NM" \
    HOSTCC="$BB_HOSTCC" \
    CFLAGS="$BB_CFLAGS" \
    LDFLAGS="$BB_LDFLAGS" \
    CONFIG_PREFIX="$BB_INSTALL" \
    install >/tmp/busybox_install.log 2>&1; then
    cat /tmp/busybox_install.log
    exit 1
fi

if [ -f "$BB_SHARED_DIR/busybox" ]; then
    install -m 755 "$BB_SHARED_DIR/busybox" "$BB_INSTALL/bin/busybox"
fi

shared_lib=""
for candidate in "$BB_SHARED_DIR"/libbusybox.so.*; do
    case "$candidate" in
        *"_unstripped"*|*.map|*.out)
            continue
            ;;
    esac
    if [ -f "$candidate" ]; then
        shared_lib="$candidate"
        break
    fi
done

if [ -n "$shared_lib" ]; then
    mkdir -p "$BB_INSTALL/lib"
    rm -f "$BB_INSTALL/lib"/libbusybox.so*
    cp -pPR "$shared_lib" "$BB_INSTALL/lib/"
    ln -sfn "$(basename "$shared_lib")" "$BB_INSTALL/lib/libbusybox.so"
fi

echo "busybox installed to $BB_INSTALL"
