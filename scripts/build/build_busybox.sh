#!/bin/bash
# Incrementally rebuild busybox for WOS and install into the sysroot.
# Expects the toolchain to already be bootstrapped (tools/bootstrap.sh).
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$WORKSPACE_ROOT/tools/ccache_env.sh"
wos_setup_ccache
WOS_CCACHE_PREFIX="$(wos_ccache_prefix)"

B=$(pwd)/toolchain
HOST="$B/host"
TARGET_SYSROOT="${WOS_SYSROOT_PATH:-$B/sysroot}"
BB_BUILD="${WOS_BUSYBOX_BUILD_DIR:-$B/busybox-build}"
BB_INSTALL="${WOS_BUSYBOX_INSTALL_DIR:-$B/busybox-install}"
BB_SHARED_DIR="$BB_BUILD/0_lib"

BB_SRC="$B/src/busybox"
if [ ! -d "$BB_SRC" ]; then
    echo "ERROR: busybox source directory not found at $BB_SRC"
    echo "Run tools/bootstrap.sh first to bootstrap sources."
    exit 1
fi
mkdir -p "$BB_BUILD"

# Cross-compilation variables - host tools, target sysroot
BB_CC="${WOS_CCACHE_PREFIX}$HOST/bin/clang --target=x86_64-pc-wos --sysroot=$TARGET_SYSROOT"
BB_AR="$HOST/bin/llvm-ar"
BB_STRIP="$HOST/bin/llvm-strip"
BB_RANLIB="$HOST/bin/llvm-ranlib"
BB_OBJCOPY="$HOST/bin/llvm-objcopy"
BB_NM="$HOST/bin/llvm-nm"
BB_HOSTCC="${WOS_CCACHE_PREFIX}gcc"
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

busybox_config_enabled() {
    local sym="$1"
    grep -q "^${sym}=y$" "$BB_BUILD/.config"
}

# Re-apply config from wos_defconfig to stay in sync.
if [ -f "$BB_SRC/configs/wos_defconfig" ]; then
    rm -f "$BB_BUILD/.config"
    if ! make -C "$BB_SRC" O="$BB_BUILD" \
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

    merge_busybox_config "$BB_BUILD/.config" "$BB_SRC/configs/wos_defconfig"

    if ! yes "" | make -C "$BB_SRC" O="$BB_BUILD" \
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
if [ -f "$BB_BUILD/busybox" ]; then
    for lib in "$TARGET_SYSROOT"/lib/libc.so "$TARGET_SYSROOT"/lib/libc++.so \
               "$TARGET_SYSROOT"/lib/libc++abi.so "$TARGET_SYSROOT"/lib/libm.so; do
        if [ -f "$lib" ] && [ "$lib" -nt "$BB_BUILD/busybox" ]; then
            echo "Sysroot library $(basename "$lib") changed - forcing relink"
            rm -f "$BB_BUILD/busybox"
            break
        fi
    done
fi

# Build busybox
make -C "$BB_BUILD" -j"$(nproc)" \
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
if ! make -C "$BB_BUILD" \
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

# Always stage the freshly linked top-level BusyBox binary. When shared BusyBox
# was enabled in an earlier build, 0_lib/ can keep stale launcher artifacts
# around even after the config flips back to monolithic mode.
if [ -f "$BB_BUILD/busybox" ]; then
    install -m 755 "$BB_BUILD/busybox" "$BB_INSTALL/bin/busybox"
fi

shared_lib=""
if busybox_config_enabled CONFIG_FEATURE_SHARED_BUSYBOX; then
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
fi

if [ -n "$shared_lib" ]; then
    mkdir -p "$BB_INSTALL/lib"
    rm -f "$BB_INSTALL/lib"/libbusybox.so*
    cp -pPR "$shared_lib" "$BB_INSTALL/lib/"
    ln -sfn "$(basename "$shared_lib")" "$BB_INSTALL/lib/libbusybox.so"
else
    rm -rf "$BB_INSTALL/lib"
fi

echo "busybox installed to $BB_INSTALL"
