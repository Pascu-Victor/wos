#!/bin/bash
# Build CPIO newc initramfs archive containing /sbin/init, boot config, and
# any dynamic loader/runtime files needed before the rootfs is mounted.
# PARTUUIDs are auto-extracted from disk images defined in configs/disks.conf.
set -e

CWD=$(pwd)
BUILD_DIR="${WOS_BUILD_DIR:-build}"
SYSROOT_DIR="${WOS_SYSROOT_PATH:-toolchain/sysroot}"
INIT_BINARY="$BUILD_DIR/modules/init/init"
INITRAMFS_OUT="$BUILD_DIR/initramfs.cpio"
ROOTFS_DISK="${WOS_ROOTFS_DISK:-mountfs.qcow2}"
READELF="${WOS_READELF:-}"

# shellcheck source=scripts/build/qcow_common.sh
source "$CWD/scripts/build/qcow_common.sh"

if [ ! -f "$INIT_BINARY" ]; then
    echo "ERROR: init binary not found at $INIT_BINARY"
    echo "Run cmake build first."
    exit 1
fi

find_readelf() {
    if [ -n "$READELF" ] && command -v "$READELF" >/dev/null 2>&1; then
        return 0
    fi
    if [ -x "$CWD/toolchain/host/bin/llvm-readelf" ]; then
        READELF="$CWD/toolchain/host/bin/llvm-readelf"
        return 0
    fi
    if command -v llvm-readelf >/dev/null 2>&1; then
        READELF="llvm-readelf"
        return 0
    fi
    if command -v readelf >/dev/null 2>&1; then
        READELF="readelf"
        return 0
    fi
    return 1
}

elf_has_interp() {
    find_readelf || return 1
    "$READELF" -l "$1" | grep -q 'Requesting program interpreter:'
}

elf_needed_libraries() {
    "$READELF" -d "$1" | sed -n 's/.*Shared library: \[\(.*\)\].*/\1/p'
}

resolve_runtime_library() {
    local needed="$1"
    local base

    base=$(basename "$needed")

    if [[ "$needed" = /* && -f "$needed" ]]; then
        printf '%s\n' "$needed"
        return 0
    fi
    if [ -f "$SYSROOT_DIR/$needed" ]; then
        printf '%s\n' "$SYSROOT_DIR/$needed"
        return 0
    fi
    if [ -f "$SYSROOT_DIR/lib/$needed" ]; then
        printf '%s\n' "$SYSROOT_DIR/lib/$needed"
        return 0
    fi
    if [ -f "$SYSROOT_DIR/lib/$base" ]; then
        printf '%s\n' "$SYSROOT_DIR/lib/$base"
        return 0
    fi
    if [ -f "$BUILD_DIR/modules/journal/$base" ]; then
        printf '%s\n' "$BUILD_DIR/modules/journal/$base"
        return 0
    fi

    return 1
}

declare -A STAGED_RUNTIME_LIBS=()

stage_runtime_file() {
    local source="$1"
    local target="$2"
    local size

    mkdir -p "$INITRAMFS_DIR$(dirname "$target")"
    cp -a "$source" "$INITRAMFS_DIR$target"
    size=$(du -h "$source" | cut -f1)
    echo "  initramfs: added $target ($size)"
}

stage_shared_object() {
    local needed="$1"
    local source
    local target

    source=$(resolve_runtime_library "$needed") || {
        echo "ERROR: initramfs runtime dependency '$needed' not found in $SYSROOT_DIR/lib" >&2
        exit 1
    }

    target="/lib/$(basename "$source")"
    if [ -n "${STAGED_RUNTIME_LIBS[$target]:-}" ]; then
        return 0
    fi
    STAGED_RUNTIME_LIBS[$target]=1
    stage_runtime_file "$source" "$target"

    while IFS= read -r child; do
        [ -n "$child" ] || continue
        stage_shared_object "$child"
    done < <(elf_needed_libraries "$source")
}

stage_init_dynamic_runtime() {
    local interp_source
    local needed

    if ! elf_has_interp "$INIT_BINARY"; then
        return 0
    fi

    interp_source=$(resolve_runtime_library "ld.so") || {
        echo "ERROR: init is dynamically linked but $SYSROOT_DIR/lib/ld.so was not found" >&2
        exit 1
    }
    STAGED_RUNTIME_LIBS["/lib/ld.so"]=1
    stage_runtime_file "$interp_source" "/lib/ld.so"

    while IFS= read -r needed; do
        [ -n "$needed" ] || continue
        stage_shared_object "$needed"
    done < <(elf_needed_libraries "$INIT_BINARY")
}

sync_rootfs_etc_tables() {
    if [ ! -f "$ROOTFS_DISK" ]; then
        echo "  rootfs: $ROOTFS_DISK not found, skipping /etc/fstab and /etc/vfstab sync"
        return 0
    fi

    wos_qcow_validate_for_update "$ROOTFS_DISK" "sync initramfs /etc tables into rootfs qcow image"
    wos_qcow_guestfish "sync initramfs /etc tables into rootfs qcow image" "$ROOTFS_DISK" --rw -a "$ROOTFS_DISK" <<EOF
run
mount /dev/sda1 /
mkdir-p /etc
upload $INITRAMFS_DIR/etc/fstab /etc/fstab
upload $INITRAMFS_DIR/etc/vfstab /etc/vfstab
sync
umount /
EOF
    echo "  rootfs: synced /etc/fstab and /etc/vfstab into $ROOTFS_DISK"
}

# Create temporary directory for initramfs contents
INITRAMFS_DIR=$(mktemp -d)
trap 'rm -rf "$INITRAMFS_DIR"' EXIT

# Create directory structure
mkdir -p "$INITRAMFS_DIR/sbin"
mkdir -p "$INITRAMFS_DIR/etc"
mkdir -p "$INITRAMFS_DIR/lib"

# Copy init binary and, when needed, the dynamic loader/runtime closure.
cp "$INIT_BINARY" "$INITRAMFS_DIR/sbin/init"
echo "  initramfs: added /sbin/init ($(du -h "$INIT_BINARY" | cut -f1))"
stage_init_dynamic_runtime

# Generate /etc/hostname from system configuration
if [ -f "configs/system.conf" ]; then
    source "configs/system.conf"
    echo -n "${WOS_HOSTNAME:-wos}" > "$INITRAMFS_DIR/etc/hostname"
    echo "  initramfs: added /etc/hostname (${WOS_HOSTNAME:-wos})"
else
    echo -n "wos" > "$INITRAMFS_DIR/etc/hostname"
    echo "  initramfs: added /etc/hostname (wos) [default, no system.conf]"
fi

# Generate /etc/fstab from disk configuration
if [ -f "configs/disks.conf" ]; then
    source "configs/disks.conf"
    echo "  initramfs: generating /etc/fstab from configs/disks.conf"
    generate_fstab "$INITRAMFS_DIR/etc/fstab"
    echo "  initramfs: /etc/fstab contents:"
    cat "$INITRAMFS_DIR/etc/fstab" | sed 's/^/    /'
else
    echo "WARNING: configs/disks.conf not found, creating empty fstab"
    echo "# /etc/fstab - empty (no disks.conf found)" > "$INITRAMFS_DIR/etc/fstab"
fi

if [ -f "configs/netdevs.conf" ]; then
    cp "configs/netdevs.conf" "$INITRAMFS_DIR/etc/netdevs"
    echo "  initramfs: added /etc/netdevs from configs/netdevs.conf"
else
    echo "WARNING: configs/netdevs.conf not found, network device assignment will use hardcoded defaults"
fi

if [ -f "configs/vfstab" ]; then
    cp "configs/vfstab" "$INITRAMFS_DIR/etc/vfstab"
    echo "  initramfs: added /etc/vfstab from configs/vfstab"
else
    cat > "$INITRAMFS_DIR/etc/vfstab" <<'EOF'
# prefix route
/wki local
/proc local
/dev local
/tmp local
/run local
/ host
EOF
    echo "  initramfs: added default /etc/vfstab"
fi

sync_rootfs_etc_tables

# Create CPIO newc archive
mkdir -p "$(dirname "$INITRAMFS_OUT")"
(cd "$INITRAMFS_DIR" && find . | cpio -o -H newc --quiet) > "$INITRAMFS_OUT"

echo "  initramfs: created $INITRAMFS_OUT ($(du -h "$INITRAMFS_OUT" | cut -f1))"
