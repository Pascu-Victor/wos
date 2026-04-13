#!/bin/bash
# Build CPIO newc initramfs archive containing only /sbin/init and boot config.
# Everything else lives on the rootfs (mountfs.qcow2).
# PARTUUIDs are auto-extracted from disk images defined in configs/disks.conf.
set -e

CWD=$(pwd)
INIT_BINARY="build/modules/init/init"
INITRAMFS_OUT="build/initramfs.cpio"

if [ ! -f "$INIT_BINARY" ]; then
    echo "ERROR: init binary not found at $INIT_BINARY"
    echo "Run cmake build first."
    exit 1
fi

# Create temporary directory for initramfs contents
INITRAMFS_DIR=$(mktemp -d)
trap 'rm -rf "$INITRAMFS_DIR"' EXIT

# Create directory structure
mkdir -p "$INITRAMFS_DIR/sbin"
mkdir -p "$INITRAMFS_DIR/etc"

# Copy init binary (statically linked — the only binary in initramfs)
cp "$INIT_BINARY" "$INITRAMFS_DIR/sbin/init"
echo "  initramfs: added /sbin/init ($(du -h "$INIT_BINARY" | cut -f1))"

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

# Create CPIO newc archive
(cd "$INITRAMFS_DIR" && find . | cpio -o -H newc --quiet) > "$INITRAMFS_OUT"

echo "  initramfs: created $INITRAMFS_OUT ($(du -h "$INITRAMFS_OUT" | cut -f1))"
