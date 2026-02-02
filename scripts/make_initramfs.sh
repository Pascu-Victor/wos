#!/bin/bash
# Build CPIO newc initramfs archive containing /sbin/init and /etc/fstab.
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

# Copy init binary
cp "$INIT_BINARY" "$INITRAMFS_DIR/sbin/init"
echo "  initramfs: added /sbin/init ($(du -h "$INIT_BINARY" | cut -f1))"

# Copy netd binary (DHCP network daemon)
NETD_BINARY="build/modules/netd/netd"
if [ -f "$NETD_BINARY" ]; then
    cp "$NETD_BINARY" "$INITRAMFS_DIR/sbin/netd"
    echo "  initramfs: added /sbin/netd ($(du -h "$NETD_BINARY" | cut -f1))"
else
    echo "WARNING: netd binary not found at $NETD_BINARY, skipping"
fi

# Copy httpd binary (HTTP server)
HTTPD_BINARY="build/modules/httpd/httpd"
if [ -f "$HTTPD_BINARY" ]; then
    cp "$HTTPD_BINARY" "$INITRAMFS_DIR/sbin/httpd"
    echo "  initramfs: added /sbin/httpd ($(du -h "$HTTPD_BINARY" | cut -f1))"
else
    echo "WARNING: httpd binary not found at $HTTPD_BINARY, skipping"
fi

# Generate /etc/fstab from disk configuration
if [ -f "configs/disks.conf" ]; then
    # shellcheck source=configs/disks.conf
    source "configs/disks.conf"
    echo "  initramfs: generating /etc/fstab from configs/disks.conf"
    generate_fstab "$INITRAMFS_DIR/etc/fstab"
    echo "  initramfs: /etc/fstab contents:"
    cat "$INITRAMFS_DIR/etc/fstab" | sed 's/^/    /'
else
    echo "WARNING: configs/disks.conf not found, creating empty fstab"
    echo "# /etc/fstab - empty (no disks.conf found)" > "$INITRAMFS_DIR/etc/fstab"
fi

# Create CPIO newc archive
(cd "$INITRAMFS_DIR" && find . | cpio -o -H newc --quiet) > "$INITRAMFS_OUT"

echo "  initramfs: created $INITRAMFS_OUT ($(du -h "$INITRAMFS_OUT" | cut -f1))"
