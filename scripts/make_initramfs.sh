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

# Create /etc/filesystems for busybox mount auto-detection
cat > "$INITRAMFS_DIR/etc/filesystems" <<'EOF'
fat32
vfat
tmpfs
EOF
echo "  initramfs: added /etc/filesystems"

# Copy busybox binary and create applet symlinks
BUSYBOX_BINARY="toolchain/target1/bin/busybox"
if [ -f "$BUSYBOX_BINARY" ]; then
    mkdir -p "$INITRAMFS_DIR/bin"
    cp "$BUSYBOX_BINARY" "$INITRAMFS_DIR/bin/busybox"
    chmod +x "$INITRAMFS_DIR/bin/busybox"
    echo "  initramfs: added /bin/busybox ($(du -h "$BUSYBOX_BINARY" | cut -f1))"

    # Create symlinks for enabled applets
    BUSYBOX_APPLETS="mount umount ls cat echo mkdir cp mv rm grep find ps df ifconfig sh"
    for applet in $BUSYBOX_APPLETS; do
        ln -sf busybox "$INITRAMFS_DIR/bin/$applet"
        echo "  initramfs: symlinked /bin/$applet -> busybox"
    done
else
    echo "WARNING: busybox binary not found at $BUSYBOX_BINARY, skipping"
fi

# Copy dropbearmulti binary and create symlinks
DROPBEAR_BINARY="toolchain/target1/bin/dropbearmulti"
if [ -f "$DROPBEAR_BINARY" ]; then
    mkdir -p "$INITRAMFS_DIR/bin"
    mkdir -p "$INITRAMFS_DIR/etc/dropbear"
    mkdir -p "$INITRAMFS_DIR/dev/pts"
    cp "$DROPBEAR_BINARY" "$INITRAMFS_DIR/bin/dropbearmulti"
    chmod +x "$INITRAMFS_DIR/bin/dropbearmulti"
    echo "  initramfs: added /bin/dropbearmulti ($(du -h "$DROPBEAR_BINARY" | cut -f1))"

    # Create symlinks for dropbear applets
    DROPBEAR_APPLETS="dropbear dbclient dropbearkey scp"
    for applet in $DROPBEAR_APPLETS; do
        ln -sf dropbearmulti "$INITRAMFS_DIR/bin/$applet"
        echo "  initramfs: symlinked /bin/$applet -> dropbearmulti"
    done

    # Create minimal /etc/passwd for SSH login (key-based auth only)
    cat > "$INITRAMFS_DIR/etc/passwd" <<'PASSWD'
root:x:0:0:root:/:/bin/sh
PASSWD
    echo "  initramfs: added /etc/passwd"

    # Create minimal /etc/group
    cat > "$INITRAMFS_DIR/etc/group" <<'GROUP'
root:x:0:root
GROUP
    echo "  initramfs: added /etc/group"

    # Install SSH authorized_keys for root (home is /)
    # Try ed25519 first, then RSA
    SSH_PUBKEY=""
    for keyfile in ~/.ssh/id_ed25519.pub ~/.ssh/id_rsa.pub ~/.ssh/id_ecdsa.pub; do
        if [ -f "$keyfile" ]; then
            SSH_PUBKEY="$keyfile"
            break
        fi
    done
    if [ -n "$SSH_PUBKEY" ]; then
        mkdir -p "$INITRAMFS_DIR/.ssh"
        chmod 700 "$INITRAMFS_DIR/.ssh"
        cp "$SSH_PUBKEY" "$INITRAMFS_DIR/.ssh/authorized_keys"
        chmod 600 "$INITRAMFS_DIR/.ssh/authorized_keys"
        echo "  initramfs: added /.ssh/authorized_keys from $SSH_PUBKEY"
    else
        echo "WARNING: no SSH public key found, skipping authorized_keys"
    fi
else
    echo "WARNING: dropbearmulti binary not found at $DROPBEAR_BINARY, skipping"
fi

# Create CPIO newc archive
(cd "$INITRAMFS_DIR" && find . | cpio -o -H newc --quiet) > "$INITRAMFS_OUT"

echo "  initramfs: created $INITRAMFS_OUT ($(du -h "$INITRAMFS_OUT" | cut -f1))"
