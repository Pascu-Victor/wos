#!/bin/bash
# Sync changed files into an existing mountfs.qcow2 rootfs image.
# Uses guestfish copy-in to delta-update only modified files.
# If mountfs.qcow2 doesn't exist, exit with error (run create_mountfs_disk.sh first).
set -e

CWD=$(pwd)
DISK="mountfs.qcow2"

if [ ! -f "$DISK" ]; then
    echo "ERROR: $DISK does not exist. Run scripts/create_mountfs_disk.sh first."
    exit 1
fi

echo "Syncing rootfs delta into $DISK..."

# Build a staging directory with only the files that need updating
STAGING=$(mktemp -d)
trap 'rm -rf "$STAGING"' EXIT

CHANGED=0

# --- /usr/lib/ — shared libraries from sysroot ---
mkdir -p "$STAGING/usr/lib"
SYSROOT_LIB="$CWD/toolchain/sysroot/lib"
if [ -d "$SYSROOT_LIB" ]; then
    for f in "$SYSROOT_LIB"/*.so "$SYSROOT_LIB"/*.so.* "$SYSROOT_LIB"/crt*.o "$SYSROOT_LIB"/ld.so; do
        [ -e "$f" ] && cp -P "$f" "$STAGING/usr/lib/" && CHANGED=1
    done
fi

# --- /usr/bin/ — userspace binaries ---
mkdir -p "$STAGING/usr/bin"
for binary in \
    "$CWD/build/modules/testprog/testprog" \
    "$CWD/build/modules/perf/perf"; do
    if [ -f "$binary" ]; then
        cp "$binary" "$STAGING/usr/bin/"
        CHANGED=1
    fi
done

# busybox
BUSYBOX_BINARY="$CWD/toolchain/sysroot/bin/busybox"
if [ -f "$BUSYBOX_BINARY" ]; then
    cp "$BUSYBOX_BINARY" "$STAGING/usr/bin/busybox"
    chmod +x "$STAGING/usr/bin/busybox"
    CHANGED=1
fi

# dropbearmulti
DROPBEAR_BINARY="$CWD/toolchain/sysroot/bin/dropbearmulti"
if [ -f "$DROPBEAR_BINARY" ]; then
    cp "$DROPBEAR_BINARY" "$STAGING/usr/bin/dropbearmulti"
    chmod +x "$STAGING/usr/bin/dropbearmulti"
    CHANGED=1
fi

# --- /usr/sbin/ — daemons ---
mkdir -p "$STAGING/usr/sbin"
for binary in \
    "$CWD/build/modules/httpd/httpd" \
    "$CWD/build/modules/netd/netd"; do
    if [ -f "$binary" ]; then
        cp "$binary" "$STAGING/usr/sbin/$(basename "$binary")"
        CHANGED=1
    fi
done

# --- /srv/ — web content ---
mkdir -p "$STAGING/srv"
if [ -d "$CWD/configs/drive/srv" ]; then
    cp -r "$CWD/configs/drive/srv/"* "$STAGING/srv/" 2>/dev/null && CHANGED=1 || true
fi

# --- /etc/ config files ---
mkdir -p "$STAGING/etc"
cat > "$STAGING/etc/passwd" <<'PASSWD'
root:x:0:0:root:/root:/bin/sh
PASSWD

cat > "$STAGING/etc/group" <<'GROUP'
root:x:0:root
GROUP

cat > "$STAGING/etc/profile" <<'PROFILE'
export USER="${USER:-root}"
HOSTNAME=$(uname -n 2>/dev/null) || { read -r HOSTNAME < /etc/hostname 2>/dev/null || HOSTNAME="wos"; }
export HOSTNAME
export HOME="${HOME:-/root}"
export PS1="$USER@$HOSTNAME:\w\$ "
export PATH="/bin:/sbin:/usr/bin:/usr/sbin"
export ENV="/etc/profile"
PROFILE

cat > "$STAGING/etc/filesystems" <<'EOF'
fat32
vfat
tmpfs
EOF
CHANGED=1

# --- /root/.ssh/ ---
mkdir -p "$STAGING/root/.ssh"
SSH_PUBKEY=""
for keyfile in ~/.ssh/id_ed25519.pub ~/.ssh/id_rsa.pub ~/.ssh/id_ecdsa.pub; do
    if [ -f "$keyfile" ]; then
        SSH_PUBKEY="$keyfile"
        break
    fi
done
if [ -n "$SSH_PUBKEY" ]; then
    cp "$SSH_PUBKEY" "$STAGING/root/.ssh/authorized_keys"
    chmod 600 "$STAGING/root/.ssh/authorized_keys"
fi

if [ "$CHANGED" -eq 0 ]; then
    echo "  rootfs sync: nothing to update"
    exit 0
fi

# Stage a tarball with root:root ownership and correct permissions
chmod 700 "$STAGING/root/.ssh"
test -f "$STAGING/root/.ssh/authorized_keys" && chmod 600 "$STAGING/root/.ssh/authorized_keys"
fakeroot sh -c "chown -R 0:0 '$STAGING' && tar cf '$STAGING.tar' --numeric-owner -C '$STAGING' ."

# Use guestfish to copy updated files into the existing disk
guestfish --rw -a "$DISK" <<_EOF_
run
mount /dev/sda1 /
tar-in $STAGING.tar /
sync
_EOF_

rm -f "$STAGING.tar"

echo "  rootfs sync: delta update complete"
