#!/bin/bash
# Create the mountfs (rootfs) disk image with XFS filesystem.
# Uses usr-merge layout: real dirs under /usr/, compat symlinks at /.
#
# Layout:
#   /usr/lib/       ← shared libraries (libc.so, libc++.so, ld.so, etc.) + CRT objects
#   /usr/bin/       ← busybox + applets, dropbearmulti + applets, perf, testprog
#   /usr/sbin/      ← httpd, netd
#   /lib -> /usr/lib
#   /bin -> /usr/bin
#   /sbin -> /usr/sbin
#   /root/          ← root user home directory
#   /root/.ssh/     ← SSH authorized_keys
#   /home/          ← future user directories
#   /etc/           ← passwd, group, profile, filesystems, hostname, dropbear/
#   /srv/           ← web content + test data
#   /dev/pts/       ← pseudo-terminal directory
#   /tmp/
#   /run/
#   /oldroot/       ← pivot_root put_old target (empty)

set -e

CWD=$(pwd)
DISK="mountfs.qcow2"

if [ -e "$DISK" ]; then
    echo "Removing existing $DISK..."
    rm "$DISK"
fi

# Create disk
DISK_SIZE="4G"
echo "Creating QCOW2 image ($DISK_SIZE)"
qemu-img create -f qcow2 "$DISK" "$DISK_SIZE"

PART_START_SECTOR=2048
PART_SIZE_MIB=3500
SECTOR_SIZE=512
PART_END_SECTOR=$((PART_START_SECTOR + (PART_SIZE_MIB * 1024 * 1024 / SECTOR_SIZE) - 1))

# Build a staging directory with the rootfs layout
STAGING=$(mktemp -d)
trap 'rm -rf "$STAGING"' EXIT

# --- /usr/lib/ — shared libraries from sysroot ---
mkdir -p "$STAGING/usr/lib"
SYSROOT_LIB="$CWD/toolchain/sysroot/lib"
if [ -d "$SYSROOT_LIB" ]; then
    # Copy all .so files, CRT objects, and the dynamic linker
    for f in "$SYSROOT_LIB"/*.so "$SYSROOT_LIB"/*.so.* "$SYSROOT_LIB"/crt*.o "$SYSROOT_LIB"/ld.so; do
        [ -e "$f" ] && cp -P "$f" "$STAGING/usr/lib/"
    done
    echo "  rootfs: added /usr/lib/ libraries from sysroot"
fi

# --- /usr/bin/ — userspace binaries ---
mkdir -p "$STAGING/usr/bin"

# testprog
if [ -f "$CWD/build/modules/testprog/testprog" ]; then
    cp "$CWD/build/modules/testprog/testprog" "$STAGING/usr/bin/testprog"
    echo "  rootfs: added /usr/bin/testprog"
fi

# perf
if [ -f "$CWD/build/modules/perf/perf" ]; then
    cp "$CWD/build/modules/perf/perf" "$STAGING/usr/bin/perf"
    echo "  rootfs: added /usr/bin/perf"
fi

# busybox + applet symlinks
BUSYBOX_BINARY="$CWD/toolchain/sysroot/bin/busybox"
if [ -f "$BUSYBOX_BINARY" ]; then
    cp "$BUSYBOX_BINARY" "$STAGING/usr/bin/busybox"
    chmod +x "$STAGING/usr/bin/busybox"
    BUSYBOX_APPLETS="yes whoami wc uniq uname umount true tr touch time test tee tail stat sort sleep sha256sum sh seq rmdir rm realpath readlink pwd ps printf mv mount mkdir md5sum ls ln ifconfig id head grep find false env echo du dirname df dd date cut cp clear chown chmod cat basename"
    for applet in $BUSYBOX_APPLETS; do
        ln -sf busybox "$STAGING/usr/bin/$applet"
    done
    echo "  rootfs: added /usr/bin/busybox + applet symlinks"
fi

# dropbearmulti + symlinks
DROPBEAR_BINARY="$CWD/toolchain/sysroot/bin/dropbearmulti"
if [ -f "$DROPBEAR_BINARY" ]; then
    cp "$DROPBEAR_BINARY" "$STAGING/usr/bin/dropbearmulti"
    chmod +x "$STAGING/usr/bin/dropbearmulti"
    for applet in dropbear dbclient dropbearkey scp; do
        ln -sf dropbearmulti "$STAGING/usr/bin/$applet"
    done
    echo "  rootfs: added /usr/bin/dropbearmulti + applet symlinks"
fi

# --- /usr/sbin/ — system daemons ---
mkdir -p "$STAGING/usr/sbin"
if [ -f "$CWD/build/modules/httpd/httpd" ]; then
    cp "$CWD/build/modules/httpd/httpd" "$STAGING/usr/sbin/httpd"
    echo "  rootfs: added /usr/sbin/httpd"
fi
if [ -f "$CWD/build/modules/netd/netd" ]; then
    cp "$CWD/build/modules/netd/netd" "$STAGING/usr/sbin/netd"
    echo "  rootfs: added /usr/sbin/netd"
fi

# --- usr-merge compat symlinks ---
ln -sf usr/lib  "$STAGING/lib"
ln -sf usr/bin  "$STAGING/bin"
ln -sf usr/sbin "$STAGING/sbin"
echo "  rootfs: created usr-merge symlinks (/lib, /bin, /sbin -> /usr/...)"

# --- /root/ — root home directory ---
mkdir -p "$STAGING/root/.ssh"
chmod 700 "$STAGING/root/.ssh"
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
    echo "  rootfs: added /root/.ssh/authorized_keys from $SSH_PUBKEY"
fi

# --- /home/ ---
mkdir -p "$STAGING/home"

# --- /etc/ ---
mkdir -p "$STAGING/etc/dropbear"

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

# /etc/hostname (default — overridden by cluster overlay per-node)
if [ -f "$CWD/configs/system.conf" ]; then
    source "$CWD/configs/system.conf"
    echo -n "${WOS_HOSTNAME:-wos}" > "$STAGING/etc/hostname"
else
    echo -n "wos" > "$STAGING/etc/hostname"
fi
echo "  rootfs: added /etc/ config files"

# --- /srv/ — web content + test data ---
mkdir -p "$STAGING/srv"
if [ -d "$CWD/configs/drive/srv" ]; then
    cp -r "$CWD/configs/drive/srv/"* "$STAGING/srv/" 2>/dev/null || true
fi
echo "Hello from XFS filesystem!" > "$STAGING/srv/hello.txt"
printf "Binary test data 1234567890ABCDEF" > "$STAGING/srv/test.bin"
echo "  rootfs: added /srv/ content"

# --- misc dirs ---
mkdir -p "$STAGING/dev/pts"
mkdir -p "$STAGING/tmp"
mkdir -p "$STAGING/run"
mkdir -p "$STAGING/oldroot"

# Stage a tarball with root:root ownership and correct permissions
chmod 700 "$STAGING/root/.ssh"
test -f "$STAGING/root/.ssh/authorized_keys" && chmod 600 "$STAGING/root/.ssh/authorized_keys"
fakeroot sh -c "chown -R 0:0 '$STAGING' && tar cf '$STAGING.tar' --numeric-owner -C '$STAGING' ."

echo "Creating GPT partition and XFS filesystem"
guestfish --rw -a "$DISK" <<_EOF_
run
part-init /dev/sda gpt
part-add /dev/sda p $PART_START_SECTOR $PART_END_SECTOR
mkfs xfs /dev/sda1
sync
mount /dev/sda1 /
tar-in $STAGING.tar /
mkdir-p /oldroot
ln-sf /usr/lib /lib
ln-sf /usr/bin /bin
ln-sf /usr/sbin /sbin
sync
_EOF_

rm -f "$STAGING.tar"

echo ""
echo "XFS rootfs disk created successfully: $DISK"
echo "Contents:"

guestfish --ro -a "$DISK" <<_EOF_
run
mount /dev/sda1 /
ls /
echo "--- /usr/lib/ ---"
ls /usr/lib/
echo "--- /usr/bin/ ---"
ls /usr/bin/
echo "--- /usr/sbin/ ---"
ls /usr/sbin/
_EOF_
