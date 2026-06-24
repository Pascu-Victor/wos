#!/bin/bash
# Create the mountfs (rootfs) disk image with XFS filesystem.
# Uses usr-merge layout: real dirs under /usr/, compat symlinks at /.
#
# Layout:
#   /usr/lib/       ← shared libraries (libc.so, libc++.so, ld.so, etc.) + CRT objects
#   /usr/include/   ← sysroot headers
#   /usr/bin/       ← busybox + applets, dropbearmulti + applets, perf, testprog, wkictl
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

WOS_ROOT="${WOS_WORKSPACE_ROOT:-$(git -C "$(dirname "$0")" rev-parse --show-toplevel)}"
cd "$WOS_ROOT"

CWD="$WOS_ROOT"
DISK="${WOS_ROOTFS_DISK:-mountfs.qcow2}"

# shellcheck disable=SC1091
source "$CWD/scripts/build/rootfs_common.sh"

wos_qcow_prepare_libguestfs_env

if [ -e "$DISK" ]; then
    wos_qcow_guard_replace "$DISK" "replace rootfs qcow image"
    echo "Removing existing $DISK..."
    rm "$DISK"
fi

# Create disk. Native WOS builds need enough space for the LLVM source tree,
# bootstrap toolchains, and CMake/Ninja build trees inside the guest.
DISK_SIZE="120G"
echo "Creating QCOW2 image ($DISK_SIZE)"
mkdir -p "$(dirname "$DISK")"
qemu-img create -f qcow2 "$DISK" "$DISK_SIZE"

PART_START_SECTOR=2048
PART_SIZE_MIB=102400
SECTOR_SIZE=512
PART_END_SECTOR=$((PART_START_SECTOR + (PART_SIZE_MIB * 1024 * 1024 / SECTOR_SIZE) - 1))

# Build a staging directory with the rootfs layout
STAGING=$(mktemp -d)
STAGING_TAR="$STAGING.tar"
trap 'rm -rf "$STAGING"; rm -f "$STAGING_TAR"; wos_qcow_cleanup_libguestfs_env' EXIT

rootfs_stage_tree "$CWD" "$STAGING"

# Stage a tarball with root:root ownership and correct permissions.
chmod 700 "$STAGING/root/.ssh"
test -f "$STAGING/root/.ssh/authorized_keys" && chmod 600 "$STAGING/root/.ssh/authorized_keys"
tar cf "$STAGING_TAR" --owner=0 --group=0 --numeric-owner -C "$STAGING" .

echo "Creating GPT partition and XFS filesystem"
wos_qcow_guestfish "create partitioned XFS rootfs qcow image" "$DISK" --rw -a "$DISK" <<_EOF_
run
part-init /dev/sda gpt
part-add /dev/sda p $PART_START_SECTOR $PART_END_SECTOR
mkfs xfs /dev/sda1
sync
mount /dev/sda1 /
tar-in $STAGING_TAR /
mkdir-p /oldroot
sync
_EOF_

rm -f "$STAGING_TAR"

echo ""
echo "XFS rootfs disk created successfully: $DISK"
echo "Contents:"

wos_qcow_guestfish "list created rootfs qcow image contents" "$DISK" --ro -a "$DISK" <<_EOF_
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
