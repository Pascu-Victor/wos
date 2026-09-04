#!/bin/bash
# Create the mountfs (rootfs) disk image with XFS filesystem.
# Uses usr-merge layout: real dirs under /usr/, compat symlinks at /.
#
# Layout:
#   /usr/lib/       ← shared/static libraries (libc.so, libc.a, libc++.a, ld.so, etc.) + CRT objects
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
ROOTFS_MTIME="${SOURCE_DATE_EPOCH:-0}"
ROOTFS_FS_UUID="${WOS_ROOTFS_UUID:-59f84530-5e3b-4f5b-a230-c9d7da79d107}"
ROOTFS_DISK_GUID="${WOS_ROOTFS_DISK_GUID:-de866036-2a89-46ee-bb7b-759061bca10b}"
ROOTFS_PART_GUID="${WOS_ROOTFS_PARTUUID:-73ec8526-3d1b-4f16-9f08-36b0e40e410e}"
KTEST_FS_UUID="${WOS_KTEST_XFS_UUID:-7f0afcd4-7342-4961-8d48-e91780568871}"
KTEST_PART_GUID="${WOS_KTEST_XFS_PARTUUID:-2e50edc2-5df6-47eb-857a-031c50df51db}"
ROOTFS_REPRODUCIBLE=0
case "${WOS_REPRODUCIBLE_BUILD:-0}" in
    1|ON|on|TRUE|true|YES|yes)
        ROOTFS_REPRODUCIBLE=1
        ;;
esac

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

# Build a staging directory with the rootfs layout.  Keep it under the build
# tree by default so rootfs_common.sh can hardlink-copy source trees instead of
# duplicating large sysroot directories through /tmp.
STAGING=$(rootfs_make_staging_dir "$CWD")
STAGING_TAR="$STAGING.tar"
trap 'rm -rf "$STAGING"; rm -f "$STAGING_TAR"; wos_qcow_cleanup_libguestfs_env' EXIT

rootfs_stage_tree "$CWD" "$STAGING"

# Stage a tarball with root:root ownership and correct permissions.
chmod 700 "$STAGING/root/.ssh"
test -f "$STAGING/root/.ssh/authorized_keys" && chmod 600 "$STAGING/root/.ssh/authorized_keys"
case "$ROOTFS_MTIME" in
    ''|*[!0-9]*)
        echo "ERROR: SOURCE_DATE_EPOCH must be a non-negative integer" >&2
        exit 1
        ;;
esac
if [ "$ROOTFS_REPRODUCIBLE" = "1" ]; then
    find "$STAGING" -exec touch -h -d "@$ROOTFS_MTIME" {} +
    tar cf "$STAGING_TAR" \
        --sort=name --mtime="@$ROOTFS_MTIME" --clamp-mtime \
        --owner=0 --group=0 --numeric-owner -C "$STAGING" .
else
    tar cf "$STAGING_TAR" --owner=0 --group=0 --numeric-owner -C "$STAGING" .
fi

echo "Creating GPT partition and XFS filesystem"
wos_qcow_guestfish "create partitioned XFS rootfs qcow image" "$DISK" --rw -a "$DISK" <<_EOF_
run
part-init /dev/sda gpt
part-set-disk-guid /dev/sda $ROOTFS_DISK_GUID
part-add /dev/sda p $PART_START_SECTOR $PART_END_SECTOR
part-set-gpt-guid /dev/sda 1 $ROOTFS_PART_GUID
debug sh "mkfs.xfs -f -m rmapbt=0,reflink=0,inobtcount=0,uuid=$ROOTFS_FS_UUID -n parent=0 /dev/sda1"
sync
mount /dev/sda1 /
tar-in $STAGING_TAR /
mkdir-p /oldroot
sync
_EOF_

if [ "${WOS_KTEST_SECOND_XFS:-0}" = "1" ]; then
    KTEST_PART_START_SECTOR=$((PART_END_SECTOR + 1))
    KTEST_PART_SIZE_MIB=8192
    KTEST_PART_END_SECTOR=$((KTEST_PART_START_SECTOR + (KTEST_PART_SIZE_MIB * 1024 * 1024 / SECTOR_SIZE) - 1))
    echo "Creating isolated KTEST secondary XFS partition (${KTEST_PART_SIZE_MIB} MiB)"
    wos_qcow_guestfish "add isolated KTEST secondary XFS partition" "$DISK" --rw -a "$DISK" <<_EOF_
run
part-add /dev/sda p $KTEST_PART_START_SECTOR $KTEST_PART_END_SECTOR
part-set-gpt-guid /dev/sda 2 $KTEST_PART_GUID
debug sh "mkfs.xfs -f -m rmapbt=0,reflink=0,inobtcount=0,uuid=$KTEST_FS_UUID -n parent=0 /dev/sda2"
sync
_EOF_
    # Reopen the appliance after changing the partition table.  Some
    # libguestfs kernels keep the pre-part-add geometry for mount(2) until the
    # disk is reattached even though mkfs.xfs can already open /dev/sda2.
    wos_qcow_guestfish "seed isolated KTEST XFS fixture" "$DISK" --rw -a "$DISK" <<_EOF_
run
mount /dev/sda2 /
touch /linux-xattr-fixture
setxattr user.linux linux 5 /linux-xattr-fixture
umount /
sync
_EOF_
fi

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
