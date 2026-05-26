#!/bin/bash
# Sync changed files into an existing mountfs.qcow2 rootfs image.
# Uses guestfish copy-in to delta-update only modified files.
# If mountfs.qcow2 doesn't exist, exit with error (run create_mountfs_disk.sh first).
set -e

CWD=$(pwd)
DISK="mountfs.qcow2"

# shellcheck disable=SC1091
source "$CWD/scripts/rootfs_common.sh"

if [ ! -f "$DISK" ]; then
    echo "ERROR: $DISK does not exist. Run scripts/create_mountfs_disk.sh first."
    exit 1
fi

echo "Syncing rootfs delta into $DISK..."

# Build a staging directory with only the files that need updating
STAGING=$(mktemp -d)
trap 'rm -rf "$STAGING"' EXIT

rootfs_stage_tree "$CWD" "$STAGING"

if [ "$ROOTFS_CHANGED" -eq 0 ]; then
    echo "  rootfs sync: nothing to update"
    exit 0
fi

# Stage a tarball with root:root ownership and correct permissions
chmod 700 "$STAGING/root/.ssh"
test -f "$STAGING/root/.ssh/authorized_keys" && chmod 600 "$STAGING/root/.ssh/authorized_keys"
fakeroot sh -c "chown -R 0:0 '$STAGING' && tar cf '$STAGING.tar' --numeric-owner -C '$STAGING' ."

rootfs_remove_old_managed_paths "$DISK"

# Use guestfish to copy updated files into the existing disk
guestfish --rw -a "$DISK" <<_EOF_
run
mount /dev/sda1 /
tar-in $STAGING.tar /
sync
_EOF_

rm -f "$STAGING.tar"

echo "  rootfs sync: delta update complete"
