#!/bin/bash
# Sync changed files into an existing mountfs.qcow2 rootfs image.
# Uses guestfish copy-in to delta-update only modified files.
# If mountfs.qcow2 doesn't exist, exit with error (run create_mountfs_disk.sh first).
set -e

WOS_ROOT="${WOS_WORKSPACE_ROOT:-$(git -C "$(dirname "$0")" rev-parse --show-toplevel)}"
cd "$WOS_ROOT"

CWD="$WOS_ROOT"
DISK="${WOS_ROOTFS_DISK:-mountfs.qcow2}"

# shellcheck disable=SC1091
source "$CWD/scripts/build/rootfs_common.sh"

wos_qcow_prepare_libguestfs_env

if [ ! -f "$DISK" ]; then
    echo "ERROR: $DISK does not exist. Run scripts/build/create_mountfs_disk.sh first."
    exit 1
fi

wos_qcow_validate_for_update "$DISK" "sync rootfs delta into qcow image"

echo "Syncing rootfs delta into $DISK..."

# Build a staging directory with only the files that need updating
STAGING=$(mktemp -d)
STAGING_TAR="$STAGING.tar"
trap 'rm -rf "$STAGING"; rm -f "$STAGING_TAR"; wos_qcow_cleanup_libguestfs_env' EXIT

rootfs_stage_tree "$CWD" "$STAGING"

if [ "$ROOTFS_CHANGED" -eq 0 ]; then
    echo "  rootfs sync: nothing to update"
    exit 0
fi

# Stage a tarball with root:root ownership and correct permissions.
chmod 700 "$STAGING/root/.ssh"
test -f "$STAGING/root/.ssh/authorized_keys" && chmod 600 "$STAGING/root/.ssh/authorized_keys"
tar cf "$STAGING_TAR" --owner=0 --group=0 --numeric-owner -C "$STAGING" .

rootfs_remove_old_managed_paths "$DISK" "$STAGING/etc/wos-managed-paths"

# Use guestfish to copy updated files into the existing disk
wos_qcow_guestfish "sync rootfs delta into qcow image" "$DISK" --rw -a "$DISK" <<_EOF_
run
mount /dev/sda1 /
tar-in $STAGING_TAR /
sync
_EOF_

rm -f "$STAGING_TAR"

echo "  rootfs sync: delta update complete"
