#!/bin/bash
# Sync changed files into an existing mountfs.qcow2 rootfs image.
# Uses qemu-storage-daemon FUSE plus direct XFS extent writes for modified files.
# If mountfs.qcow2 doesn't exist, exit with error (run create_mountfs_disk.sh first).
set -e

WOS_ROOT="${WOS_WORKSPACE_ROOT:-$(git -C "$(dirname "$0")" rev-parse --show-toplevel)}"
cd "$WOS_ROOT"

CWD="$WOS_ROOT"
DISK="${WOS_ROOTFS_DISK:-mountfs.qcow2}"

# shellcheck disable=SC1091
source "$CWD/scripts/build/rootfs_common.sh"

rootfs_format_bytes() {
    local bytes="$1"

    if command -v numfmt >/dev/null 2>&1; then
        numfmt --to=iec --suffix=B "$bytes"
    else
        printf '%s bytes\n' "$bytes"
    fi
}

rootfs_log_delta_payload() {
    local bytes=0
    local dirs=0
    local entries=0
    local files=0
    local links=0
    local path
    local rel
    local size
    local tar_bytes

    while IFS= read -r rel; do
        [ -n "$rel" ] || continue
        entries=$((entries + 1))
        path="$STAGING/$rel"
        if [ -L "$path" ]; then
            links=$((links + 1))
        elif [ -f "$path" ]; then
            files=$((files + 1))
            size=$(stat -c %s "$path")
            bytes=$((bytes + size))
        elif [ -d "$path" ]; then
            dirs=$((dirs + 1))
        fi
    done < "$TAR_PATHS"

    tar_bytes=$(stat -c %s "$STAGING_TAR")
    printf '  rootfs sync: delta payload: %d files, %d dirs, %d symlinks (%d paths); %s file data, %s tar\n' \
        "$files" "$dirs" "$links" "$entries" "$(rootfs_format_bytes "$bytes")" "$(rootfs_format_bytes "$tar_bytes")"
}

if [ ! -f "$DISK" ]; then
    echo "ERROR: $DISK does not exist. Run scripts/build/create_mountfs_disk.sh first."
    exit 1
fi

echo "Syncing rootfs delta into $DISK..."

ROOTFS_REPO="$CWD"
MOUNTFS_STAMP="${WOS_MOUNTFS_STAMP:-${ROOTFS_BUILD_DIR%/}/stamps/mountfs_disk.stamp}"
if [[ "$MOUNTFS_STAMP" != /* ]]; then
    MOUNTFS_STAMP="$CWD/$MOUNTFS_STAMP"
fi
SOURCE_CACHE="${WOS_ROOTFS_SOURCE_CACHE:-${ROOTFS_BUILD_DIR%/}/rootfs-sync/source-state.tsv}"
if [[ "$SOURCE_CACHE" != /* ]]; then
    SOURCE_CACHE="$CWD/$SOURCE_CACHE"
fi
SOURCE_CACHE_WAS_MISSING=0
if [ ! -f "$SOURCE_CACHE" ]; then
    SOURCE_CACHE_WAS_MISSING=1
fi

rootfs_recreate_full_image() {
    local reason="$1"

    echo "  rootfs sync: $reason; recreating full image"
    rm -f "$STAGING_TAR"
    bash "$CWD/scripts/build/create_mountfs_disk.sh"
    mkdir -p "$(dirname "$SOURCE_CACHE")"
    mv -f "$NEW_SOURCE_CACHE" "$SOURCE_CACHE"
    touch -c "$DISK"
}

# Build a small delta staging tree from the complete declared source set. The
# helper stats every managed source entry but copies only entries whose source
# metadata changed since the last successful sync.
STAGING=$(rootfs_make_staging_dir "$CWD")
STAGING_TAR="$STAGING.tar"
NEW_SOURCE_CACHE="$STAGING/source-state.tsv"
CHANGED_PATHS="$STAGING.changed-paths"
TAR_PATHS="$STAGING.tar-paths"
trap 'rm -rf "$STAGING"; rm -f "$STAGING_TAR" "$CHANGED_PATHS" "$TAR_PATHS"' EXIT

python3 "$CWD/scripts/build/rootfs_delta.py" \
    --repo "$CWD" \
    --staging "$STAGING" \
    --cache "$SOURCE_CACHE" \
    --new-cache "$NEW_SOURCE_CACHE" \
    --stamp "$MOUNTFS_STAMP" \
    --changed-paths "$CHANGED_PATHS" \
    --tar-paths "$TAR_PATHS"

if [ "$SOURCE_CACHE_WAS_MISSING" -eq 1 ]; then
    rootfs_recreate_full_image "source cache missing"
    exit 0
fi

if [ ! -s "$CHANGED_PATHS" ]; then
    echo "  rootfs sync: nothing to update"
    mkdir -p "$(dirname "$SOURCE_CACHE")"
    mv -f "$NEW_SOURCE_CACHE" "$SOURCE_CACHE"
    touch -c "$DISK"
    exit 0
fi

# Stage a tarball with only changed paths, root:root ownership, and correct
# permissions. Generated rootfs state files are included with any delta so the
# next sync compares against exactly what was installed.
test ! -d "$STAGING/root/.ssh" || chmod 700 "$STAGING/root/.ssh"
test ! -f "$STAGING/root/.ssh/authorized_keys" || chmod 600 "$STAGING/root/.ssh/authorized_keys"
tar cf "$STAGING_TAR" \
    --owner=0 --group=0 --numeric-owner \
    --no-recursion --verbatim-files-from \
    -C "$STAGING" --files-from "$TAR_PATHS"
rootfs_log_delta_payload

echo "  rootfs sync: applying qcow/XFS delta without libguestfs"
if ! python3 "$CWD/scripts/build/rootfs_qcow_xfs_delta.py" \
    --disk "$DISK" \
    --staging "$STAGING" \
    --paths "$TAR_PATHS" \
    --partition 1; then
    rootfs_recreate_full_image "qcow/XFS delta unavailable"
    exit 0
fi

rm -f "$STAGING_TAR"
mkdir -p "$(dirname "$SOURCE_CACHE")"
mv -f "$NEW_SOURCE_CACHE" "$SOURCE_CACHE"

echo "  rootfs sync: delta update complete"
