#!/bin/bash
# Select rootfs recreation or incremental synchronization without embedding a
# shell program in CMake's custom-command argv.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

case "${WOS_REPRODUCIBLE_BUILD:-0}" in
    1|ON|on|TRUE|true|YES|yes)
        exec "$SCRIPT_DIR/create_mountfs_disk.sh"
        ;;
esac

if [ ! -f "${WOS_ROOTFS_DISK:-}" ]; then
    exec "$SCRIPT_DIR/create_mountfs_disk.sh"
fi

exec "$SCRIPT_DIR/sync_rootfs.sh"
