#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
COREDUMPS_DIR="$PROJECT_ROOT/coredumps"
DISK_IMAGE="$PROJECT_ROOT/test_fat32.qcow2"

# Create coredumps directory if it doesn't exist
mkdir -p "$COREDUMPS_DIR"

# Check if disk image exists
if [ ! -f "$DISK_IMAGE" ]; then
    echo "Disk image not found: $DISK_IMAGE"
    exit 1
fi

# Extract all _coredump.bin files using guestfish
echo "Extracting coredump files from $DISK_IMAGE..."

guestfish --ro -a "$DISK_IMAGE" <<_EOF_
run
mount /dev/sda1 /
-glob copy-out /*_coredump.bin $COREDUMPS_DIR/
umount /
_EOF_

# List extracted files
COREDUMP_COUNT=$(find "$COREDUMPS_DIR" -name "*_coredump.bin" -type f 2>/dev/null | wc -l)
echo "Extracted $COREDUMP_COUNT coredump file(s) to $COREDUMPS_DIR"

if [ "$COREDUMP_COUNT" -gt 0 ]; then
    ls -la "$COREDUMPS_DIR"/*_coredump.bin
fi
