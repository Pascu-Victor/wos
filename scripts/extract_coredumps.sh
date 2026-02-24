#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
COREDUMPS_DIR="$PROJECT_ROOT/coredumps"
CLUSTER_MODE=0

# Parse arguments
for arg in "$@"; do
    case "$arg" in
        --cluster)
            CLUSTER_MODE=1
            ;;
        *)
            echo "Unknown argument: $arg"
            echo "Usage: $0 [--cluster]"
            exit 1
            ;;
    esac
done

# Create coredumps directory if it doesn't exist
mkdir -p "$COREDUMPS_DIR"

# Build list of disk images to process
DISK_IMAGES=()
if [ "$CLUSTER_MODE" -eq 1 ]; then
    OVERLAY_DIR="$PROJECT_ROOT/cluster-overlays"
    if [ ! -d "$OVERLAY_DIR" ]; then
        echo "Cluster overlays directory not found: $OVERLAY_DIR"
        exit 1
    fi
    for img in "$OVERLAY_DIR"/fat32-vm*.qcow2; do
        if [ -f "$img" ]; then
            DISK_IMAGES+=("$img")
        fi
    done
    if [ ${#DISK_IMAGES[@]} -eq 0 ]; then
        echo "No fat32 overlay images found in $OVERLAY_DIR"
        exit 1
    fi
else
    DISK_IMAGE="$PROJECT_ROOT/test_fat32.qcow2"
    if [ ! -f "$DISK_IMAGE" ]; then
        echo "Disk image not found: $DISK_IMAGE"
        exit 1
    fi
    DISK_IMAGES+=("$DISK_IMAGE")
fi

# Extract coredumps from each disk image
for DISK_IMAGE in "${DISK_IMAGES[@]}"; do
    VM_NAME=$(basename "$DISK_IMAGE" .qcow2)
    if [ "$CLUSTER_MODE" -eq 1 ]; then
        DEST_DIR="$COREDUMPS_DIR/$VM_NAME"
        mkdir -p "$DEST_DIR"
    else
        DEST_DIR="$COREDUMPS_DIR"
    fi

    echo "Extracting coredump files from $DISK_IMAGE..."

    guestfish --ro -a "$DISK_IMAGE" <<_EOF_
run
mount /dev/sda1 /
-glob copy-out /*_coredump.bin $DEST_DIR/
umount /
_EOF_
done

# List extracted files
COREDUMP_COUNT=$(find "$COREDUMPS_DIR" -name "*_coredump.bin" -type f 2>/dev/null | wc -l)
echo "Extracted $COREDUMP_COUNT coredump file(s) to $COREDUMPS_DIR"

if [ "$COREDUMP_COUNT" -gt 0 ]; then
    find "$COREDUMPS_DIR" -name "*_coredump.bin" -type f -exec ls -la {} +
fi
