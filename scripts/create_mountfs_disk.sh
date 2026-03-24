#!/bin/bash
# Create the mountfs disk image with XFS filesystem
# This script creates an XFS filesystem with test data and binaries

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

echo "Creating GPT partition and XFS filesystem"
# Use guestfish to create GPT and format as XFS
guestfish --rw -a "$DISK" <<_EOF_
run
part-init /dev/sda gpt
# Create partition starting at sector 2048
part-add /dev/sda p $PART_START_SECTOR $PART_END_SECTOR

mkfs xfs /dev/sda1
mount /dev/sda1 /
mkdir /testdata
copy-in $CWD/build/modules/testprog/testprog /
copy-in $CWD/build/modules/init/init /
copy-in $CWD/configs/drive/srv /
write /hello.txt "Hello from XFS filesystem!\n"
write /test.bin "Binary test data 1234567890ABCDEF"
_EOF_

echo ""
echo "XFS mountfs disk created successfully: $DISK"
echo "Contents:"

guestfish --rw -a "$DISK" <<_EOF_
run
mount /dev/sda1 /
ls /
_EOF_
