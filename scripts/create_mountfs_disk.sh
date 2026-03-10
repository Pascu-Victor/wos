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

# Create 1GB disk
DISK_SIZE="1G"
echo "Creating QCOW2 image ($DISK_SIZE)"
qemu-img create -f qcow2 "$DISK" "$DISK_SIZE"

echo "Creating GPT partition and XFS filesystem"
# Use guestfish to create GPT and format as XFS
guestfish --rw -a "$DISK" <<_EOF_
run
part-init /dev/sda gpt
# Create 900MB partition starting at sector 2048
part-add /dev/sda p 2048 1845247

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
