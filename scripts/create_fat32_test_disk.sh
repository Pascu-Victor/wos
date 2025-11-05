#!/bin/bash
# Create a test FAT32 disk image with testprog for filesystem testing
# This script creates a minimal FAT32 filesystem with test data

set -e

CWD=$(pwd)
TEST_DISK="test_fat32.qcow2"

if [ -e "$TEST_DISK" ]; then
    echo "Removing existing $TEST_DISK..."
    rm "$TEST_DISK"
fi

# Create 1GB disk (FAT32 needs space)
DISK_SIZE="1G"
echo "Creating QCOW2 image ($DISK_SIZE)"
qemu-img create -f qcow2 "$TEST_DISK" "$DISK_SIZE"

echo "Creating GPT partition and FAT32 filesystem"
# Use guestfish to create GPT and format as FAT32
guestfish --rw -a "$TEST_DISK" <<_EOF_
run
part-init /dev/sda gpt
# Create 900MB partition starting at sector 2048
part-add /dev/sda p 2048 1845247

mkfs fat /dev/sda1
mount /dev/sda1 /
mkdir /testdata
copy-in $CWD/build/modules/testprog/testprog /
write /hello.txt "Hello from FAT32 filesystem!"
write /test.bin "Binary test data 1234567890ABCDEF"
_EOF_

echo ""
echo "FAT32 test disk created successfully: $TEST_DISK"
echo "Contents:"

guestfish --rw -a "$TEST_DISK" <<_EOF_
run
mount /dev/sda1 /
ls /
_EOF_

