# Create the boot disk image, build the initramfs (which needs final
# PARTUUIDs from both disks), then populate the boot partition.
set -e

if [ -e disk.qcow2 ]; then
    rm disk.qcow2
fi

CWD=$(pwd)

# Phase 1: Create boot disk with GPT + FAT32 partition.
# This assigns the final PARTUUID for the boot partition.
qemu-img create -f qcow2 disk.qcow2 1G
guestfish --rw -a disk.qcow2 <<_EOF_
run
part-init /dev/sda gpt
part-add /dev/sda p 2048 1845247
mkfs fat /dev/sda1
_EOF_

# Phase 2: Build the CPIO initramfs now that all disk images exist
# with their final PARTUUIDs (disk.qcow2 + test_fat32.qcow2).
sh "$CWD/scripts/make_initramfs.sh"

# Phase 3: Populate the boot partition with kernel, bootloader, and initramfs.
guestfish --rw -a disk.qcow2 <<_EOF_
run
mount /dev/sda1 /
mkdir /EFI
mkdir /EFI/BOOT
mkdir /boot
mkdir /boot/limine
copy-in $CWD/build/modules/kern/wos /boot
copy-in $CWD/modules/kern/limine.conf /boot/limine
copy-in /usr/share/limine/BOOTX64.EFI /EFI/BOOT
copy-in $CWD/build/initramfs.cpio /boot
umount /
_EOF_

