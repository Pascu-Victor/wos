#!/bin/bash
# Create the boot disk image, build the initramfs (which needs final
# PARTUUIDs from both disks), then populate the boot partition.
set -e

WOS_ROOT="${WOS_WORKSPACE_ROOT:-$(git -C "$(dirname "$0")" rev-parse --show-toplevel)}"
cd "$WOS_ROOT"

CWD="$WOS_ROOT"
BUILD_DIR="${WOS_BUILD_DIR:-build}"
BOOT_DISK="${WOS_BOOT_DISK:-disk.qcow2}"
KERNEL_BINARY="$BUILD_DIR/modules/kern/wos"
INITRAMFS_OUT="$BUILD_DIR/initramfs.cpio"

# shellcheck source=scripts/build/qcow_common.sh
source "$CWD/scripts/build/qcow_common.sh"

TMP_LIMINE_CONF=""

cleanup() {
    test -z "$TMP_LIMINE_CONF" || rm -f "$TMP_LIMINE_CONF"
    wos_qcow_cleanup_libguestfs_env
}

wos_qcow_prepare_libguestfs_env
trap cleanup EXIT

if [[ "$KERNEL_BINARY" = /* ]]; then
    KERNEL_COPY="$KERNEL_BINARY"
else
    KERNEL_COPY="$CWD/$KERNEL_BINARY"
fi
if [[ "$INITRAMFS_OUT" = /* ]]; then
    INITRAMFS_COPY="$INITRAMFS_OUT"
else
    INITRAMFS_COPY="$CWD/$INITRAMFS_OUT"
fi

if [ -e "$BOOT_DISK" ]; then
    wos_qcow_guard_replace "$BOOT_DISK" "replace boot qcow image"
    rm "$BOOT_DISK"
fi

# Phase 1: Create boot disk with GPT + FAT32 partition.
# This assigns the final PARTUUID for the boot partition.
mkdir -p "$(dirname "$BOOT_DISK")"
qemu-img create -f qcow2 "$BOOT_DISK" 1G
wos_qcow_guestfish "create partitioned boot qcow image" "$BOOT_DISK" --rw -a "$BOOT_DISK" <<_EOF_
run
part-init /dev/sda gpt
part-add /dev/sda p 2048 1845247
mkfs fat /dev/sda1
_EOF_

# Phase 2: Build the CPIO initramfs now that all disk images exist
# with their final PARTUUIDs (disk.qcow2 + mountfs.qcow2).
bash "$CWD/scripts/build/make_initramfs.sh"

LIMINE_CONF="$CWD/modules/kern/limine.conf"
if [ "${WOS_KERNEL_CMDLINE+x}" ]; then
    TMP_LIMINE_CONF="$(mktemp)"
    cat > "$TMP_LIMINE_CONF" <<_EOF_
timeout: 0

/wos
    protocol: limine
    kaslr: no
    kernel_path: boot():/wos
    module_path: boot():/initramfs.cpio
    cmdline: $WOS_KERNEL_CMDLINE
_EOF_
    LIMINE_CONF="$TMP_LIMINE_CONF"
fi

# Phase 3: Populate the boot partition with kernel, bootloader, and initramfs.
wos_qcow_guestfish "populate boot qcow image" "$BOOT_DISK" --rw -a "$BOOT_DISK" <<_EOF_
run
mount /dev/sda1 /
mkdir /EFI
mkdir /EFI/BOOT
mkdir /limine
upload $KERNEL_COPY /wos
upload $LIMINE_CONF /limine/limine.conf
upload /usr/share/limine/BOOTX64.EFI /EFI/BOOT/BOOTX64.EFI
upload $INITRAMFS_COPY /initramfs.cpio
umount /
_EOF_
