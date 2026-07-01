#!/bin/bash
# Build the initramfs, then create and populate the boot disk image.
set -e

WOS_ROOT="${WOS_WORKSPACE_ROOT:-$(git -C "$(dirname "$0")" rev-parse --show-toplevel)}"
cd "$WOS_ROOT"

CWD="$WOS_ROOT"
BUILD_DIR="${WOS_BUILD_DIR:-build}"
BOOT_DISK="${WOS_BOOT_DISK:-disk.qcow2}"
KERNEL_BINARY="$BUILD_DIR/modules/kern/wos"
INITRAMFS_OUT="$BUILD_DIR/initramfs.cpio"
BOOTLOADER="${WOS_BOOTLOADER:-/usr/share/limine/BOOTX64.EFI}"
BOOT_IMAGE_MANIFEST="${WOS_BOOT_IMAGE_MANIFEST:-$BUILD_DIR/boot_image.manifest}"
BOOT_PART_START_SECTOR=2048
BOOT_PART_END_SECTOR=1845247
BOOT_PART_GUID="${WOS_BOOT_PARTUUID:-11044da0-352d-480a-9ef3-f995f3ac3f8b}"
SECTOR_SIZE=512

# shellcheck source=scripts/build/qcow_common.sh
source "$CWD/scripts/build/qcow_common.sh"

TMP_LIMINE_CONF=""
BOOT_RAW=""
BOOT_FAT=""
BOOT_TMP=""
BOOT_MANIFEST_TMP=""

cleanup() {
    test -z "$TMP_LIMINE_CONF" || rm -f "$TMP_LIMINE_CONF"
    test -z "$BOOT_RAW" || rm -f "$BOOT_RAW"
    test -z "$BOOT_FAT" || rm -f "$BOOT_FAT"
    test -z "$BOOT_TMP" || rm -f "$BOOT_TMP"
    test -z "$BOOT_MANIFEST_TMP" || rm -f "$BOOT_MANIFEST_TMP"
    wos_qcow_cleanup_libguestfs_env
}

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
if [[ "$BOOT_IMAGE_MANIFEST" = /* ]]; then
    BOOT_IMAGE_MANIFEST_PATH="$BOOT_IMAGE_MANIFEST"
else
    BOOT_IMAGE_MANIFEST_PATH="$CWD/$BOOT_IMAGE_MANIFEST"
fi

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

create_boot_disk_guestfish() {
    local populate="${1:-1}"

    if [ -e "$BOOT_DISK" ]; then
        wos_qcow_guard_replace "$BOOT_DISK" "replace boot qcow image"
        rm "$BOOT_DISK"
    fi

    mkdir -p "$(dirname "$BOOT_DISK")"
    qemu-img create -f qcow2 "$BOOT_DISK" 1G
    wos_qcow_guestfish "create partitioned boot qcow image" "$BOOT_DISK" --rw -a "$BOOT_DISK" <<_EOF_
run
part-init /dev/sda gpt
part-add /dev/sda p $BOOT_PART_START_SECTOR $BOOT_PART_END_SECTOR
part-set-gpt-guid /dev/sda 1 $BOOT_PART_GUID
mkfs fat /dev/sda1
_EOF_

    if [ "$populate" -ne 1 ]; then
        return 0
    fi

    wos_qcow_guestfish "populate boot qcow image" "$BOOT_DISK" --rw -a "$BOOT_DISK" <<_EOF_
run
mount /dev/sda1 /
mkdir /EFI
mkdir /EFI/BOOT
mkdir /limine
upload $KERNEL_COPY /wos
upload $LIMINE_CONF /limine/limine.conf
upload $BOOTLOADER /EFI/BOOT/BOOTX64.EFI
upload $INITRAMFS_COPY /initramfs.cpio
umount /
_EOF_
}

create_boot_disk_fast() {
    local populate="${1:-1}"
    local part_size_sectors
    local part_size_bytes
    local boot_dir

    for tool in sgdisk mformat mmd mcopy qemu-img dd truncate; do
        command -v "$tool" >/dev/null 2>&1 || return 1
    done

    if [ "$populate" -eq 1 ]; then
        [ -f "$BOOTLOADER" ] || return 1
        [ -f "$KERNEL_COPY" ] || return 1
        [ -f "$LIMINE_CONF" ] || return 1
        [ -f "$INITRAMFS_COPY" ] || return 1
    fi

    boot_dir="$(dirname "$BOOT_DISK")"
    mkdir -p "$boot_dir"
    if [ -e "$BOOT_DISK" ]; then
        wos_qcow_guard_replace "$BOOT_DISK" "replace boot qcow image"
    fi

    BOOT_RAW="$(mktemp "${TMPDIR:-/tmp}/wos-boot-raw.XXXXXX")"
    BOOT_FAT="$(mktemp "${TMPDIR:-/tmp}/wos-boot-fat.XXXXXX")"
    BOOT_TMP="$(mktemp "$boot_dir/$(basename "$BOOT_DISK").tmp.XXXXXX")"
    rm -f "$BOOT_RAW" "$BOOT_FAT" "$BOOT_TMP"

    part_size_sectors=$((BOOT_PART_END_SECTOR - BOOT_PART_START_SECTOR + 1))
    part_size_bytes=$((part_size_sectors * SECTOR_SIZE))

    truncate -s 1G "$BOOT_RAW"
    sgdisk --clear --new=1:$BOOT_PART_START_SECTOR:$BOOT_PART_END_SECTOR --typecode=1:ef00 --change-name=1:WOSBOOT \
        --partition-guid=1:"$BOOT_PART_GUID" "$BOOT_RAW" >/dev/null

    truncate -s "$part_size_bytes" "$BOOT_FAT"
    mformat -i "$BOOT_FAT" -F -v WOSBOOT ::
    if [ "$populate" -eq 1 ]; then
        mmd -i "$BOOT_FAT" ::/EFI
        mmd -i "$BOOT_FAT" ::/EFI/BOOT
        mmd -i "$BOOT_FAT" ::/limine
        mcopy -o -i "$BOOT_FAT" "$KERNEL_COPY" ::/wos
        mcopy -o -i "$BOOT_FAT" "$LIMINE_CONF" ::/limine/limine.conf
        mcopy -o -i "$BOOT_FAT" "$BOOTLOADER" ::/EFI/BOOT/BOOTX64.EFI
        mcopy -o -i "$BOOT_FAT" "$INITRAMFS_COPY" ::/initramfs.cpio
    fi
    dd if="$BOOT_FAT" of="$BOOT_RAW" bs="$SECTOR_SIZE" seek="$BOOT_PART_START_SECTOR" conv=notrunc status=none

    qemu-img convert -f raw -O qcow2 "$BOOT_RAW" "$BOOT_TMP"
    mv -f "$BOOT_TMP" "$BOOT_DISK"
    rm -f "$BOOT_RAW" "$BOOT_FAT"
    BOOT_RAW=""
    BOOT_FAT=""
    BOOT_TMP=""
}

boot_manifest_file_entry() {
    local label="$1"
    local path="$2"
    local hash
    local size

    if [ ! -f "$path" ]; then
        echo "ERROR: boot image input not found: $path" >&2
        return 1
    fi

    hash=$(sha256sum -b "$path" | awk '{print $1}')
    size=$(stat -c %s "$path")
    printf 'file\t%s\t%s\t%s\t%s\n' "$label" "$size" "$hash" "$path"
}

write_boot_payload_manifest() {
    local manifest="$1"

    {
        printf 'version\t1\n'
        printf 'partition\t%s\t%s\t%s\t%s\n' "$BOOT_PART_START_SECTOR" "$BOOT_PART_END_SECTOR" "$BOOT_PART_GUID" "$SECTOR_SIZE"
        boot_manifest_file_entry kernel "$KERNEL_COPY"
        boot_manifest_file_entry limine "$LIMINE_CONF"
        boot_manifest_file_entry bootloader "$BOOTLOADER"
        boot_manifest_file_entry initramfs "$INITRAMFS_COPY"
    } > "$manifest"
}

# Phase 1: Build the CPIO initramfs.  configs/disks.conf records the stable
# boot PARTUUID, so the boot disk does not need to be created once merely to
# read back a GUID before being recreated with the final initramfs.
bash "$CWD/scripts/build/make_initramfs.sh"

mkdir -p "$(dirname "$BOOT_IMAGE_MANIFEST_PATH")"
BOOT_MANIFEST_TMP=$(mktemp "$(dirname "$BOOT_IMAGE_MANIFEST_PATH")/.boot_image.manifest.XXXXXX")
write_boot_payload_manifest "$BOOT_MANIFEST_TMP"
if [ "${WOS_BOOT_IMAGE_FORCE:-0}" != "1" ] && [ -f "$BOOT_DISK" ] && [ -f "$BOOT_IMAGE_MANIFEST_PATH" ] &&
   cmp -s "$BOOT_MANIFEST_TMP" "$BOOT_IMAGE_MANIFEST_PATH"; then
    rm -f "$BOOT_MANIFEST_TMP"
    BOOT_MANIFEST_TMP=""
    touch -c "$BOOT_DISK"
    echo "  boot image: payload unchanged; keeping $BOOT_DISK"
    exit 0
fi

# Phase 2: Create the boot disk with the final initramfs contents.  Prefer the
# mtools path to avoid libguestfs appliance startup for the small FAT boot disk.
if [ "${WOS_BOOT_IMAGE_LEGACY_GUESTFS:-0}" = "1" ] || ! create_boot_disk_fast 1; then
    echo "  boot image: falling back to libguestfs boot disk population"
    create_boot_disk_guestfish 1
fi
mv -f "$BOOT_MANIFEST_TMP" "$BOOT_IMAGE_MANIFEST_PATH"
BOOT_MANIFEST_TMP=""
