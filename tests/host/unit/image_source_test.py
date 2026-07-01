#!/usr/bin/env python3

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
DISKS_CONF = ROOT / "configs" / "disks.conf"
MAKE_IMAGE = ROOT / "scripts" / "build" / "make_image.sh"
MAKE_INITRAMFS = ROOT / "scripts" / "build" / "make_initramfs.sh"


def fail(message: str) -> None:
    raise AssertionError(message)


def require_tokens(source: str, tokens: list[str], context: str) -> None:
    missing = [token for token in tokens if token not in source]
    if missing:
        fail(f"{context}: missing {', '.join(missing)}")


def main() -> None:
    disks = DISKS_CONF.read_text(encoding="utf-8")
    make_image = MAKE_IMAGE.read_text(encoding="utf-8")
    make_initramfs = MAKE_INITRAMFS.read_text(encoding="utf-8")

    require_tokens(
        disks,
        [
            'WOS_BOOT_PARTUUID="${WOS_BOOT_PARTUUID:-11044da0-352d-480a-9ef3-f995f3ac3f8b}"',
            '"$WOS_BOOT_DISK:1         /boot       fat32     $WOS_BOOT_PARTUUID"',
            'local partuuid',
            'partuuid=$(echo "$entry" | awk \'{print $4}\')',
            'if [ -z "$partuuid" ] && [ ! -f "$disk_image" ]; then',
            'if [ -z "$partuuid" ]; then',
            'partuuid=$(extract_partuuid "$disk_image" "$part_num")',
        ],
        "stable boot PARTUUID disk config",
    )

    if "create_boot_disk_fast 0" in make_image or "create_boot_disk_guestfish 0" in make_image:
        fail("make_image.sh must not create an empty boot disk before initramfs generation")

    initramfs_pos = make_image.find('bash "$CWD/scripts/build/make_initramfs.sh"')
    boot_pos = make_image.find("create_boot_disk_fast 1")
    if initramfs_pos < 0 or boot_pos < 0 or initramfs_pos > boot_pos:
        fail("make_image.sh must generate initramfs before creating the populated boot disk")

    require_tokens(
        make_image,
        [
            "BOOT_IMAGE_MANIFEST=\"${WOS_BOOT_IMAGE_MANIFEST:-$BUILD_DIR/boot_image.manifest}\"",
            "write_boot_payload_manifest()",
            "cmp -s \"$BOOT_MANIFEST_TMP\" \"$BOOT_IMAGE_MANIFEST_PATH\"",
            "touch -c \"$BOOT_DISK\"",
            "boot image: payload unchanged; keeping",
            "mv -f \"$BOOT_MANIFEST_TMP\" \"$BOOT_IMAGE_MANIFEST_PATH\"",
        ],
        "boot payload manifest reuse",
    )

    require_tokens(
        make_initramfs,
        [
            "INITRAMFS_MTIME=\"${SOURCE_DATE_EPOCH:-0}\"",
            "cpio -0 -o -H newc --reproducible --quiet",
            "cmp -s \"$INITRAMFS_TMP\" \"$INITRAMFS_OUT\"",
            "touch -c \"$INITRAMFS_OUT\"",
            "initramfs: unchanged",
        ],
        "deterministic initramfs reuse",
    )

    print("image source invariants hold")


if __name__ == "__main__":
    main()
