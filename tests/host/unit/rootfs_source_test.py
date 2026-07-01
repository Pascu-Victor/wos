#!/usr/bin/env python3

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
ROOTFS_COMMON = ROOT / "scripts" / "build" / "rootfs_common.sh"
SYNC_ROOTFS = ROOT / "scripts" / "build" / "sync_rootfs.sh"
ROOTFS_DELTA = ROOT / "scripts" / "build" / "rootfs_delta.py"
ROOTFS_QCOW_XFS_DELTA = ROOT / "scripts" / "build" / "rootfs_qcow_xfs_delta.py"


def fail(message: str) -> None:
    raise AssertionError(message)


def main() -> None:
    source = ROOTFS_COMMON.read_text(encoding="utf-8")
    sync_rootfs = SYNC_ROOTFS.read_text(encoding="utf-8")
    delta = ROOTFS_DELTA.read_text(encoding="utf-8")
    qcow_xfs_delta = ROOTFS_QCOW_XFS_DELTA.read_text(encoding="utf-8")
    if "root:!:0:0:root:/root:/bin/bash" not in source:
        fail("root passwd entry must be locked locally instead of forcing shadow lookup")
    if "root:x:0:0:root:/root:/bin/bash" in source:
        fail("root passwd entry must not force /etc/shadow lookup")
    for token in [
        "cat > \"$ROOTFS_STAGING/etc/shells\"",
        "/bin/bash",
        "/usr/bin/bash",
        "cp \"$ssh_pubkey\" \"$ROOTFS_STAGING/root/.ssh/authorized_keys\"",
        "chmod 600 \"$ROOTFS_STAGING/root/.ssh/authorized_keys\"",
        "rootfs_make_staging_dir()",
        'base="${WOS_ROOTFS_STAGING_TMPDIR:-}"',
        'mktemp -d "$base/staging.XXXXXX"',
        'cp -al "$resolved" "$target_path"',
        "rootfs_copy_glob_entry()",
        "compgen -G \"$resolved_pattern\" | sort",
        "[ -e \"$resolved\" ] || [ -L \"$resolved\" ] || continue",
        "copy-glob)",
        "ROOTFS_CONTENT_MANIFEST=\"/etc/wos-rootfs-manifest.tsv\"",
        "rootfs_write_content_manifest()",
        "ROOTFS_MANIFEST_HASH_LIMIT_BYTES=\"${WOS_ROOTFS_MANIFEST_HASH_LIMIT_BYTES:-16777216}\"",
        "links=$(stat -c %h \"$entry\")",
        "sha256sum -b \"$entry\"",
        "mtime=$(TZ=UTC0 stat -c %y \"$entry\")",
        "identity_kind=\"mtime\"",
        "WOS_ROOTFS_MANIFEST_FORCE_HASH",
        "rootfs_record_managed_path \"$ROOTFS_CONTENT_MANIFEST\"",
        "rootfs_write_content_manifest \"$ROOTFS_STAGING$ROOTFS_CONTENT_MANIFEST\"",
    ]:
        if token not in source:
            fail(f"rootfs auth staging missing {token!r}")

    for token in [
        "MOUNTFS_STAMP=\"${WOS_MOUNTFS_STAMP:-${ROOTFS_BUILD_DIR%/}/stamps/mountfs_disk.stamp}\"",
        "SOURCE_CACHE=\"${WOS_ROOTFS_SOURCE_CACHE:-${ROOTFS_BUILD_DIR%/}/rootfs-sync/source-state.tsv}\"",
        "SOURCE_CACHE_WAS_MISSING=0",
        "[ ! -f \"$SOURCE_CACHE\" ]",
        "rootfs_recreate_full_image()",
        "rootfs sync: $reason; recreating full image",
        "bash \"$CWD/scripts/build/create_mountfs_disk.sh\"",
        "python3 \"$CWD/scripts/build/rootfs_delta.py\"",
        "--cache \"$SOURCE_CACHE\"",
        "--new-cache \"$NEW_SOURCE_CACHE\"",
        "--stamp \"$MOUNTFS_STAMP\"",
        "if [ ! -s \"$CHANGED_PATHS\" ]; then",
        "rootfs_recreate_full_image \"source cache missing\"",
        "mv -f \"$NEW_SOURCE_CACHE\" \"$SOURCE_CACHE\"",
        "touch -c \"$DISK\"",
        "--no-recursion --verbatim-files-from",
        "rootfs_log_delta_payload",
        "rootfs sync: delta payload:",
        "file data",
        "rootfs_qcow_xfs_delta.py",
        "rootfs sync: applying qcow/XFS delta without libguestfs",
        "if ! python3 \"$CWD/scripts/build/rootfs_qcow_xfs_delta.py\"",
        "rootfs_recreate_full_image \"qcow/XFS delta unavailable\"",
    ]:
        if token not in sync_rootfs:
            fail(f"rootfs delta sync missing {token!r}")

    if "rootfs_stage_tree \"$CWD\" \"$STAGING\"" in sync_rootfs:
        fail("sync_rootfs.sh must not stage the full rootfs before deciding what changed")
    if "[ \"$ROOTFS_CHANGED\" -eq 0 ]" in sync_rootfs:
        fail("sync_rootfs.sh must not rely on the old always-true ROOTFS_CHANGED flag")
    for forbidden in [
        "wos_qcow_guestfish",
        "guestfish",
        "rootfs_remove_old_managed_paths",
        "wos_qcow_prepare_libguestfs_env",
        "wos_qcow_validate_for_update",
    ]:
        if forbidden in sync_rootfs:
            fail(f"sync_rootfs.sh must not use libguestfs delta backend token {forbidden!r}")

    for token in [
        "class RootfsDelta",
        "collect_alias_manifest",
        "collect_sysroot_libs",
        "collect_sysroot_headers",
        "collect_sysroot_cmake_data",
        "collect_sysroot_python",
        "collect_busybox",
        "changed_targets",
        "entries_newer_than_stamp",
        "self.stage_entry(entry)",
        "WOS_ROOTFS_FORCE_SYNC",
        "/etc/wos-rootfs-source-state.tsv",
    ]:
        if token not in delta:
            fail(f"rootfs delta helper missing {token!r}")

    for token in [
        "qemu-storage-daemon",
        "--export",
        "type=fuse",
        "writable=on,allow-other=off",
        "xfs_db",
        "write core.size",
        "write core.mode",
        "crc -r",
        "grew to",
        "recreate mountfs.qcow2",
    ]:
        if token not in qcow_xfs_delta:
            fail(f"rootfs qcow/XFS delta helper missing {token!r}")

    print("rootfs auth source invariants hold")


if __name__ == "__main__":
    main()
