#!/usr/bin/env python3
"""Source invariants for XFS metadata checksum byte order."""

from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
DIR2 = ROOT / "modules/kern/src/vfs/fs/xfs/xfs_dir2.cpp"
ATTR = ROOT / "modules/kern/src/vfs/fs/xfs/xfs_attr.cpp"


def fail(message: str) -> None:
    raise SystemExit(f"xfs_metadata_crc_source_test: {message}")


def require(source: str, needle: str, description: str) -> None:
    if needle not in source:
        fail(f"missing {description}: {needle}")


def main() -> None:
    dir2 = DIR2.read_text()
    attr = ATTR.read_text()

    if "Be32::from_cpu(CRC)" in dir2 or "Be32::from_cpu(CRC)" in attr:
        fail("metadata checksums must not be serialized as big-endian values")

    for target in (
        "&hdr->info.crc",
        "&hdr->hdr.crc",
        "&hdr3->hdr.crc",
        "&mutable_hdr->hdr.crc",
    ):
        require(dir2, f"__builtin_memcpy({target}, &CRC, sizeof(CRC));", f"raw checksum store to {target}")

    require(
        attr,
        "__builtin_memcpy(block + XFS_ATTR3_LEAF_CRC_OFF, &CRC, sizeof(CRC));",
        "raw attribute-leaf checksum store",
    )


if __name__ == "__main__":
    main()
