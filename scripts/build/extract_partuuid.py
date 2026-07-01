#!/usr/bin/env python3
"""Extract a GPT partition UUID from a raw/qcow disk image without libguestfs."""

from __future__ import annotations

import math
import struct
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path


SECTOR_SIZE = 512
INITIAL_SECTORS = 2048


def read_image_prefix(image: Path, bytes_needed: int) -> bytes:
    sectors = max(INITIAL_SECTORS, math.ceil(bytes_needed / SECTOR_SIZE))
    with tempfile.NamedTemporaryFile() as out:
        subprocess.run(
            [
                "qemu-img",
                "dd",
                "-U",
                f"if={image}",
                f"of={out.name}",
                f"bs={SECTOR_SIZE}",
                f"count={sectors}",
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return Path(out.name).read_bytes()


def parse_gpt_partuuid(data: bytes, part_num: int) -> uuid.UUID:
    if part_num <= 0:
        raise ValueError("partition number must be positive")
    if len(data) < SECTOR_SIZE * 2:
        raise ValueError("image prefix is too small for a GPT header")

    header = data[SECTOR_SIZE : SECTOR_SIZE * 2]
    if header[:8] != b"EFI PART":
        raise ValueError("primary GPT header not found")

    entry_lba = struct.unpack_from("<Q", header, 72)[0]
    entry_count = struct.unpack_from("<I", header, 80)[0]
    entry_size = struct.unpack_from("<I", header, 84)[0]
    if part_num > entry_count:
        raise ValueError(f"partition {part_num} is outside GPT entry table")
    if entry_size < 128:
        raise ValueError(f"unsupported GPT entry size {entry_size}")

    entry_offset = (entry_lba * SECTOR_SIZE) + ((part_num - 1) * entry_size)
    entry_end = entry_offset + entry_size
    if entry_end > len(data):
        raise ValueError("image prefix does not contain requested GPT entry")

    entry = data[entry_offset:entry_end]
    type_guid = entry[0:16]
    unique_guid = entry[16:32]
    if type_guid == b"\0" * 16 or unique_guid == b"\0" * 16:
        raise ValueError(f"partition {part_num} is empty")
    return uuid.UUID(bytes_le=unique_guid)


def main() -> int:
    if len(sys.argv) != 3:
        print("usage: extract_partuuid.py IMAGE PARTNUM", file=sys.stderr)
        return 2

    image = Path(sys.argv[1])
    part_num = int(sys.argv[2], 10)
    data = read_image_prefix(image, SECTOR_SIZE * INITIAL_SECTORS)
    try:
        partuuid = parse_gpt_partuuid(data, part_num)
    except ValueError as first_error:
        # Very large GPT entry arrays can sit beyond the initial prefix.
        if "does not contain" not in str(first_error):
            raise
        header = data[SECTOR_SIZE : SECTOR_SIZE * 2]
        entry_lba = struct.unpack_from("<Q", header, 72)[0]
        entry_size = struct.unpack_from("<I", header, 84)[0]
        needed = (entry_lba * SECTOR_SIZE) + (part_num * entry_size)
        partuuid = parse_gpt_partuuid(read_image_prefix(image, needed), part_num)

    print(str(partuuid))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, subprocess.CalledProcessError, ValueError) as exc:
        print(f"extract_partuuid.py: {exc}", file=sys.stderr)
        raise SystemExit(1)
