#!/usr/bin/env python3
"""Apply a staged rootfs delta to mountfs.qcow2 without libguestfs.

The updater intentionally uses only non-root, non-appliance tools:

* qemu-storage-daemon exposes the qcow2 partition as a writable raw FUSE file.
* xfs_db reads existing inode metadata and recalculates inode CRCs.
* Python writes changed regular-file bytes directly into already allocated XFS
  data extents.

This is a delta updater, not a general XFS mutator. If a change requires
allocating new XFS metadata/data blocks, creating paths, changing symlinks, or
deleting stale paths, the script fails and asks for a rootfs image recreate
rather than pretending a partial delta was applied.
"""

from __future__ import annotations

import argparse
import math
import os
import re
import shutil
import stat
import struct
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path


SECTOR_SIZE = 512
GPT_HEADER_LBA = 1
GPT_HEADER_SIZE = SECTOR_SIZE
XFS_REGULAR_MASK = 0o170000
XFS_REGULAR_MODE = 0o100000
ZERO_CHUNK = b"\0" * 1024 * 1024


@dataclass(frozen=True)
class Partition:
    start_lba: int
    end_lba: int

    @property
    def offset_bytes(self) -> int:
        return self.start_lba * SECTOR_SIZE

    @property
    def size_bytes(self) -> int:
        return (self.end_lba - self.start_lba + 1) * SECTOR_SIZE


@dataclass(frozen=True)
class Extent:
    file_block: int
    fs_block: int
    block_count: int


@dataclass(frozen=True)
class InodeInfo:
    mode: int
    size: int
    nblocks: int
    block_size: int
    extents: tuple[Extent, ...]

    @property
    def capacity_bytes(self) -> int:
        if not self.extents:
            return 0
        last = max(ext.file_block + ext.block_count for ext in self.extents)
        return last * self.block_size


def run(args: list[str], *, input_text: str | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, input=input_text, text=True, capture_output=True, check=False)


def require_tool(name: str) -> None:
    if shutil.which(name) is None:
        raise RuntimeError(f"required tool not found: {name}")


def reject_xfs_db_path(path: str) -> None:
    if not path.startswith("/") or "\n" in path or "\t" in path or " " in path:
        raise RuntimeError(f"unsupported XFS path for delta updater: {path!r}")
    if "/../" in f"{path}/" or path == "/":
        raise RuntimeError(f"unsafe XFS path for delta updater: {path!r}")


def qemu_img_prefix(image: Path, sectors: int) -> bytes:
    with tempfile.NamedTemporaryFile() as out:
        result = run(
            [
                "qemu-img",
                "dd",
                "-U",
                f"if={image}",
                f"of={out.name}",
                f"bs={SECTOR_SIZE}",
                f"count={sectors}",
            ]
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip() or f"qemu-img dd failed for {image}")
        return Path(out.name).read_bytes()


def parse_gpt_partition(image: Path, part_num: int) -> Partition:
    if part_num <= 0:
        raise RuntimeError("partition number must be positive")

    initial = qemu_img_prefix(image, 2048)
    if len(initial) < (GPT_HEADER_LBA + 1) * SECTOR_SIZE:
        raise RuntimeError(f"{image} is too small for a GPT header")

    header = initial[GPT_HEADER_LBA * SECTOR_SIZE : GPT_HEADER_LBA * SECTOR_SIZE + GPT_HEADER_SIZE]
    if header[:8] != b"EFI PART":
        raise RuntimeError(f"{image} does not have a primary GPT header")

    entry_lba = struct.unpack_from("<Q", header, 72)[0]
    entry_count = struct.unpack_from("<I", header, 80)[0]
    entry_size = struct.unpack_from("<I", header, 84)[0]
    if part_num > entry_count:
        raise RuntimeError(f"{image} has no GPT partition {part_num}")
    if entry_size < 128:
        raise RuntimeError(f"{image} has unsupported GPT entry size {entry_size}")

    entry_end = (entry_lba * SECTOR_SIZE) + (part_num * entry_size)
    sectors = max(2048, math.ceil(entry_end / SECTOR_SIZE))
    data = initial if len(initial) >= entry_end else qemu_img_prefix(image, sectors)
    entry_offset = (entry_lba * SECTOR_SIZE) + ((part_num - 1) * entry_size)
    entry = data[entry_offset : entry_offset + entry_size]
    if entry[0:16] == b"\0" * 16:
        raise RuntimeError(f"{image} GPT partition {part_num} is empty")

    start_lba = struct.unpack_from("<Q", entry, 32)[0]
    end_lba = struct.unpack_from("<Q", entry, 40)[0]
    if start_lba == 0 or end_lba < start_lba:
        raise RuntimeError(f"{image} GPT partition {part_num} has invalid bounds")
    return Partition(start_lba, end_lba)


class QcowPartitionFuse:
    def __init__(self, disk: Path, partition: Partition, temp_dir: Path) -> None:
        self.disk = disk
        self.partition = partition
        self.temp_dir = temp_dir
        self.part_file = temp_dir / "rootfs.part"
        self.pid_file = temp_dir / "qemu-storage-daemon.pid"

    def __enter__(self) -> Path:
        self.part_file.touch()
        command = [
            "qemu-storage-daemon",
            "--daemonize",
            "--pidfile",
            str(self.pid_file),
            "--blockdev",
            f"driver=file,node-name=file,filename={self.disk}",
            "--blockdev",
            "driver=qcow2,node-name=qcow2,file=file",
            "--blockdev",
            f"driver=raw,node-name=part,file=qcow2,offset={self.partition.offset_bytes},size={self.partition.size_bytes}",
            "--export",
            f"type=fuse,id=part,node-name=part,mountpoint={self.part_file},writable=on,allow-other=off",
        ]
        result = run(command)
        if result.returncode != 0:
            detail = (result.stderr or result.stdout).strip()
            raise RuntimeError(detail or "qemu-storage-daemon FUSE export failed")
        return self.part_file

    def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[no-untyped-def]
        for tool in ("fusermount3", "fusermount"):
            if shutil.which(tool) is not None:
                run([tool, "-u", str(self.part_file)])
                break
        if self.pid_file.exists():
            try:
                pid = int(self.pid_file.read_text(encoding="utf-8").strip())
                os.kill(pid, 15)
            except (OSError, ValueError):
                pass


def xfs_db(partition_file: Path, commands: list[str], *, write: bool = False) -> str:
    args = ["xfs_db", "-f"]
    if write:
        args.append("-x")
    else:
        args.append("-r")
    for command in commands:
        args.extend(["-c", command])
    args.append(str(partition_file))
    result = run(args)
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        raise RuntimeError(detail or f"xfs_db failed while running {commands!r}")
    return result.stdout


def parse_int_field(output: str, name: str, *, base: int = 10) -> int:
    match = re.search(rf"^{re.escape(name)} = ([^\s]+)", output, re.MULTILINE)
    if match is None:
        raise RuntimeError(f"xfs_db output missing {name}")
    value = match.group(1)
    return int(value, base)


def read_inode_info(partition_file: Path, target: str) -> InodeInfo:
    reject_xfs_db_path(target)
    output = xfs_db(
        partition_file,
        [
            "sb 0",
            "p blocksize",
            f"path {target}",
            "p core.mode core.size core.nblocks core.format",
            "bmap",
        ],
    )
    if "No such file or directory" in output:
        raise RuntimeError(f"{target} is not present in the XFS image")

    block_size = parse_int_field(output, "blocksize")
    mode = parse_int_field(output, "core.mode", base=8)
    size = parse_int_field(output, "core.size")
    nblocks = parse_int_field(output, "core.nblocks")
    extents: list[Extent] = []
    for match in re.finditer(r"^data offset (\d+) startblock (\d+) .* count (\d+) flag", output, re.MULTILINE):
        extents.append(Extent(int(match.group(1)), int(match.group(2)), int(match.group(3))))
    return InodeInfo(mode, size, nblocks, block_size, tuple(extents))


def zero_range(handle, start: int, length: int) -> None:  # type: ignore[no-untyped-def]
    handle.seek(start)
    remaining = length
    while remaining > 0:
        chunk = ZERO_CHUNK if remaining >= len(ZERO_CHUNK) else ZERO_CHUNK[:remaining]
        handle.write(chunk)
        remaining -= len(chunk)


def copy_regular_file(partition_file: Path, source: Path, target: str, mode: int) -> None:
    info = read_inode_info(partition_file, target)
    if (info.mode & XFS_REGULAR_MASK) != XFS_REGULAR_MODE:
        raise RuntimeError(f"{target} exists but is not a regular file")
    size = source.stat().st_size
    if size > info.capacity_bytes:
        raise RuntimeError(
            f"{target} grew to {size} bytes but only {info.capacity_bytes} bytes are allocated; recreate mountfs.qcow2"
        )
    if size > 0 and not info.extents:
        raise RuntimeError(f"{target} has no allocated data extents; recreate mountfs.qcow2")

    written = 0
    with source.open("rb") as src, partition_file.open("r+b", buffering=0) as dst:
        for extent in sorted(info.extents, key=lambda item: item.file_block):
            file_start = extent.file_block * info.block_size
            extent_bytes = extent.block_count * info.block_size
            if file_start >= size:
                break
            to_copy = min(extent_bytes, size - file_start)
            if to_copy <= 0:
                continue
            src.seek(file_start)
            dst.seek(extent.fs_block * info.block_size)
            remaining = to_copy
            while remaining > 0:
                chunk = src.read(min(1024 * 1024, remaining))
                if not chunk:
                    raise RuntimeError(f"short read while copying {source}")
                dst.write(chunk)
                remaining -= len(chunk)
            written += to_copy

        if written != size:
            raise RuntimeError(f"{target} has a hole in its allocated extent map; recreate mountfs.qcow2")

        tail_end = min(((size + info.block_size - 1) // info.block_size) * info.block_size, info.capacity_bytes)
        if tail_end > size:
            eof_block = size // info.block_size
            eof_extent = next(
                (
                    ext
                    for ext in info.extents
                    if ext.file_block <= eof_block < ext.file_block + ext.block_count
                ),
                None,
            )
            if eof_extent is None:
                raise RuntimeError(f"{target} EOF falls in an unallocated hole; recreate mountfs.qcow2")
            block_offset = size - (eof_extent.file_block * info.block_size)
            zero_range(dst, (eof_extent.fs_block * info.block_size) + block_offset, tail_end - size)
        dst.flush()
        os.fsync(dst.fileno())

    xfs_db(
        partition_file,
        [
            f"path {target}",
            f"write core.size {size}",
            f"write core.mode 0{(XFS_REGULAR_MODE | mode):o}",
            "crc -r",
        ],
        write=True,
    )


def verify_existing_directory(partition_file: Path, target: str, mode: int) -> None:
    info = read_inode_info(partition_file, target)
    if not stat.S_ISDIR(info.mode):
        raise RuntimeError(f"{target} exists but is not a directory")
    xfs_db(partition_file, [f"path {target}", f"write core.mode 0{(0o040000 | mode):o}", "crc -r"], write=True)


def changed_relpaths(paths_file: Path) -> list[str]:
    return [line.strip() for line in paths_file.read_text(encoding="utf-8").splitlines() if line.strip()]


def apply_delta(args: argparse.Namespace) -> int:
    disk = args.disk.resolve()
    staging = args.staging.resolve()
    require_tool("qemu-img")
    require_tool("qemu-storage-daemon")
    require_tool("xfs_db")
    if shutil.which("fusermount3") is None and shutil.which("fusermount") is None:
        raise RuntimeError("required tool not found: fusermount3 or fusermount")

    partition = parse_gpt_partition(disk, args.partition)
    relpaths = changed_relpaths(args.paths)
    with tempfile.TemporaryDirectory(prefix="wos-rootfs-qcow-xfs.") as temp:
        with QcowPartitionFuse(disk, partition, Path(temp)) as partition_file:
            for rel in relpaths:
                source = staging / rel
                target = "/" + rel
                if source.is_symlink():
                    raise RuntimeError(f"{target} is a symlink delta; recreate mountfs.qcow2")
                if source.is_dir():
                    verify_existing_directory(partition_file, target, stat.S_IMODE(source.stat().st_mode))
                elif source.is_file():
                    copy_regular_file(partition_file, source, target, stat.S_IMODE(source.stat().st_mode))
                else:
                    raise RuntimeError(f"{target} is a deletion or missing staged path; recreate mountfs.qcow2")
    print(f"  rootfs sync: xfs qcow delta applied: {len(relpaths)} paths")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--disk", type=Path, required=True)
    parser.add_argument("--staging", type=Path, required=True)
    parser.add_argument("--paths", type=Path, required=True)
    parser.add_argument("--partition", type=int, default=1)
    return parser.parse_args()


def main() -> int:
    try:
        return apply_delta(parse_args())
    except RuntimeError as exc:
        print(f"rootfs_qcow_xfs_delta.py: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
