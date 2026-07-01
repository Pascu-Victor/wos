#!/usr/bin/env python3

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
DEVFS_CPP = ROOT / "modules" / "kern" / "src" / "vfs" / "fs" / "devfs.cpp"
TMPFS_CPP = ROOT / "modules" / "kern" / "src" / "vfs" / "fs" / "tmpfs.cpp"
PROCFS_CPP = ROOT / "modules" / "kern" / "src" / "vfs" / "fs" / "procfs.cpp"
FAT32_CPP = ROOT / "modules" / "kern" / "src" / "vfs" / "fs" / "fat32.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def require(source: str, snippet: str, context: str) -> None:
    if snippet not in source:
        fail(f"{context}: missing {snippet}")


def test_devfs_open_preserves_flags() -> None:
    source = DEVFS_CPP.read_text()
    require(source, "auto devfs_open_path(const char* path, int flags, int /*mode*/) -> File*", "devfs open signature")
    require(source, "file->open_flags = flags;", "devfs File::open_flags")
    require(source, "file->fd_flags = 0;", "devfs File::fd_flags")


def test_tmpfs_open_preserves_flags() -> None:
    source = TMPFS_CPP.read_text()
    require(source, "auto create_root_file_with_flags(TmpNode* root, int open_flags) -> ker::vfs::File*", "tmpfs root helper")
    require(source, "auto create_root_file(TmpNode* root) -> ker::vfs::File*", "tmpfs public root helper")
    require(source, "f->open_flags = open_flags;", "tmpfs root open flags")
    if source.count("return create_root_file_with_flags(root, flags);") < 2:
        fail("tmpfs root opens must pass caller flags into create_root_file_with_flags")
    require(source, "f->open_flags = flags;", "tmpfs non-root open flags")
    require(source, "f->fd_flags = 0;", "tmpfs fd flags")


def test_procfs_open_preserves_flags() -> None:
    source = PROCFS_CPP.read_text()
    require(source, "auto procfs_open_path(const char* path, int flags, int mode) -> File*", "procfs open signature")
    require(source, "auto make_file = [flags]", "procfs make_file captures open flags")
    require(source, "f->open_flags = flags;", "procfs File::open_flags")
    require(source, "f->fd_flags = 0;", "procfs File::fd_flags")


def test_fat32_open_preserves_flags() -> None:
    source = FAT32_CPP.read_text()
    require(
        source,
        "static auto create_file_in_directory(FAT32MountContext* ctx, uint32_t parent_cluster, const char* filename, int open_flags)",
        "fat32 create helper signature",
    )
    require(source, "f->open_flags = open_flags;", "fat32 O_CREAT helper open flags")
    require(source, "return create_file_in_directory(ctx, current_cluster, component.data(), flags);", "fat32 O_CREAT path flags")
    require(source, "file->open_flags = flags;", "fat32 root open flags")
    require(source, "f->open_flags = flags;", "fat32 existing path open flags")


if __name__ == "__main__":
    test_devfs_open_preserves_flags()
    test_tmpfs_open_preserves_flags()
    test_procfs_open_preserves_flags()
    test_fat32_open_preserves_flags()
    print("VFS backend open flag invariants hold")
