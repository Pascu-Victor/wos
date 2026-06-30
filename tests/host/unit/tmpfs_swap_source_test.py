#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
TMPFS_HPP = ROOT / "modules" / "kern" / "src" / "vfs" / "fs" / "tmpfs.hpp"
TMPFS_CPP = ROOT / "modules" / "kern" / "src" / "vfs" / "fs" / "tmpfs.cpp"
SWAP_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "mm" / "swap.cpp"
XFS_CPP = ROOT / "modules" / "kern" / "src" / "vfs" / "fs" / "xfs" / "xfs_vfs.cpp"
SYS_VFS_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "vfs" / "sys_vfs.cpp"
SYS_VMEM_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "vmem" / "sys_vmem.cpp"
KERNEL_VMEM_ABI = ROOT / "modules" / "kern" / "src" / "abi" / "callnums" / "vmem.h"
MLIBC_VFS_H = ROOT / "toolchain" / "src" / "mlibc" / "sysdeps" / "wos" / "include" / "sys" / "vfs.h"
MLIBC_VMEM_H = ROOT / "toolchain" / "src" / "mlibc" / "sysdeps" / "wos" / "include" / "sys" / "vmem.h"
MLIBC_SYSDEPS_CPP = ROOT / "toolchain" / "src" / "mlibc" / "sysdeps" / "wos" / "generic" / "sysdeps.cpp"
MLIBC_SYSDEPS_HPP = ROOT / "toolchain" / "src" / "mlibc" / "sysdeps" / "wos" / "include" / "mlibc" / "sysdeps.hpp"
MLIBC_SWAP_H = ROOT / "toolchain" / "src" / "mlibc" / "options" / "wos" / "include" / "sys" / "swap.h"
INIT_FSTAB = ROOT / "modules" / "init" / "src" / "fstab.cpp"
PROCFS_CPP = ROOT / "modules" / "kern" / "src" / "vfs" / "fs" / "procfs.cpp"
MOUNT_CPP = ROOT / "modules" / "kern" / "src" / "vfs" / "mount.cpp"
STRACE_SRC_DIR = ROOT / "modules" / "strace" / "src"


def fail(message: str) -> None:
    raise AssertionError(message)


def read_strace_source() -> str:
    paths = [*sorted(STRACE_SRC_DIR.glob("*.cpp")), *sorted(STRACE_SRC_DIR.glob("*.hpp"))]
    return "\n".join(path.read_text() for path in paths)


def function_body(source: str, name_pattern: str) -> str:
    match = re.search(rf"{name_pattern}\s*\{{", source, flags=re.DOTALL)
    if match is None:
        fail(f"missing function matching {name_pattern}")

    depth = 1
    pos = match.end()
    while pos < len(source) and depth > 0:
        if source[pos] == "{":
            depth += 1
        elif source[pos] == "}":
            depth -= 1
        pos += 1
    if depth != 0:
        fail(f"unterminated function matching {name_pattern}")
    return source[match.end() : pos - 1]


def require_tokens(source: str, tokens: list[str], context: str) -> None:
    missing = [token for token in tokens if token not in source]
    if missing:
        fail(f"{context}: missing {', '.join(missing)}")


def require_order(source: str, first: str, second: str, context: str) -> None:
    first_index = source.find(first)
    second_index = source.find(second)
    if first_index < 0:
        fail(f"{context}: missing {first}")
    if second_index < 0:
        fail(f"{context}: missing {second}")
    if first_index >= second_index:
        fail(f"{context}: expected {first} before {second}")


def test_mount_data_and_tmpfs_size_cap_are_wired() -> None:
    sys_vfs = SYS_VFS_CPP.read_text()
    mlibc_vfs = MLIBC_VFS_H.read_text()
    mount_cpp = MOUNT_CPP.read_text()
    tmpfs = TMPFS_CPP.read_text()

    require_tokens(
        sys_vfs,
        [
            "unsigned long const FLAGS = static_cast<unsigned long>(a4);",
            "data = reinterpret_cast<const char*>(a5);",
            "ker::vfs::vfs_mount(source, target, fstype, FLAGS, data)",
        ],
        "kernel mount syscall must forward flags and data",
    )
    require_tokens(
        mlibc_vfs,
        [
            "static inline int mount(",
            "unsigned long flags = 0,",
            "const void *data = nullptr",
            "static_cast<uint64_t>(flags)",
            "reinterpret_cast<uint64_t>(data)",
        ],
        "mlibc WOS mount ABI wrapper",
    )
    require_tokens(
        mount_cpp,
        [
            "create_mount_context(root, data, ROOT_COMPAT, &tmpfs_error)",
            "destroy_mount_private_data",
        ],
        "VFS tmpfs mount context ownership",
    )
    require_tokens(
        tmpfs,
        [
            "parse_mount_options",
            'std::strncmp(START, "size=", SIZE_PREFIX_LEN) == 0',
            "root_compat ? 0 : static_cast<size_t>(ker::mod::mm::phys::get_total_mem_bytes() / 2)",
            "mount_try_charge",
            "mount_uncharge",
            "tmpfs_statvfs",
        ],
        "tmpfs size option and accounting",
    )


def test_tmpfs_page_model_keeps_sparse_pages_and_swaps_cold_resident_pages() -> None:
    header = TMPFS_HPP.read_text()
    source = TMPFS_CPP.read_text()

    require_tokens(
        header,
        [
            "enum class TmpPageState : uint8_t { HOLE, RESIDENT, SWAPPED };",
            "ker::mod::mm::swap::SwapSlot swap_slot{};",
            "TmpfsMount* mount = nullptr;",
            "size_t charged_pages = 0;",
        ],
        "tmpfs page descriptor ABI",
    )
    require_tokens(
        source,
        [
            "std::memset(data, 0, DEFAULT_TMPFS_BLOCK_SIZE);",
            "if (PAGE_INDEX >= n->page_count || n->pages[PAGE_INDEX].state == TmpPageState::HOLE)",
            "ker::mod::mm::swap::read_slot(page.swap_slot, data)",
            "ker::mod::mm::swap::write_slot(slot, page.data)",
            "auto tmpfs_reclaim_pages(size_t target_pages) -> size_t",
        ],
        "tmpfs sparse/read/swap page flow",
    )
    evict_body = function_body(source, r"auto\s+evict_page_locked\([^)]*\)\s*->\s*int")
    require_order(evict_body, "swap::write_slot(slot, page.data)", "phys::page_free(page.data)", "tmpfs evicts only after swap write")


def test_swap_backings_are_local_block_or_xfs_and_swapoff_checks_use() -> None:
    swap = SWAP_CPP.read_text()
    xfs = XFS_CPP.read_text()

    require_tokens(
        swap,
        [
            "ker::vfs::mounted_block_device_overlaps(device)",
            "vfs_open_file(path, O_RDWR | ker::vfs::O_LOCAL | ker::vfs::O_NO_CACHE, 0)",
            "file->fs_type != ker::vfs::FSType::XFS",
            "xfs_collect_swap_extents(file, &extents, &extent_count)",
            "area->used_pages != 0",
            "return -EBUSY;",
        ],
        "swap backing validation and swapoff busy behavior",
    )
    require_tokens(
        xfs,
        [
            "auto xfs_collect_swap_extents",
            "ctx->read_only",
            "!xfs_inode_isreg(ip)",
            "bmap.is_hole",
            "bmap.unwritten",
            "page_count",
        ],
        "XFS swap file extent validation",
    )


def test_swapon_swapoff_are_appended_kernel_and_mlibc_abi_ops() -> None:
    kernel_abi = KERNEL_VMEM_ABI.read_text()
    mlibc_vmem = MLIBC_VMEM_H.read_text()
    sys_vmem = SYS_VMEM_CPP.read_text()
    sysdeps_cpp = MLIBC_SYSDEPS_CPP.read_text()
    sysdeps_hpp = MLIBC_SYSDEPS_HPP.read_text()
    swap_h = MLIBC_SWAP_H.read_text()
    strace = read_strace_source()

    require_order(kernel_abi, "MSYNC,", "SWAPON,", "kernel VMEM ABI appends SWAPON after MSYNC")
    require_order(kernel_abi, "SWAPON,", "SWAPOFF,", "kernel VMEM ABI appends SWAPOFF after SWAPON")
    require_tokens(
        mlibc_vmem,
        [
            "msync = 4,",
            "swapon = 5,",
            "swapoff = 6,",
            "static inline int64_t swapon(const char *path, int flags)",
            "static inline int64_t swapoff(const char *path)",
        ],
        "mlibc VMEM wrapper ABI",
    )
    require_tokens(
        sys_vmem,
        [
            "case ker::abi::vmem::ops::SWAPON:",
            "case ker::abi::vmem::ops::SWAPOFF:",
            "ker::mod::mm::swap::swapon_path(reinterpret_cast<const char*>(a1), static_cast<int>(a2))",
            "ker::mod::mm::swap::swapoff_path(reinterpret_cast<const char*>(a1))",
        ],
        "kernel VMEM swap syscall dispatch",
    )
    require_tokens(
        sysdeps_cpp + sysdeps_hpp + swap_h + strace,
        [
            "Sysdeps<Swapon>",
            "Sysdeps<Swapoff>",
            "int swapon(const char *__path, int __flags);",
            "int swapoff(const char *__path);",
            'return "swapon";',
            'return "swapoff";',
        ],
        "userspace swap integration",
    )


def test_init_fstab_and_proc_meminfo_consume_swap_stats() -> None:
    init = INIT_FSTAB.read_text()
    procfs = PROCFS_CPP.read_text()

    require_tokens(
        init,
        [
            'std::strcmp(fstype.data(), "swap") == 0',
            "ker::vmem::swapon(device.data(), 0)",
            "ker::abi::vfs::mount(device.data(), mountpoint.data(), fstype.data(), 0, mount_options)",
        ],
        "init fstab swap and mount option handling",
    )
    require_tokens(
        procfs,
        [
            "ker::mod::mm::swap::SwapStats swap_stats{};",
            "ker::mod::mm::swap::get_stats(&swap_stats);",
            'append_kb_line("SwapCached", swap_stats.cached_bytes);',
            'append_kb_line("SwapTotal", swap_stats.total_bytes);',
            'append_kb_line("SwapFree", swap_stats.free_bytes);',
        ],
        "/proc/meminfo swap counters",
    )


if __name__ == "__main__":
    test_mount_data_and_tmpfs_size_cap_are_wired()
    test_tmpfs_page_model_keeps_sparse_pages_and_swaps_cold_resident_pages()
    test_swap_backings_are_local_block_or_xfs_and_swapoff_checks_use()
    test_swapon_swapoff_are_appended_kernel_and_mlibc_abi_ops()
    test_init_fstab_and_proc_meminfo_consume_swap_stats()
    print("tmpfs swap source invariants hold")
