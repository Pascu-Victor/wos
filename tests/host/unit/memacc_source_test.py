#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
MEMACC_IO_CPP = ROOT / "modules" / "memacc" / "src" / "procfs_io.cpp"
MEMACC_IO_HPP = ROOT / "modules" / "memacc" / "src" / "procfs_io.hpp"
PROCFS_CPP = ROOT / "modules" / "kern" / "src" / "vfs" / "fs" / "procfs.cpp"
PHYS_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "mm" / "phys.opt.cpp"
OOM_DUMP_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "mm" / "oom_dump.cpp"
PACKET_CPP = ROOT / "modules" / "kern" / "src" / "net" / "packet.cpp"
E1000_CPP = ROOT / "modules" / "kern" / "src" / "dev" / "e1000e" / "e1000e.cpp"
VIRT_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "mm" / "virt.opt.cpp"
VMEM_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "vmem" / "sys_vmem.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(
        rf"\b(?:auto|void|int)\s+{name}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>,\s*&]+)?\s*\{{",
        source,
        flags=re.DOTALL,
    )
    if match is None:
        fail(f"missing function {name}")

    depth = 1
    pos = match.end()
    while pos < len(source) and depth > 0:
        if source[pos] == "{":
            depth += 1
        elif source[pos] == "}":
            depth -= 1
        pos += 1
    if depth != 0:
        fail(f"unterminated function {name}")
    return source[match.end() : pos - 1]


def require_tokens(source: str, tokens: list[str], context: str) -> None:
    missing = [token for token in tokens if token not in source]
    if missing:
        fail(f"{context}: missing {', '.join(missing)}")


def require_absent(source: str, tokens: list[str], context: str) -> None:
    present = [token for token in tokens if token in source]
    if present:
        fail(f"{context}: forbidden {', '.join(present)}")


def test_memacc_reads_are_byte_capped() -> None:
    source = MEMACC_IO_CPP.read_text()
    header = MEMACC_IO_HPP.read_text()
    require_tokens(
        header,
        [
            "READ_CHUNK_CAPACITY",
            "MEMACC_READ_LIMIT",
            "auto read_file(std::string_view path, size_t max_bytes = MEMACC_READ_LIMIT) -> std::optional<std::string>",
        ],
        "memacc bounded read surface",
    )

    read_file_body = function_body(source, "read_file")
    require_tokens(
        read_file_body,
        [
            "max_bytes - out.size()",
            "read(fd.get(), &extra, 1)",
            "COUNT < 0 && errno == EINTR",
            "COUNT < 0 || COUNT > 0",
            "return std::nullopt",
            "read(fd.get(), buf.data(), std::min(buf.size(), REMAINING))",
            "out.append(buf.data(), static_cast<size_t>(COUNT))",
        ],
        "memacc bounded read loop",
    )
    if "char buf[4096]" in read_file_body:
        fail("memacc read_file must not use an uncapped raw buffer loop")


def test_physical_balance_is_exact_and_concrete() -> None:
    procfs = PROCFS_CPP.read_text()
    summary = function_body(procfs, "generate_memacc_summary")
    require_tokens(
        summary,
        [
            "get_physical_balance_snapshot(balance)",
            '"schema", 2',
            '"identity_mismatch_pages"',
            '"untracked_unreclaimable_pages"',
            'append_sconst(p, end, "physical_owner")',
            '"objects"',
            '"lifetime"',
            '"reclaimability"',
            '"scaling_bound"',
            '"physical_allocator_embedded_metadata"',
            '"per_cpu_physical_page_cache_metadata"',
        ],
        "exact physical balance export",
    )
    require_absent(
        summary,
        [
            "unaccounted_estimate",
            '"unknown"',
            '"other"',
            '"estimated"',
        ],
        "physical balance must not contain residual categories",
    )

    snapshot = function_body(PHYS_CPP.read_text(), "get_physical_balance_snapshot")
    require_tokens(
        snapshot,
        [
            "allocators.at(i)->lock_irq()",
            "physical balance zone bound exceeded",
            "PhysicalBalanceComponents const COMPONENTS",
            "out.identity_pages = physical_balance_identity_pages(COMPONENTS)",
            "out.identity_mismatch_pages = physical_balance_mismatch_pages(COMPONENTS)",
            "out.untracked_unreclaimable_pages = out.identity_mismatch_pages",
        ],
        "coherent bounded physical snapshot",
    )
    require_tokens(
        function_body(PHYS_CPP.read_text(), "init"),
        ['panic_handler("unsupported physical reserve memory-map type")'],
        "physical reserve accounting must fail closed",
    )

    oom_dump = function_body(OOM_DUMP_CPP.read_text(), "dump_page_allocations_oom")
    require_tokens(
        oom_dump,
        [
            "get_physical_balance_snapshot(physical_balance)",
            '"Managed-page equation: total="',
            '"Physical-address equation: total="',
            '" untracked_unreclaimable="',
            "physical_owner_descriptors()",
            "physical_reserve_descriptors()",
        ],
        "exact emergency physical balance export",
    )
    require_absent(
        oom_dump,
        ["unaccounted", "unknown physical", "estimated physical"],
        "emergency physical balance must not contain residual categories",
    )


def test_pressure_reclaim_is_real_and_preserves_network_reserve() -> None:
    packet = PACKET_CPP.read_text()
    require_tokens(
        packet,
        [
            "PacketBuffer* reserve_free_list",
            "PacketBuffer* reclaimable_free_list",
            "auto pkt_global_alloc(bool prefer_reserve)",
            "auto pkt_alloc_rx()",
        ],
        "packet reserve and reclaimable free-list separation",
    )
    require_tokens(
        function_body(packet, "pkt_global_alloc"),
        [
            "(prefer_reserve && reserve_free_list != nullptr) || reclaimable_free_list == nullptr",
            "USE_RESERVE ? &reserve_free_list : &reclaimable_free_list",
        ],
        "persistent RX reserve preference with availability fallback",
    )
    e1000 = E1000_CPP.read_text()
    require_tokens(function_body(e1000, "init_rx"), ["ker::net::pkt_alloc_rx()"], "e1000 persistent RX allocation")
    require_tokens(function_body(e1000, "process_rx_budget"), ["ker::net::pkt_alloc_rx()"], "e1000 RX replacement allocation")

    reclaim = function_body(packet, "pkt_pool_reclaim_free")
    require_tokens(
        reclaim,
        [
            "target_capacity = std::max(target_capacity, pool_reserve_capacity)",
            "!chunk->reclaimable || chunk->free != chunk->count",
            "pool_capacity - chunk->count < target_capacity",
            "free_count.fetch_sub(chunk->count",
            "free_packet_buffer_array(chunk->buffers, chunk->count)",
            "chunk->draining = true",
            "free_count.fetch_sub(removed",
            "active_capacity - chunk->count < pool_reserve_capacity",
            "stats.marked_draining_buffers += chunk->count",
        ],
        "packet growth-chunk pressure reclaim",
    )

    procfs = PROCFS_CPP.read_text()
    require_tokens(
        procfs,
        [
            '"memacc/reclaim/file_mmap_cache"',
            "file_mmap_cache_reclaim",
            '"memacc/reclaim/xfs_inode"',
            "xfs_icache_reclaim_for_pressure",
        ],
        "controlled cache pressure interfaces",
    )


def test_persistent_and_transferred_pages_have_lifetime_owners() -> None:
    virt = VIRT_CPP.read_text()
    require_tokens(
        function_body(virt, "try_alloc_page_table_from_pool"),
        ["page_reassign_owner(table, PhysicalPageOwner::PAGE_TABLE)"],
        "active page-table ownership",
    )
    require_tokens(
        function_body(virt, "try_release_page_table_to_pool"),
        ["page_reassign_owner(table, PhysicalPageOwner::PAGE_TABLE_POOL_RESERVE)"],
        "page-table pool reserve ownership",
    )

    vmem = VMEM_CPP.read_text()
    require_tokens(
        function_body(vmem, "get_anon_zero_page"),
        ["PhysicalPageOwner::ANON_ZERO_PAGE_RESERVE"],
        "anonymous zero-page reserve ownership",
    )
    require_tokens(
        function_body(vmem, "release_file_mmap_cache_page"),
        ["page_reassign_owner(page, ker::mod::mm::PhysicalPageOwner::USER_FILE_MAPPING)", "page_ref_dec(page)"],
        "evicted file-cache ownership transfer",
    )


def main() -> None:
    test_memacc_reads_are_byte_capped()
    test_physical_balance_is_exact_and_concrete()
    test_pressure_reclaim_is_real_and_preserves_network_reserve()
    test_persistent_and_transferred_pages_have_lifetime_owners()
    print("memacc reads and exact physical balance invariants hold")


if __name__ == "__main__":
    main()
