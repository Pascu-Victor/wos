#!/usr/bin/env python3

"""Source invariants for buffer-cache cold-miss locking behavior."""

from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
BUFFER_CACHE = ROOT / "modules/kern/src/vfs/buffer_cache.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(
        rf"\b(?:auto|void)\s+{re.escape(name)}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>*]+)?\s*\{{",
        source,
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


def require(source: str, token: str, context: str) -> None:
    if token not in source:
        fail(f"{context}: missing {token!r}")


def require_absent(source: str, token: str, context: str) -> None:
    if token in source:
        fail(f"{context}: unexpected {token!r}")


def require_order(source: str, tokens: list[str], context: str) -> None:
    cursor = 0
    for token in tokens:
        found = source.find(token, cursor)
        if found < 0:
            fail(f"{context}: missing ordered token {token!r}")
        cursor = found + len(token)


def main() -> None:
    source = BUFFER_CACHE.read_text()

    require_absent(source, "evict_lru();", "cold miss eviction must be allocation-sized")
    require_absent(source, "alloc_buffer_with_size", "buffers must be detached until inserted")
    require(
        source,
        "constexpr size_t BUFFER_CACHE_CONTIG_ALLOC_MAX_BYTES = size_t{2} * 1024 * 1024;",
        "buffer-cache contiguous allocation cap",
    )
    require(
        source,
        "constexpr size_t DIRTY_WRITEBACK_RUN_MAX_BYTES = BUFFER_CACHE_CONTIG_ALLOC_MAX_BYTES;",
        "dirty writeback must share the contiguous allocation cap",
    )
    require(
        source,
        "constexpr size_t BUFFER_CACHE_MEMORY_DIVISOR = 4;",
        "buffer-cache max must leave headroom for non-cache kernel allocations",
    )
    require(
        source,
        "constexpr size_t DIRTY_TARGET_MEMORY_DIVISOR = 20;",
        "dirty target must stay below global memory pressure thresholds",
    )
    require(
        source,
        "constexpr size_t BUFFER_CACHE_MAX_SIZE = size_t{1} * 1024 * 1024 * 1024;",
        "buffer-cache max must be capped until global reclaim is mature",
    )
    require(
        source,
        "constexpr size_t HOT_EVICT_MAX_BYTES = size_t{16} * 1024 * 1024;",
        "hot cache reclaim must repay bounded chunks of old clean-cache debt",
    )

    allocate_body = function_body(source, "allocate_buffer_data")
    require_order(
        allocate_body,
        [
            "flags &= ~BH_DATA_PAGE_ALLOC",
            "if (size > BUFFER_CACHE_CONTIG_ALLOC_MAX_BYTES)",
            "return nullptr",
            "ker::mod::mm::phys::page_alloc(size, \"buffer_cache\")",
        ],
        "buffer data allocation must reject overlarge contiguous buffers before page allocation",
    )

    hard_limit_body = function_body(source, "choose_dirty_hard_limit_bytes")
    require_order(
        hard_limit_body,
        [
            "if (target_bytes > max_bytes / 2)",
            "return max_bytes",
            "return std::min(max_bytes, target_bytes * 2)",
        ],
        "dirty hard limit must be bounded to about twice the dirty target",
    )

    evict_body = function_body(source, "evict_lru_for_allocation")
    require_order(
        evict_body,
        [
            "cache_allocation_would_exceed_limit_locked(incoming_bytes)",
            "TARGET_BYTES",
            "OVERAGE",
            "BYTE_BUDGET",
            "reclaim_clean_cache_locked(TARGET_BYTES, BYTE_BUDGET, HOT_EVICT_MAX_VICTIMS, HOT_EVICT_SCAN_BUDGET, false)",
        ],
        "hot eviction should repay bounded clean-cache overage",
    )

    reclaim_body = function_body(source, "reclaim_clean_cache_locked")
    require_order(
        reclaim_body,
        [
            "cache_total_bytes > target_bytes",
            "stats.freed_bytes < byte_budget",
            "find_reclaimable_lru_buffer(scan_budget, honor_second_chance)",
            "stats.freed_bytes += victim->size",
            "free_buffer(victim)",
        ],
        "shared clean-cache reclaim should honor byte and victim budgets",
    )

    writeback_body = function_body(source, "writeback_dirty_one_after")
    require_order(
        writeback_body,
        [
            "brelse(run.buffers.at(i))",
            "if (result.status == 0)",
            "reclaim_clean_cache_over_limit_locked()",
        ],
        "successful dirty writeback should shrink clean cache after releasing writeback refs",
    )

    account_body = function_body(source, "account_buffer_locked")
    require(account_body, "cache_total_bytes += bh->size", "cache accounting")
    require(account_body, "cache_total_buffers++", "cache accounting")

    insert_body = function_body(source, "insert_allocated_buffer_locked")
    require_order(
        insert_body,
        ["account_buffer_locked(bh)", "hash_insert(bh)", "lru_touch(bh)"],
        "insert must account before indexing a prepared buffer",
    )

    for name, allocator, size_token in [
        ("bread", "alloc_detached_buffer(bdev, block_no)", "bdev->block_size"),
        ("bread_multi", "alloc_detached_buffer_with_size(bdev, block_no, TOTAL_SIZE, 0)", "TOTAL_SIZE"),
        ("bget", "alloc_detached_buffer(bdev, block_no)", "bdev->block_size"),
        ("bget_multi", "alloc_detached_buffer_with_size(bdev, block_no, TOTAL_SIZE, BH_VALID)", "TOTAL_SIZE"),
    ]:
        body = function_body(source, name)
        require_order(
            body,
            [
                f"evict_lru_for_allocation({size_token})",
                "cache_lock.unlock_irqrestore(irqflags)",
                allocator,
                "irqflags = cache_lock.lock_irqsave()",
                "BufHead* existing = hash_lookup",
                "free_detached_buffer(bh)",
                "insert_allocated_buffer_locked(bh)",
            ],
            f"{name} cold miss must allocate detached and recheck before insert",
        )

    print("buffer cache cold-miss locking invariants hold")


if __name__ == "__main__":
    main()
