#!/usr/bin/env python3

"""Source invariants for buffer-cache cold-miss locking behavior."""

from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
BUFFER_CACHE = ROOT / "modules/kern/src/vfs/buffer_cache.cpp"
XFS_MOUNT = ROOT / "modules/kern/src/vfs/fs/xfs/xfs_mount.cpp"


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
    xfs_mount = XFS_MOUNT.read_text()

    require_absent(source, "evict_lru();", "cold miss eviction must be allocation-sized")
    require_absent(source, "alloc_buffer_with_size", "buffers must be detached until inserted")
    require(
        source,
        "constexpr size_t BUFFER_CACHE_CONTIG_ALLOC_MAX_BYTES = size_t{2} * 1024 * 1024;",
        "buffer-cache contiguous allocation cap",
    )
    require(
        source,
        "constexpr size_t BUFFER_CACHE_BUDDY_ALLOC_MAX_BYTES = size_t{64} * 1024;",
        "buffer-cache buddy allocation cap",
    )
    require(
        source,
        "constexpr size_t DIRTY_WRITEBACK_RUN_MAX_BYTES = BUFFER_CACHE_CONTIG_ALLOC_MAX_BYTES;",
        "dirty writeback must share the contiguous allocation cap",
    )
    require(
        source,
        "constexpr size_t BUFFER_CACHE_MEMORY_NUMERATOR = 7;",
        "buffer-cache max numerator",
    )
    require(
        source,
        "constexpr size_t BUFFER_CACHE_MEMORY_DENOMINATOR = 16;",
        "buffer-cache max denominator",
    )
    require(
        source,
        "constexpr size_t DIRTY_TARGET_MEMORY_DIVISOR = 8;",
        "normal-memory dirty target divisor",
    )
    require(
        source,
        "constexpr size_t DIRTY_TARGET_LARGE_MEMORY_DIVISOR = 4;",
        "large-memory dirty target divisor",
    )
    require(
        source,
        "constexpr uint64_t DIRTY_TARGET_LARGE_MEMORY_THRESHOLD = uint64_t{8} * 1024 * 1024 * 1024;",
        "large-memory dirty target threshold",
    )
    require(
        source,
        "constexpr size_t BUFFER_CACHE_MAX_SIZE = size_t{16} * 1024 * 1024 * 1024;",
        "buffer-cache max cap",
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
            "flags &= ~(BH_DATA_PAGE_ALLOC | BH_DATA_VMAP)",
            "if (size > BUFFER_CACHE_CONTIG_ALLOC_MAX_BYTES)",
            "return nullptr",
            "if (size > BUFFER_CACHE_BUDDY_ALLOC_MAX_BYTES)",
            'kernel_vmap_alloc(size, "buffer_cache")',
            "flags |= BH_DATA_VMAP",
            "ker::mod::mm::phys::page_alloc_full_overwrite_may_fail(size, \"buffer_cache\")",
            "if (page_data != nullptr)",
            "flags |= BH_DATA_PAGE_ALLOC",
            'kernel_vmap_alloc(size, "buffer_cache")',
            "flags |= BH_DATA_VMAP",
        ],
        "buffer data allocation must use order-0 virtual backing for large runs and as the bounded buddy fallback",
    )

    free_data_body = function_body(source, "free_data_buffer")
    require_order(
        free_data_body,
        [
            "if ((flags & BH_DATA_VMAP) != 0)",
            "kernel_vmap_free(data, size)",
            "if ((flags & BH_DATA_PAGE_ALLOC) != 0)",
            "page_free(data)",
            "delete[] data",
        ],
        "buffer data free must match virtual, page, and heap backing",
    )

    conditional_drain_body = function_body(source, "drain_deferred_data_buffer_frees_if_over_limit")
    require(
        conditional_drain_body,
        "ker::mod::mm::virt::drain_kernel_vmap_frees_if_over_limit()",
        "buffer cache must expose a safe post-lock warm-pool drain",
    )

    dirty_worker_body = function_body(source, "dirty_writeback_worker")
    require_order(
        dirty_worker_body,
        [
            "dirty_writeback_queued.store(false",
            "cache_lock.unlock_irqrestore(IRQFLAGS)",
            "if (STILL_DIRTY)",
            "request_dirty_writeback()",
            "drain_deferred_data_buffer_frees_if_over_limit()",
        ],
        "writeback worker completion must enforce the vmap cap outside filesystem locks",
    )

    snapshot_body = function_body(source, "make_writeback_run_snapshot")
    require_order(
        snapshot_body,
        [
            "snapshot.data = allocate_buffer_data(run.bytes, snapshot.flags)",
            "if (snapshot.data == nullptr)",
            "snapshot.size = run.bytes",
        ],
        "writeback snapshots must retain the exact vmap teardown size",
    )

    hard_limit_body = function_body(source, "choose_dirty_hard_limit_bytes")
    require_order(
        hard_limit_body,
        [
            "if (target_bytes == 0)",
            "return 0",
            "return max_bytes",
        ],
        "dirty hard limit must preserve the zero target and otherwise use the cache cap",
    )

    dirty_target_body = function_body(source, "choose_dirty_target_bytes_for_total")
    require_order(
        dirty_target_body,
        [
            "size_t const MEMORY_DIVISOR",
            "DIRTY_TARGET_LARGE_MEMORY_THRESHOLD",
            "DIRTY_TARGET_LARGE_MEMORY_DIVISOR",
            "DIRTY_TARGET_MEMORY_DIVISOR",
            "uint64_t const MIN_TARGET",
            "uint64_t const MAX_TARGET",
            "DIRTY_TARGET_MAX_NUMERATOR",
            "DIRTY_TARGET_MAX_DENOMINATOR",
            "std::clamp<uint64_t>(SCALED, MIN_TARGET, MAX_TARGET)",
        ],
        "dirty target must scale by memory class and stay below the configured fraction of the cache cap",
    )

    candidate_body = function_body(source, "find_writeback_candidate_locked")
    require_order(
        candidate_body,
        [
            "if (skip_writeback && (bh->flags & BH_WRITEBACK) != 0)",
            "search_min_epoch = bh->dirty_epoch",
            "continue",
            "if (!has_older_overlapping_dirty_buffer_locked(bh))",
        ],
        "dirty writeback scan must not let one in-flight buffer block independent later writeback",
    )
    background_one_body = function_body(source, "writeback_dirty_one")
    require(
        background_one_body,
        "return writeback_dirty_one_after(filter, 0, UINT64_MAX, true)",
        "background dirty writeback should skip in-flight buffers",
    )

    journal_hold_body = function_body(source, "bjournal_hold")
    require_order(
        journal_hold_body,
        [
            "cache_lock.lock_irqsave()",
            "journal_pending.fetch_add",
            "while ((bh->flags & BH_WRITEBACK) != 0)",
            "cache_lock.unlock_irqrestore(irqflags)",
            "kern_yield()",
            "cache_lock.lock_irqsave()",
        ],
        "journal capture must publish its hold with candidate selection and wait out older writeback",
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
            "BufferCacheReclaimStats const RECLAIM = reclaim_clean_cache_over_limit_locked()",
            "cache_lock.unlock_irqrestore(IRQFLAGS)",
            "if (RECLAIM.freed_bytes != 0)",
            "drain_deferred_data_buffer_frees()",
        ],
        "successful dirty writeback should reclaim vmap buffers only after releasing the cache lock",
    )

    require(source, "write_dirty_run_buffers_individually", "dirty writeback allocation fallback")
    fallback_body = function_body(source, "write_dirty_run_buffers_individually")
    require_order(
        fallback_body,
        [
            "WritebackSnapshot snapshot = make_writeback_snapshot_for_epoch(bh, run.dirty_epochs.at(i))",
            "rc = write_block_to_disk(bh, snapshot.data)",
            "bh->dirty_epoch == run.dirty_epochs.at(i)",
            "clear_buffer_dirty_locked(bh)",
            "owns_writeback_epoch(bh, run.writeback_epoch)",
            "clear_buffer_writeback(bh)",
            "wake_dirty_waiters(wake_list)",
        ],
        "dirty writeback fallback must write smaller snapshots without losing dirty epoch protection",
    )
    run_snapshot_body = function_body(source, "write_dirty_run_snapshot")
    require_order(
        run_snapshot_body,
        [
            "if (snapshot.data == nullptr)",
            "if (run.count > 1)",
            "return write_dirty_run_buffers_individually(run)",
            "finish_dirty_writeback_run(run, -ENOMEM)",
        ],
        "dirty writeback must fall back before abandoning a multi-buffer run",
    )

    throttle_body = function_body(source, "throttle_dirty_buffer_cache")
    require_order(
        throttle_body,
        [
            "static_cast<void>(request_dirty_writeback())",
            "writeback_dirty_budgeted(fallback_filter, DIRTY_HARD_FALLBACK_BUDGET, DIRTY_HARD_FALLBACK_BYTES)",
            "bool const DRAINED_TO_RESUME",
            "if (CAN_PARK)",
            'ker::mod::sched::preemptible_syscall_park("dirty_bcache", DEADLINE_US)',
        ],
        "hard dirty throttle must make foreground writers assist writeback before parking",
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

    allocate_body = function_body(source, "alloc_detached_buffer_with_size")
    require_order(
        allocate_body,
        ["bh->refcount.store(1", "bh->retired.store(false", "bh->journal_pending.store(0"],
        "reused buffer heads must clear deferred-retirement state",
    )
    retire_one_body = function_body(source, "retire_buffer_locked")
    require_order(
        retire_one_body,
        [
            "bh->refcount.fetch_add(1",
            "bh->retired.exchange(true",
            "hash_remove(bh)",
            "lru_remove(bh)",
            "clear_buffer_dirty_locked(bh)",
            "bh->refcount.fetch_sub(1",
            "free_retired_buffer_if_reclaimable_locked(bh)",
        ],
        "retirement must publish an unreachable alias while holding a temporary lifetime reference",
    )
    retire_range_body = function_body(source, "retire_bdev_range")
    require_order(
        retire_range_body,
        ["if (find_overlap(true) != nullptr)", "return false", "while (BufHead* bh = find_overlap(false))", "retire_buffer_locked(bh)"],
        "range retirement must wait out writeback before detaching ordinary holders",
    )
    brelse_body = function_body(source, "brelse")
    require_order(
        brelse_body,
        ["bool const RETIRED = bh->retired.load", "bh->refcount.fetch_sub", "if (PREV == 1 && RETIRED)"],
        "final release must snapshot retirement before dropping its reference",
    )

    span_body = function_body(source, "checked_block_span_size")
    require_order(
        span_body,
        [
            "bdev == nullptr || bdev->block_size == 0 || count == 0 || count > SIZE_MAX / bdev->block_size",
            "return false",
            "total_size = bdev->block_size * count",
            "return true",
        ],
        "checked buffer-cache block span sizing",
    )

    for name in ("bread_multi", "bget_multi_impl"):
        body = function_body(source, name)
        require_order(
            body,
            [
                "size_t total_size = 0",
                "if (!checked_block_span_size(bdev, count, total_size))",
                "return nullptr",
                "hash_lookup(bdev, block_no, total_size)",
                "evict_lru_for_allocation(total_size)",
            ],
            f"{name} must validate byte size before cache lookup/allocation",
        )
        require_absent(body, "BLK_SIZE * count", f"{name} unchecked byte-size multiply")
        require_absent(body, "bdev->block_size * count", f"{name} unchecked byte-size multiply")

    for name, allocator, size_token in [
        ("bread", "alloc_detached_buffer(bdev, block_no)", "bdev->block_size"),
        ("bread_multi", "alloc_detached_buffer_with_size(bdev, block_no, total_size, 0)", "total_size"),
        ("bget_impl", "alloc_detached_buffer(bdev, block_no)", "bdev->block_size"),
        ("bget_multi_impl", "alloc_detached_buffer_with_size(bdev, block_no, total_size, BH_VALID)", "total_size"),
    ]:
        body = function_body(source, name)
        require_order(
            body,
            [
                f"evict_lru_for_allocation({size_token})",
                "cache_lock.unlock_irqrestore(irqflags)",
                "drain_deferred_data_buffer_frees_if_over_limit()",
                allocator,
                "irqflags = cache_lock.lock_irqsave()",
                "BufHead* existing = hash_lookup",
                "free_detached_buffer(bh)",
                "insert_allocated_buffer_locked(bh)",
                "cache_lock.unlock_irqrestore(irqflags)",
                "drain_deferred_data_buffer_frees_if_over_limit()",
            ],
            f"{name} cold miss must enforce the vmap cap outside the cache lock",
        )

    xfs_span_body = function_body(xfs_mount, "xfs_device_block_span")
    for token in [
        "ctx == nullptr || ctx->device == nullptr || count == 0 || ctx->device->block_size == 0",
        "ctx->block_size < ctx->device->block_size",
        "(ctx->block_size % ctx->device->block_size) != 0",
        "ctx->ag_blk_log >= 64",
        "static_cast<uint64_t>(AGNO) > UINT64_MAX / ctx->ag_blocks",
        "AGBNO > UINT64_MAX - AG_BASE",
        "count > SIZE_MAX / RATIO",
        "LINEAR_BLOCK > UINT64_MAX / RATIO",
        "dev_block = LINEAR_BLOCK * RATIO",
        "dev_count = count * RATIO",
    ]:
        require(xfs_span_body, token, "XFS device block-span guard")

    for name, io_call in [
        ("xfs_buf_read", "bread(ctx->device, dev_block, BufferReadClass::FILESYSTEM_METADATA)"),
        ("xfs_buf_read_data", "bread(ctx->device, dev_block, BufferReadClass::FILE_DATA)"),
        ("xfs_buf_read_multi", "bread_multi(ctx->device, dev_block, dev_count, BufferReadClass::FILESYSTEM_METADATA)"),
        ("xfs_buf_get", "bget(ctx->device, dev_block)"),
        ("xfs_buf_get_multi", "bget_multi(ctx->device, dev_block, dev_count)"),
    ]:
        body = function_body(xfs_mount, name)
        require_order(
            body,
            [
                "uint64_t dev_block = 0",
                "size_t dev_count = 0",
                "if (!xfs_device_block_span(ctx, xfs_block",
                "return nullptr",
                io_call,
            ],
            f"{name} must validate device block span before buffer-cache I/O",
        )
        require_absent(body, "count * RATIO", f"{name} unchecked XFS device-count multiply")
        require_absent(body, "LINEAR_BLOCK * RATIO", f"{name} unchecked XFS device-block multiply")

    print("buffer cache cold-miss locking invariants hold")


if __name__ == "__main__":
    main()
