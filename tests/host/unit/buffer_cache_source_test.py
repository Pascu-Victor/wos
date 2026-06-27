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

    evict_body = function_body(source, "evict_lru_for_allocation")
    require_order(
        evict_body,
        [
            "cache_allocation_would_exceed_limit_locked(incoming_bytes)",
            "reclaimed_bytes < incoming_bytes",
            "find_reclaimable_lru_buffer(HOT_EVICT_SCAN_BUDGET)",
            "reclaimed_bytes += victim->size",
            "free_buffer(victim)",
        ],
        "hot eviction should only repay the incoming allocation size",
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
