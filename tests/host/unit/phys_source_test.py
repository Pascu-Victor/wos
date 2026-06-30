#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
PHYS_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "mm" / "phys.opt.cpp"
PHYS_HPP = ROOT / "modules" / "kern" / "src" / "platform" / "mm" / "phys.hpp"
PAGE_ALLOC_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "mm" / "page_alloc.cpp"
MM_KTEST = ROOT / "modules" / "kern" / "src" / "test" / "mm_ktest.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def find_matching_brace(source: str, brace: int) -> int:
    depth = 0
    for index in range(brace, len(source)):
        if source[index] == "{":
            depth += 1
        elif source[index] == "}":
            depth -= 1
            if depth == 0:
                return index
    fail("unterminated braced block")


def function_body(source: str, name: str) -> str:
    match = re.search(
        rf"\b(?:auto|bool|void|uint64_t|void\*)\s+(?:[A-Za-z0-9_:]+::)?{name}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>,\s*&]+)?\s*\{{",
        source,
        flags=re.DOTALL,
    )
    if match is None:
        fail(f"missing function {name}")
    end = find_matching_brace(source, match.end() - 1)
    return source[match.end() : end]


def require_tokens(source: str, tokens: list[str], context: str) -> None:
    missing = [token for token in tokens if token not in source]
    if missing:
        fail(f"{context}: missing {', '.join(missing)}")


def require_order(source: str, tokens: list[str], context: str) -> None:
    cursor = 0
    for token in tokens:
        found = source.find(token, cursor)
        if found < 0:
            fail(f"{context}: missing ordered token {token}")
        cursor = found + len(token)


def test_internal_reservation_requires_nonzero_base_and_spare_page() -> None:
    source = PHYS_CPP.read_text()
    body = function_body(source, "select_internal_reservation")
    require_order(
        body,
        [
            "uint64_t const ALIGNED_START = page_align_up(base)",
            "uint64_t const ALIGNED_END = page_align_down(END)",
            "uint64_t const NEEDED = page_align_up(requested_size)",
            "ALIGNED_START == 0",
            "uint64_t const AVAILABLE = ALIGNED_END - ALIGNED_START",
            "AVAILABLE - NEEDED < paging::PAGE_SIZE",
            "out = {.base = ALIGNED_START, .end = ALIGNED_START + NEEDED}",
        ],
        "internal physical reservation selector",
    )


def test_per_cpu_cache_reservation_uses_selector_not_raw_memmap_base() -> None:
    source = PHYS_CPP.read_text()
    body = function_body(source, "init")
    require_tokens(
        body,
        [
            "PhysRange cache_range{}",
            "select_internal_reservation(MEMMAP.entries[i]->base, MEMMAP.entries[i]->length, per_cpu_caches_size, cache_range)",
            "per_cpu_caches_phys_base = cache_range.base",
            "append_range(reserved_ranges, reserved_range_count, cache_range)",
        ],
        "per-CPU cache reservation path",
    )
    forbidden = [
        "per_cpu_caches_phys_base = MEMMAP.entries[i]->base",
        ".end = per_cpu_caches_phys_base + per_cpu_caches_size",
    ]
    for token in forbidden:
        if token in body:
            fail(f"per-CPU cache reservation path still uses raw memmap arithmetic: {token}")


def test_selftest_and_ktest_cover_boot_reservation_edges() -> None:
    phys_cpp = PHYS_CPP.read_text()
    phys_hpp = PHYS_HPP.read_text()
    ktest = MM_KTEST.read_text()
    helper = "selftest_internal_reservation_carveout_preserves_page_alignment"

    require_tokens(
        phys_cpp,
        [
            f"auto {helper}() -> bool",
            "select_internal_reservation(0, paging::PAGE_SIZE * 8, paging::PAGE_SIZE, zero_base)",
            "select_internal_reservation(0x1000, paging::PAGE_SIZE * 2, paging::PAGE_SIZE * 2, no_spare)",
        ],
        "internal reservation selftest helper",
    )
    require_tokens(
        phys_hpp,
        [
            "#ifdef WOS_SELFTEST",
            f"auto {helper}() -> bool;",
        ],
        "internal reservation selftest declaration",
    )
    require_tokens(
        ktest,
        [
            "KTEST(MM, InternalReservationCarveoutPreservesPageAlignment)",
            f"phys::{helper}()",
        ],
        "internal reservation KTEST hook",
    )


def test_buddy_free_list_repair_rebuilds_from_flags() -> None:
    source = PAGE_ALLOC_CPP.read_text()
    free_body = function_body(source, "free_allocated_block")
    alloc_body = function_body(source, "alloc")

    require_tokens(
        source,
        [
            "void rebuild_free_lists_from_flags(PageAllocator* alloc, const char* reason)",
            "for (auto*& list_head : alloc->free_list)",
            "link_free_block_unchecked(alloc, ORDER, block);",
            "auto remove_free_block_with_repair(PageAllocator* alloc, int order, PageAllocator::FreeBlock* block, const char* reason)",
            "rebuild_free_lists_from_flags(alloc, reason);",
            "free_block_is_reachable(alloc, order, block)",
        ],
        "buddy allocator free-list repair",
    )
    if "drop_corrupt_free_list_head" in source:
        fail("buddy allocator must rebuild corrupt free lists instead of orphaning flagged free heads")
    require_order(
        free_body,
        [
            "insert_free_block(alloc, k, free_block);",
            "alloc->free_count += BLOCK_SIZE;",
        ],
        "free_count must be updated after rebuilt/coalesced free-head publication",
    )
    require_tokens(
        alloc_body,
        [
            "rebuild_free_lists_from_flags(this, \"alloc corrupt head\")",
            "remove_free_block_with_repair(this, k, block, \"alloc unlink\")",
            "repaired_free_lists = true;",
            "k = ORDER;",
        ],
        "allocation must retry after free-list rebuild",
    )


def test_direct_page_free_rejects_refcounted_blocks() -> None:
    source = PAGE_ALLOC_CPP.read_text()
    helper_body = function_body(source, "allocated_block_has_direct_free_refs")
    free_body = function_body(source, "free")

    require_tokens(
        helper_body,
        [
            "uint32_t const BLOCK_SIZE = 1U << order;",
            "alloc->page_refcounts[page_idx + i].load(std::memory_order_acquire) != 1",
        ],
        "direct page_free refcount guard helper",
    )
    require_tokens(
        free_body,
        [
            "allocated_block_has_direct_free_refs(this, page_idx, ORDER)",
            "page_alloc: rejecting direct free of refcounted block",
            "return 0;",
            "return free_allocated_block(this, page_idx, ORDER);",
        ],
        "direct page_free refcount guard",
    )


def test_live_slab_pages_are_not_reused_by_physical_allocator() -> None:
    page_alloc = PAGE_ALLOC_CPP.read_text()
    phys = PHYS_CPP.read_text()

    require_tokens(
        page_alloc,
        [
            "auto page_has_live_slab_kind(PageAllocator* alloc, uint32_t page_idx) -> bool",
            "PageKind::SLAB",
            "report_live_slab_page_free(this, PAGE_IDX, \"page cache free\")",
            "report_live_slab_page_free(this, page_idx + i, \"page_free\")",
            "report_live_slab_page_free(this, page_idx, \"page_free_order0\")",
            "report_live_slab_page_free(this, CUR_IDX, \"page_free_order0_range\")",
        ],
        "physical allocator must reject live mini-slab pages before freeing or caching them",
    )

    require_tokens(
        phys,
        [
            "void detect_live_slab_page_in_returned_block(void* block, uint64_t scan_size, const char* source)",
            "offset < scan_size",
            "offset += paging::PAGE_SIZE",
            "DETECT: pageAlloc (%s) returning live slab page",
        ],
        "physical allocator must scan returned allocations for embedded live slab headers",
    )

    cache_body = function_body(phys, "try_alloc_from_per_cpu_cache")
    alloc_body = function_body(phys, "page_alloc_impl")
    huge_body = function_body(phys, "page_alloc_huge")
    require_order(
        cache_body,
        [
            "detect_live_slab_page_in_returned_block(page, paging::PAGE_SIZE, \"cache\");",
            "prepare_allocated_block(page, paging::PAGE_SIZE, zeroing);",
        ],
        "per-CPU page-cache allocation must trap live slab pages before zeroing",
    )
    require_order(
        alloc_body,
        [
            "detect_live_slab_page_in_returned_block(block, buddy_accounting_size(size), \"buddy\");",
            "prepare_allocated_block(block, size, zeroing);",
        ],
        "buddy allocation must scan the full returned span before zeroing",
    )
    require_order(
        huge_body,
        [
            "detect_live_slab_page_in_returned_block(block, buddy_accounting_size(size), \"huge\");",
            "std::memset(block, 0, size);",
        ],
        "huge allocation must scan the full returned span before zeroing",
    )


def main() -> None:
    test_internal_reservation_requires_nonzero_base_and_spare_page()
    test_per_cpu_cache_reservation_uses_selector_not_raw_memmap_base()
    test_selftest_and_ktest_cover_boot_reservation_edges()
    test_buddy_free_list_repair_rebuilds_from_flags()
    test_direct_page_free_rejects_refcounted_blocks()
    test_live_slab_pages_are_not_reused_by_physical_allocator()
    print("physical memory source invariants hold")


if __name__ == "__main__":
    main()
