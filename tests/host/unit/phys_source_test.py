#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
PHYS_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "mm" / "phys.opt.cpp"
PHYS_HPP = ROOT / "modules" / "kern" / "src" / "platform" / "mm" / "phys.hpp"
PAGE_ALLOC_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "mm" / "page_alloc.cpp"
KMALLOC_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "mm" / "dyn" / "kmalloc.opt.cpp"
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


def require_absent(source: str, token: str, context: str) -> None:
    if token in source:
        fail(f"{context}: unexpected {token}")


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
            "k = order;",
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


def test_allocator_size_rounding_rejects_overflow_and_overmax_requests() -> None:
    page_alloc = PAGE_ALLOC_CPP.read_text()
    phys = PHYS_CPP.read_text()
    kmalloc = KMALLOC_CPP.read_text()

    size_order_body = function_body(page_alloc, "size_to_order")
    require_order(
        size_order_body,
        [
            "uint64_t const PAGES = (size_bytes / paging::PAGE_SIZE) + ((size_bytes % paging::PAGE_SIZE) != 0 ? 1 : 0)",
            "uint64_t const MAX_PAGES = uint64_t{1} << PageAllocator::MAX_ORDER",
            "if (PAGES > MAX_PAGES)",
            "return false",
            "out_order = 64 - __builtin_clzll(PAGES - 1)",
            "return true",
        ],
        "buddy allocator size-to-order conversion must be checked",
    )
    require_absent(size_order_body, "size_bytes + paging::PAGE_SIZE - 1", "buddy allocator size rounding")
    require_absent(size_order_body, "std::min(bits, PageAllocator::MAX_ORDER)", "buddy allocator order conversion")

    alloc_body = function_body(page_alloc, "alloc")
    require_order(
        alloc_body,
        [
            "int order = 0",
            "if (!size_to_order(size_bytes, order))",
            "return nullptr",
            "int k = order",
            "while (k > order)",
            "uint32_t const BLOCK_SIZE = 1U << order",
        ],
        "PageAllocator::alloc must reject impossible orders before publishing a block",
    )

    limit_body = function_body(phys, "page_alloc_size_within_buddy_limit")
    require_order(
        limit_body,
        [
            "out_pages = (size / paging::PAGE_SIZE) + ((size % paging::PAGE_SIZE) != 0 ? 1 : 0)",
            "return out_pages <= (uint64_t{1} << PageAllocator::MAX_ORDER)",
        ],
        "phys allocation public size limit",
    )

    phys_alloc_body = function_body(phys, "page_alloc_impl")
    require_order(
        phys_alloc_body,
        [
            "uint64_t requested_pages = 0",
            "if (!page_alloc_size_within_buddy_limit(size, requested_pages))",
            "return nullptr",
            "find_free_block(size, CALLER_TAG)",
            "prepare_allocated_block(block, size, zeroing)",
        ],
        "regular phys allocation must validate before zone search and zeroing",
    )
    require_tokens(
        phys_alloc_body,
        ["record_page_alloc_caller(caller_addr, requested_pages)"],
        "regular phys caller stats must use checked page count",
    )

    reclaim_body = function_body(phys, "page_alloc_with_reclaim_impl")
    require_order(
        reclaim_body,
        [
            "uint64_t requested_pages = 0",
            "if (!page_alloc_size_within_buddy_limit(size, requested_pages))",
            "return nullptr",
            "for (uint32_t attempt = 0; attempt < retry_count; ++attempt)",
        ],
        "reclaim allocation must reject invalid sizes before reclaim retries",
    )

    huge_body = function_body(phys, "page_alloc_huge")
    require_order(
        huge_body,
        [
            "if (!page_alloc_size_within_buddy_limit(size, requested_pages))",
            "return nullptr",
            "find_free_block_huge(size, CALLER_TAG)",
            "std::memset(block, 0, size)",
        ],
        "huge phys allocation must validate before allocation and zeroing",
    )
    require_tokens(
        huge_body,
        ["record_page_alloc_caller(caller_addr, requested_pages)"],
        "huge phys caller stats must use checked page count",
    )

    rounded_body = function_body(kmalloc, "checked_page_rounded_alloc_size")
    require_order(
        rounded_body,
        [
            "payload_size > UINT64_MAX - header_size",
            "return false",
            "uint64_t const TOTAL_SIZE = payload_size + header_size",
            "uint64_t const PAGES = (TOTAL_SIZE / PAGE_SIZE) + ((TOTAL_SIZE % PAGE_SIZE) != 0 ? 1 : 0)",
            "PAGES == 0 || PAGES > UINT64_MAX / PAGE_SIZE",
            "uint64_t const ALIGNED_SIZE = PAGES * PAGE_SIZE",
            "ALIGNED_SIZE > MAX_BUDDY_ALLOCATION_BYTES",
            "out_rounded_size = ALIGNED_SIZE",
            "return true",
        ],
        "kmalloc tracked allocation size rounding must be checked",
    )

    malloc_body = function_body(kmalloc, "malloc_impl")
    require_order(
        malloc_body,
        [
            "if (!checked_page_rounded_alloc_size(size, sizeof(MediumAllocationHeader), rounded_size))",
            "return nullptr",
            "alloc_medium_backing(rounded_size)",
            "header->size = rounded_size",
            "if (!checked_page_rounded_alloc_size(size, sizeof(LargeAllocationHeader), rounded_size))",
            "return nullptr",
            "alloc_large_backing(rounded_size)",
            "header->size = rounded_size",
        ],
        "kmalloc malloc must reject overflowed tracked allocation sizes",
    )

    realloc_body = function_body(kmalloc, "realloc")
    require_order(
        realloc_body,
        [
            "if (!checked_page_rounded_alloc_size(NEW_SIZE, sizeof(LargeAllocationHeader), new_rounded_size))",
            "return nullptr",
            "alloc_large_backing(new_rounded_size)",
            "new_header->size = new_rounded_size",
            "if (!checked_page_rounded_alloc_size(NEW_SIZE, sizeof(MediumAllocationHeader), new_rounded_size))",
            "return nullptr",
            "alloc_medium_backing(new_rounded_size)",
            "new_header->size = new_rounded_size",
        ],
        "kmalloc realloc tracked growth must reject overflowed allocation sizes",
    )
    require_absent(kmalloc, "NEW_SIZE + sizeof(LargeAllocationHeader) + PAGE_SIZE - 1", "kmalloc large realloc rounding")
    require_absent(kmalloc, "NEW_SIZE + sizeof(MediumAllocationHeader) + PAGE_SIZE - 1", "kmalloc medium realloc rounding")


def test_live_slab_pages_are_not_reused_by_physical_allocator() -> None:
    page_alloc = PAGE_ALLOC_CPP.read_text()
    phys = PHYS_CPP.read_text()
    ktest = MM_KTEST.read_text()

    require_tokens(
        page_alloc,
        [
            "auto page_has_live_slab_kind(PageAllocator* alloc, uint32_t page_idx) -> bool",
            "PageKind::SLAB",
            "auto free_block_pages_are_reusable(PageAllocator* alloc, uint32_t page_idx, int order, const char* reason) -> bool",
            "struct FreeBlockPageIssue",
            "panic_non_reusable_free_block_page(alloc, {.free_head_idx = page_idx, .bad_page_idx = CUR_IDX}, order, reason)",
            "ker::mod::dbg::panic_handler(\"page_alloc free block overlaps live page\")",
            "decode_page_kind(alloc->page_kinds[CUR_IDX].load(std::memory_order_acquire)) != PageKind::FREE",
            "alloc->page_refcounts[CUR_IDX].load(std::memory_order_acquire) != 0",
            "report_live_slab_page_free(this, PAGE_IDX, \"page cache free\")",
            "report_live_slab_page_free(this, page_idx + i, \"page_free\")",
            "report_live_slab_page_free(this, page_idx, \"page_free_order0\")",
            "report_live_slab_page_free(this, CUR_IDX, \"page_free_order0_range\")",
        ],
        "physical allocator must reject live mini-slab pages before freeing, caching, or reusing free blocks",
    )

    rebuild_body = function_body(page_alloc, "rebuild_free_lists_from_flags")
    phys_alloc_body = function_body(phys, "page_alloc_impl")
    huge_body = function_body(phys, "page_alloc_huge")
    require_order(
        function_body(page_alloc, "alloc"),
        [
            "remove_free_block_with_repair(this, k, block, \"alloc unlink\")",
            "free_block_pages_are_reusable(this, page_idx, k, \"alloc candidate\")",
            "// Split down: put the upper buddy of each split into the free list.",
        ],
        "buddy allocation must validate free-block metadata before publishing or splitting a block",
    )
    require_order(
        rebuild_body,
        [
            "free_block_pages_are_reusable(alloc, page_idx, ORDER, \"rebuild free head\")",
            "link_free_block_unchecked(alloc, ORDER, block);",
        ],
        "free-list rebuild must not relink blocks whose metadata still belongs to live pages",
    )
    require_order(
        phys_alloc_body,
        [
            "The owning",
            "PageAllocator validates free-block metadata before publishing the block.",
            "prepare_allocated_block(block, size, zeroing);",
        ],
        "phys allocation must rely on pre-publication PageAllocator metadata validation before zeroing",
    )
    require_tokens(
        huge_body,
        [
            "PageAllocator validates free-block metadata before publishing the block.",
            "std::memset(block, 0, size);",
        ],
        "huge allocation must keep the pre-publication validation contract documented",
    )
    if "detect_live_slab_page_in_returned_block" in phys:
        fail("post-allocation content-only slab magic traps can panic on stale freed bytes")
    require_tokens(
        ktest,
        [
            "KTEST(MM, StaleMiniSlabMagicOnFreePageDoesNotTripAllocator)",
            "MINI_SLAB_MAGIC",
            "KEXPECT_EQ(fresh_page[0], 0U)",
        ],
        "MM ktest must cover stale freed slab magic",
    )


def main() -> None:
    test_internal_reservation_requires_nonzero_base_and_spare_page()
    test_per_cpu_cache_reservation_uses_selector_not_raw_memmap_base()
    test_selftest_and_ktest_cover_boot_reservation_edges()
    test_buddy_free_list_repair_rebuilds_from_flags()
    test_direct_page_free_rejects_refcounted_blocks()
    test_allocator_size_rounding_rejects_overflow_and_overmax_requests()
    test_live_slab_pages_are_not_reused_by_physical_allocator()
    print("physical memory source invariants hold")


if __name__ == "__main__":
    main()
