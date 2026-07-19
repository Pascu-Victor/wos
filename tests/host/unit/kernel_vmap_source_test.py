#!/usr/bin/env python3

"""Source invariants for non-contiguous kernel virtual allocations."""

from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
VIRT = ROOT / "modules/kern/src/platform/mm/virt.opt.cpp"
MM = ROOT / "modules/kern/src/platform/mm/mm.opt.cpp"


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


def require_order(source: str, tokens: list[str], context: str) -> None:
    cursor = 0
    for token in tokens:
        found = source.find(token, cursor)
        if found < 0:
            fail(f"{context}: missing ordered token {token!r}")
        cursor = found + len(token)


def main() -> None:
    source = VIRT.read_text()
    mm_source = MM.read_text()

    require_order(
        function_body(mm_source, "init"),
        [
            "virt::init_pagemap()",
            "phys::set_kernel_cr3",
            "virt::init_kernel_vmap()",
            "phys::init_kernel_stack_pool()",
        ],
        "vmap page-table paths must precede task and stack runtime activity",
    )

    require_order(
        function_body(source, "init_kernel_vmap"),
        [
            "TOTAL_MEM / 2",
            "kernel_vmap_bitmap.fill(0)",
            "offset += LARGE_PAGE_2M_BYTES",
            "advance_page_table(kernel_pagemap, index_of(VADDR, 4), FLAGS)",
            "advance_page_table(pml3, index_of(VADDR, 3), FLAGS)",
            "advance_page_table(pml2, index_of(VADDR, 2), FLAGS)",
            "kernel_vmap_initialized = true",
        ],
        "vmap initialization must prepare all runtime page-table paths",
    )

    require_order(
        function_body(source, "kernel_vmap_alloc"),
        [
            "reserve_kernel_vmap_span(PAGE_COUNT, first_page)",
            "entry->present != 0 && entry->frame != 0",
            "continue",
            "page_alloc_full_overwrite_page_with_reclaim(name)",
            "map_page(kernel_pagemap",
            "if (mapped_pages != PAGE_COUNT)",
            "queue_kernel_vmap_free(first_page, mapped_pages)",
            "release_kernel_vmap_span(first_page + mapped_pages, PAGE_COUNT - mapped_pages)",
            "drain_kernel_vmap_frees()",
        ],
        "vmap allocation must reuse cached mappings and reclaim partial failures",
    )

    require_order(
        function_body(source, "release_kernel_vmap_mappings"),
        [
            "entry == nullptr || entry->present == 0 || entry->frame == 0",
            "*entry = paging::purge_page_table_entry()",
            "shootdown_shared_kernel_mappings()",
            "phys::page_ref_dec(frames.at(page))",
        ],
        "vmap teardown must invalidate every CPU before releasing physical frames",
    )

    require_order(
        function_body(source, "kernel_vmap_free"),
        [
            "queue_kernel_vmap_free(FIRST_PAGE, PAGE_COUNT)",
            "kernel_vmap_warm_pool_over_limit()",
            "drain_kernel_vmap_frees()",
        ],
        "vmap free must bound cached mappings through the context-gated drainer",
    )

    require_order(
        source,
        [
            "KERNEL_VMAP_WARM_POOL_MAX_PAGES",
            "size_t kernel_vmap_pending_pages = 0",
            "kernel_vmap_pending_pages += page_count",
            "kernel_vmap_pending_pages >= KERNEL_VMAP_WARM_POOL_MAX_PAGES",
        ],
        "vmap warm-pool accounting must prevent unbounded physical retention",
    )

    require_order(
        function_body(source, "drain_kernel_vmap_frees"),
        [
            "!kernel_vmap_context_can_drain()",
            "kernel_vmap_draining.compare_exchange_strong",
            "find_pending_kernel_vmap_run(first_page, page_count)",
            "release_kernel_vmap_mappings(START, page_count)",
            "release_kernel_vmap_span(first_page, page_count, true)",
        ],
        "deferred frees must shoot down only from a safe single drainer",
    )

    service_body = function_body(source, "service_tlb_shootdown_requests_for_cpu")
    require_order(
        service_body,
        [
            "request.shared_kernel.load",
            "if (SHARED_KERNEL)",
            "wrcr3(rdcr3())",
            "else if (TARGET_PAGEMAP != nullptr)",
        ],
        "shared kernel shootdowns must flush any active task CR3",
    )

    print("kernel vmap source invariants hold")


if __name__ == "__main__":
    main()
