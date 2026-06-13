#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
PHYS_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "mm" / "phys.opt.cpp"
PHYS_HPP = ROOT / "modules" / "kern" / "src" / "platform" / "mm" / "phys.hpp"
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
        rf"\b(?:auto|void)\s+{name}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>,\s*&]+)?\s*\{{",
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


def main() -> None:
    test_internal_reservation_requires_nonzero_base_and_spare_page()
    test_per_cpu_cache_reservation_uses_selector_not_raw_memmap_base()
    test_selftest_and_ktest_cover_boot_reservation_edges()
    print("physical memory source invariants hold")


if __name__ == "__main__":
    main()
