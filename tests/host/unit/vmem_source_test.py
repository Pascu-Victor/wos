#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SYS_VMEM_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "vmem" / "sys_vmem.cpp"
VIRT_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "mm" / "virt.opt.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(
        rf"\b(?:auto|void)\s+{name}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>,\s*&]+)?\s*\{{",
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


def require_order(source: str, first: str, second: str, context: str) -> None:
    first_index = source.find(first)
    second_index = source.find(second)
    if first_index < 0:
        fail(f"{context}: missing {first}")
    if second_index < 0:
        fail(f"{context}: missing {second}")
    if first_index >= second_index:
        fail(f"{context}: expected {first} before {second}")


def test_nonfixed_mmap_address_selection_is_reserved_before_mapping() -> None:
    source = SYS_VMEM_CPP.read_text()
    reserve_body = function_body(source, "reserve_free_mmap_range")
    anon_body = function_body(source, "anon_allocate")
    file_body = function_body(source, "file_allocate")

    require_tokens(
        source,
        [
            "ker::mod::sys::Mutex g_mmap_reserve_lock;",
            "release_mmap_reservation(task, vaddr, size, HAS_ADDRESS_RESERVATION)",
        ],
        "mmap address reservation surface",
    )
    require_tokens(
        reserve_body,
        [
            "ker::mod::sys::MutexGuard guard(g_mmap_reserve_lock)",
            "uint64_t const VADDR = find_free_range(task, size, hint)",
            "add_shared_vmem_range(task, VADDR, size, 0, 0)",
            "out_vaddr = VADDR",
        ],
        "mmap reservation helper",
    )
    require_order(
        reserve_body,
        "uint64_t const VADDR = find_free_range(task, size, hint)",
        "add_shared_vmem_range(task, VADDR, size, 0, 0)",
        "non-fixed mmap must publish a reservation before mapping work starts",
    )

    for body_name, body in [("anon_allocate", anon_body), ("file_allocate", file_body)]:
        if "vaddr = find_free_range(task, size, hint)" in body:
            fail(f"{body_name} must reserve non-fixed mmap ranges instead of raw find_free_range")
        require_tokens(
            body,
            [
                "reserve_free_mmap_range(task, size, hint, vaddr)",
                "bool const HAS_ADDRESS_RESERVATION = !IS_FIXED;",
            ],
            f"{body_name} non-fixed reservation path",
        )

    require_order(
        file_body,
        "reserve_free_mmap_range(task, size, hint, vaddr)",
        "if (file_mmap_can_share(st, prot))",
        "file mmap must reserve the address before eager file mapping",
    )
    require_order(
        file_body,
        "release_mmap_reservation(task, vaddr, size, HAS_ADDRESS_RESERVATION)",
        "advance_shared_mmap_cursor(task, vaddr, size)",
        "file mmap reservation must be dropped only after page tables occupy the range",
    )


def test_owned_frame_tracking_is_disabled_off_the_fault_path() -> None:
    source = VIRT_CPP.read_text()
    insert_body = function_body(source, "owned_frame_insert_private_mapping")
    untrack_body = function_body(source, "owned_frame_untrack_mapping")
    purge_body = function_body(source, "owned_frame_purge_pagemap")
    snapshot_body = function_body(source, "get_owned_frame_stats_snapshot")

    require_tokens(source, ["constexpr bool OWNED_FRAME_TRACKING_ENABLED = false;"], "owned frame tracking switch")
    require_tokens(
        insert_body,
        [
            "if constexpr (!OWNED_FRAME_TRACKING_ENABLED)",
            "owned_frame_stats.track_skipped.fetch_add(1, std::memory_order_relaxed);",
            "return;",
        ],
        "owned frame insert disabled path",
    )
    require_order(
        insert_body,
        "if constexpr (!OWNED_FRAME_TRACKING_ENABLED)",
        "auto& table = owned_frame_table",
        "owned frame insert must skip shard locks while disabled",
    )

    require_tokens(
        untrack_body,
        [
            "owned_frame_stats.untrack_attempts.fetch_add(1, std::memory_order_relaxed);",
            "if constexpr (!OWNED_FRAME_TRACKING_ENABLED)",
            "owned_frame_stats.untrack_missed.fetch_add(1, std::memory_order_relaxed);",
            "return;",
        ],
        "owned frame untrack disabled path",
    )
    require_order(
        untrack_body,
        "if constexpr (!OWNED_FRAME_TRACKING_ENABLED)",
        "auto& table = owned_frame_table",
        "owned frame untrack must skip shard locks while disabled",
    )

    require_order(
        purge_body,
        "owned_frame_stats.purge_calls.fetch_add(1, std::memory_order_relaxed);",
        "if constexpr (!OWNED_FRAME_TRACKING_ENABLED)",
        "owned frame purge must preserve the call counter before returning",
    )
    require_order(
        purge_body,
        "if constexpr (!OWNED_FRAME_TRACKING_ENABLED)",
        "auto& table = owned_frame_table",
        "owned frame purge must skip table scans while disabled",
    )
    require_tokens(
        snapshot_body,
        [".capacity = OWNED_FRAME_TRACKING_ENABLED ? OWNED_FRAME_TABLE_CAPACITY : 0,"],
        "owned frame stats snapshot disabled capacity",
    )


def test_cow_write_resolution_serializes_pte_reference_consumption() -> None:
    source = VIRT_CPP.read_text()
    resolver_body = function_body(source, "resolve_user_write_mapping")
    ensure_body = function_body(source, "ensure_user_page_writable_for_task")
    fault_body = function_body(source, "pagefault_handler")

    require_tokens(
        source,
        [
            "sys::Spinlock cow_pte_lock;",
            "enum class UserWriteFaultStatus",
            "auto resolve_user_write_mapping(sched::task::Task* task, vaddr_t vaddr) -> UserWriteFaultStatus",
        ],
        "COW write resolver surface",
    )
    require_tokens(
        resolver_body,
        [
            "cow_pte_lock.lock_irqsave()",
            "phys::page_ref_inc(old_virt, &cow_lookup);",
            "void* new_page = alloc_cow_destination_page(DESTINATION_FULL_OVERWRITE_BEFORE_EXPOSURE);",
            "CURRENT_PHYS == old_phys",
            "installed_private_page = true;",
            "phys::page_ref_dec(old_virt, &cow_lookup);\n    phys::page_ref_dec(old_virt, &cow_lookup);",
        ],
        "COW resolver locking and refcount protocol",
    )
    require_order(
        resolver_body,
        "phys::page_ref_inc(old_virt, &cow_lookup);",
        "bool const DESTINATION_FULL_OVERWRITE_BEFORE_EXPOSURE = !old_is_zero_page;",
        "COW resolver must pin the old frame before allocating outside the PTE lock",
    )
    require_order(
        resolver_body,
        "void* new_page = alloc_cow_destination_page(DESTINATION_FULL_OVERWRITE_BEFORE_EXPOSURE);",
        "CURRENT_PHYS == old_phys",
        "COW resolver must allocate/copy before the locked same-frame commit check",
    )
    require_order(
        resolver_body,
        "CURRENT_PHYS == old_phys",
        "pte = pte_from_raw(raw_now);",
        "COW resolver must recheck the old frame before consuming the PTE reference",
    )
    if source.count("alloc_cow_destination_page(DESTINATION_FULL_OVERWRITE_BEFORE_EXPOSURE)") != 1:
        fail("COW destination allocation must be centralized in resolve_user_write_mapping")

    require_tokens(
        ensure_body,
        [
            "switch (resolve_user_write_mapping(task, vaddr))",
            "case UserWriteFaultStatus::NEED_LAZY_BACKING:",
            "handle_lazy_vmem_fault(task, vaddr, WRITE_FAULT)",
        ],
        "syscall copy COW path must share the serialized resolver",
    )
    require_tokens(
        fault_body,
        [
            "resolve_user_write_mapping(current_task, control_register) == UserWriteFaultStatus::HANDLED",
            "return true;",
        ],
        "page fault COW path must share the serialized resolver",
    )


def test_page_table_pool_duplicate_release_does_not_fall_through_to_page_free() -> None:
    source = VIRT_CPP.read_text()
    release_body = function_body(source, "try_release_page_table_to_pool")
    release_pagemap_body = function_body(source, "release_pagemap")

    require_tokens(
        release_body,
        [
            "for (size_t i = 0; i < pool.count; ++i)",
            "pool.pages.at(i) == table",
            "duplicate page-table pool release",
            "return true;",
            "raced duplicate page-table pool release",
            "zero_page_table_for_pool(table);",
        ],
        "page-table pool duplicate release guard",
    )
    require_order(
        release_body,
        "pool.pages.at(i) == table",
        "zero_page_table_for_pool(table);",
        "page-table pool duplicate detection must run before zeroing/reinserting",
    )
    require_tokens(
        release_pagemap_body,
        [
            "if (try_release_page_table_to_pool(pagemap))",
            "return;",
            "phys::page_free(pagemap);",
        ],
        "release_pagemap pool handoff",
    )


def main() -> None:
    test_nonfixed_mmap_address_selection_is_reserved_before_mapping()
    test_owned_frame_tracking_is_disabled_off_the_fault_path()
    test_cow_write_resolution_serializes_pte_reference_consumption()
    test_page_table_pool_duplicate_release_does_not_fall_through_to_page_free()
    print("vmem mmap, owned-frame, and COW invariants hold")


if __name__ == "__main__":
    main()
