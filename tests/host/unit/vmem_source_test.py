#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SYS_VMEM_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "vmem" / "sys_vmem.cpp"
SYS_VMEM_HPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "vmem" / "sys_vmem.hpp"
THREAD_CONTROL_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "multiproc" / "threadControl.cpp"
PROCESS_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "process" / "process.cpp"
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


def require_ordered_tokens(source: str, tokens: list[str], context: str) -> None:
    cursor = 0
    for token in tokens:
        found = source.find(token, cursor)
        if found < 0:
            fail(f"{context}: missing ordered token {token}")
        cursor = found + len(token)


def block_between(source: str, start_token: str, end_token: str, context: str) -> str:
    start = source.find(start_token)
    if start < 0:
        fail(f"{context}: missing start token {start_token}")
    end = source.find(end_token, start + len(start_token))
    if end < 0:
        fail(f"{context}: missing end token {end_token}")
    return source[start:end]


def test_munmap_and_mprotect_reject_overflowing_lengths() -> None:
    source = SYS_VMEM_CPP.read_text()
    helper = function_body(source, "align_user_vmem_size")
    require_tokens(
        helper,
        [
            "constexpr uint64_t PAGE_MASK = ker::mod::mm::paging::PAGE_SIZE - 1",
            "size == 0 || aligned_size == nullptr || size > UINT64_MAX - PAGE_MASK",
            "return -ker::abi::vmem::VMEM_EINVAL",
            "uint64_t const ALIGNED = page_align_up(size)",
            "if (ALIGNED == 0)",
            "*aligned_size = ALIGNED",
        ],
        "checked user vmem size alignment",
    )

    anon_free = function_body(source, "anon_free")
    require_order(
        anon_free,
        "int const SIZE_RET = align_user_vmem_size(size, &size)",
        "if (addr + size > USER_SPACE_END || addr + size < addr)",
        "munmap must validate overflow before range checks",
    )
    require_order(
        anon_free,
        "int const SIZE_RET = align_user_vmem_size(size, &size)",
        "sync_file_mmap_range(task->pagemap, addr, size)",
        "munmap must validate overflow before side effects",
    )
    if "size = page_align_up(size)" in anon_free:
        fail("munmap must not use unchecked page_align_up(size)")

    protect = block_between(
        source,
        "case ker::abi::vmem::ops::PROTECT:",
        "case ker::abi::vmem::ops::MREMAP:",
        "mprotect syscall case",
    )
    require_order(
        protect,
        "int const SIZE_RET = align_user_vmem_size(size, &size)",
        "if (ADDR + size > USER_SPACE_END || ADDR + size < ADDR)",
        "mprotect must validate overflow before range checks",
    )
    require_order(
        protect,
        "int const SIZE_RET = align_user_vmem_size(size, &size)",
        "protect_shared_vmem_range(task, ADDR, size, PROT)",
        "mprotect must validate overflow before side effects",
    )
    if "size = page_align_up(size)" in protect:
        fail("mprotect must not use unchecked page_align_up(size)")


def test_nonfixed_mmap_address_selection_is_reserved_before_mapping() -> None:
    source = SYS_VMEM_CPP.read_text()
    reserve_body = function_body(source, "reserve_free_mmap_range")
    anon_body = function_body(source, "anon_allocate")
    file_body = function_body(source, "file_allocate")

    require_tokens(
        source,
        [
            "ker::mod::sys::Mutex g_shared_vmem_publication_lock;",
            "release_mmap_reservation(task, vaddr, size, HAS_ADDRESS_RESERVATION)",
        ],
        "mmap address reservation surface",
    )
    if "g_mmap_reserve_lock" in source:
        fail("mmap reservation must not nest a standalone sleeping mutex inside shared VM publication")
    require_tokens(
        reserve_body,
        [
            "SharedVmemPublicationGuard publication_guard;",
            "uint64_t const VADDR = find_free_range(task, size, hint)",
            "update_shared_vmem_ranges_locked(",
            "add_lazy_vmem_range(candidate, VADDR, size, 0, 0)",
            "out_vaddr = VADDR",
        ],
        "mmap reservation helper",
    )
    require_order(
        reserve_body,
        "uint64_t const VADDR = find_free_range(task, size, hint)",
        "add_lazy_vmem_range(candidate, VADDR, size, 0, 0)",
        "non-fixed mmap must publish a reservation before mapping work starts",
    )
    require_ordered_tokens(
        reserve_body,
        [
            "bool const RESERVED = update_shared_vmem_ranges_locked(",
            "if (!RESERVED)",
            "remove_lazy_vmem_range(candidate, VADDR, size)",
            "return ker::abi::vmem::VMEM_ENOMEM",
        ],
        "failed mmap reservation publication must roll back partial shared ranges",
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

    noreserve_block = block_between(
        anon_body,
        "if (is_prot_none(prot) || (flags & ker::abi::vmem::MAP_NORESERVE) != 0)",
        "if ((prot & ker::abi::vmem::PROT_WRITE) == 0)",
        "anonymous PROT_NONE/MAP_NORESERVE branch",
    )
    require_ordered_tokens(
        noreserve_block,
        [
            "if (!add_shared_vmem_range(task, vaddr, size, prot, flags))",
            "release_mmap_reservation(task, vaddr, size, HAS_ADDRESS_RESERVATION)",
            "return static_cast<uint64_t>(-ker::abi::vmem::VMEM_ENOMEM)",
        ],
        "anonymous lazy metadata failure must release non-fixed reservation",
    )

    require_ordered_tokens(
        anon_body,
        [
            "uint64_t const RESULT = private_anon_allocate(task, vaddr, size, prot, hint, flags)",
            "int const RESULT_STATUS = perf_status_from_vmem_result(RESULT)",
            "record_local_vmem_event",
            "if (RESULT_STATUS != 0)",
            "release_mmap_reservation(task, vaddr, size, HAS_ADDRESS_RESERVATION)",
            "return RESULT",
        ],
        "private anonymous failure must release non-fixed reservation",
    )

    zero_page_block = block_between(
        anon_body,
        "auto const ZERO_PADDR",
        "ker::mod::mm::phys::page_ref_add(ZERO_PAGE, NUM_PAGES)",
        "anonymous zero-page metadata branch",
    )
    require_ordered_tokens(
        zero_page_block,
        [
            "if (!add_shared_vmem_range(task, vaddr, size, prot, flags))",
            "release_mmap_reservation(task, vaddr, size, HAS_ADDRESS_RESERVATION)",
            "return static_cast<uint64_t>(-ker::abi::vmem::VMEM_ENOMEM)",
        ],
        "zero-page anonymous metadata failure must release non-fixed reservation",
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
            "phys::page_ref_dec(old_virt, &cow_lookup);",
            "if (old_mapping_refcounted)",
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


def test_user_memory_pressure_does_not_enter_fatal_oom() -> None:
    vmem = SYS_VMEM_CPP.read_text()
    virt = VIRT_CPP.read_text()

    require_tokens(
        function_body(vmem, "file_mmap_cached_page_for_file"),
        [
            'page_alloc_full_overwrite_page_with_reclaim_may_fail("vmem-file-cache")',
            'page_alloc_with_reclaim_may_fail(ker::mod::mm::paging::PAGE_SIZE, "vmem-file-cache")',
        ],
        "file mmap cache allocation pressure",
    )
    require_tokens(
        function_body(virt, "alloc_cow_destination_page"),
        [
            'page_alloc_full_overwrite_page_with_reclaim_may_fail("cow_copy")',
            'page_alloc_with_reclaim_may_fail(paging::PAGE_SIZE, "cow_zero")',
        ],
        "COW destination allocation pressure",
    )
    cow_body = function_body(virt, "resolve_user_write_mapping")
    require_tokens(
        cow_body,
        [
            "if (new_page == nullptr)",
            "phys::page_ref_dec(old_virt, &cow_lookup);",
            "return UserWriteFaultStatus::NOT_WRITABLE;",
        ],
        "COW allocation failure rollback",
    )
    cow_failure = block_between(cow_body, "if (new_page == nullptr)", "if (DESTINATION_FULL_OVERWRITE_BEFORE_EXPOSURE)", "COW OOM branch")
    if "hcf()" in cow_failure:
        fail("recoverable COW allocation failure must not halt the kernel")
    require_tokens(
        function_body(virt, "handle_lazy_vmem_fault"),
        ['page_alloc_may_fail(paging::PAGE_SIZE, "lazy-vmem")'],
        "IRQ-disabled anonymous lazy fault allocation",
    )


def test_thread_publication_is_serialized_with_shared_vmem_updates() -> None:
    header = SYS_VMEM_HPP.read_text()
    vmem = SYS_VMEM_CPP.read_text()
    thread_control = THREAD_CONTROL_CPP.read_text()
    process = PROCESS_CPP.read_text()

    require_tokens(
        header,
        [
            "class SharedVmemPublicationGuard",
            "SharedVmemPublicationGuard();",
            "~SharedVmemPublicationGuard();",
        ],
        "shared VM publication guard API",
    )
    require_tokens(
        function_body(vmem, "update_shared_vmem_ranges"),
        ["SharedVmemPublicationGuard publication_guard;", "update_shared_vmem_ranges_locked(task, std::move(update))"],
        "shared VM metadata update serialization",
    )
    require_tokens(
        function_body(vmem, "update_shared_vmem_ranges_locked"),
        [
            "find_active_task_lifetime_ref_if(MATCH_UNVISITED_SIBLING, &scan)",
            "if (!tasks.push_back(candidate))",
            "for (auto* candidate : tasks)",
            "candidate->release();",
        ],
        "stable shared VM metadata task snapshot",
    )
    shared_update = function_body(vmem, "update_shared_vmem_ranges_locked")
    if "get_active_task_count()" in shared_update or "get_active_task_at_safe(i)" in shared_update:
        fail("shared VM propagation must not iterate a mutable active-task array by count/index")

    create_case = block_between(
        thread_control,
        "case abi::multiproc::threadControlOps::THREAD_CREATE:",
        "case abi::multiproc::threadControlOps::THREAD_EXIT:",
        "thread creation syscall case",
    )
    require_ordered_tokens(
        create_case,
        [
            "ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;",
            "Task::create_user_thread(parent, tcb_va, user_sp, enter_va)",
            "post_task_for_cpu(TARGET_CPU, t)",
        ],
        "thread lazy-range clone through scheduler publication",
    )

    clone_vm = function_body(process, "wos_proc_clone_vm")
    require_ordered_tokens(
        clone_vm,
        [
            "ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;",
            "child->pagemap = parent->pagemap;",
            "clone_lazy_vmem_ranges(*child, *parent)",
            "post_task_balanced(child)",
        ],
        "clone-VM lazy-range clone through scheduler publication",
    )


def main() -> None:
    test_munmap_and_mprotect_reject_overflowing_lengths()
    test_nonfixed_mmap_address_selection_is_reserved_before_mapping()
    test_owned_frame_tracking_is_disabled_off_the_fault_path()
    test_cow_write_resolution_serializes_pte_reference_consumption()
    test_page_table_pool_duplicate_release_does_not_fall_through_to_page_free()
    test_user_memory_pressure_does_not_enter_fatal_oom()
    test_thread_publication_is_serialized_with_shared_vmem_updates()
    print("vmem mmap, owned-frame, and COW invariants hold")


if __name__ == "__main__":
    main()
