#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SYS_VMEM_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "vmem" / "sys_vmem.cpp"
SYS_VMEM_HPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "vmem" / "sys_vmem.hpp"
THREAD_CONTROL_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "multiproc" / "threadControl.cpp"
PROCESS_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "process" / "process.cpp"
VIRT_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "mm" / "virt.opt.cpp"
TASK_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "task.cpp"
TASK_HPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "task.hpp"
THREADING_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "threading.cpp"
THREADING_HPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "threading.hpp"
SHM_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "shm" / "shm.cpp"
VMEM_ABI_HPP = ROOT / "modules" / "kern" / "src" / "abi" / "callnums" / "vmem.h"
DEBUG_FLAGS_DOC = ROOT / "docs" / "kernel_debug_flags.md"


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
        "sync_file_mmap_range(*task, addr, size)",
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
        "protect_shared_vmem_range_locked(task, ADDR, size, PROT)",
        "mprotect must validate overflow before side effects",
    )
    if "size = page_align_up(size)" in protect:
        fail("mprotect must not use unchecked page_align_up(size)")


def test_nonfixed_mmap_address_selection_is_reserved_before_mapping() -> None:
    source = SYS_VMEM_CPP.read_text()
    reserve_body = function_body(source, "reserve_free_mmap_range_locked")
    anon_body = function_body(source, "anon_allocate")
    file_body = function_body(source, "file_allocate")

    require_tokens(
        source,
        [
            "ker::mod::sys::Mutex g_shared_vmem_publication_lock;",
            "release_mmap_reservation_locked(task, vaddr, size, HAS_ADDRESS_RESERVATION)",
        ],
        "mmap address reservation surface",
    )
    if "g_mmap_reserve_lock" in source:
        fail("mmap reservation must not nest a standalone sleeping mutex inside shared VM publication")
    require_tokens(
        reserve_body,
        [
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
                "SharedVmemPublicationGuard publication_guard;",
                "reserve_free_mmap_range_locked(task, size, hint, vaddr)",
                "bool const HAS_ADDRESS_RESERVATION = !IS_FIXED;",
            ],
            f"{body_name} non-fixed reservation path",
        )

    require_order(
        file_body,
        "reserve_free_mmap_range_locked(task, size, hint, vaddr)",
        "if (file_mmap_can_share(st, prot))",
        "file mmap must reserve the address before eager file mapping",
    )
    require_order(
        file_body,
        "release_mmap_reservation_locked(task, vaddr, size, HAS_ADDRESS_RESERVATION)",
        "advance_shared_mmap_cursor_locked(task, vaddr, size)",
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
            "if (!add_shared_vmem_range_locked(task, vaddr, size, prot, flags))",
            "release_mmap_reservation_locked(task, vaddr, size, HAS_ADDRESS_RESERVATION)",
            "return static_cast<uint64_t>(-ker::abi::vmem::VMEM_ENOMEM)",
        ],
        "anonymous lazy metadata failure must release non-fixed reservation",
    )

    require_ordered_tokens(
        anon_body,
        [
            "uint64_t const RESULT = private_anon_allocate_locked(task, vaddr, size, prot, hint, flags)",
            "int const RESULT_STATUS = perf_status_from_vmem_result(RESULT)",
            "record_local_vmem_event",
            "if (RESULT_STATUS != 0)",
            "release_mmap_reservation_locked(task, vaddr, size, HAS_ADDRESS_RESERVATION)",
            "return RESULT",
        ],
        "private anonymous failure must release non-fixed reservation",
    )

    zero_page_block = block_between(
        anon_body,
        "ker::mod::mm::phys::page_ref_add(ZERO_PAGE, NUM_PAGES)",
        "advance_shared_mmap_cursor_locked(task, vaddr, size)",
        "anonymous zero-page metadata branch",
    )
    require_ordered_tokens(
        zero_page_block,
        [
            "map_same_page_range(task->pagemap, vaddr, ZERO_PADDR, NUM_PAGES, PAGE_FLAGS)",
            "if (!add_shared_vmem_range_locked(task, vaddr, size, prot, flags))",
            "rollback_mapped_pages(task, vaddr, NUM_PAGES)",
            "release_mmap_reservation_locked(task, vaddr, size, HAS_ADDRESS_RESERVATION)",
            "return static_cast<uint64_t>(-ker::abi::vmem::VMEM_ENOMEM)",
        ],
        "zero-page anonymous metadata failure must release non-fixed reservation",
    )

    private_anon = function_body(source, "private_anon_allocate_locked")
    require_ordered_tokens(
        private_anon,
        [
            "map_page(task->pagemap, current_vaddr, PADDR, PAGE_FLAGS)",
            "if (!add_shared_vmem_range_locked(task, vaddr, size, prot, flags))",
            "rollback_mapped_pages(task, vaddr, mapped_pages)",
        ],
        "eager private anonymous mapping publication and rollback",
    )


def test_kernel_managed_initial_layout_cannot_be_replaced() -> None:
    vmem = SYS_VMEM_CPP.read_text()
    task_source = TASK_CPP.read_text()
    threading_source = THREADING_CPP.read_text()
    threading_header = THREADING_HPP.read_text()

    require_tokens(
        threading_header,
        [
            "uint64_t layout_base_virt{};",
            "uint64_t layout_size{};",
            "range_overlaps_initial_layout(const Thread* thread, uint64_t start, uint64_t size)",
        ],
        "kernel-managed initial layout metadata",
    )
    require_tokens(
        threading_source,
        [
            "mm::virt::reserve_page_range(page_table, layout_base, layout_size / mm::paging::PAGE_SIZE)",
            "thread->layout_base_virt = layout_base",
            "thread->layout_size = layout_size",
        ],
        "guarded initial layout reservation",
    )
    require_tokens(
        task_source,
        [
            "thr->layout_base_virt = parent->thread->layout_base_virt",
            "thr->layout_size = parent->thread->layout_size",
        ],
        "shared-address-space layout metadata inheritance",
    )

    overlap_helper = function_body(vmem, "overlaps_kernel_managed_user_layout_locked")
    require_tokens(
        overlap_helper,
        [
            "ker::mod::sched::threading::range_overlaps_initial_layout(task->thread, start, size)",
            "find_active_task_lifetime_ref_if(task_has_overlapping_kernel_managed_layout, &query)",
            "owner->release();",
        ],
        "shared-pagemap-wide VM guard-range check",
    )
    for function_name in ["anon_allocate", "file_allocate", "anon_free"]:
        body = function_body(vmem, function_name)
        require_order(
            body,
            "SharedVmemPublicationGuard publication_guard;",
            "overlaps_kernel_managed_user_layout_locked(task,",
            f"{function_name} guard-range check must hold shared VM publication",
        )
        if "overlaps_kernel_managed_user_layout_locked(task," not in body:
            fail(f"{function_name} must reject overlap with the kernel-managed initial layout")
    protect = block_between(
        vmem,
        "case ker::abi::vmem::ops::PROTECT:",
        "case ker::abi::vmem::ops::MREMAP:",
        "mprotect syscall case",
    )
    require_order(
        protect,
        "SharedVmemPublicationGuard publication_guard;",
        "overlaps_kernel_managed_user_layout_locked(task, ADDR, size)",
        "mprotect guard-range check must hold shared VM publication",
    )
    if "overlaps_kernel_managed_user_layout_locked(task, ADDR, size)" not in protect:
        fail("mprotect must not materialize or weaken kernel-managed guard pages")


def test_sysv_shm_publication_respects_vm_reservations_and_guards() -> None:
    shm = SHM_CPP.read_text()

    range_check = function_body(shm, "range_is_free")
    require_tokens(
        range_check,
        [
            "range_overlaps_shared_initial_layout(task, addr, size)",
            "range_overlaps_lazy_vmem(task, addr, size)",
            "is_page_mapped_or_reserved(task->pagemap, current)",
        ],
        "SysV SHM occupied-range check",
    )

    layout_check = function_body(shm, "range_overlaps_shared_initial_layout")
    require_tokens(
        layout_check,
        [
            "range_overlaps_initial_layout(task->thread, addr, size)",
            "find_active_task_lifetime_ref_if(task_has_overlapping_initial_layout, &query)",
            "owner->release();",
        ],
        "SysV SHM shared-pagemap guard check",
    )

    attach = function_body(shm, "shmat_impl")
    require_ordered_tokens(
        attach,
        [
            "SharedVmemPublicationGuard publication_guard;",
            "g_lock.lock_irqsave();",
            ".publishing = true,",
            "segment->attach_count++;",
            "g_lock.unlock_irqrestore(FLAGS);",
            "is_page_mapped_or_reserved(task->pagemap, PAGE_ADDR)",
            "map_page(task->pagemap, PAGE_ADDR",
            "attachment->publishing = false;",
            "attachment->active = true;",
        ],
        "SysV SHM check/reserve/publish transaction",
    )
    require_tokens(
        attach,
        ["rollback_attachment_pages(task, segment, ADDR, mapped_pages);", "abandon_attachment_publication(attachment);"],
        "SysV SHM partial-publication rollback",
    )
    require_tokens(
        function_body(shm, "rollback_attachment_pages"),
        ["translate(task->pagemap, PAGE_ADDR) == backing_paddr(*segment, i)"],
        "SysV SHM rollback must not unmap a replacement publisher's page",
    )

    detach = function_body(shm, "shmdt_impl")
    require_ordered_tokens(
        detach,
        [
            "SharedVmemPublicationGuard publication_guard;",
            "g_lock.lock_irqsave();",
            "ShmAttachment const DETACHED = *attachment;",
            "g_lock.unlock_irqrestore(FLAGS);",
            "translate(task->pagemap, ADDR) == backing_paddr(*segment, i)",
            "unmap_page(task->pagemap, ADDR);",
            "g_lock.lock_irqsave();",
            "detach_attachment_locked(*attachment, task);",
        ],
        "SysV SHM detach lock order",
    )


def test_owned_frame_tracking_is_disabled_off_the_fault_path() -> None:
    source = VIRT_CPP.read_text()
    snapshot_body = function_body(source, "get_owned_frame_stats_snapshot")

    require_tokens(source, ["constexpr bool OWNED_FRAME_TRACKING_ENABLED = false;"], "owned frame tracking switch")
    require_tokens(
        (ROOT / "modules/kern/src/test/mm_ktest.cpp").read_text(),
        [
            "KEXPECT_EQ(after_map.track_attempts, before.track_attempts);",
            "KEXPECT_EQ(after_map.track_skipped, before.track_skipped);",
            "KEXPECT_EQ(after_unmap.untrack_missed, after_map.untrack_missed);",
        ],
        "disabled owned-frame tracking KTEST contract",
    )
    early_return_entrypoints = {
        "owned_frame_insert_private_mapping": "auto& table = owned_frame_table",
        "owned_frame_track_private_mapping": "uint64_t const PHYS_ADDR",
        "owned_frame_track_fresh_normal_mapping": "uint64_t const PHYS_ADDR",
        "owned_frame_untrack_mapping": "owned_frame_stats.untrack_attempts.fetch_add",
        "owned_frame_untrack_leaf": "if (entry.frame == 0)",
        "owned_frame_refresh_leaf": "if (entry.present == 0",
        "owned_frame_purge_pagemap": "owned_frame_stats.purge_calls.fetch_add",
    }
    for function_name, expensive_token in early_return_entrypoints.items():
        body = function_body(source, function_name)
        require_ordered_tokens(
            body,
            [
                "if constexpr (!OWNED_FRAME_TRACKING_ENABLED)",
                "return;",
                expensive_token,
            ],
            f"{function_name} disabled fast path",
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


def test_mapping_replacement_releases_displaced_reference_after_tlb_flush() -> None:
    source = VIRT_CPP.read_text()
    map_body = function_body(source, "map_page")
    batch_body = function_body(source, "map_page_batched")

    require_tokens(
        map_body,
        [
            "PageTableEntry old_entry{};",
            "bool replaced_present_frame",
            "drop_present_leaf_ref(old_entry);",
        ],
        "single-page mapping replacement ownership",
    )
    require_ordered_tokens(
        map_body,
        [
            "entry = paging::create_page_table_entry(PADDR, FLAGS);",
            "shootdown_remote_user_pagemap(page_table, VADDR, path_promoted);",
            "if (replaced_present_frame)",
            "drop_present_leaf_ref(old_entry);",
        ],
        "single-page replacement must invalidate before releasing the displaced frame",
    )

    require_tokens(
        batch_body,
        [
            "PageTableEntry old_entry{};",
            "bool replaced_present_frame",
            "flush_page_map_batch(batch);",
            "drop_present_leaf_ref(old_entry);",
        ],
        "batched mapping replacement ownership",
    )
    require_ordered_tokens(
        batch_body,
        [
            "entry = paging::create_page_table_entry(PADDR, FLAGS);",
            "if (replaced_present_frame)",
            "flush_page_map_batch(batch);",
            "drop_present_leaf_ref(old_entry);",
        ],
        "batched replacement must invalidate before releasing the displaced frame",
    )


def test_user_memory_pressure_does_not_enter_fatal_oom() -> None:
    vmem = SYS_VMEM_CPP.read_text()
    virt = VIRT_CPP.read_text()

    require_tokens(
        function_body(vmem, "file_mmap_cached_page_for_file"),
        [
            "page_alloc_full_overwrite_page_with_reclaim_may_fail(",
            'ker::mod::mm::PhysicalPageOwner::USER_FILE_CACHE, "vmem-file-cache")',
            "page_alloc_with_reclaim_may_fail(ker::mod::mm::PhysicalPageOwner::USER_FILE_CACHE,",
            'ker::mod::mm::paging::PAGE_SIZE, "vmem-file-cache")',
        ],
        "file mmap cache allocation pressure",
    )
    require_tokens(
        function_body(virt, "alloc_cow_destination_page"),
        [
            'page_alloc_full_overwrite_page_with_reclaim_may_fail(PhysicalPageOwner::USER_PRIVATE_MAPPING, "cow_copy")',
            'page_alloc_with_reclaim_may_fail(PhysicalPageOwner::USER_PRIVATE_MAPPING, paging::PAGE_SIZE, "cow_zero")',
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
        [
            'page_alloc_may_fail(PhysicalPageOwner::USER_PRIVATE_MAPPING, paging::PAGE_SIZE, "lazy-vmem")',
            "cow_pte_lock.lock_irqsave()",
            "anonymous_lazy_range_allows_fault(task, PAGE_VADDR, fault, current_prot)",
            "translate(task->pagemap, PAGE_VADDR) != PADDR_INVALID",
            "if (ALLOWED && !ALREADY_MAPPED)",
            "phys::page_ref_dec(PAGE);",
        ],
        "IRQ-disabled anonymous lazy fault allocation and shared-pagemap install",
    )
    lazy_body = function_body(virt, "handle_lazy_vmem_fault")
    require_order(
        lazy_body,
        "task->lazy_vmem_lock.unlock_irqrestore(IRQF);",
        'page_alloc_may_fail(PhysicalPageOwner::USER_PRIVATE_MAPPING, paging::PAGE_SIZE, "lazy-vmem")',
        "lazy anonymous allocation must happen outside the task range lock",
    )
    require_order(
        lazy_body,
        "cow_pte_lock.lock_irqsave()",
        "map_page(task->pagemap, PAGE_VADDR, PADDR, lazy_user_vmem_flags(current_prot));",
        "lazy anonymous PTE install must happen under the shared PTE lock",
    )


def test_kasan_excluded_lazy_file_snapshot_is_unpoisoned_after_unlock() -> None:
    lazy_body = function_body(VIRT_CPP.read_text(), "handle_lazy_vmem_fault")
    require_ordered_tokens(
        lazy_body,
        [
            "auto file_range = range;",
            "ker::vfs::vfs_retain_file(file_range.file);",
            "task->lazy_vmem_lock.unlock_irqrestore(IRQF);",
            "if (kasan::is_enabled())",
            "kasan::unpoison_range(&file_range, sizeof(file_range));",
            "ker::syscall::vmem::materialize_lazy_file_page(task, file_range, PAGE_VADDR, fault)",
        ],
        "KASAN-excluded lazy file snapshot boundary",
    )


def test_file_mmap_cache_is_sharded_without_changing_page_ownership() -> None:
    vmem = SYS_VMEM_CPP.read_text()
    require_tokens(
        vmem,
        [
            "constexpr size_t FILE_MMAP_CACHE_LOCK_COUNT = 256",
            "struct alignas(64) FileMmapCacheLock",
            "std::array<FileMmapCacheLock, FILE_MMAP_CACHE_LOCK_COUNT> g_file_mmap_cache_locks{}",
            "size_t const LOCK_INDEX = SET_INDEX & (FILE_MMAP_CACHE_LOCK_COUNT - 1)",
        ],
        "sharded file mmap cache",
    )
    if re.search(r"\bg_file_mmap_cache_lock\b", vmem) or re.search(r"\bg_file_mmap_cache_clock\b", vmem):
        fail("file mmap cache must not retain its global lock or timestamp atomic")

    lookup = function_body(vmem, "file_mmap_cache_lookup")
    require_ordered_tokens(
        lookup,
        [
            "cache_lock.mutex.lock()",
            "ker::mod::mm::phys::page_ref_inc(entry.page)",
            "cache_lock.mutex.unlock()",
            "return page",
        ],
        "file mmap cache lookup page ownership",
    )

    insert = function_body(vmem, "file_mmap_cache_insert_or_discard")
    require_ordered_tokens(
        insert,
        [
            "ker::mod::mm::phys::page_ref_inc(new_page)",
            "*page_for_mapping = new_page",
            "cache_lock.mutex.unlock()",
            "release_file_mmap_cache_page(evicted)",
        ],
        "file mmap cache insertion page ownership",
    )
    require_ordered_tokens(
        function_body(vmem, "release_file_mmap_cache_page"),
        [
            "ker::mod::mm::phys::page_ref_get(page) > 1",
            "ker::mod::mm::phys::page_reassign_owner(page, ker::mod::mm::PhysicalPageOwner::USER_FILE_MAPPING)",
            "ker::mod::mm::phys::page_ref_dec(page)",
        ],
        "file mmap cache eviction ownership transfer",
    )


def test_default_writable_anon_mmap_is_demand_paged() -> None:
    vmem = SYS_VMEM_CPP.read_text()
    abi = VMEM_ABI_HPP.read_text()
    docs = DEBUG_FLAGS_DOC.read_text()
    anon_body = function_body(vmem, "anon_allocate")
    lazy_branch = block_between(
        anon_body,
        "if (lazy_anon_mmap_enabled() && (flags & ker::abi::vmem::MAP_POPULATE) == 0)",
        "if (!anon_zero_cow_enabled())",
        "default writable anonymous lazy branch",
    )

    require_tokens(
        function_body(vmem, "lazy_anon_mmap_enabled"),
        [
            'cmdline_has_token(ker::init::get_kernel_cmdline(), "vmem.no_lazy_anon")',
            'log::info("lazy anonymous mmap %s"',
        ],
        "lazy anonymous mmap runtime control",
    )
    require_tokens(
        abi,
        ["constexpr uint64_t MAP_POPULATE = 0x8000;"],
        "kernel mmap flag ABI",
    )
    require_ordered_tokens(
        lazy_branch,
        [
            "if (!add_shared_vmem_range_locked(task, vaddr, size, prot, flags))",
            "release_mmap_reservation_locked(task, vaddr, size, HAS_ADDRESS_RESERVATION)",
            "advance_shared_mmap_cursor_locked(task, vaddr, size)",
            "WkiPerfLocalVmemOp::ANON_MMAP",
            "return vaddr;",
        ],
        "default writable anonymous mmap demand paging",
    )
    if "get_anon_zero_page()" in lazy_branch or "map_same_page_range(" in lazy_branch:
        fail("default writable anonymous mmap must not prepopulate zero-page PTEs")
    require_tokens(
        docs,
        ["`vmem.no_lazy_anon`"],
        "lazy anonymous mmap diagnostic documentation",
    )


def test_thread_publication_is_serialized_with_shared_vmem_updates() -> None:
    header = SYS_VMEM_HPP.read_text()
    vmem = SYS_VMEM_CPP.read_text()
    thread_control = THREAD_CONTROL_CPP.read_text()
    process = PROCESS_CPP.read_text()
    task_source = TASK_CPP.read_text()
    task_header = TASK_HPP.read_text()

    require_tokens(
        header,
        [
            "class SharedVmemPublicationGuard",
            "SharedVmemPublicationGuard();",
            "~SharedVmemPublicationGuard();",
        ],
        "shared VM publication guard API",
    )
    for function_name in ["anon_allocate", "file_allocate", "anon_free"]:
        require_tokens(
            function_body(vmem, function_name),
            ["SharedVmemPublicationGuard publication_guard;"],
            f"{function_name} shared VM transaction serialization",
        )
    require_tokens(
        function_body(vmem, "update_shared_vmem_ranges_locked"),
        [
            "std::array<ker::mod::sched::task::Task*, INLINE_TASKS> inline_tasks{}",
            "snapshot_active_task_lifetime_refs_if(MATCH_SIBLING, PAGEMAP, tasks, capacity)",
            "new (std::nothrow) ker::mod::sched::task::Task*[task_count]",
            "for (size_t i = 0; i < task_count; ++i)",
            "candidate->release();",
            "delete[] heap_tasks;",
        ],
        "stable shared VM metadata task snapshot",
    )
    shared_update = function_body(vmem, "update_shared_vmem_ranges_locked")
    require_ordered_tokens(
        shared_update,
        [
            "if (!task->shares_user_pagemap.load(std::memory_order_acquire))",
            "return update(task);",
            "snapshot_active_task_lifetime_refs_if(MATCH_SIBLING, PAGEMAP, tasks, capacity)",
        ],
        "unshared pagemap metadata update fast path",
    )
    if "get_active_task_count()" in shared_update or "get_active_task_at_safe(i)" in shared_update:
        fail("shared VM propagation must not iterate a mutable active-task array by count/index")
    if "find_active_task_lifetime_ref_if" in shared_update:
        fail("shared VM propagation must not rescan the registry once per sibling")

    create_case = block_between(
        thread_control,
        "case abi::multiproc::threadControlOps::THREAD_CREATE:",
        "case abi::multiproc::threadControlOps::THREAD_EXIT:",
        "thread creation syscall case",
    )
    require_ordered_tokens(
        create_case,
        [
            "std::array<uint64_t, 2> prepared_stack_words{};",
            "copy_from_task(*parent, user_sp, prepared_stack_words.data(), sizeof(prepared_stack_words))",
            "ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;",
            "Task::create_user_thread(parent, tcb_va, user_sp, enter_va, prepared_stack_words.at(0),",
            "prepared_stack_words.at(1));",
            "post_task_for_cpu(TARGET_CPU, t)",
        ],
        "thread lazy-range clone through scheduler publication",
    )
    inherited_exit_pos = create_case.find("inherit_pending_process_exit_request(parent, t)")
    saved_pid_pos = create_case.find("thread_pid = t->pid;", inherited_exit_pos)
    guard_scope_end = create_case.find("\n            }\n", saved_pid_pos)
    current_exit_pos = create_case.find("exit_current_if_process_exit_requested();", saved_pid_pos)
    if (
        inherited_exit_pos < 0
        or saved_pid_pos < 0
        or guard_scope_end < 0
        or current_exit_pos < guard_scope_end
    ):
        fail("pending group exit must be checked only after the shared VM publication guard is destroyed")
    require_tokens(
        task_header,
        ["std::atomic<bool> shares_user_pagemap{false};"],
        "shared user pagemap marker",
    )
    require_ordered_tokens(
        task_source,
        [
            "Task* Task::create_user_thread(Task* parent",
            "clone_lazy_vmem_ranges(*t, *parent)",
            "parent->shares_user_pagemap.store(true, std::memory_order_release);",
            "t->shares_user_pagemap.store(true, std::memory_order_release);",
        ],
        "thread shared-pagemap marking",
    )

    clone_vm = function_body(process, "wos_proc_clone_vm")
    require_ordered_tokens(
        clone_vm,
        [
            "ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;",
            "child->pagemap = parent->pagemap;",
            "clone_lazy_vmem_ranges(*child, *parent)",
            "parent->shares_user_pagemap.store(true, std::memory_order_release);",
            "child->shares_user_pagemap.store(true, std::memory_order_release);",
            "post_task_balanced(child)",
        ],
        "clone-VM lazy-range clone through scheduler publication",
    )


def test_fork_snapshots_all_vm_surfaces_under_one_publication_guard() -> None:
    process = PROCESS_CPP.read_text()
    vmem = SYS_VMEM_CPP.read_text()
    fork = function_body(process, "wos_proc_fork")
    snapshot = block_between(
        fork,
        "bool fork_vm_snapshot_ok = false;",
        "if (!fork_vm_snapshot_ok)",
        "regular fork VM snapshot",
    )
    require_ordered_tokens(
        snapshot,
        [
            "SharedVmemPublicationGuard publication_guard;",
            "child->mmap_next.store(parent->mmap_next.load(std::memory_order_relaxed)",
            "clone_lazy_vmem_ranges(*child, *parent)",
            "deep_copy_user_pagemap_cow(parent->pagemap, child->pagemap)",
            "shm_clone_for_fork_locked(parent, child)",
            "clone_file_mmap_ranges_for_pagemap_locked(parent->pagemap, child->pagemap)",
        ],
        "coherent regular fork VM snapshot",
    )
    if "release_file_mmap_ranges_for_pagemap" in snapshot or "shm_cleanup_for_task" in snapshot:
        fail("regular fork rollback must run after dropping the shared VM publication guard")

    failed_snapshot = block_between(
        fork,
        "if (!fork_vm_snapshot_ok)",
        "// --- Clone thread metadata ---",
        "regular fork VM snapshot rollback",
    )
    require_ordered_tokens(
        failed_snapshot,
        [
            "shm_cleanup_for_task(child)",
            "release_file_mmap_ranges_for_pagemap(child->pagemap)",
            "release_lazy_vmem_ranges(*child)",
            "destroy_user_space(child->pagemap",
        ],
        "regular fork VM snapshot rollback",
    )
    if fork.count("sched::task::release_lazy_vmem_ranges(*child);") < 5:
        fail("every post-snapshot regular fork failure must release cloned lazy VM references")

    file_clone = function_body(vmem, "clone_file_mmap_ranges_for_pagemap_locked")
    if "release_file_mmap_ranges_for_pagemap(dst)" in file_clone:
        fail("file mmap clone rollback may block and must not run under the fork publication guard")


def main() -> None:
    test_munmap_and_mprotect_reject_overflowing_lengths()
    test_nonfixed_mmap_address_selection_is_reserved_before_mapping()
    test_kernel_managed_initial_layout_cannot_be_replaced()
    test_sysv_shm_publication_respects_vm_reservations_and_guards()
    test_owned_frame_tracking_is_disabled_off_the_fault_path()
    test_cow_write_resolution_serializes_pte_reference_consumption()
    test_page_table_pool_duplicate_release_does_not_fall_through_to_page_free()
    test_mapping_replacement_releases_displaced_reference_after_tlb_flush()
    test_user_memory_pressure_does_not_enter_fatal_oom()
    test_kasan_excluded_lazy_file_snapshot_is_unpoisoned_after_unlock()
    test_default_writable_anon_mmap_is_demand_paged()
    test_thread_publication_is_serialized_with_shared_vmem_updates()
    test_fork_snapshots_all_vm_surfaces_under_one_publication_guard()
    print("vmem mmap, owned-frame, and COW invariants hold")


if __name__ == "__main__":
    main()
