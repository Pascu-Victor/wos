#!/usr/bin/env python3

import re
import os
from pathlib import Path


ROOT = Path(os.environ.get("WOS_LAZY_BACKING_ROOT", Path(__file__).resolve().parents[3]))
SYS_VMEM_CPP = Path(os.environ.get("WOS_LAZY_BACKING_VMEM_SOURCE", ROOT / "modules/kern/src/syscalls_impl/vmem/sys_vmem.cpp"))
SYS_VMEM_HPP = Path(os.environ.get("WOS_LAZY_BACKING_VMEM_HEADER", ROOT / "modules/kern/src/syscalls_impl/vmem/sys_vmem.hpp"))
THREAD_CONTROL_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "multiproc" / "threadControl.cpp"
PROCESS_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "process" / "process.cpp"
VIRT_CPP = Path(os.environ.get("WOS_LAZY_BACKING_VIRT_SOURCE", ROOT / "modules/kern/src/platform/mm/virt.opt.cpp"))
TASK_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "task.cpp"
TASK_HPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "task.hpp"
THREADING_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "threading.cpp"
THREADING_HPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "threading.hpp"
SHM_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "shm" / "shm.cpp"
VMEM_ABI_HPP = ROOT / "modules" / "kern" / "src" / "abi" / "callnums" / "vmem.h"
DEBUG_FLAGS_DOC = ROOT / "docs" / "kernel_debug_flags.md"
SYSCALL_PREPARATION_HPP = ROOT / "modules/kern/src/platform/sys/syscall_preparation.hpp"
NUMA_ALLOC_CPP = ROOT / "modules/kern/src/platform/mm/numa_alloc.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(
        rf"\b(?:auto|void)\s+{name}\([^)]*\)\s*(?:noexcept\s*)?(?:->\s*[A-Za-z0-9_:<>,\s*&]+)?\s*\{{",
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


def require_cpp_tokens(source: str, tokens: list[str], context: str) -> None:
    # Exact argument/operation spelling, independent of clang-format line wraps.
    require_tokens(re.sub(r"\s+", "", source), [re.sub(r"\s+", "", token) for token in tokens], context)


def require_order(source: str, first: str, second: str, context: str) -> None:
    first_index = source.find(first)
    second_index = source.find(second)
    if first_index < 0:
        fail(f"{context}: missing {first}")
    if second_index < 0:
        fail(f"{context}: missing {second}")
    if first_index >= second_index:
        fail(f"{context}: expected {first} before {second}")


def require_cpp_order(source: str, first: str, second: str, context: str) -> None:
    require_order(re.sub(r"\s+", "", source), re.sub(r"\s+", "", first), re.sub(r"\s+", "", second), context)


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
        file_body[file_body.index("if (file_mmap_can_share(st, prot))"):],
        "release_mmap_reservation_locked(task, vaddr, size, HAS_ADDRESS_RESERVATION)",
        "advance_shared_mmap_cursor_locked(task, vaddr, size)",
        "file mmap reservation must be dropped only after page tables occupy the range",
    )

    noreserve_block = block_between(
        anon_body,
        "if (RESERVE_ONLY)",
        "if (PRIVATE_READ)",
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
            "uint64_t const RESULT = private_anon_allocate_locked(task, vaddr, size, prot, hint, flags, placement)",
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
        ["usercopy::mapped_physical_address(*task, PAGE_ADDR) == backing_paddr(*segment, i)"],
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
            "usercopy::mapped_physical_address(*task, ADDR) == backing_paddr(*segment, i)",
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
    ensure_entry = function_body(source, "ensure_user_page_writable_for_task")
    ensure_body = function_body(source, "ensure_user_page_writable_with_context")
    fault_body = function_body(source, "pagefault_handler")

    require_cpp_tokens(
        source,
        [
            "sys::Spinlock cow_pte_lock;",
            "enum class UserWriteFaultStatus",
            "auto resolve_user_write_mapping(sched::task::Task* task, vaddr_t vaddr, const DemandContext& demand) -> UserWriteFaultStatus",
        ],
        "COW write resolver surface",
    )
    require_cpp_tokens(
        resolver_body,
        [
            "cow_pte_lock.lock_irqsave()",
            "phys::page_ref_inc(old_virt, &cow_lookup);",
            "void* new_page = nullptr;",
            "alloc_cow_destination_page(DESTINATION_FULL_OVERWRITE_BEFORE_EXPOSURE, demand, allocation);",
            "new_page = allocation.span.data();",
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
    require_cpp_order(
        resolver_body,
        "alloc_cow_destination_page(DESTINATION_FULL_OVERWRITE_BEFORE_EXPOSURE, demand, allocation);",
        "CURRENT_PHYS == old_phys",
        "COW resolver must allocate/copy before the locked same-frame commit check",
    )
    require_order(
        resolver_body,
        "CURRENT_PHYS == old_phys",
        "pte = pte_from_raw(raw_now);",
        "COW resolver must recheck the old frame before consuming the PTE reference",
    )
    if re.sub(r"\s+", "", source).count("alloc_cow_destination_page(DESTINATION_FULL_OVERWRITE_BEFORE_EXPOSURE,") != 1:
        fail("COW destination allocation must be centralized in resolve_user_write_mapping")

    require_cpp_tokens(
        ensure_body,
        [
            "switch (resolve_user_write_mapping(task, vaddr, demand))",
            "case UserWriteFaultStatus::NEED_LAZY_BACKING:",
            "handle_lazy_vmem_fault(task, vaddr, WRITE_FAULT, demand)",
        ],
        "syscall copy COW path must share the serialized resolver",
    )
    require_cpp_tokens(
        ensure_entry,
        ["capture_demand_context(task, vaddr, DemandOrigin::SoftwareEnsure, false, demand)",
         "ensure_user_page_writable_with_context(task, vaddr, demand)"],
        "software entry captures target policy and preserves its explicit origin",
    )
    require_cpp_tokens(
        fault_body,
        [
            "resolve_user_write_mapping(current_task, control_register, demand) == UserWriteFaultStatus::HANDLED",
            "DemandOrigin::HardwareFault",
            "(frame.cs & 3U) == 3U",
            "return true;",
        ],
        "page fault COW path must share the serialized resolver",
    )


def test_private_demand_observer_uses_supplied_root_incarnation() -> None:
    source = VIRT_CPP.read_text()
    capture = function_body(source, "capture_demand_context")
    require_cpp_tokens(capture, ["uint64_t observer_root_serial = 0;",
                                "capture_user_root_incarnation(target->pagemap, observer_root_serial)",
                                "demand_observer.root_serial.load(std::memory_order_relaxed) == observer_root_serial"],
                       "foreign demand observer requires exact supplied root incarnation")
    require_cpp_order(capture, "demand_observer.root.load(std::memory_order_relaxed) == target->pagemap",
                      "capture_user_root_incarnation(target->pagemap, observer_root_serial)",
                      "cheap supplied-root identity precedes exact incarnation lookup")
    require_cpp_order(capture, "demand_observer.root_serial.load(std::memory_order_relaxed) == observer_root_serial",
                      "output.observer_cookie = WORD;", "stale incarnation must refuse observation before consuming cookie")
    if "current_user_root_incarnation" in capture:
        fail("foreign demand observation cannot use a current-process-only incarnation helper")
    exact = function_body(source, "capture_user_root_incarnation")
    require_cpp_tokens(exact, ["if (root == nullptr || empty != 0) return false;",
                              "user_mapping_pin_lock.lock_irqsave()", "find_user_root_control_locked(root)",
                              "if (control != nullptr) empty = control->serial;",
                              "user_mapping_pin_lock.unlock_irqrestore(FLAGS)", "return empty != 0;"],
                       "exact root lookup returns only a copied registered nonzero serial")
    current = function_body(source, "current_user_root_incarnation")
    require_cpp_tokens(current, ["if (!current_process_root(task)) return 0;"],
                       "current-process incarnation API must retain its original admission")
    observer = function_body(source, "selftest_observe_private_demand")
    require_cpp_order(observer, "subject.try_acquire_usercopy_pagemap(root)",
                      "capture_user_root_incarnation(root, serial)", "observer owns target root through initial capture")
    require_cpp_order(observer, "demand_observer.root_serial.store(serial, std::memory_order_relaxed)",
                      "demand_observer.word.store(READY, std::memory_order_release)",
                      "fully bound exact root serial precedes observer publication")


def test_page_table_pool_duplicate_release_does_not_fall_through_to_page_free() -> None:
    source = VIRT_CPP.read_text()
    release_body = function_body(source, "try_release_page_table_to_pool")
    release_pagemap_body = function_body(source, "release_pagemap")
    release_owned = function_body(source, "release_pagemap_exclusive_owned")

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
        release_owned,
        [
            "if (try_release_page_table_to_pool(pagemap, {caller, owner_pid}))",
            "return;",
            "phys::page_free(pagemap);",
        ],
        "exclusive-owned root release pool handoff",
    )
    require_ordered_tokens(release_owned,
                           ["phys::page_kind_get(pagemap) != PageKind::PAGE_TABLE", "phys::page_ref_get(pagemap) != 1U",
                            "user_mapping_pin_lock.lock_irqsave()", "file_mapping_index.root_empty_locked(pagemap)",
                            "retire_user_root_control_locked(pagemap, empty)",
                            "user_mapping_pin_lock.unlock_irqrestore(CONTROL_FLAGS)",
                            "if (empty != nullptr) phys::page_free(empty);", "owned_frame_purge_pagemap(pagemap)",
                            "if (try_release_page_table_to_pool(pagemap, {caller, owner_pid}))", "return;",
                            "phys::page_free(pagemap);"], "sole-owner/root-index/control/pool retirement order")
    require_ordered_tokens(release_pagemap_body,
                           ["if (pagemap == nullptr)", "pagemap == kernel_pagemap || is_current_pagemap(pagemap)",
                            "return;", "UserPagemapExclusiveScope teardown_scope(pagemap);",
                            "release_pagemap_exclusive_owned(pagemap, owner_pid, __builtin_return_address(0));"],
                           "RAW root release keeps its gate and original caller provenance")
    if "phys::page_free" in release_pagemap_body or "try_release_page_table_to_pool" in release_pagemap_body:
        fail("RAW wrapper must not bypass the shared exact-owned release helper")


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
    require_cpp_tokens(
        function_body(virt, "alloc_cow_destination_page"),
        [
            "numa::try_allocate_pages_into(output, demand.allocation, 1, paging::PAGE_SIZE, PhysicalPageOwner::USER_PRIVATE_MAPPING,",
            "full_overwrite ? phys::PageInitialization::FULL_OVERWRITE : phys::PageInitialization::ZERO",
            'numa::ReclaimMode::LEGACY_UMA, full_overwrite ? "cow_copy" : "cow_zero"',
        ],
        "COW destination allocation pressure",
    )
    factory = function_body(NUMA_ALLOC_CPP.read_text(), "try_allocate_pages")
    require_cpp_tokens(factory, [
        "topology->node_count == 1 && context.policy.mode == Mode::LOCAL && pages == 1 && alignment_bytes == paging::PAGE_SIZE",
        "reclaim == ReclaimMode::LEGACY_UMA",
        "phys::page_alloc_with_reclaim_may_fail(owner, paging::PAGE_SIZE, SITE)",
        "phys::page_alloc_full_overwrite_page_with_reclaim_may_fail(owner, SITE)",
        "phys::page_alloc_may_fail(owner, paging::PAGE_SIZE, SITE)",
        "phys::page_alloc_full_overwrite_page_may_fail(owner, SITE)",
    ], "exact UMA compatibility delegates to existing fallible physical helpers")
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
    require_cpp_tokens(
        function_body(virt, "handle_lazy_vmem_fault"),
        [
            "numa::try_allocate_pages_into(allocation, demand.allocation, 1, paging::PAGE_SIZE,",
            'PhysicalPageOwner::USER_PRIVATE_MAPPING, phys::PageInitialization::FULL_OVERWRITE, numa::ReclaimMode::NEVER, "lazy-vmem")',
            "cow_pte_lock.lock_irqsave()",
            "anonymous_lazy_range_allows_fault(task, PAGE_VADDR, fault, current_prot)",
            "translate(task->pagemap, PAGE_VADDR) != PADDR_INVALID",
            "if (ALLOWED && !ALREADY_MAPPED)",
            "allocation.span.release()",
            "allocation.span.reset()",
        ],
        "IRQ-disabled anonymous lazy fault allocation and shared-pagemap install",
    )
    lazy_body = function_body(virt, "handle_lazy_vmem_fault")
    require_cpp_order(
        lazy_body,
        "task->lazy_vmem_lock.unlock_irqrestore(IRQF);",
        "numa::try_allocate_pages_into(allocation, demand.allocation, 1, paging::PAGE_SIZE,",
        "lazy anonymous allocation must happen outside the task range lock",
    )
    require_order(
        lazy_body,
        "std::memset(PAGE, 0, paging::PAGE_SIZE);",
        "map_page(task->pagemap, PAGE_VADDR, PADDR, lazy_user_vmem_flags(current_prot));",
        "full-overwrite lazy anonymous pages must be zero before publication",
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
            "if (file_range.kind == sched::task::LazyVmemKind::NATIVE_FILE_BACKED)",
            "file_range.lifetime->try_acquire(file_mapping::FamilyOwner::SNAPSHOT)",
            "} else if (file_range.file != nullptr)",
            "ker::vfs::vfs_retain_file(file_range.file);",
            "task->lazy_vmem_lock.unlock_irqrestore(IRQF);",
            "if (kasan::is_enabled())",
            "kasan::unpoison_range(&file_range, sizeof(file_range));",
            "ker::syscall::vmem::materialize_lazy_file_page(task, file_range, PAGE_VADDR, fault, demand.allocation)",
        ],
        "KASAN-excluded lazy file snapshot boundary",
    )
    require_ordered_tokens(lazy_body,
                           ["ker::syscall::vmem::materialize_lazy_file_page(task, file_range, PAGE_VADDR, fault, demand.allocation)",
                            "if (file_range.kind == sched::task::LazyVmemKind::NATIVE_FILE_BACKED)",
                            "file_range.lifetime->release(file_mapping::FamilyOwner::SNAPSHOT);",
                            "} else if (file_range.file != nullptr)", "ker::vfs::vfs_put_file(file_range.file);", "return OK;"],
                           "native family versus RAW File snapshot releases after materialization")


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
        "if (USE_LAZY)",
        "if (PRIVATE_EAGER)",
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


def test_eager_and_reserved_placement_use_copied_actor_before_publication() -> None:
    source = SYS_VMEM_CPP.read_text()
    anon = function_body(source, "anon_allocate")
    require_ordered_tokens(
        anon,
        [
            "bool const RESERVE_ONLY = is_prot_none(prot) || (flags & ker::abi::vmem::MAP_NORESERVE) != 0;",
            "bool const PRIVATE_READ = !RESERVE_ONLY && (prot & ker::abi::vmem::PROT_WRITE) == 0;",
            "bool const USE_LAZY = !RESERVE_ONLY && !PRIVATE_READ && lazy_anon_mmap_enabled() && (flags & ker::abi::vmem::MAP_POPULATE) == 0;",
            "bool const PRIVATE_EAGER = PRIVATE_READ || (!RESERVE_ONLY && !USE_LAZY && !anon_zero_cow_enabled());",
            "auto const MEMORY_POLICY = task->memory_policy_snapshot();",
            "numa::capture_allocation_context(MEMORY_POLICY, placement)",
            "if (CAPTURE != numa::AllocationError::NONE)",
            "SharedVmemPublicationGuard publication_guard;",
            "AddressSpaceObservation observation(task);",
            "observation.admit_mapping(task->pagemap, size, hint, size)",
            "release_fixed_mmap_range_locked(task, vaddr, size);",
        ],
        "private eager backing decision and actor capture before admission/replacement",
    )
    if anon.count("numa::capture_allocation_context(MEMORY_POLICY, placement)") != 1:
        fail("anonymous private preparation must capture exactly once")
    for name in ("private_anon_allocate_locked", "materialize_reserved_page"):
        body = function_body(source, name)
        require_ordered_tokens(
            body,
            [
                "auto page_context = placement;",
                "page_context.virtual_page_index =",
                "numa::try_allocate_pages_into(allocation, page_context, 1,",
                "ker::mod::mm::phys::PageInitialization::ZERO",
                "numa::ReclaimMode::LEGACY_UMA",
                "allocation.actual != numa::INVALID_NODE",
                "std::memset(PHYS_PAGE, 0, ker::mod::mm::paging::PAGE_SIZE)",
                "map_page(task->pagemap,",
                "(void)allocation.span.release();",
            ],
            name + " actual placed ZERO/legacy-UMA allocation and adoption",
        )
    materialize = function_body(source, "materialize_reserved_page")
    require_ordered_tokens(
        materialize,
        ["if (is_prot_none(prot))", "get_anon_zero_page()", "page_ref_inc(ZERO_PAGE)",
         "anon_zero_page_flags(prot)", "auto page_context = placement;"],
        "NONE and genuine shared-zero branches precede private allocation",
    )
    protect = block_between(source, "case ker::abi::vmem::ops::PROTECT:", "case ker::abi::vmem::ops::MREMAP:", "mprotect dispatch")
    require_ordered_tokens(
        protect,
        ["change_public_shared_file(*task, ADDR, size, true, PROT)",
         "auto const MEMORY_POLICY = task->memory_policy_snapshot();",
         "numa::capture_allocation_context(MEMORY_POLICY, placement)",
         "SharedVmemPublicationGuard publication_guard;", "authorize_shared_file_protection_locked",
         "protect_shared_vmem_range_locked", "protect_anonymous_swap_page", "unify_page_flags",
         "materialize_reserved_page(task, current_vaddr, PROT, placement)"],
        "reserved placement preserves protection/metadata/swap/leaf transaction order",
    )
    zero = function_body(source, "selftest_map_anonymous_zero_page")
    require_ordered_tokens(zero,
                          ["subject.memory_policy_snapshot()", "numa::capture_allocation_context(MEMORY_POLICY, placement)",
                           "materialize_reserved_page(&subject, address, ker::abi::vmem::PROT_READ | ker::abi::vmem::PROT_WRITE, placement)"],
                          "genuine zero delegate keeps explicit supplied actor")
    for name in ("selftest_allocate_private_anonymous", "selftest_materialize_anonymous_reserved"):
        body = function_body(source, name)
        require_ordered_tokens(body,
                              ["subject.memory_policy_snapshot()", "numa::capture_allocation_context(MEMORY_POLICY, placement)",
                               "SharedVmemPublicationGuard publication;"],
                              name + " never borrows ambient current policy")


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
            "SyscallPreparationScope preparation(*parent);",
            "if (!preparation.ready())",
            "LazyVmemClone lazy_snapshot;",
            "lazy_snapshot.prepare(*parent)",
            "abandoned(",
            "LegacyInterval legacy_context(preparation);",
            "ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;",
            "if (!lazy_snapshot.matches(*parent))",
            "Task::create_user_thread(parent, tcb_va, user_sp, enter_va, prepared_stack_words.at(0),",
            "prepared_stack_words.at(1), MEMORY_POLICY, &stack_exhausted,",
            "&lazy_snapshot);",
            "uint64_t const NEW_TID = t->namespace_tid();",
            "post_task_round_robin_inherited(t, inherited_placement, PLACEMENT_SEED)",
        ],
        "thread lazy-range clone through scheduler publication",
    )
    inherited_exit_pos = create_case.find("inherit_pending_process_exit_request(parent, t)")
    saved_pid_pos = create_case.find("thread_pid = NEW_TID;", inherited_exit_pos)
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
            "t->publish_memory_policy_snapshot(memory_policy);",
            "prepared_lazy != nullptr ? !prepared_lazy->matches(*parent) : !clone_lazy_vmem_ranges(*t, *parent)",
            "parent->shares_user_pagemap.store(true, std::memory_order_release);",
            "t->shares_user_pagemap.store(true, std::memory_order_release);",
            "resource::bind_thread_owner(*t, *parent)",
            "prepared_lazy != nullptr && !prepared_lazy->consume(*t)",
            "destroy_unpublished_user_thread(t)",
        ],
        "thread shared-pagemap marking",
    )

    clone_vm = function_body(process, "wos_proc_clone_vm")
    require_ordered_tokens(
        clone_vm,
        [
            "SyscallPreparationScope preparation(*parent);",
            "if (!preparation.ready())",
            "LazyVmemClone lazy_snapshot;",
            "lazy_snapshot.prepare(*parent)",
            "LegacyInterval legacy_context(preparation);",
            "ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;",
            "if (!lazy_snapshot.matches(*parent))",
            "child->pagemap = parent->pagemap;",
            "!lazy_snapshot.consume(*child)",
            "parent->shares_user_pagemap.store(true, std::memory_order_release);",
            "child->shares_user_pagemap.store(true, std::memory_order_release);",
            "post_task_balanced_inherited(child, inherited_placement)",
        ],
        "clone-VM prepared lazy rows through scheduler publication",
    )
    if "clone_lazy_vmem_ranges(*child, *parent)" in clone_vm:
        fail("CLONE_VM must consume prepared owners rather than allocate a row clone under publication")
    require_ordered_tokens(
        SYSCALL_PREPARATION_HPP.read_text(),
        ["class LegacyInterval", "restore_enabled_(preparation.ready() && sched::interrupts_enabled())",
         'if (restore_enabled_) asm volatile("cli"', "~LegacyInterval()",
         'if (restore_enabled_) asm volatile("sti"'],
        "legacy constructor interval restores preparation IF before deferred owner cleanup",
    )


def test_process_memory_policy_is_captured_before_creation_and_retry() -> None:
    process = PROCESS_CPP.read_text()
    thread_control = THREAD_CONTROL_CPP.read_text()
    create_case = block_between(
        thread_control,
        "case abi::multiproc::threadControlOps::THREAD_CREATE:",
        "case abi::multiproc::threadControlOps::THREAD_EXIT:",
        "thread creation syscall case",
    )
    capture = "auto const MEMORY_POLICY = parent->memory_policy_snapshot();"
    if create_case.count(capture) != 1:
        fail("thread creation must capture the actual parent policy once across retries")
    require_ordered_tokens(
        create_case,
        [capture, "ensure_writable(*parent", "for (uint32_t attempt = 0; attempt < 2; ++attempt)",
         "SharedVmemPublicationGuard publication_guard;", "prepared_stack_words.at(1), MEMORY_POLICY,"],
        "thread policy capture before prefault, retry and publication",
    )
    for name in ("wos_proc_fork", "wos_proc_clone_vm"):
        body = function_body(process, name)
        if body.count(capture) != 1:
            fail(f"{name} must capture the actual parent policy exactly once")
        require_ordered_tokens(
            body,
            [capture, "child->publish_memory_policy_snapshot(MEMORY_POLICY);",
             "resource::inherit(*child, *parent)",
             "detail::prepare_fork_vm_snapshot(" if name == "wos_proc_fork" else "SharedVmemPublicationGuard publication_guard;"],
            f"{name} copied policy before resource-owner binding and VM publication",
        )
    clone = function_body(process, "wos_proc_clone_vm")
    require_order(clone, capture, "copy_value_from_task(*parent", "clone policy capture before user input")


def test_cpu_placement_capture_precedes_creation_and_reuses_legacy_publishers() -> None:
    process = PROCESS_CPP.read_text()
    control = THREAD_CONTROL_CPP.read_text()
    capture = "capture_task_placement_snapshot(parent, inherited_placement)"
    initialize = "initialize_inherited_task_placement(child, inherited_placement)"
    for name in ("wos_proc_fork", "wos_proc_clone_vm"):
        body = function_body(process, name)
        if body.count(capture) != 1:
            fail(f"{name} must capture the actual caller CPU placement once")
        require_cpp_order(body, capture, "ProcessReservation process_reservation", "CPU capture before reservation")
        require_cpp_order(body, capture, "auto const MEMORY_POLICY = parent->memory_policy_snapshot();", "CPU tuple before creation")
        require_cpp_order(body, "child->sched_queue = sched::task::Task::sched_queue::NONE;", initialize,
                          "fresh scheduling state before exclusive initialization")
        require_cpp_order(body, initialize, "post_task_balanced_inherited(child, inherited_placement)",
                          "copied CPU tuple before visibility")
        require_cpp_tokens(body, ["if (int const ERROR = sched::initialize_inherited_task_placement(child, inherited_placement); ERROR != 0)"],
                           "exact initialization error admission")
    fork = function_body(process, "wos_proc_fork")
    failed = block_between(fork, "if (int const ERROR = sched::initialize_inherited_task_placement(child, inherited_placement);",
                           "// Copy fixed per-process path storage.", "fork initialization failure prefix")
    require_ordered_tokens(failed, ["delete[] child->name;", "mm::phys::page_free(reinterpret_cast<void*>(KERNEL_STACK_BASE));",
                                  "delete child;", "return finish_fork(ERROR);"], "owned fresh prefix rollback")
    clone = function_body(process, "wos_proc_clone_vm")
    require_cpp_order(clone, "child_owner(child", initialize, "clone RAII owns initialized prefix")
    require_cpp_tokens(clone, ["if (int const ERROR = sched::initialize_inherited_task_placement(child, inherited_placement); ERROR != 0) return static_cast<uint64_t>(ERROR);"],
                       "clone initialization errors survive ordinary cleanup")
    create = block_between(control, "case abi::multiproc::threadControlOps::THREAD_CREATE:",
                           "case abi::multiproc::threadControlOps::THREAD_EXIT:", "thread CPU placement")
    if create.count(capture) != 1:
        fail("THREAD_CREATE must retain one caller CPU tuple through retry")
    require_cpp_order(create, capture, "ProcessReservation process_reservation", "thread capture before quota/prefault")
    require_cpp_order(create, capture, "for (uint32_t attempt = 0; attempt < 2; ++attempt)", "thread capture before retry")
    require_cpp_order(create, "abandoned.reset(t);", "initialize_inherited_task_placement(t, inherited_placement)", "actual owner before initializer")
    require_cpp_order(create, "initialize_inherited_task_placement(t, inherited_placement)", "uint64_t const NEW_TID = t->namespace_tid();",
                      "initializer failure before TCB/visibility")
    require_cpp_tokens(create, ["if (int const ERROR = mod::sched::initialize_inherited_task_placement(t, inherited_placement); ERROR != 0) return static_cast<uint64_t>(ERROR);",
                               "post_task_round_robin_inherited(t, inherited_placement, PLACEMENT_SEED)"], "same copied tuple and exact errors")
    scheduler = (ROOT / "modules/kern/src/platform/sched/scheduler.cpp").read_text()
    balanced = function_body(scheduler, "post_task_balanced_inherited")
    require_cpp_tokens(balanced, ["inherited_task_placement_matches(subject, snapshot, smt::get_core_count(), allowed)",
                                  "return snapshot.cpu_pinned ? post_task_for_cpu(snapshot.owner_cpu, subject) : post_task_balanced(subject);"],
                       "fresh tuple validation delegates the original publishers")
    rr = function_body(scheduler, "post_task_round_robin_inherited")
    require_cpp_tokens(rr, ["inherited_task_placement_matches(subject, snapshot, CORE_COUNT, allowed)",
                           "inherited_round_robin_cpu(snapshot, CORE_COUNT, seed)", "post_task_for_cpu(TARGET, subject)"],
                       "allowed RR selection retains the original CPU publisher")


def test_eager_fork_uses_copied_policy_and_caller_storage_before_owners() -> None:
    source = PROCESS_CPP.read_text()
    fork = function_body(source, "wos_proc_fork")
    require_cpp_order(fork, "auto const MEMORY_POLICY = parent->memory_policy_snapshot();",
                      "mm::numa::capture_allocation_context(MEMORY_POLICY, fork_placement)", "copied initiating policy")
    require_cpp_order(fork, "mm::numa::AllocationResult fork_page{};", "ProcessReservation process_reservation", "result lifetime before reservations")
    require_cpp_order(fork, "mm::numa::capture_allocation_context(MEMORY_POLICY, fork_placement)",
                      "ProcessReservation process_reservation", "placement capture before pressure/owners")
    require_cpp_tokens(fork, ["if (EAGER_COPY)",
        "detail::prepare_fork_vm_snapshot(*parent, *child, preparation, EAGER_COPY ? &fork_placement : nullptr,",
        "EAGER_COPY ? &fork_page : nullptr, &point)"], "prepared route borrows caller-born eager storage only when requested")
    coordinator = function_body(source, "prepare_fork_vm_snapshot")
    require_cpp_tokens(coordinator, [
        "deep_copy_current_user_pagemap_prepared(actor, child.pagemap, preparation, publication, survey, tables, eager_context, empty_result, swap)"
    ], "prepared coordinator transfers the same borrowed eager context and result")
    virt = VIRT_CPP.read_text()
    wrapper = function_body(virt, "deep_copy_user_pagemap_cow_impl")
    require_cpp_order(wrapper, "UserPagemapExclusiveScope source_usercopy_scope(src);",
                      "return deep_copy_user_pagemap_cow_held(src, dst, eager_copy, placement, output, fail_before_private_page, source_usercopy_scope, false);",
                      "legacy exclusive and unchanged copied context transfer")
    prepared = function_body(virt, "copy_current_user_pagemap_exclusive")
    exact_prepared = "".join(prepared.split())
    if not (exact_prepared.index("PreparedSwapForkAccess::preflight(*prepared_swap,source,child)") < exact_prepared.index("commit_managed_split_plan_exclusive(source,survey,tables,source_usercopy_scope)")):
        fail("prepared metadata must be fully validated before split or COW publication")
    if "intprepared_error=0;" not in exact_prepared:
        fail("the compatible branch must preserve its original ENOMEM fallback")
    require_cpp_order(prepared, "commit_managed_split_plan_exclusive(source, survey, tables, source_usercopy_scope)",
                      "sched::PreemptGuard const COW_PREEMPT;", "prepared split ACK before COW interval")
    require_cpp_order(prepared, "sched::PreemptGuard const COW_PREEMPT;",
                      "deep_copy_user_pagemap_cow_held(source, child, EAGER, placement, empty_result, UINT64_MAX, source_usercopy_scope, true, prepared_swap, &prepared_error)",
                      "prepared route borrows the same original exclusive and copied context")
    copier = function_body(virt, "deep_copy_user_pagemap_cow_held")
    require_cpp_order(copier, "if (!exclusive.owns(src)) return false;", "auto& src_pml4e = entry_at(src, i4)",
                      "source ownership before any leaf walk")
    require_cpp_order(copier, "placement->virtual_page_index = VADDR / paging::PAGE_SIZE;",
                      "numa::try_allocate_pages_into(*output, *placement", "per-VA copied context before allocation")
    require_cpp_order(copier, "std::memcpy(child_page, data_virt, paging::PAGE_SIZE);",
                      "owned_frame_track_fresh_normal_mapping(dst, VADDR, CHILD_PHYS", "full bytes before real leaf adoption")
    require_cpp_order(copier, "owned_frame_track_fresh_normal_mapping(dst, VADDR, CHILD_PHYS",
                      "output->span.release()", "exact adoption before scalar owner transfer")
    require_cpp_order(copier, "if (!phys::page_ref_try_inc(data_virt, &ref_lookup))",
                      "src_pml1e = pte_from_raw(raw);", "real shared-frame owner before parent/child COW leaf publication")
    require_cpp_tokens(copier, ["if (!phys::page_ref_try_inc(data_virt, &ref_lookup)) { flush_pagemap_after_update(src, 0, true); return false; }"],
                       "failed shared-frame acquisition flushes the changed source prefix before caller rollback")


def test_fork_snapshots_all_vm_surfaces_under_one_publication_guard() -> None:
    process = PROCESS_CPP.read_text()
    fork = function_body(process, "wos_proc_fork")
    snapshot = function_body(process, "prepare_fork_vm_snapshot")
    require_ordered_tokens(snapshot, [
        "LazyVmemClone lazy;", "PublicFileForkPreparation tmpfs;",
        "LegacyFileForkPreparation legacy_file;", "PreparedAnonymousSwapFork swap;",
        "ShmForkPreparation shm;", "RangeSplitSurvey survey;", "PreparedTables tables;",
        "lazy.prepare(actor)", "tmpfs.prepare(actor, lazy)",
        "legacy_file.prepare(actor, child, preparation)", "swap.prepare(actor, child, preparation)",
        "shm.prepare(actor, child, preparation)",
        "prepare_current_range_split(actor, preparation, 0, USER_END, survey, tables)",
        "SharedVmemPublicationGuard publication;",
        "!lazy.matches(actor) || !tmpfs.matches(actor)",
        "legacy_file.matches(actor, child, preparation, publication)",
        "shm.matches(actor, child, preparation, publication)",
        "LegacyInterval legacy_context(preparation);",
        "child.mmap_next.store(actor.mmap_next.load(std::memory_order_relaxed)",
        "lazy.consume(child)", "deep_copy_current_user_pagemap_prepared(",
        "tmpfs.attach(child)", "shm.commit(actor, child, preparation, publication)",
        "legacy_file.commit(actor, child, preparation, publication)",
    ], "all five actual owners and geometry prepared before the one final PUB then legacy interval")
    if snapshot.count("tmpfs.matches(actor)") != 1 or snapshot.count("SharedVmemPublicationGuard publication;") != 1:
        fail("final snapshot must take one PUB and retain only one TMPFS snapshot")
    for forbidden in ("release_file_mmap_ranges_for_pagemap", "shm_cleanup_for_task",
                      "clone_lazy_vmem_ranges", "UserPagemapReadScope", "UserPagemapExclusiveScope", "PreemptGuard"):
        if forbidden in snapshot:
            fail(f"coordinator must leave reader/exclusive ownership and caller rollback to their actual owners: {forbidden}")
    caller = block_between(fork, "sys::SyscallPreparationScope preparation(*parent);",
                           "// --- Clone thread metadata ---", "ordinary regular fork VM caller")
    require_cpp_tokens(caller, ["detail::ForkVmFailurePoint point{};",
        "detail::prepare_fork_vm_snapshot(*parent, *child, preparation, EAGER_COPY ? &fork_placement : nullptr, EAGER_COPY ? &fork_page : nullptr, &point)",
        'return finish_fork(detail::public_fork_vm_error(point, FORK_VM_ERROR), 0, "vm-snapshot");'],
        "public caller passes borrowed storage and translates the witnessed private refusal")
    require_ordered_tokens(caller, ["detail::prepare_fork_vm_snapshot(", "if (FORK_VM_ERROR != 0)",
        "shm_cleanup_for_task(child)", "release_file_mmap_ranges_for_pagemap(child->pagemap)",
        "release_lazy_vmem_ranges(*child)", "destroy_user_space(child->pagemap",
        "release_pagemap(child->pagemap", "delete[] child->name", "page_free(", "delete child;",
        "public_fork_vm_error(point, FORK_VM_ERROR)", "observe_address_space(*child)"],
        "all adopted prefixes roll back after coordinator owners drop and before successful frontier")
    for forbidden in ("SharedVmemPublicationGuard", "LegacyInterval", "PreemptGuard", "deep_copy_user_pagemap_cow"):
        if forbidden in caller:
            fail(f"public caller must use only the prepared coordinator: {forbidden}")
    if fork.count("sys::SyscallPreparationScope rollback(*parent);") != 6:
        fail("each existing later failure and the new PerCpu denial must open its own ordinary rollback scope")
    for anchor in ("if (child_thread == nullptr)", "if (!child->pid_namespace()->is_host()",
                   "if (per_cpu == nullptr)", "if (!clone_fd_table_shared_checked(parent, child))",
                   "if (!child_events::begin_publication(*parent, *child))",
                   "if (!PUBLICATION_REF || !process_reservation.attach(*child)"):
        branch = fork[fork.index(anchor):]
        opening = branch.index("{")
        if not branch[opening + 1:].lstrip().startswith("sys::SyscallPreparationScope rollback(*parent);"):
            fail(f"ordinary rollback must be the first owner of failure branch {anchor}")
    if fork.count("sched::task::release_lazy_vmem_ranges(*child);") != 7:
        fail("VM refusal, the existing five later failures and PerCpu denial must release lazy references")
    require_ordered_tokens(fork, ["observe_address_space(*child)", "// --- Clone thread metadata ---",
        "if (!child_events::begin_publication(*parent, *child))", "child->try_acquire_lifetime_ref()",
        "post_task_balanced_inherited(child, inherited_placement)", "child_events::commit_publication(*child)",
        "uint64_t const CHILD_CPU = child->load_cpu();", "child->release();",
        "finish_fork(static_cast<int64_t>(CHILD_VISIBLE_PID), CHILD_CPU)"], "unchanged successful publication tail")
    file_clone = function_body(SYS_VMEM_CPP.read_text(), "clone_file_mmap_ranges_for_pagemap_locked")
    if "release_file_mmap_ranges_for_pagemap(dst)" in file_clone:
        fail("file mmap clone rollback may block and must not run under PUB")


def test_fork_failure_phase_is_scalar_and_precedes_each_refusal() -> None:
    source = PROCESS_CPP.read_text()
    coordinator = function_body(source, "prepare_fork_vm_snapshot")
    require_cpp_tokens(source, ["ForkVmFailurePoint* failure_point = nullptr"],
                       "old five-argument coordinator calls remain source compatible")
    require_cpp_tokens(coordinator, ["if (failure_point != nullptr) *failure_point = point;"],
                       "phase is an optional borrowed scalar witness")
    require_ordered_tokens(coordinator, [
        "fail_at(ForkVmFailurePoint::Context);", "if (!preparation.ready()",
        "fail_at(ForkVmFailurePoint::LazyPrepare);", "lazy.prepare(actor)",
        "fail_at(ForkVmFailurePoint::TmpfsPrepare);", "tmpfs.prepare(actor, lazy)",
        "fail_at(ForkVmFailurePoint::LegacyPrepare);", "legacy_file.prepare(actor, child, preparation)",
        "fail_at(ForkVmFailurePoint::SwapPrepare);", "swap.prepare(actor, child, preparation)",
        "fail_at(ForkVmFailurePoint::ShmPrepare);", "shm.prepare(actor, child, preparation)",
        "fail_at(ForkVmFailurePoint::GeometryPrepare);", "prepare_current_range_split(",
        "fail_at(ForkVmFailurePoint::FinalLazyTmpfsMatch);", "SharedVmemPublicationGuard publication;",
        "fail_at(ForkVmFailurePoint::LegacyMatch);", "legacy_file.matches(",
        "fail_at(ForkVmFailurePoint::ShmMatch);", "shm.matches(",
        "fail_at(ForkVmFailurePoint::LazyConsume);", "lazy.consume(child)",
        "fail_at(ForkVmFailurePoint::Copy);", "deep_copy_current_user_pagemap_prepared(",
        "fail_at(ForkVmFailurePoint::TmpfsAttach);", "tmpfs.attach(child)",
        "fail_at(ForkVmFailurePoint::ShmCommit);", "shm.commit(",
        "fail_at(ForkVmFailurePoint::LegacyCommit);", "legacy_file.commit(",
        "if (status == 0) fail_at(ForkVmFailurePoint::Complete);", "return status;"
    ], "phase identifies the operation which can refuse without granting authority")


def test_fork_allocation_denial_precedes_initialization_and_preserves_prefixes() -> None:
    fork = function_body(PROCESS_CPP.read_text(), "wos_proc_fork")
    for expression in ("new (std::nothrow) sched::task::Task{}", "new (std::nothrow) char[NAME_LEN + 1]",
                       "new (std::nothrow) sched::threading::Thread()", "new (std::nothrow) cpu::PerCpu()"):
        if fork.count(expression) != 1:
            fail(f"fork must use the supported nonthrowing allocation expression exactly once: {expression}")
    name = block_between(fork, "char* name_copy = new (std::nothrow)", "child->pid =", "name refusal prefix")
    require_ordered_tokens(name, ["if (name_copy == nullptr)", "page_free(", "delete child;",
        'finish_fork(-ENOMEM, 0, "name")', "std::memcpy(name_copy", "child->name = name_copy"],
        "name refusal frees only the exact early stack/Task prefix before memcpy/adoption")
    for forbidden in ("destroy_user_space", "release_lazy_vmem_ranges", "abort_publication", "release_cloned_fd_table_refs"):
        if forbidden in name:fail("name denial cannot clean an unacquired VM/publication prefix: " + forbidden)
    per_cpu = block_between(fork, "auto* per_cpu = new (std::nothrow)", "// Resume from the immutable", "PerCpu refusal prefix")
    require_ordered_tokens(per_cpu, ["if (per_cpu == nullptr)", "SyscallPreparationScope rollback(*parent)",
        "delete child->thread;", "shm_cleanup_for_task(child)", "release_file_mmap_ranges_for_pagemap(child->pagemap)",
        "release_lazy_vmem_ranges(*child)", "destroy_user_space(child->pagemap", "release_pagemap(child->pagemap",
        "delete[] child->name", "page_free(", "delete child;", 'finish_fork(-ENOMEM, 0, "per-cpu")',
        "per_cpu->syscall_stack", "per_cpu->cpu_id", "child->context.syscall_scratch_area"],
        "PerCpu denial cleans the exact ordinary VM+optional Thread prefix before first member access")
    for forbidden in ("abort_publication", "release_cloned_fd_table_refs", "child->release()", "delete per_cpu"):
        if forbidden in per_cpu:fail("PerCpu denial cannot return an unacquired prefix: " + forbidden)


def test_fork_child_registers_use_immutable_syscall_entry_snapshot() -> None:
    process = PROCESS_CPP.read_text()
    fork = function_body(process, "wos_proc_fork")
    require_ordered_tokens(
        fork,
        [
            "ker::mod::cpu::GPRegs const FORK_ENTRY_REGS = gpr;",
            "auto const KERNEL_STACK_BASE = alloc_fork_kernel_stack_from_syscall(parent);",
            "child->context.regs = fork_child_return_regs(FORK_ENTRY_REGS);",
            "post_task_balanced_inherited(child, inherited_placement)",
        ],
        "immutable fork syscall-entry register snapshot",
    )
    if "child->context.regs = parent->context.regs" in fork:
        fail("fork child registers must not come from preemptible parent task context")


def test_as_admission_precedes_mapping_mutation() -> None:
    source = SYS_VMEM_CPP.read_text()
    for name in ("anon_allocate", "file_allocate"):
        body = function_body(source, name)
        require_order(body, "SharedVmemPublicationGuard publication_guard", "observation.admit_mapping(task->pagemap, size",
                      "AS admission must serialize with shared-root publishers")
        require_order(body, "observation.admit_mapping(task->pagemap, size, hint, size)", "release_fixed_mmap_range_locked",
                      "AS denial must preserve MAP_FIXED source mappings")
        require_order(body, "observation.admit_mapping(task->pagemap, size))", "reserve_free_mmap_range_locked",
                      "AS denial must precede nonfixed VA reservation")
        require_tokens(body, ["AddressSpaceObservation observation(task)"], "AS observes commit/rollback before publication unlock")
    attach = function_body(SHM_CPP.read_text(), "shmat_impl")
    require_order(attach, "admit_address_space(task, task->pagemap, SIZE)", "uint64_t mapped_pages",
                  "AS shm denial must precede mapping references")
    require_tokens(attach, ["abandon_attachment_publication(attachment)"], "shm denial returns segment pin")
    remap = function_body(source, "anon_mremap")
    require_tokens(remap, ["(void)anon_free(NEW_ADDR, NEW_SIZE)"], "failed remap finalization releases destination")
    walk = function_body(VIRT_CPP.read_text(), "count_user_virtual_bytes")
    require_ordered_tokens(walk, ["sched::PreemptGuard guard", "acquire_user_pagemap_access", "count_virtual_bytes_in_table",
                                 "release_user_pagemap_access"], "AS walk root pins cannot survive fatal handoff")
    proc = (ROOT / "modules/kern/src/vfs/fs/procfs.cpp").read_text()
    limits = function_body(proc, "procfs_read_limits")
    if "SharedVmemPublicationGuard" in limits:
        fail("procfs limits cannot recursively acquire the VM publication mutex during file mmap")


def test_managed_anonymous_remove_preserves_prepared_ownership_and_stays_unwired() -> None:
    source = SYS_VMEM_CPP.read_text()
    body = function_body(source, "remove_current_anonymous_range")
    profile = function_body(source, "stage_anonymous_remove")
    require_cpp_tokens(profile, [
        "row.kind != LazyVmemKind::ANONYMOUS", "row.file != nullptr",
        "(row.flags & ker::abi::vmem::MAP_SHARED) != 0", "row.start >= row.end",
        "(row.start | row.end) & (ker::mod::mm::paging::PAGE_SIZE - 1)",
        "if (covered) return refuse(-EOPNOTSUPP)", "if (!covered || next <= cursor)",
        "prepared_subject_ = nullptr", "staged_begin_ = staged_end_ = 0",
        "return stage(Change::REMOVE, start, end)",
    ], "managed removal requires complete private anonymous coverage in every retained sibling")
    require_ordered_tokens(body, [
        "!preparation.ready()", "sched::get_current_task() != &actor",
        "actor.syscall_account_start_us == 0", "!sched::interrupts_enabled()",
        "start < USER_SPACE_START", "native_file_mapping_retirement.load",
        "SharedLazyVmemUpdate metadata{}", "RangeSplitSurvey survey{}", "PreparedTables tables{}",
        "metadata.prepare(actor)", "metadata.stage_anonymous_remove(start, end)",
        "prepare_current_range_split(actor, preparation, start, end, survey, tables)",
        "plan_storage(tables)", "survey.count() != LEAVES", "storage->descriptor_count != LEAVES",
        "storage->descriptors[i].key.va_2m", "file_snapshot(nullptr, release_snapshot)",
        "SharedVmemPublicationGuard publication", "metadata.serial()",
        "overlaps_kernel_managed_user_layout_locked(&actor, start, end - start)",
        "g_file_mmap_ranges_lock.lock()", "g_file_mmap_ranges_lock.unlock()",
        "if (legacy_overlap) return -EOPNOTSUPP", "query_current_file_binding",
        "file_snapshot.reset(captured)", "if (!QUERIED) return -EAGAIN",
        "if (file_snapshot) return -EOPNOTSUPP", "AddressSpaceObservation observation(&actor)",
        "LegacyInterval legacy_context(preparation)", "CurrentRangeSplitAdmission admission",
        "admission.validate_split_and_arm(survey, tables)",
        "metadata.commit(admission, actor, start, end)", "mm::virt::unmap_page(ROOT, page)",
    ], "managed removal prepares before owners and commits before actual nonfallible unmap")
    if body.count("metadata.commit(") != 1 or "metadata.matches(" in body:
        fail("typed managed metadata commit must consume exactly one final match attempt")
    if source.count("remove_current_anonymous_range(") != 1:
        fail("managed anonymous removal must remain an unwired private helper")
    error = function_body(source, "managed_range_error")
    require_tokens(error, ["Error::Invalid", "return -EINVAL", "Error::Unsupported", "return -EOPNOTSUPP",
                           "Error::GenerationExhausted", "return -EOVERFLOW", "Error::Capacity",
                           "Error::Exhausted", "return -ENOMEM", "return -EAGAIN"],
                   "managed removal retains existing geometry errno classes")


def main() -> None:
    test_managed_anonymous_remove_preserves_prepared_ownership_and_stays_unwired()
    test_as_admission_precedes_mapping_mutation()
    test_munmap_and_mprotect_reject_overflowing_lengths()
    test_nonfixed_mmap_address_selection_is_reserved_before_mapping()
    test_kernel_managed_initial_layout_cannot_be_replaced()
    test_sysv_shm_publication_respects_vm_reservations_and_guards()
    test_owned_frame_tracking_is_disabled_off_the_fault_path()
    test_cow_write_resolution_serializes_pte_reference_consumption()
    test_private_demand_observer_uses_supplied_root_incarnation()
    test_page_table_pool_duplicate_release_does_not_fall_through_to_page_free()
    test_mapping_replacement_releases_displaced_reference_after_tlb_flush()
    test_user_memory_pressure_does_not_enter_fatal_oom()
    test_kasan_excluded_lazy_file_snapshot_is_unpoisoned_after_unlock()
    test_default_writable_anon_mmap_is_demand_paged()
    test_eager_and_reserved_placement_use_copied_actor_before_publication()
    test_file_mmap_cache_is_sharded_without_changing_page_ownership()
    test_thread_publication_is_serialized_with_shared_vmem_updates()
    test_process_memory_policy_is_captured_before_creation_and_retry()
    test_cpu_placement_capture_precedes_creation_and_reuses_legacy_publishers()
    test_eager_fork_uses_copied_policy_and_caller_storage_before_owners()
    test_fork_snapshots_all_vm_surfaces_under_one_publication_guard()
    test_fork_failure_phase_is_scalar_and_precedes_each_refusal()
    test_fork_allocation_denial_precedes_initialization_and_preserves_prefixes()
    test_fork_child_registers_use_immutable_syscall_entry_snapshot()
    print("vmem mmap, owned-frame, and COW invariants hold")


if __name__ == "__main__":
    main()
