#!/usr/bin/env python3

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
VIRT_HPP = ROOT / "modules" / "kern" / "src" / "platform" / "mm" / "virt.hpp"
TLB_SHOOTDOWN_HPP = ROOT / "modules" / "kern" / "src" / "platform" / "mm" / "tlb_shootdown.hpp"
VIRT_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "mm" / "virt.opt.cpp"
SCHEDULER_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "scheduler.cpp"
SPINLOCK_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "sys" / "spinlock.cpp"
PAGE_ALLOC_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "mm" / "page_alloc.cpp"
PHYS_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "mm" / "phys.opt.cpp"
SERIAL_CPP = ROOT / "modules" / "kern" / "src" / "mod" / "io" / "serial" / "serial.cpp"


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
    starts: set[int] = set()
    for needle in [
        f"auto {name}(",
        f"void {name}(",
        f"inline void {name}(",
    ]:
        candidate = source.find(needle)
        while candidate >= 0:
            starts.add(candidate)
            candidate = source.find(needle, candidate + 1)
    for start in sorted(starts):
        close = source.find(")", start)
        brace = source.find("{", close)
        semicolon = source.find(";", close)
        if close >= 0 and brace >= 0 and (semicolon < 0 or brace < semicolon):
            end = find_matching_brace(source, brace)
            return source[brace + 1 : end]
    fail(f"{name} function not found")


def signature_body(source: str, signature: str) -> str:
    start = source.find(signature)
    if start < 0:
        fail(f"{signature} function not found")
    brace = source.find("{", start + len(signature))
    if brace < 0:
        fail(f"{signature} function body not found")
    end = find_matching_brace(source, brace)
    return source[brace + 1 : end]


def require_public_hooks(header: str, service_header: str) -> None:
    for snippet in ["void init_tlb_shootdown();", "void note_tlb_shootdown_cpu_online();"]:
        if snippet not in header:
            fail(f"TLB shootdown hook missing from virt.hpp: {snippet}")
    if "#include <platform/mm/tlb_shootdown.hpp>" not in header:
        fail("virt.hpp must preserve the lightweight cooperative-service API include")
    if "void service_pending_tlb_shootdowns();" not in service_header:
        fail("lightweight TLB shootdown service hook is missing")


def require_atomic_per_origin_protocol(source: str) -> None:
    for snippet in [
        "struct TlbShootdownRequest",
        "std::array<TlbShootdownRequest, TLB_SHOOTDOWN_MAX_CPUS>",
        "std::array<std::atomic<uint8_t>, TLB_SHOOTDOWN_MAX_CPUS>",
        "std::atomic<uint32_t> pending",
        "std::array<std::atomic<uint64_t>, TLB_SHOOTDOWN_MAX_CPUS> observed",
    ]:
        if snippet not in source:
            fail(f"per-origin shootdown protocol is missing snippet: {snippet}")
    if "tlb_shootdown_lock" in source:
        fail("TLB shootdown must not use a global spinlock that can deadlock with IF masked")

    init_body = function_body(source, "init_tlb_shootdown")
    for snippet in [
        "gates::allocate_vector()",
        "gates::set_interrupt_handler(tlb_shootdown_vector, tlb_shootdown_handler)",
    ]:
        if snippet not in init_body:
            fail(f"TLB shootdown init missing vector setup: {snippet}")

    wait_body = function_body(source, "wait_for_tlb_shootdown_completion")
    for snippet in [
        "request.pending.load(std::memory_order_acquire) == 0U",
        "service_tlb_shootdown_requests_for_cpu(origin_cpu)",
        'asm volatile("pause" ::: "memory")',
    ]:
        if snippet not in wait_body:
            fail(f"shootdown wait loop must service incoming requests while spinning: {snippet}")

    handler_body = function_body(source, "tlb_shootdown_handler")
    if "service_tlb_shootdown_requests_for_cpu(cpu::current_cpu())" not in handler_body:
        fail("IPI handler must service pending shootdown requests for the interrupted CPU")


def require_generation_publication_is_quiescent(source: str) -> None:
    service_body = function_body(source, "service_tlb_shootdown_requests_for_cpu")
    first_generation = service_body.find("uint64_t const GENERATION = request.generation.load(std::memory_order_acquire)")
    target = service_body.find("request.targets[static_cast<size_t>(cpu_no)].load(std::memory_order_acquire)")
    target_revalidation = service_body.find("request.generation.load(std::memory_order_acquire) != GENERATION", target)
    fields = service_body.find("bool const SHARED_KERNEL = request.shared_kernel.load(std::memory_order_acquire)")
    field_revalidation = service_body.find("request.generation.load(std::memory_order_acquire) != GENERATION", fields)
    if not (0 <= first_generation < target < target_revalidation < fields < field_revalidation):
        fail("shootdown service must validate one nonzero generation around target and field reads")

    for name in ["shootdown_remote_user_pagemap", "shootdown_shared_kernel_mappings"]:
        body = function_body(source, name)
        quiesce = body.find("request.generation.exchange(0, std::memory_order_acq_rel)")
        targets = body.find("request.targets[static_cast<size_t>(target_cpu)].store(TARGET ? 1U : 0U, std::memory_order_release)")
        publish = body.rfind("request.generation.store(next_generation, std::memory_order_release)")
        if not (0 <= quiesce < targets < publish):
            fail(f"{name} must quiesce the old generation before writing targets and release-publish the new generation")


def require_cooperative_service_remains_raw_context_safe(source: str) -> None:
    bodies = [
        function_body(source, "service_pending_tlb_shootdowns"),
        function_body(source, "service_tlb_shootdown_requests_for_cpu"),
        function_body(source, "bounded_core_count"),
        function_body(source, "invalidate_local_tlb_if_current"),
    ]
    for forbidden in ["new ", "page_alloc(", "kmalloc(", "kern_yield(", "log::", "dbg::", ".lock(", "lock_irq"]:
        if any(forbidden in body for body in bodies):
            fail(f"cooperative TLB service must remain allocation-free, nonblocking, and log-free: {forbidden}")


def require_bounded_cooperative_service(body: str, context: str, needs_irq_guard: bool) -> None:
    for snippet in [
        "++service_spins;",
        "(service_spins & 0xFFU) == 0U",
        "service_pending_tlb_shootdowns();",
    ]:
        if snippet not in body:
            fail(f"{context} must periodically service pending shootdowns: {snippet}")
    has_irq_guard = "interrupts_enabled()" in body
    if has_irq_guard != needs_irq_guard:
        fail(f"{context} IRQ-state guard does not match whether the wait always masks interrupts")


def require_test_and_test_set_service_is_bounded(body: str, lock_name: str, context: str) -> None:
    failed_exchange = body.find(f"{lock_name}.exchange(true, std::memory_order_acquire)")
    service_tick = body.find("++service_spins;", failed_exchange)
    retry_load = body.find(f"{lock_name}.load(std::memory_order_relaxed)", failed_exchange)
    if not (0 <= failed_exchange < service_tick < retry_load):
        fail(f"{context} must count every failed exchange before its relaxed retry wait")


def require_irq_masked_raw_waits_service_shootdowns(spinlock: str, page_alloc: str, phys: str, serial: str) -> None:
    require_bounded_cooperative_service(function_body(spinlock, "lock_ticket"), "ticket lock wait", True)
    page_alloc_body = signature_body(page_alloc, "auto PageAllocator::lock_irq()")
    phys_body = function_body(phys, "lock_irq")
    require_bounded_cooperative_service(page_alloc_body, "page allocator wait", False)
    require_bounded_cooperative_service(phys_body, "physical allocator wait", False)
    require_test_and_test_set_service_is_bounded(page_alloc_body, "lock_held", "page allocator wait")
    require_test_and_test_set_service_is_bounded(phys_body, "locked", "physical allocator wait")
    require_bounded_cooperative_service(function_body(serial, "acquire_lock"), "serial lock wait", True)

    for source, context in [(page_alloc, "page allocator"), (serial, "serial")]:
        if "#include <platform/mm/virt.hpp>" in source:
            fail(f"{context} must not introduce a low-level MM/serial header cycle")


def require_flush_helper_reaches_remote_cpus(source: str) -> None:
    flush_body = function_body(source, "flush_pagemap_after_update")
    if "invalidate_local_tlb_if_current(pagemap, vaddr, reload_cr3)" not in flush_body:
        fail("flush helper must preserve local invalidation")
    if "shootdown_remote_user_pagemap(pagemap, vaddr, reload_cr3)" not in flush_body:
        fail("flush helper must send remote shootdowns for user pagemaps")

    remote_body = function_body(source, "shootdown_remote_user_pagemap")
    for snippet in [
        "user_pagemap_can_need_remote_shootdown(pagemap)",
        "cpu_may_have_active_pagemap(target_cpu, pagemap)",
        "target_cpu != ORIGIN_CPU",
        "request.generation.store(next_generation, std::memory_order_release)",
        "send_tlb_shootdown_ipi(target_cpu)",
        "wait_for_tlb_shootdown_completion(request, ORIGIN_CPU, next_generation, CORE_COUNT)",
    ]:
        if snippet not in remote_body:
            fail(f"remote shootdown path missing snippet: {snippet}")


def require_mapping_mutations_use_shootdown(source: str) -> None:
    map_body = function_body(source, "map_page")
    for snippet in [
        "bool const REPLACED_TRANSLATION = OLD_ENTRY.present != 0 || is_reserved_leaf(OLD_ENTRY)",
        "invalidate_local_tlb_if_current(page_table, VADDR, path_promoted)",
        "if (path_promoted || REPLACED_TRANSLATION)",
        "shootdown_remote_user_pagemap(page_table, VADDR, path_promoted)",
    ]:
        if snippet not in map_body:
            fail(f"map_page must avoid remote waits for pure new mappings but flush replacements: {snippet}")

    unmap_body = function_body(source, "unmap_page")
    flush = unmap_body.find("flush_pagemap_after_update(page_table, vaddr, false)")
    drop = unmap_body.find("drop_present_leaf_ref(OLD_ENTRY)")
    if flush < 0 or drop < 0 or flush > drop:
        fail("unmap_page must complete the shootdown before dropping the old page reference")

    reserve_body = function_body(source, "reserve_page_range")
    flush = reserve_body.find("flush_pagemap_after_update(page_table, CURRENT_VADDR, path_promoted)")
    drop = reserve_body.find("drop_present_leaf_ref(OLD_ENTRY)")
    if flush < 0 or drop < 0 or flush > drop:
        fail("reserve_page_range must complete the shootdown before dropping the old page reference")

    for name in ["unify_page_flags", "flush_page_map_batch", "destroy_user_space_budgeted", "destroy_user_space", "deep_copy_user_pagemap_cow"]:
        body = function_body(source, name)
        if "flush_pagemap_after_update(" not in body:
            fail(f"{name} must route stale-TLB-sensitive updates through the shootdown helper")

    if "flush_user_mapping_if_current" in source:
        fail("old local-only user mapping flush helper must not remain")


def require_scheduler_registers_and_marks_online(source: str) -> None:
    for snippet in [
        "mm::virt::init_tlb_shootdown();",
        "mm::virt::note_tlb_shootdown_cpu_online();",
    ]:
        if snippet not in source:
            fail(f"scheduler must wire TLB shootdown lifecycle hook: {snippet}")


def main() -> None:
    header = VIRT_HPP.read_text()
    service_header = TLB_SHOOTDOWN_HPP.read_text()
    virt = VIRT_CPP.read_text()
    scheduler = SCHEDULER_CPP.read_text()
    spinlock = SPINLOCK_CPP.read_text()
    page_alloc = PAGE_ALLOC_CPP.read_text()
    phys = PHYS_CPP.read_text()
    serial = SERIAL_CPP.read_text()
    require_public_hooks(header, service_header)
    require_atomic_per_origin_protocol(virt)
    require_generation_publication_is_quiescent(virt)
    require_cooperative_service_remains_raw_context_safe(virt)
    require_irq_masked_raw_waits_service_shootdowns(spinlock, page_alloc, phys, serial)
    require_flush_helper_reaches_remote_cpus(virt)
    require_mapping_mutations_use_shootdown(virt)
    require_scheduler_registers_and_marks_online(scheduler)
    print("TLB shootdown source invariants hold")


if __name__ == "__main__":
    main()
