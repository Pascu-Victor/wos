#!/usr/bin/env python3

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
VIRT_HPP = ROOT / "modules" / "kern" / "src" / "platform" / "mm" / "virt.hpp"
VIRT_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "mm" / "virt.opt.cpp"
SCHEDULER_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "scheduler.cpp"


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


def require_public_hooks(header: str) -> None:
    for snippet in [
        "void init_tlb_shootdown();",
        "void note_tlb_shootdown_cpu_online();",
    ]:
        if snippet not in header:
            fail(f"TLB shootdown hook missing from virt.hpp: {snippet}")


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
        "request.pending.load(std::memory_order_acquire) != 0",
        "service_tlb_shootdown_requests_for_cpu(origin_cpu)",
        'asm volatile("pause" ::: "memory")',
    ]:
        if snippet not in wait_body:
            fail(f"shootdown wait loop must service incoming requests while spinning: {snippet}")

    handler_body = function_body(source, "tlb_shootdown_handler")
    if "service_tlb_shootdown_requests_for_cpu(cpu::current_cpu())" not in handler_body:
        fail("IPI handler must service pending shootdown requests for the interrupted CPU")


def require_flush_helper_reaches_remote_cpus(source: str) -> None:
    flush_body = function_body(source, "flush_pagemap_after_update")
    if "invalidate_local_tlb_if_current(pagemap, vaddr, reload_cr3)" not in flush_body:
        fail("flush helper must preserve local invalidation")
    if "shootdown_remote_user_pagemap(pagemap, vaddr, reload_cr3)" not in flush_body:
        fail("flush helper must send remote shootdowns for user pagemaps")

    remote_body = function_body(source, "shootdown_remote_user_pagemap")
    for snippet in [
        "user_pagemap_can_need_remote_shootdown(pagemap)",
        "tlb_shootdown_cpu_online",
        "target_cpu != ORIGIN_CPU",
        "request.generation.store(next_generation, std::memory_order_release)",
        "send_tlb_shootdown_ipi(target_cpu)",
        "wait_for_tlb_shootdown_completion(request, ORIGIN_CPU)",
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
    virt = VIRT_CPP.read_text()
    scheduler = SCHEDULER_CPP.read_text()
    require_public_hooks(header)
    require_atomic_per_origin_protocol(virt)
    require_flush_helper_reaches_remote_cpus(virt)
    require_mapping_mutations_use_shootdown(virt)
    require_scheduler_registers_and_marks_online(scheduler)
    print("TLB shootdown source invariants hold")


if __name__ == "__main__":
    main()
