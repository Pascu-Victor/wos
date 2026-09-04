#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
KERN = ROOT / "modules" / "kern" / "src"
RECLAIM_HPP = KERN / "platform" / "mm" / "reclaim.hpp"
RECLAIM_CPP = KERN / "platform" / "mm" / "reclaim.cpp"
RECLAIM_POLICY = KERN / "platform" / "mm" / "reclaim_policy.hpp"
PHYS_CPP = KERN / "platform" / "mm" / "phys.opt.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


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


def test_registry_and_request_contract_are_fixed_and_bounded() -> None:
    header = RECLAIM_HPP.read_text()
    source = RECLAIM_CPP.read_text()
    require_tokens(
        header,
        [
            "MAX_SHRINKERS = 16",
            "MAX_WATERMARK_ZONES = 128",
            "DIRECT_RECLAIM_MAX_PASSES = 8",
            "enum class ReclaimContext",
            "enum ReclaimCapability",
            "RECLAIM_MAY_BLOCK",
            "RECLAIM_MAY_IO",
            "RECLAIM_MAY_ALLOCATE",
            "uint64_t budget_units{}",
            "uint64_t scan_budget_units{}",
            "CountCallback count{}",
            "ScanCallback scan{}",
            "request_explicit(const ReclaimRequest& request, std::string_view only_name)",
        ],
        "public reclaim contract",
    )
    require_tokens(
        source,
        [
            "std::array<RegistryEntry, MAX_SHRINKERS> entries{}",
            "std::array<Candidate, MAX_SHRINKERS> candidates{}",
            "sort_candidates(out, count)",
            "callback_request.budget_units = std::min(callback_request.budget_units, COUNT.reclaimable)",
            "CALLBACK_RESULT.scanned > callback_request.scan_budget_units",
            "CALLBACK_RESULT.reclaimed > callback_request.budget_units",
        ],
        "allocation-free bounded coordinator registry",
    )


def test_explicit_reclaim_is_deferred_to_the_safe_worker() -> None:
    source = RECLAIM_CPP.read_text()
    require_tokens(
        source,
        [
            "EXPLICIT_RECLAIM_MAX_PASSES = 1024",
            "EXPLICIT_NO_PROGRESS_PASS_LIMIT = 64",
            "std::array<char, EXPLICIT_SHRINKER_NAME_CAPACITY> shrinker_name{}",
            "ExplicitJobSlot explicit_job_slot{}",
            "take_explicit_job(explicit_job)",
            "run_explicit_job(explicit_job)",
            "result.has_more |= CALLBACK_RESULT.has_more",
            "else if (!RESULT.has_more || ++consecutive_no_progress >= EXPLICIT_NO_PROGRESS_PASS_LIMIT)",
            "coordinator.stats.explicit_completions.store(job.sequence",
            "explicit_job_slot.pending = true",
            "sched::wake_task_from_event(TASK)",
        ],
        "fixed-capacity explicit worker queue",
    )


def test_context_recursion_and_no_progress_guards_are_enforced() -> None:
    source = RECLAIM_CPP.read_text()
    phys = PHYS_CPP.read_text()
    require_tokens(
        source,
        [
            "phys::can_wait_for_reclaim()",
            "state.scan_owner.compare_exchange_strong",
            "result.recursion_avoided = true",
            "result.lease_contended = true",
            "capabilities_allowed(shrinker, request)",
            "entry.cooldown_until_round = saturating_add(round, backoff_rounds(entry.no_progress_streak))",
            "request.priority != ReclaimPriority::CRITICAL && ROUND < entry.cooldown_until_round",
        ],
        "coordinator execution guards",
    )
    require_order(
        phys,
        [
            "nested_reclaim = PREFLIGHT.recursion_avoided",
            "if (!CAN_RECLAIM || nested_reclaim || reclaim_passes >= RECLAIM_PASS_LIMIT)",
            "if (RECLAIM.recursion_avoided)",
            "sched::kern_yield_impl(reinterpret_cast<uint64_t>(caller_addr))",
        ],
        "allocator nested-reclaim exit",
    )


def test_watermarks_and_background_reclaim_are_order_aware() -> None:
    policy = RECLAIM_POLICY.read_text()
    source = RECLAIM_CPP.read_text()
    require_tokens(
        policy,
        [
            "struct Watermarks",
            "critical_pages{}",
            "low_pages{}",
            "high_pages{}",
            "order_pages(requested_order)",
            "largest_free_order",
            "PressureLevel::CRITICAL",
            "PressureLevel::LOW",
        ],
        "order-aware watermark policy",
    )
    require_tokens(
        source,
        [
            "BACKGROUND_RECLAIM_MAX_PASSES = 64",
            "ALLOCATION_PRESSURE_SAMPLE_PAGES = 256",
            "IMMEDIATE_PRESSURE_SAMPLE_ORDER = 4",
            "!above_high_watermark(REQUESTED_ORDER)",
            "pages_below_high_watermark(REQUESTED_ORDER)",
            "sched::task::Task::create_kernel_thread(\"mm_reclaim\", reclaim_worker_main)",
            "request_background(requested_order, ALLOCATED_PAGES)",
        ],
        "proactive background reclaim",
    )


def test_all_eight_shrinkers_register_in_stable_rank_order() -> None:
    descriptors = [
        ("platform/mm/virt.opt.cpp", "kernel_vmap", 0),
        ("platform/sched/scheduler.cpp", "scheduler_gc", 1),
        ("vfs/buffer_cache.cpp", "buffer_cache", 2),
        ("syscalls_impl/vmem/sys_vmem.cpp", "file_mmap_cache", 3),
        ("vfs/fs/xfs/xfs_inode.cpp", "xfs_inode", 4),
        ("net/packet.cpp", "packet_pool", 5),
        ("platform/mm/virt.opt.cpp", "anonymous_swap", 6),
        ("vfs/fs/tmpfs.cpp", "tmpfs", 7),
    ]
    for relative, name, rank in descriptors:
        source = (KERN / relative).read_text()
        descriptor = re.search(rf'\.name = "{re.escape(name)}",(?P<body>.*?)\.rank = {rank},', source, re.DOTALL)
        if descriptor is None:
            fail(f"shrinker {name} is missing rank {rank} in {relative}")
        tail = source[descriptor.end() : descriptor.end() + 1000]
        if "register_shrinker(SHRINKER)" not in tail:
            fail(f"shrinker {name} is not registered in {relative}")

    scheduler = (KERN / "platform" / "sched" / "scheduler.cpp").read_text()
    scheduler_header = (KERN / "platform" / "sched" / "scheduler.hpp").read_text()
    require_tokens(
        scheduler_header,
        ["task::Task* gc_reclaim_scan_cursor{nullptr}"],
        "bounded scheduler-GC cursor",
    )
    require_order(
        scheduler,
        [
            "rq->gc_reclaim_scan_cursor != nullptr ? rq->gc_reclaim_scan_cursor : rq->dead_list.head",
            "uint32_t remaining = rq->dead_list.count",
            "remaining-- != 0",
            "rq->gc_reclaim_scan_cursor = next",
        ],
        "fair bounded scheduler-GC scan",
    )


def test_allocator_is_decoupled_and_boot_hooks_are_safe() -> None:
    phys = PHYS_CPP.read_text()
    for direct_call in (
        "virt::drain_kernel_vmap_frees",
        "sched::reclaim_memory_pressure",
        "reclaim_clean_buffer_cache_for_pressure",
        "file_mmap_cache_reclaim",
        "xfs_icache_reclaim_for_pressure",
        "pkt_pool_reclaim_for_pressure",
        "tmpfs_reclaim_pages",
    ):
        if direct_call in phys:
            fail(f"physical allocator directly names subsystem reclaimer {direct_call}")

    mm_init = (KERN / "platform" / "mm" / "mm.opt.cpp").read_text()
    smt = (KERN / "platform" / "smt" / "smt.cpp").read_text()
    cmake = (ROOT / "modules" / "kern" / "CMakeLists.txt").read_text()
    require_order(mm_init, ["phys::init(memmap_request.response)", "reclaim::init()", "virt::init("], "early coordinator init")
    require_order(smt, ["sched::start_gc_worker()", "mm::reclaim::start_worker()"], "post-scheduler worker start")
    kasan_sources_start = cmake.find("set(KASAN_EXCLUDED_SRCS")
    kasan_sources_end = cmake.find("\n    )", kasan_sources_start)
    if (
        kasan_sources_start < 0
        or kasan_sources_end < 0
        or "src/platform/mm/reclaim.cpp" not in cmake[kasan_sources_start:kasan_sources_end]
    ):
        fail("early reclaim coordinator must remain excluded from KASAN instrumentation")


def test_ktest_surface_covers_policy_failure_modes() -> None:
    ktest = (KERN / "test" / "reclaim_ktest.cpp").read_text()
    require_tokens(
        ktest,
        [
            "KTEST(MMReclaim, PriorityAndProgress)",
            "KTEST(MMReclaim, NoProgressBackoff)",
            "KTEST(MMReclaim, RecursionGuard)",
            "KTEST(MMReclaim, ContextFiltering)",
            "KTEST(MMReclaim, BudgetBounds)",
        ],
        "isolated coordinator KTEST coverage",
    )


def main() -> None:
    test_registry_and_request_contract_are_fixed_and_bounded()
    test_explicit_reclaim_is_deferred_to_the_safe_worker()
    test_context_recursion_and_no_progress_guards_are_enforced()
    test_watermarks_and_background_reclaim_are_order_aware()
    test_all_eight_shrinkers_register_in_stable_rank_order()
    test_allocator_is_decoupled_and_boot_hooks_are_safe()
    test_ktest_surface_covers_policy_failure_modes()
    print("unified reclaim source invariants hold")


if __name__ == "__main__":
    main()
