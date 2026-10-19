#!/usr/bin/env python3
"""Exercise whole production vmap ownership/rollback bodies at finite boundaries.

The real reservation/debt bitmaps, allocation/free/rollback, context gate and
PTE-clear/shootdown/ref-dec sequence are selected without body substitution.
Only allocator owner/frame operations, leaf lookup/publication, IRQ/scheduler
facts and the shootdown endpoint are finite Host ports. This proves caller
ownership and debt transitions, not real SMP/TLB or PMM zone accounting.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import tempfile
import time


PRELUDE = r'''
#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string_view>

namespace ker::mod::mm {
using vaddr_t = uint64_t;
using paddr_t = uint64_t;
@OWNER_ENUM@
@OWNER_VALID@
namespace paging {
@PAGE_CONSTANTS@
@PAGE_TYPES@
auto purge_page_table_entry() -> PageTableEntry { return {}; }
}
}

namespace fixture {
using Owner = ker::mod::mm::PhysicalPageOwner;
using Entry = ker::mod::mm::paging::PageTableEntry;
constexpr size_t SLOTS = 32, FRAMES = 128;
struct Frame {
    bool live{}, mapped{}, flushed{};
    Owner owner{Owner::INVALID};
    bool operator==(const Frame&) const = default;
};
std::array<Entry, SLOTS> leaves{};
std::array<Frame, FRAMES> frames{};
std::array<size_t, static_cast<size_t>(Owner::COUNT)> owner_pages{};
size_t checks{}, cases{}, lock_depth{}, allocations{}, transfers{}, frees{}, shootdowns{};
int fail_allocation_after{-1}, fail_transfer_after{-1};
bool cpu_data{true}, queues{true}, irq{true};
size_t preempt{};
const char* case_name{};
void check(bool value, const char* message) {
    ++checks;
    if (!value) {
        std::fprintf(stderr, "VMAP_CHECK case=%s: %s\n", case_name, message);
        std::exit(1);
    }
}
auto id(void* address) -> size_t {
    auto value = reinterpret_cast<uintptr_t>(address);
    check(value && value % 4096 == 0 && value / 4096 <= FRAMES, "finite physical frame identity");
    return value / 4096 - 1;
}
auto mapped(size_t frame) -> bool {
    return std::any_of(leaves.begin(), leaves.end(), [frame](const auto& entry) {
        return entry.present && entry.frame == frame + 1;
    });
}
struct Lock {
    auto lock_irqsave() -> uint64_t { check(lock_depth == 0, "vmap lock not nested"); ++lock_depth; return 0; }
    void unlock_irqrestore(uint64_t) { check(lock_depth == 1, "exact vmap unlock"); --lock_depth; }
};
}

namespace ker::mod::sys { using Spinlock = fixture::Lock; }
namespace ker::mod::smt { auto has_cpu_data() -> bool { return fixture::cpu_data; } }
namespace ker::mod::sched {
auto has_run_queues() -> bool { return fixture::queues; }
auto preempt_count() -> size_t { return fixture::preempt; }
auto interrupts_enabled() -> bool { return fixture::irq; }
}
namespace ker::mod::dbg {
void panic_handler(const char* message) { fixture::check(false, message); }
}
namespace ker::mod::mm::addr {
auto get_phys_pointer(vaddr_t address) -> paddr_t { return address; }
auto get_virt_pointer(paddr_t address) -> vaddr_t { return address; }
}
namespace ker::mod::mm::virt {
using namespace paging;
namespace sys = ker::mod::sys;
namespace sched = ker::mod::sched;
namespace smt = ker::mod::smt;
namespace dbg = ker::mod::dbg;
namespace log {
template <typename... Args> void critical(const char*, Args...) { fixture::check(false, "unexpected production vmap invariant log"); }
}
@RECLAIM_STATS@
@VMAP_STORAGE@
static_assert(KERNEL_VMAP_MIN_BYTES >= fixture::SLOTS * paging::PAGE_SIZE);
PageTable root{};
PageTable* kernel_pagemap = &root;
uint64_t tlb_shootdown_vector = 1;
@BITMAP_BODIES@

auto slot(vaddr_t address) -> size_t {
    fixture::check(address >= KERNEL_VMAP_BASE && (address - KERNEL_VMAP_BASE) % paging::PAGE_SIZE == 0,
                   "finite vmap address");
    size_t index = (address - KERNEL_VMAP_BASE) / paging::PAGE_SIZE;
    fixture::check(index < fixture::SLOTS, "finite active prefix");
    return index;
}
auto leaf_entry(PageTable* table, vaddr_t address) -> PageTableEntry* {
    fixture::check(table == kernel_pagemap && fixture::lock_depth == 0, "leaf access outside vmap lock");
    size_t index = slot(address);
    fixture::check(kernel_vmap_bit_is_set(index), "leaf access retains exclusive reservation");
    return &fixture::leaves[index];
}
auto kernel_data_page_flags() -> uint64_t { return 3; }
void map_page(PageTable* table, vaddr_t address, paddr_t physical, uint64_t) {
    auto* entry = leaf_entry(table, address);
    auto index = fixture::id(reinterpret_cast<void*>(physical));
    fixture::check(!entry->present && fixture::frames[index].live, "publish only a fresh owned frame into a hole");
    *entry = {};
    entry->present = 1;
    entry->frame = index + 1;
    fixture::frames[index].mapped = true;
    fixture::frames[index].flushed = false;
}
void shootdown_shared_kernel_mappings() {
    fixture::check(fixture::lock_depth == 0, "shootdown outside vmap lock");
    ++fixture::shootdowns;
    for (size_t i = 0; i < fixture::FRAMES; ++i)
        if (fixture::frames[i].live && fixture::frames[i].mapped && !fixture::mapped(i))
            fixture::frames[i].flushed = true;
}
auto kernel_vmap_pending_free_pages() -> size_t;
auto drain_kernel_vmap_frees_bounded(size_t) -> KernelVmapReclaimStats;
void drain_kernel_vmap_frees();
void drain_kernel_vmap_frees_if_over_limit();
}

namespace ker::mod::mm::phys {
auto page_alloc_full_overwrite_page_with_reclaim_may_fail(PhysicalPageOwner owner, std::string_view) -> void* {
    fixture::check(fixture::lock_depth == 0 && physical_page_owner_is_valid(owner), "valid physical allocation outside vmap lock");
    ++fixture::allocations;
    if (fixture::fail_allocation_after == 0) return nullptr;
    if (fixture::fail_allocation_after > 0) --fixture::fail_allocation_after;
    for (size_t i = 0; i < fixture::FRAMES; ++i) {
        if (!fixture::frames[i].live) {
            fixture::frames[i] = {.live = true, .owner = owner};
            ++fixture::owner_pages[static_cast<size_t>(owner)];
            return reinterpret_cast<void*>((i + 1) * paging::PAGE_SIZE);
        }
    }
    fixture::check(false, "bounded physical fixture exhausted");
    return nullptr;
}
auto page_reassign_owner(void* address, PhysicalPageOwner owner) -> bool {
    fixture::check(fixture::lock_depth == 0, "owner transfer outside vmap lock");
    ++fixture::transfers;
    auto& frame = fixture::frames[fixture::id(address)];
    fixture::check(frame.live, "owner transfer retains a live physical frame");
    if (!physical_page_owner_is_valid(owner) || fixture::fail_transfer_after == 0) return false;
    if (fixture::fail_transfer_after > 0) --fixture::fail_transfer_after;
    --fixture::owner_pages[static_cast<size_t>(frame.owner)];
    ++fixture::owner_pages[static_cast<size_t>(owner)];
    frame.owner = owner;
    return true;
}
void page_ref_dec(void* address) {
    fixture::check(fixture::lock_depth == 0, "physical release outside vmap lock");
    auto index = fixture::id(address);
    auto& frame = fixture::frames[index];
    fixture::check(frame.live && frame.mapped && frame.flushed && !fixture::mapped(index),
                   "PTE clear then shared shootdown must precede physical release");
    --fixture::owner_pages[static_cast<size_t>(frame.owner)];
    frame = {};
    ++fixture::frees;
}
}
namespace ker::mod::mm::virt {
@VMAP_BODIES@
}
'''


CASES = r'''
namespace fixture {
namespace v = ker::mod::mm::virt;
constexpr size_t PAGE = ker::mod::mm::paging::PAGE_SIZE;
void reset(const char* name) {
    check(lock_depth == 0, "previous case left a lock");
    check(std::none_of(frames.begin(), frames.end(), [](const auto& f) { return f.live; }), "previous case leaked physical backing");
    case_name = name;
    ++cases;
    leaves = {};
    owner_pages = {};
    allocations = transfers = frees = shootdowns = 0;
    fail_allocation_after = fail_transfer_after = -1;
    cpu_data = queues = irq = true;
    preempt = 0;
    v::kernel_vmap_bitmap.fill(0);
    v::kernel_vmap_pending_bitmap.fill(0);
    v::kernel_vmap_draining.store(false);
    v::kernel_vmap_active_pages = SLOTS;
    v::kernel_vmap_next_page = 0;
    v::kernel_vmap_pending_first_page = SLOTS;
    v::kernel_vmap_pending_pages = 0;
    v::kernel_vmap_initialized = true;
}
auto allocate(size_t count, Owner owner) -> void* { return v::kernel_vmap_alloc(owner, count * PAGE, "actual-vmap-host"); }
auto physical_count() -> size_t {
    return std::count_if(frames.begin(), frames.end(), [](const auto& f) { return f.live; });
}
void pending_exact(size_t count) {
    size_t pending = 0;
    for (size_t i = 0; i < SLOTS; ++i) {
        bool backed = leaves[i].present && leaves[i].frame;
        bool queued = v::kernel_vmap_pending_bit_is_set(i);
        bool owned = v::kernel_vmap_bit_is_set(i);
        check(!queued || (backed && !owned), "queued debt must be backed, not a cold hole or live allocation");
        check(!backed || queued || owned, "every retained mapping must stay pending or actively owned");
        pending += queued;
    }
    check(pending == count && v::kernel_vmap_pending_free_pages() == count, "exact pending physical page debt");
}
void drain(size_t quantum = 2) {
    size_t initial = v::kernel_vmap_pending_free_pages();
    size_t reclaimed = 0, calls = 0;
    while (v::kernel_vmap_pending_free_pages()) {
        check(++calls <= SLOTS, "bounded drain progress");
        size_t before = v::kernel_vmap_pending_free_pages();
        size_t free_before = frees;
        auto stats = v::drain_kernel_vmap_frees_bounded(quantum);
        size_t expected = std::min(before, quantum);
        check(!stats.failed && !stats.unsafe_context && stats.scanned_pages == expected && stats.reclaimed_pages == expected,
              "bounded drain returns only actual backed pages");
        check(frees - free_before == expected && v::kernel_vmap_pending_free_pages() == before - expected,
              "bounded drain/ref-dec/debt agree");
        check(stats.has_more == (before != expected), "bounded drain has_more exact");
        reclaimed += stats.reclaimed_pages;
        pending_exact(before - expected);
    }
    check(reclaimed == initial, "all original pending backing reclaimed once");
}
void empty() {
    pending_exact(0);
    check(physical_count() == 0, "physical allocations all returned");
    for (auto count : owner_pages) check(count == 0, "physical owner counters all returned");
    for (size_t i = 0; i < SLOTS; ++i) check(!v::kernel_vmap_bit_is_set(i) && !leaves[i].present, "no reserved slot or PTE remains");
}
void owner_cases() {
    for (bool reverse : {false, true}) {
        reset(reverse ? "buffer-to-large" : "large-to-buffer");
        Owner a = reverse ? Owner::BUFFER_CACHE_DATA : Owner::KMALLOC_LARGE;
        Owner b = reverse ? Owner::KMALLOC_LARGE : Owner::BUFFER_CACHE_DATA;
        auto* address = allocate(3, a);
        check(address != nullptr, "initial owner allocation");
        std::array<uint64_t, 3> backing{};
        for (size_t i = 0; i < 3; ++i) backing[i] = leaves[i].frame;
        v::kernel_vmap_free(address, 3 * PAGE);
        pending_exact(3);
        check(allocate(3, b) == address, "warm owner reuse preserves virtual address");
        for (size_t i = 0; i < 3; ++i) {
            check(leaves[i].frame == backing[i], "warm owner reuse preserves physical backing");
            check(frames[backing[i] - 1].owner == b, "warm owner request not applied");
        }
        check(owner_pages[static_cast<size_t>(a)] == 0 && owner_pages[static_cast<size_t>(b)] == 3,
              "old owner loses exactly the adopted pages");
        check(allocations == 3 && transfers == 3 && physical_count() == 3, "warm transfer allocates no replacement frame");
        v::kernel_vmap_free(address, 3 * PAGE);
        check(allocate(3, b) == address && transfers == 6 && allocations == 3, "same-owner reuse remains stable");
        check(owner_pages[static_cast<size_t>(b)] == 3, "same-owner reuse preserves counts");
        v::kernel_vmap_free(address, 3 * PAGE);
        drain(1);
        empty();
    }
}
struct Snapshot {
    uint64_t occupied{}, pending{};
    size_t debt{}, next{}, first{}, allocs{}, retags{}, released{}, flushes{};
    std::array<uint64_t, SLOTS> ptes{};
    std::array<Frame, FRAMES> physical{};
    decltype(owner_pages) owners{};
    bool operator==(const Snapshot&) const = default;
};
auto snapshot() -> Snapshot {
    Snapshot value{};
    value.occupied = v::kernel_vmap_bitmap[0];
    value.pending = v::kernel_vmap_pending_bitmap[0];
    value.debt = v::kernel_vmap_pending_pages;
    value.next = v::kernel_vmap_next_page;
    value.first = v::kernel_vmap_pending_first_page;
    value.allocs = allocations;
    value.retags = transfers;
    value.released = frees;
    value.flushes = shootdowns;
    static_assert(sizeof(value.ptes) == sizeof(leaves));
    std::memcpy(value.ptes.data(), leaves.data(), sizeof(leaves));
    value.physical = frames;
    value.owners = owner_pages;
    return value;
}
void invalid_owner_cases() {
    for (Owner owner : {Owner::INVALID, Owner::COUNT, static_cast<Owner>(255)}) {
        for (bool warm : {false, true}) {
            reset(warm ? "invalid-warm-owner" : "invalid-cold-owner");
            if (warm) {
                auto* p = allocate(3, Owner::BUFFER_CACHE_DATA);
                check(p != nullptr, "invalid-owner setup");
                v::kernel_vmap_free(p, 3 * PAGE);
            }
            auto before = snapshot();
            check(allocate(3, owner) == nullptr, "invalid owner refused");
            check(snapshot() == before, "invalid owner changes no reservation/debt/physical state");
            drain();
            empty();
        }
    }
}
void suffix_failure_cases() {
    for (int prefix : {0, 2}) {
        reset("cold-prefix-warm-suffix-refusal");
        auto* p = allocate(8, Owner::BUFFER_CACHE_DATA);
        check(p != nullptr, "mixed suffix setup allocation");
        v::kernel_vmap_free(p, 8 * PAGE);
        auto before = v::drain_kernel_vmap_frees_bounded(4);
        check(before.reclaimed_pages == 4 && !before.failed, "construct genuine cold prefix/warm suffix");
        pending_exact(4);
        irq = false;
        fail_allocation_after = prefix;
        check(allocate(8, Owner::KMALLOC_LARGE) == nullptr, "mixed allocation refuses at selected physical boundary");
        pending_exact(4 + prefix);
        check(physical_count() == size_t(4 + prefix), "all warm suffix and new prefix frames accounted");
        for (size_t i = prefix; i < 4; ++i) check(!leaves[i].present && !v::kernel_vmap_pending_bit_is_set(i), "cold suffix holes create no debt");
        irq = true;
        fail_allocation_after = -1;
        drain();
        empty();
    }
}
void prefix_failure_cases() {
    for (int added : {0, 1}) {
        reset("warm-prefix-cold-suffix-refusal");
        auto* p = allocate(4, Owner::BUFFER_CACHE_DATA);
        check(p != nullptr, "mixed prefix setup");
        v::kernel_vmap_free(p, 4 * PAGE);
        irq = false;
        fail_allocation_after = added;
        check(allocate(8, Owner::KMALLOC_LARGE) == nullptr, "cold suffix refuses after genuine warm prefix adoption");
        pending_exact(4 + added);
        check(transfers == 4 && owner_pages[static_cast<size_t>(Owner::KMALLOC_LARGE)] == size_t(4 + added),
              "adopted warm prefix and allocated pages retain requested charge");
        irq = true;
        fail_allocation_after = -1;
        drain();
        empty();
    }
}
void hole_and_live_cases() {
    for (int added : {0, 1}) {
        reset("separated-warm-runs-with-live-neighbor");
        auto* first = allocate(2, Owner::BUFFER_CACHE_DATA);
        auto* middle = allocate(2, Owner::BUFFER_CACHE_DATA);
        auto* last = allocate(2, Owner::BUFFER_CACHE_DATA);
        auto* live = allocate(2, Owner::SELFTEST);
        check(first && middle && last && live, "three finite runs and separate live allocation");
        auto live_a = leaves[6].frame, live_b = leaves[7].frame;
        v::kernel_vmap_free(middle, 2 * PAGE);
        drain();
        v::kernel_vmap_free(first, 2 * PAGE);
        v::kernel_vmap_free(last, 2 * PAGE);
        irq = false;
        fail_allocation_after = added;
        check(allocate(6, Owner::KMALLOC_LARGE) == nullptr, "mixed run allocation refused");
        pending_exact(4 + added);
        irq = true;
        fail_allocation_after = -1;
        drain(1);
        check(leaves[6].present && leaves[6].frame == live_a && leaves[7].present && leaves[7].frame == live_b &&
                  v::kernel_vmap_bit_is_set(6) && v::kernel_vmap_bit_is_set(7) && physical_count() == 2 &&
                  owner_pages[static_cast<size_t>(Owner::SELFTEST)] == 2,
              "rollback/drain cannot reclaim a live neighboring allocation");
        v::kernel_vmap_free(live, 2 * PAGE);
        drain();
        empty();
    }
}
void transfer_refusal_cases() {
    for (int prefix : {0, 2}) {
        reset("owner-transfer-refusal");
        auto* p = allocate(4, Owner::BUFFER_CACHE_DATA);
        check(p != nullptr, "owner refusal setup");
        v::kernel_vmap_free(p, 4 * PAGE);
        irq = false;
        fail_transfer_after = prefix;
        check(allocate(4, Owner::KMALLOC_LARGE) == nullptr, "owner transfer refusal does not publish allocation");
        pending_exact(4);
        check(allocations == 4 && transfers == size_t(prefix + 1), "owner refusal does not allocate replacements or retry");
        check(owner_pages[static_cast<size_t>(Owner::KMALLOC_LARGE)] == size_t(prefix) &&
                  owner_pages[static_cast<size_t>(Owner::BUFFER_CACHE_DATA)] == size_t(4 - prefix),
              "partly retagged failure remains completely charged");
        irq = true;
        fail_transfer_after = -1;
        drain();
        empty();
    }
}
void cold_failure_cases() {
    for (int prefix : {0, 2}) {
        reset("all-cold-physical-refusal");
        irq = false;
        fail_allocation_after = prefix;
        check(allocate(8, Owner::KMALLOC_LARGE) == nullptr, "all-cold allocation refusal");
        pending_exact(prefix);
        check(physical_count() == size_t(prefix), "unbacked reservation creates no physical debt");
        irq = true;
        fail_allocation_after = -1;
        drain();
        empty();
    }
}
void unsafe_drain_cases() {
    for (bool disabled_irq : {false, true}) {
        reset(disabled_irq ? "IRQ-disabled-drain" : "preemption-disabled-drain");
        auto* p = allocate(3, Owner::BUFFER_CACHE_DATA);
        check(p != nullptr, "unsafe drain setup");
        v::kernel_vmap_free(p, 3 * PAGE);
        if (disabled_irq) irq = false;
        else preempt = 1;
        auto stats = v::drain_kernel_vmap_frees_bounded(2);
        check(stats.unsafe_context && stats.has_more && !stats.failed && stats.scanned_pages == 0 && stats.reclaimed_pages == 0 &&
                  frees == 0 && shootdowns == 0 && physical_count() == 3,
              "unsafe drain preserves debt/backing without TLB or free work");
        pending_exact(3);
        irq = true;
        preempt = 0;
        drain(1);
        empty();
    }
}
}
int main(int argc, char** argv) {
    using namespace fixture;
    check(argc == 2, "explicit finite oracle selection");
    std::string_view mode(argv[1]);
    check(mode == "all" || mode == "owners" || mode == "rollback", "known finite oracle");
    if (mode != "rollback") {
        owner_cases();
        invalid_owner_cases();
    }
    if (mode != "owners") {
        suffix_failure_cases();
        prefix_failure_cases();
        hole_and_live_cases();
        transfer_refusal_cases();
        cold_failure_cases();
        unsafe_drain_cases();
    }
    std::printf("PASS actual_vmap cases=%zu checks=%zu mode=%s\n", cases, checks, argv[1]);
}
'''


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, default=Path(__file__).resolve().parents[3])
    parser.add_argument("--virt-source", type=Path, help="explicit private source; all declarations remain genuine source-root")
    parser.add_argument("--cxx", default=os.environ.get("WOS_HOST_CXX", "g++"))
    args = parser.parse_args()
    root = args.source_root.resolve(strict=True)
    selected = (args.virt_source or root / "modules/kern/src/platform/mm/virt.opt.cpp").resolve(strict=True)
    support = root / "tests/host/unit/file_mmap_registry_runtime_test.py"
    loader = importlib.util.spec_from_file_location("vmap_host_owned_commands", support)
    owner = importlib.util.module_from_spec(loader)
    loader.loader.exec_module(owner)
    compiler = Path(shutil.which(args.cxx) or args.cxx).resolve(strict=True)
    kern = root / "modules/kern/src"
    header = kern / "platform/mm/virt.hpp"
    paging_header = kern / "platform/mm/paging.hpp"
    owner_header = kern / "platform/mm/page_alloc.hpp"
    watched = [Path(__file__).resolve(), selected, support, header, paging_header, owner_header, compiler]
    before = [owner.pin(path) for path in watched]
    text = selected.read_text()
    paging = paging_header.read_text()
    declarations = owner_header.read_text()
    names = (
        "kernel_vmap_bit_is_set", "kernel_vmap_bit_set", "kernel_vmap_bit_clear",
        "kernel_vmap_pending_bit_is_set", "kernel_vmap_pending_bit_set", "kernel_vmap_pending_bit_clear",
        "find_kernel_vmap_span_locked", "reserve_kernel_vmap_span", "release_kernel_vmap_span",
        "queue_kernel_vmap_free", "find_pending_kernel_vmap_run", "kernel_vmap_has_pending_frees", "kernel_vmap_warm_pool_over_limit",
        "rollback_kernel_vmap_reservation", "release_kernel_vmap_mappings", "kernel_vmap_context_can_drain",
        "kernel_vmap_pending_free_pages", "drain_kernel_vmap_frees_bounded", "drain_kernel_vmap_frees",
        "drain_kernel_vmap_frees_if_over_limit", "kernel_vmap_alloc", "kernel_vmap_free",
    )
    bodies, actual = [], []
    for name in names:
        prefix = "void " if name in (
            "kernel_vmap_bit_set", "kernel_vmap_bit_clear", "kernel_vmap_pending_bit_set", "kernel_vmap_pending_bit_clear",
            "drain_kernel_vmap_frees", "drain_kernel_vmap_frees_if_over_limit", "kernel_vmap_free",
        ) else "auto "
        body, row = owner.capture(text, prefix + name + "(")
        bodies.append(body)
        actual.append(dict(row, source=str(selected)))
    # The six tiny bitmap helpers must precede the explicit leaf endpoint.
    bitmap_count = 6
    enum, row = owner.capture(declarations, "enum class PhysicalPageOwner", declaration=True)
    actual.append(dict(row, source=str(owner_header)))
    valid, row = owner.capture(declarations, "physical_page_owner_is_valid(")
    actual.append(dict(row, source=str(owner_header)))
    stats, row = owner.capture(header.read_text(), "struct KernelVmapReclaimStats", declaration=True)
    actual.append(dict(row, source=str(header)))
    page_constants = paging[paging.index("constexpr uint64_t PAGE_SHIFT"):paging.index("}  // namespace ker::mod::mm::paging")]
    page_types = paging[paging.index("struct PageTableEntry {"):paging.index("struct PageFault {")]
    storage = text[text.index("constexpr vaddr_t KERNEL_VMAP_BASE ="):text.index("// The arena is fixed and fully mapped", text.index("constexpr vaddr_t KERNEL_VMAP_BASE ="))]
    cpp = PRELUDE
    for key, value in {
        "OWNER_ENUM": enum, "OWNER_VALID": valid, "PAGE_CONSTANTS": page_constants,
        "PAGE_TYPES": page_types, "RECLAIM_STATS": stats, "VMAP_STORAGE": storage,
        "BITMAP_BODIES": "\n\n".join(bodies[:bitmap_count]),
        "VMAP_BODIES": "\n\n".join(bodies[bitmap_count:]),
    }.items():
        cpp = cpp.replace("@" + key + "@", value)
    cpp += CASES
    transfer = "if (!phys::page_reassign_owner(FRAME, owner)) break;"
    # Remove only the decisive owner operation; retaining its address expression
    # keeps the control warning-clean rather than treating compile failure as credit.
    assert cpp.count(transfer) == 1
    omit_owner = cpp.replace(transfer, "(void)FRAME;")
    rollback = 'if (!rollback_kernel_vmap_reservation(first_page, PAGE_COUNT)) dbg::panic_handler("kernel vmap rollback lost reserved ownership");'
    predecessor = """if (mapped_pages != 0) {
            static_cast<void>(queue_kernel_vmap_free(first_page, mapped_pages));
        }
        if (mapped_pages < PAGE_COUNT) {
            static_cast<void>(release_kernel_vmap_span(first_page + mapped_pages, PAGE_COUNT - mapped_pages));
        }"""
    assert cpp.count(rollback) == 1
    old_rollback = cpp.replace(rollback, predecessor)
    owner.subreaper()
    started = time.monotonic()
    deadline = started + 90
    signals = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)
    handlers = {sig: signal.getsignal(sig) for sig in signals}
    groups, commands = [], []
    work = Path(tempfile.mkdtemp(prefix="wos-vmap-runtime-"))
    errors, after = [], []

    def cancelled(number, _frame):
        raise RuntimeError("vmap regression signal " + str(number))

    try:
        for sig in signals:
            signal.signal(sig, cancelled)
        for label, program, mode, expected, message in (
            ("current", cpp, "all", 0, "PASS actual_vmap cases=20 "),
            ("omit-owner", omit_owner, "owners", 1, "warm owner request not applied"),
            ("old-rollback", old_rollback, "rollback", 1, "every retained mapping must stay pending or actively owned"),
        ):
            source, binary = work / (label + ".cpp"), work / label
            source.write_text(program)
            argv = [str(compiler), "-std=c++23", "-O1", "-g", "-Wall", "-Wextra", "-Werror",
                    "-fsanitize=address,undefined", "-fno-omit-frame-pointer", str(source), "-o", str(binary)]
            compiled = owner.command(argv, work / (label + "-compile.log"), work, deadline, groups)
            compiled["expected_exit"] = 0
            commands.append(compiled)
            if compiled["exit"] != 0:
                raise ValueError(label + " must compile before its oracle can run: " + Path(compiled["log"]["path"]).read_text())
            executed = owner.command([str(binary), mode], work / (label + "-run.log"), work, deadline, groups)
            executed["expected_exit"] = expected
            commands.append(executed)
            output = Path(executed["log"]["path"]).read_text()
            if executed["exit"] != expected or message not in output:
                raise ValueError(label + " actual-body oracle mismatch: " + output)
            print(output, end="")
    except BaseException as error:
        errors.append(type(error).__name__ + ": " + str(error))
    finally:
        # The canonical owner already settles each child independently. Keep
        # scratch if session absence is uncertain, even on source/postflight error.
        for sig in signals:
            signal.signal(sig, signal.SIG_IGN)
        try:
            after = [owner.pin(path) for path in watched]
            if after != before:
                errors.append("watched source/compiler bytes changed")
        except BaseException as error:
            errors.append("source postflight " + repr(error))
        try:
            if groups:
                raise ValueError("owned command groups uncertain; preserve " + str(work))
            shutil.rmtree(work)
        except BaseException as error:
            errors.append("scratch cleanup " + repr(error))
        for sig, handler in handlers.items():
            signal.signal(sig, handler)
        if time.monotonic() >= deadline:
            errors.append("original90s whole regression deadline exceeded")
    print(json.dumps(dict(result="FAIL" if errors else "PASS", errors=errors, commands=commands,
                          source_before=before, source_after=after, selected_actual_bodies=actual,
                          seconds=time.monotonic() - started, pending_groups=groups,
                          scratch_removed=not work.exists(), scope="Actual-body Host boundaries; no actual SMP/TLB/PMM attribution")))
    return int(bool(errors))


if __name__ == "__main__":
    raise SystemExit(main())
