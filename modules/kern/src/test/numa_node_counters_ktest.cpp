#ifdef WOS_SELFTEST

#include <cstdint>
#include <platform/acpi/numa_boot.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/numa_alloc.hpp>
#include <platform/mm/numa_balance.hpp>
#include <platform/smt/smt.hpp>
#include <test/ktest.hpp>
#include <type_traits>

#ifdef WOS_KASAN
#include <array>
#include <platform/mm/mm.hpp>
#include <platform/mm/virt.hpp>
#include <sanitizer/kasan.hpp>
#endif

namespace {
namespace mm = ker::mod::mm;
namespace numa = mm::numa;
namespace balance = numa::balance;
namespace topology = ker::mod::acpi::numa;
using log = ker::mod::dbg::logger<"node-counter-test">;
constexpr uint64_t MAX_FRAMES = 8 * 1024 * 1024;
constexpr size_t OWNER = static_cast<size_t>(mm::PhysicalPageOwner::SELFTEST);

// Exclusive BSS, materialized by this instrumented test outside collector
// locks. A native kernel stack must never own the diagnostic scanner's buffers.
constinit mm::phys::NodeCounterSnapshot cheap{};
constinit mm::phys::NodeCounterSnapshot bracket{};
constinit mm::phys::NodeCounterSnapshot baseline{};
constinit mm::phys::NodeCounterWorkspace cheap_work{};
constinit balance::Snapshot scanned{};
constinit balance::Workspace scan_work{};
constinit mm::phys::PhysicalOwnerSnapshot owner_baseline[topology::MAX_NODES]{};
constinit mm::phys::PhysicalOwnerSnapshot warm_owners_before[mm::PHYSICAL_PAGE_OWNER_COUNT]{};
constinit mm::phys::PhysicalOwnerSnapshot warm_owners_held[mm::PHYSICAL_PAGE_OWNER_COUNT]{};
constinit mm::phys::PhysicalOwnerSnapshot warm_shadow_before[topology::MAX_NODES]{};
constinit void* claimed_pages[512]{};

// A collector in an excluded TU cannot prepare its caller's diagnostic shadow.
// This actual instrumented byte store covers each buffer before ANY sample.
template <typename T>
[[gnu::noinline]] void prepare_observation_buffer(T& object) {
    static_assert(std::is_trivially_copyable_v<T>);
    auto* bytes = reinterpret_cast<volatile uint8_t*>(&object);
    for (size_t i = 0; i < sizeof(T); ++i) bytes[i] = 0;
}

void prepare_observation_storage() {
    prepare_observation_buffer(cheap);
    prepare_observation_buffer(bracket);
    prepare_observation_buffer(baseline);
    prepare_observation_buffer(cheap_work);
    prepare_observation_buffer(scanned);
    prepare_observation_buffer(scan_work);
    prepare_observation_buffer(owner_baseline);
    prepare_observation_buffer(warm_owners_before);
    prepare_observation_buffer(warm_owners_held);
    prepare_observation_buffer(warm_shadow_before);
    prepare_observation_buffer(claimed_pages);
}

// Actual one-frame claims may come from separate buddy blocks. Keep only
// their recorded ownership prefix here, not a 4KiB automatic pointer array.
struct CounterPages {
    CounterPages() = default;
    CounterPages(const CounterPages&) = delete;
    auto operator=(const CounterPages&) -> CounterPages& = delete;
    ~CounterPages() {
        while (count != 0) {
            void* const PAGE = claimed_pages[--count];
            claimed_pages[count] = nullptr;
            mm::phys::page_ref_dec(PAGE);
        }
    }
    // Only the bounded512-iteration loop calls this, immediately after success.
    void adopt(void* page) { claimed_pages[count++] = page; }
    size_t count{};
};

auto collect_cheap(mm::phys::NodeCounterSnapshot& output) -> bool {
    auto const ERROR = mm::phys::snapshot_node_counters(output, cheap_work);
    KEXPECT_EQ(ERROR, mm::NodeCounterError::NONE);
    KEXPECT_TRUE(output.complete);
    return ERROR == mm::NodeCounterError::NONE && output.complete;
}

void expect_row(const mm::node_counters::Counts& actual, const mm::node_counters::Counts& expected) {
    KEXPECT_EQ(actual.usable, expected.usable);
    KEXPECT_EQ(actual.free_inclusive, expected.free_inclusive);
    KEXPECT_EQ(actual.cached, expected.cached);
    KEXPECT_EQ(actual.buddy_free, expected.buddy_free);
    KEXPECT_EQ(actual.free_inclusive, actual.cached + actual.buddy_free);
}

void expect_same(const mm::phys::NodeCounterSnapshot& a, const mm::phys::NodeCounterSnapshot& b) {
    KEXPECT_EQ(a.node_count, b.node_count);
    KEXPECT_EQ(a.allocator_count, b.allocator_count);
    expect_row(a.global, b.global);
    for (size_t node = 0; node < topology::MAX_NODES; ++node) expect_row(a.nodes[node], b.nodes[node]);
}

auto collect_comparable() -> bool {
    // This is an explicit boot fixture, not an assumption that ordinary
    // scheduled CPUs cannot mutate allocator state between separate samples.
    KEXPECT_FALSE(ker::mod::smt::has_cpu_data());
    if (ker::mod::smt::has_cpu_data() || !collect_cheap(cheap)) return false;
    auto const RESULT = balance::snapshot(scanned, scan_work, MAX_FRAMES);
    KEXPECT_TRUE(RESULT.valid());
    KEXPECT_TRUE(scanned.complete);
    if (!RESULT.valid() || !scanned.complete || !collect_cheap(bracket)) return false;
    expect_same(cheap, bracket);
    KEXPECT_EQ(cheap.node_count, scanned.node_count);
    KEXPECT_EQ(cheap.allocator_count, scanned.zone_count);
    auto const& total = scanned.scanned;
    KEXPECT_EQ(cheap.global.usable + total.allocator_metadata_pages + total.zone_descriptor_pages, total.managed_pages);
    KEXPECT_EQ(cheap.global.free_inclusive, total.free_pages);
    KEXPECT_EQ(cheap.global.cached, total.cached_free_pages);
    KEXPECT_EQ(cheap.global.buddy_free + total.cached_free_pages, total.free_pages);
    KEXPECT_EQ(scanned.global.identity_mismatch_pages, 0U);
    uint64_t usable = 0, free = 0, cached = 0, buddy = 0;
    for (size_t node = 0; node < cheap.node_count; ++node) {
        auto const& counters = cheap.nodes[node];
        auto const& frames = scanned.nodes[node];
        KEXPECT_EQ(counters.usable + frames.allocator_metadata_pages + frames.zone_descriptor_pages, frames.managed_pages);
        KEXPECT_EQ(counters.free_inclusive, frames.free_pages);
        KEXPECT_EQ(counters.cached, frames.cached_free_pages);
        KEXPECT_EQ(counters.buddy_free + frames.cached_free_pages, frames.free_pages);
        KEXPECT_EQ(frames.managed_pages,
                   frames.free_pages + frames.allocator_metadata_pages + frames.zone_descriptor_pages + frames.owner_pages);
        usable += counters.usable;
        free += counters.free_inclusive;
        cached += counters.cached;
        buddy += counters.buddy_free;
    }
    KEXPECT_EQ(usable, cheap.global.usable);
    KEXPECT_EQ(free, cheap.global.free_inclusive);
    KEXPECT_EQ(cached, cheap.global.cached);
    KEXPECT_EQ(buddy, cheap.global.buddy_free);
    return true;
}

#ifdef WOS_KASAN
constexpr size_t KASAN_REUSE_ITERATIONS = 32;
constexpr size_t KASAN_VMAP_PAGES = 16;
constexpr uint64_t KASAN_VMAP_BYTES = KASAN_VMAP_PAGES * mm::paging::PAGE_SIZE;
constexpr size_t KASAN_DRAIN_PAGES = 128;
// Match the allocator's existing256MiB warm-pool policy; setup only.
constexpr size_t KASAN_SETUP_MAX_PAGES = (size_t{256} * 1024 * 1024) / mm::paging::PAGE_SIZE;
constinit balance::Snapshot kasan_reuse_baseline{};
constinit std::array<mm::addr::paddr_t, KASAN_VMAP_PAGES> kasan_reuse_frames{};
static_assert(sizeof(kasan_reuse_baseline) + sizeof(kasan_reuse_frames) <= 96 * 1024);

void prepare_kasan_reuse_storage() {
    prepare_observation_storage();
    prepare_observation_buffer(kasan_reuse_baseline);
    prepare_observation_buffer(kasan_reuse_frames);
}

void expect_owner_rows_same(const balance::Counts& actual, const balance::Counts& expected) {
    KEXPECT_EQ(actual.managed_pages, expected.managed_pages);
    KEXPECT_EQ(actual.free_pages, expected.free_pages);
    KEXPECT_EQ(actual.allocator_metadata_pages, expected.allocator_metadata_pages);
    KEXPECT_EQ(actual.zone_descriptor_pages, expected.zone_descriptor_pages);
    KEXPECT_EQ(actual.owner_pages, expected.owner_pages);
    for (size_t owner = 0; owner < mm::PHYSICAL_PAGE_OWNER_COUNT; ++owner) {
        KEXPECT_EQ(actual.owners[owner].pages, expected.owners[owner].pages);
        KEXPECT_EQ(actual.owners[owner].objects, expected.owners[owner].objects);
    }
}

void expect_kasan_reuse_owners_same() {
    KEXPECT_EQ(scanned.node_count, kasan_reuse_baseline.node_count);
    KEXPECT_EQ(scanned.zone_count, kasan_reuse_baseline.zone_count);
    expect_owner_rows_same(scanned.scanned, kasan_reuse_baseline.scanned);
    for (size_t node = 0; node < scanned.node_count; ++node) expect_owner_rows_same(scanned.nodes[node], kasan_reuse_baseline.nodes[node]);
}

struct KasanStackSlot {
    void* value{};
    KasanStackSlot() = default;
    KasanStackSlot(const KasanStackSlot&) = delete;
    auto operator=(const KasanStackSlot&) -> KasanStackSlot& = delete;
    ~KasanStackSlot() { reset(); }
    void reset() {
        if (value != nullptr) {
            void* const STACK = value;
            value = nullptr;
            mm::phys::page_free(STACK);
        }
    }
};

// One sequential test owner. Free transfers the mapping to the existing warm
// pool; it does not drain between iterations or release the reused backing.
struct KasanVmapSpan {
    void* value{};
    bool acquired{};
    bool finished{};
    KasanVmapSpan() = default;
    KasanVmapSpan(const KasanVmapSpan&) = delete;
    auto operator=(const KasanVmapSpan&) -> KasanVmapSpan& = delete;
    ~KasanVmapSpan() {
        if (acquired && !finished) (void)finish();
    }
    void adopt(void* address) {
        value = address;
        acquired = acquired || address != nullptr;
    }
    void park() {
        if (value != nullptr) {
            void* const SPAN = value;
            value = nullptr;
            ker::mod::kasan::poison_range(SPAN, KASAN_VMAP_BYTES, ker::mod::kasan::SHADOW_HEAP_FREED);
            mm::virt::kernel_vmap_free(SPAN, KASAN_VMAP_BYTES);
        }
    }
    auto finish() -> bool {
        park();
        finished = true;  // Exactly one bounded attempt, including assertion exits.
        auto const PENDING = mm::virt::kernel_vmap_pending_free_pages();
        auto const DRAIN = mm::virt::drain_kernel_vmap_frees_bounded(KASAN_DRAIN_PAGES);
        bool const OK = PENDING == KASAN_VMAP_PAGES && !DRAIN.failed && !DRAIN.unsafe_context && !DRAIN.has_more &&
                        DRAIN.scanned_pages == PENDING && DRAIN.reclaimed_pages == PENDING &&
                        mm::virt::kernel_vmap_pending_free_pages() == 0;
        KEXPECT_TRUE(OK);
        return OK;
    }
};

void touch_kasan_vmap_span(void* address) {
    ker::mod::kasan::unpoison_range(address, KASAN_VMAP_BYTES);
    auto* bytes = static_cast<volatile uint8_t*>(address);
    for (size_t page = 0; page < KASAN_VMAP_PAGES; ++page) bytes[page * mm::paging::PAGE_SIZE] = static_cast<uint8_t>(page);
    bytes[KASAN_VMAP_BYTES - 1] = 0x5a;
}
#endif

}  // namespace

KTEST(NumaNodeCounters, BootBindingAndDiagnosticDenominatorsAgree) {
    prepare_observation_storage();
    KREQUIRE_TRUE(mm::phys::node_counters_ready());
    KREQUIRE_TRUE(collect_comparable());
    baseline = cheap;
    KEXPECT_EQ(mm::phys::initialize_node_counters_after_topology(), mm::NodeCounterError::ALREADY_BOUND);
    KREQUIRE_TRUE(collect_cheap(bracket));
    expect_same(baseline, bracket);
    log::info("bound allocators=%u nodes=%u usable=%llu free-inclusive=%llu cached=%llu buddy=%llu; prescheduler only",
              static_cast<unsigned>(cheap.allocator_count), static_cast<unsigned>(cheap.node_count),
              static_cast<unsigned long long>(cheap.global.usable), static_cast<unsigned long long>(cheap.global.free_inclusive),
              static_cast<unsigned long long>(cheap.global.cached), static_cast<unsigned long long>(cheap.global.buddy_free));
}

KTEST(NumaNodeCounters, ActualPerNodeSpansCloseCountersAndOwners) {
    prepare_observation_storage();
    auto const* published = topology::boot_topology();
    KREQUIRE_TRUE(published != nullptr);
    KREQUIRE_TRUE(mm::phys::node_counters_ready());
    for (numa::NodeId node = 0; node < published->node_count; ++node) {
        KREQUIRE_TRUE(collect_comparable());
        if (cheap.nodes[node].usable == 0) {
            KEXPECT_EQ(cheap.nodes[node].free_inclusive, 0U);
            KEXPECT_EQ(cheap.nodes[node].cached, 0U);
            continue;
        }
        // Warm the actual cold span outside the measured interval. Retain it
        // while recording every global owner row, not a guessed64-page offset.
        for (size_t owner = 0; owner < mm::PHYSICAL_PAGE_OWNER_COUNT; ++owner) warm_owners_before[owner] = scanned.scanned.owners[owner];
        constexpr size_t SHADOW = static_cast<size_t>(mm::PhysicalPageOwner::KASAN_SHADOW);
        for (size_t observed = 0; observed < cheap.node_count; ++observed) {
            owner_baseline[observed] = scanned.nodes[observed].owners[OWNER];
            warm_shadow_before[observed] = scanned.nodes[observed].owners[SHADOW];
        }
        numa::AllocationContext const WARM_CONTEXT{
            .policy = {.nodes = uint64_t{1} << node, .mode = numa::Mode::BIND}, .local_node = node, .virtual_page_index = 0};
        auto warm = numa::try_allocate_pages(WARM_CONTEXT, 512, 512 * mm::paging::PAGE_SIZE, mm::PhysicalPageOwner::SELFTEST);
        KREQUIRE_EQ(warm.error, numa::AllocationError::NONE);
        KREQUIRE_NE(warm.span.data(), nullptr);
        KREQUIRE_EQ(warm.span.pages(), 512U);
        KREQUIRE_EQ(warm.actual, node);
        uintptr_t const WARM_ADDRESS = reinterpret_cast<uintptr_t>(warm.span.data());
        KREQUIRE_TRUE(collect_comparable());
        for (size_t owner = 0; owner < mm::PHYSICAL_PAGE_OWNER_COUNT; ++owner) {
            warm_owners_held[owner] = scanned.scanned.owners[owner];
            log::info("warm target=%u owner=%zu before=%llu/%llu held=%llu/%llu", static_cast<unsigned>(node), owner,
                      static_cast<unsigned long long>(warm_owners_before[owner].pages),
                      static_cast<unsigned long long>(warm_owners_before[owner].objects),
                      static_cast<unsigned long long>(warm_owners_held[owner].pages),
                      static_cast<unsigned long long>(warm_owners_held[owner].objects));
        }
        for (size_t observed = 0; observed < cheap.node_count; ++observed) {
            uint64_t const EXTRA = observed == node ? 512 : 0;
            KEXPECT_EQ(scanned.nodes[observed].owners[OWNER].pages, owner_baseline[observed].pages + EXTRA);
            KEXPECT_EQ(scanned.nodes[observed].owners[OWNER].objects, owner_baseline[observed].objects + EXTRA);
            log::info("warm target=%u shadow-node=%zu before=%llu/%llu held=%llu/%llu", static_cast<unsigned>(node), observed,
                      static_cast<unsigned long long>(warm_shadow_before[observed].pages),
                      static_cast<unsigned long long>(warm_shadow_before[observed].objects),
                      static_cast<unsigned long long>(scanned.nodes[observed].owners[SHADOW].pages),
                      static_cast<unsigned long long>(scanned.nodes[observed].owners[SHADOW].objects));
        }
        // Logging is itself outside every measured interval. Resample after
        // it, so any first observer/logging shadow belongs to setup as well.
        KREQUIRE_TRUE(collect_comparable());
        for (size_t owner = 0; owner < mm::PHYSICAL_PAGE_OWNER_COUNT; ++owner) warm_owners_held[owner] = scanned.scanned.owners[owner];
        // Shadow backing remains diagnostic kernel-lifetime ownership. Free
        // the same actual frame span and prove its return BEFORE the baseline.
        baseline = cheap;
        warm.span.reset();
        KREQUIRE_TRUE(collect_comparable());
        KEXPECT_EQ(cheap.global.free_inclusive, baseline.global.free_inclusive + 512);
        for (size_t observed = 0; observed < cheap.node_count; ++observed) {
            uint64_t const EXTRA = observed == node ? 512 : 0;
            KEXPECT_EQ(cheap.nodes[observed].free_inclusive, baseline.nodes[observed].free_inclusive + EXTRA);
            KEXPECT_EQ(cheap.nodes[observed].cached, baseline.nodes[observed].cached);
            KEXPECT_EQ(scanned.nodes[observed].owners[OWNER].pages, owner_baseline[observed].pages);
            KEXPECT_EQ(scanned.nodes[observed].owners[OWNER].objects, owner_baseline[observed].objects);
        }
        for (size_t owner = 0; owner < mm::PHYSICAL_PAGE_OWNER_COUNT; ++owner) {
            uint64_t const RETURNED = owner == OWNER ? 512 : 0;
            KEXPECT_EQ(scanned.scanned.owners[owner].pages + RETURNED, warm_owners_held[owner].pages);
            KEXPECT_EQ(scanned.scanned.owners[owner].objects + RETURNED, warm_owners_held[owner].objects);
        }
        baseline = cheap;
        for (size_t observed = 0; observed < cheap.node_count; ++observed) owner_baseline[observed] = scanned.nodes[observed].owners[OWNER];
        {
            // Contiguous free frames need not share one buddy block. Claim
            // each exact already-warmed frame through the production bridge.
            uint64_t const PHYSICAL = reinterpret_cast<uint64_t>(mm::addr::get_phys_pointer(WARM_ADDRESS));
            constexpr uint64_t PAGE = mm::paging::PAGE_SIZE;
            constexpr uint64_t BYTES = 512 * PAGE;
            KREQUIRE_TRUE(PHYSICAL <= UINT64_MAX - BYTES);
            CounterPages allocation;
            for (size_t page = 0; page < 512; ++page) {
                uint64_t const OFFSET = page * PAGE;
                uint64_t const EXPECTED_PHYSICAL = PHYSICAL + OFFSET;
                void* const CLAIM = mm::phys::page_alloc_order0_span_in_range_may_fail(mm::PhysicalPageOwner::SELFTEST, EXPECTED_PHYSICAL,
                                                                                       EXPECTED_PHYSICAL + PAGE, 1, PAGE,
                                                                                       mm::phys::PageInitialization::ZERO);
                if (CLAIM != nullptr) allocation.adopt(CLAIM);
                KREQUIRE_NE(CLAIM, nullptr);
                KREQUIRE_EQ(reinterpret_cast<uintptr_t>(CLAIM), WARM_ADDRESS + OFFSET);
                KREQUIRE_EQ(reinterpret_cast<uint64_t>(mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(CLAIM))), EXPECTED_PHYSICAL);
                KREQUIRE_EQ(numa::physical_node(EXPECTED_PHYSICAL), node);
            }
            KREQUIRE_EQ(allocation.count, 512U);
            KREQUIRE_TRUE(collect_comparable());
            KEXPECT_EQ(cheap.global.usable, baseline.global.usable);
            KEXPECT_EQ(cheap.global.free_inclusive + 512, baseline.global.free_inclusive);
            for (size_t observed = 0; observed < cheap.node_count; ++observed) {
                uint64_t const EXTRA = observed == node ? 512 : 0;
                KEXPECT_EQ(cheap.nodes[observed].free_inclusive + EXTRA, baseline.nodes[observed].free_inclusive);
                KEXPECT_EQ(cheap.nodes[observed].cached, baseline.nodes[observed].cached);
                KEXPECT_EQ(scanned.nodes[observed].owners[OWNER].pages, owner_baseline[observed].pages + EXTRA);
                KEXPECT_EQ(scanned.nodes[observed].owners[OWNER].objects, owner_baseline[observed].objects + EXTRA);
            }
        }
        KREQUIRE_TRUE(collect_comparable());
        KEXPECT_EQ(cheap.global.free_inclusive, baseline.global.free_inclusive);
        for (size_t observed = 0; observed < cheap.node_count; ++observed) {
            KEXPECT_EQ(cheap.nodes[observed].free_inclusive, baseline.nodes[observed].free_inclusive);
            KEXPECT_EQ(scanned.nodes[observed].owners[OWNER].pages, owner_baseline[observed].pages);
            KEXPECT_EQ(scanned.nodes[observed].owners[OWNER].objects, owner_baseline[observed].objects);
        }
    }
    log::info(
        "actual per-memory-node512 independent warmed-frame counter/owner transitions; BIND512 factory warmup; no scheduled pressure or "
        "SMP race claim");
}

#ifdef WOS_KASAN
KTEST(Kasan, PermanentStackPoolIsFullyBackedAndReensureKeepsOwners) {
    prepare_kasan_reuse_storage();
    KREQUIRE_TRUE(ker::mod::kasan::is_enabled());
    KREQUIRE_TRUE(mm::phys::node_counters_ready());
    auto const POOL = mm::phys::get_kernel_stack_pool_stats();
    KREQUIRE_TRUE(POOL.capacity != 0 && POOL.capacity <= mm::KERNEL_STACK_POOL_CAPACITY);
    auto* const ROOT = mm::virt::get_kernel_pagemap();
    KREQUIRE_NE(ROOT, nullptr);
    uintptr_t const ARENA = mm::GUARDED_KERNEL_STACK_ARENA_BASE;
    size_t const ARENA_BYTES = static_cast<size_t>(POOL.capacity) * mm::KERNEL_STACK_SLOT_STRIDE;
    uintptr_t const FIRST = reinterpret_cast<uintptr_t>(ker::mod::kasan::addr_to_shadow(ARENA)) & ~(mm::paging::PAGE_SIZE - 1);
    uintptr_t const LAST =
        reinterpret_cast<uintptr_t>(ker::mod::kasan::addr_to_shadow(ARENA + ARENA_BYTES - 1)) & ~(mm::paging::PAGE_SIZE - 1);
    // Inspect every covering PTE before any ensure or explicit pool-data access.
    // This must fail missing boot preparation, rather than prepare the baseline.
    for (uintptr_t page = FIRST;; page += mm::paging::PAGE_SIZE) {
        KREQUIRE_TRUE(mm::virt::translate(ROOT, page) != mm::virt::PADDR_INVALID);
        if (page == LAST) break;
    }
    for (size_t slot = 0; slot < POOL.capacity; ++slot) {
        auto* const STACK = mm::virt::guarded_kernel_stack_slot(slot);
        KREQUIRE_NE(STACK, nullptr);
        KEXPECT_TRUE(mm::virt::audit_guarded_kernel_stack(STACK));
    }
    KREQUIRE_TRUE(collect_comparable());
    baseline = cheap;
    kasan_reuse_baseline = scanned;

    // The full published arena is already backed; neither call may allocate.
    for (size_t repeat = 0; repeat < 2; ++repeat)
        KEXPECT_TRUE(ker::mod::kasan::ensure_shadow_backing(reinterpret_cast<const void*>(ARENA), ARENA_BYTES));
    auto const AFTER = mm::phys::get_kernel_stack_pool_stats();
    KEXPECT_EQ(AFTER.capacity, POOL.capacity);
    KEXPECT_EQ(AFTER.free_slots, POOL.free_slots);
    KEXPECT_EQ(AFTER.high_watermark, POOL.high_watermark);
    for (size_t slot = 0; slot < AFTER.capacity; ++slot)
        KEXPECT_TRUE(mm::virt::audit_guarded_kernel_stack(mm::virt::guarded_kernel_stack_slot(slot)));
    KREQUIRE_TRUE(collect_comparable());
    expect_same(cheap, baseline);
    expect_kasan_reuse_owners_same();
}
KTEST(Kasan, WarmedKernelStackSlotKeepsPhysicalOwners) {
    prepare_kasan_reuse_storage();
    KREQUIRE_TRUE(ker::mod::kasan::is_enabled());
    KREQUIRE_TRUE(mm::phys::node_counters_ready());
    KREQUIRE_TRUE(collect_comparable());

    KasanStackSlot slot;
    slot.value = mm::phys::kernel_stack_alloc("kasan_reuse_warm_stack");
    KREQUIRE_NE(slot.value, nullptr);
    uintptr_t const WARM_ADDRESS = reinterpret_cast<uintptr_t>(slot.value);
    slot.reset();  // Allocator already unpoisoned and zeroed the entire slot.
    auto const POOL = mm::phys::get_kernel_stack_pool_stats();
    KREQUIRE_TRUE(POOL.capacity != 0 && POOL.free_slots != 0);
    KREQUIRE_TRUE(collect_comparable());
    kasan_reuse_baseline = scanned;
    expect_kasan_reuse_owners_same();  // Warm this same comparator before measuring.
    KREQUIRE_TRUE(collect_comparable());
    kasan_reuse_baseline = scanned;

    size_t completed = 0;
    for (; completed < KASAN_REUSE_ITERATIONS; ++completed) {
        slot.value = mm::phys::kernel_stack_alloc("kasan_reuse_stack");
        bool const SAME = slot.value != nullptr && reinterpret_cast<uintptr_t>(slot.value) == WARM_ADDRESS;
        slot.reset();
        if (!SAME) break;
    }
    KEXPECT_EQ(completed, KASAN_REUSE_ITERATIONS);
    auto const AFTER = mm::phys::get_kernel_stack_pool_stats();
    KEXPECT_EQ(AFTER.capacity, POOL.capacity);
    KEXPECT_EQ(AFTER.free_slots, POOL.free_slots);
    KEXPECT_EQ(AFTER.high_watermark, POOL.high_watermark);
    KREQUIRE_TRUE(collect_comparable());
    expect_kasan_reuse_owners_same();
}

KTEST(Kasan, WarmedVmapSpanKeepsPhysicalOwners) {
    prepare_kasan_reuse_storage();
    KREQUIRE_TRUE(ker::mod::kasan::is_enabled());
    KREQUIRE_TRUE(mm::phys::node_counters_ready());
    KREQUIRE_TRUE(collect_comparable());

    // Empty the captured legitimate warm pool before this test owns a span.
    // Setup is bounded by the existing256MiB policy and128-page operations;
    // every mismatch refuses before a measured baseline or own allocation.
    auto const SETUP_PENDING = mm::virt::kernel_vmap_pending_free_pages();
    log::info("kasan vmap reuse setup: captured=%zu cap=%zu quantum=%zu", SETUP_PENDING, KASAN_SETUP_MAX_PAGES, KASAN_DRAIN_PAGES);
    KREQUIRE_TRUE(SETUP_PENDING <= KASAN_SETUP_MAX_PAGES);
    size_t const SETUP_CALLS = (SETUP_PENDING + KASAN_DRAIN_PAGES - 1) / KASAN_DRAIN_PAGES;
    size_t remaining = SETUP_PENDING;
    for (size_t call = 0; call < SETUP_CALLS; ++call) {
        KREQUIRE_TRUE(remaining != 0);
        auto const BEFORE = mm::virt::kernel_vmap_pending_free_pages();
        KREQUIRE_EQ(BEFORE, remaining);
        size_t const BUDGET = remaining < KASAN_DRAIN_PAGES ? remaining : KASAN_DRAIN_PAGES;
        auto const SETUP = mm::virt::drain_kernel_vmap_frees_bounded(BUDGET);
        auto const AFTER = mm::virt::kernel_vmap_pending_free_pages();
        log::info(
            "kasan vmap reuse setup: call=%zu/%zu before=%zu budget=%zu scanned=%zu reclaimed=%zu after=%zu "
            "failed=%u unsafe=%u has_more=%u",
            call + 1, SETUP_CALLS, BEFORE, BUDGET, SETUP.scanned_pages, SETUP.reclaimed_pages, AFTER, static_cast<unsigned>(SETUP.failed),
            static_cast<unsigned>(SETUP.unsafe_context), static_cast<unsigned>(SETUP.has_more));
        KREQUIRE_TRUE(!SETUP.failed);
        KREQUIRE_TRUE(!SETUP.unsafe_context);
        KREQUIRE_TRUE(SETUP.scanned_pages <= BUDGET);
        KREQUIRE_TRUE(SETUP.reclaimed_pages <= BUDGET);
        KREQUIRE_TRUE(SETUP.reclaimed_pages != 0);
        KREQUIRE_EQ(SETUP.scanned_pages, SETUP.reclaimed_pages);
        KREQUIRE_EQ(SETUP.reclaimed_pages, BUDGET);
        KREQUIRE_TRUE(SETUP.reclaimed_pages <= remaining);
        KREQUIRE_EQ(AFTER, remaining - SETUP.reclaimed_pages);
        KREQUIRE_EQ(SETUP.has_more, AFTER != 0);
        remaining = AFTER;
    }
    KREQUIRE_EQ(remaining, 0U);
    KREQUIRE_EQ(mm::virt::kernel_vmap_pending_free_pages(), 0U);
    {
        // Exercise actual teardown before measuring too: its first stack/data
        // shadow must not be confused with a warmed-address reuse failure.
        KasanVmapSpan teardown_warm;
        teardown_warm.adopt(mm::virt::kernel_vmap_alloc(mm::PhysicalPageOwner::SELFTEST, KASAN_VMAP_BYTES, "kasan_reuse_teardown_warm"));
        KREQUIRE_NE(teardown_warm.value, nullptr);
        touch_kasan_vmap_span(teardown_warm.value);
        KREQUIRE_TRUE(teardown_warm.finish());
    }
    KREQUIRE_TRUE(collect_comparable());
    warm_owners_before[OWNER] = scanned.scanned.owners[OWNER];
    for (size_t node = 0; node < scanned.node_count; ++node) owner_baseline[node] = scanned.nodes[node].owners[OWNER];

    KasanVmapSpan span;
    span.adopt(mm::virt::kernel_vmap_alloc(mm::PhysicalPageOwner::SELFTEST, KASAN_VMAP_BYTES, "kasan_reuse_warm_vmap"));
    KREQUIRE_NE(span.value, nullptr);
    KREQUIRE_TRUE(mm::virt::kernel_vmap_contains(span.value));
    uintptr_t const WARM_ADDRESS = reinterpret_cast<uintptr_t>(span.value);
    for (size_t page = 0; page < KASAN_VMAP_PAGES; ++page) {
        kasan_reuse_frames[page] = mm::virt::translate(mm::virt::get_kernel_pagemap(), WARM_ADDRESS + page * mm::paging::PAGE_SIZE);
        KREQUIRE_NE(kasan_reuse_frames[page], mm::virt::PADDR_INVALID);
    }
    touch_kasan_vmap_span(span.value);
    span.park();
    KREQUIRE_EQ(mm::virt::kernel_vmap_pending_free_pages(), KASAN_VMAP_PAGES);
    KREQUIRE_TRUE(collect_comparable());
    kasan_reuse_baseline = scanned;
    KEXPECT_EQ(scanned.scanned.owners[OWNER].pages, warm_owners_before[OWNER].pages + KASAN_VMAP_PAGES);
    KEXPECT_EQ(scanned.scanned.owners[OWNER].objects, warm_owners_before[OWNER].objects + KASAN_VMAP_PAGES);
    expect_kasan_reuse_owners_same();
    KREQUIRE_TRUE(collect_comparable());
    kasan_reuse_baseline = scanned;

    size_t completed = 0;
    for (; completed < KASAN_REUSE_ITERATIONS; ++completed) {
        span.adopt(mm::virt::kernel_vmap_alloc(mm::PhysicalPageOwner::SELFTEST, KASAN_VMAP_BYTES, "kasan_reuse_vmap"));
        bool same = span.value != nullptr && reinterpret_cast<uintptr_t>(span.value) == WARM_ADDRESS;
        if (same) {
            for (size_t page = 0; page < KASAN_VMAP_PAGES; ++page)
                same = same && mm::virt::translate(mm::virt::get_kernel_pagemap(), WARM_ADDRESS + page * mm::paging::PAGE_SIZE) ==
                                   kasan_reuse_frames[page];
        }
        if (same) touch_kasan_vmap_span(span.value);
        span.park();
        if (!same || mm::virt::kernel_vmap_pending_free_pages() != KASAN_VMAP_PAGES) break;
    }
    KEXPECT_EQ(completed, KASAN_REUSE_ITERATIONS);
    KREQUIRE_TRUE(collect_comparable());
    expect_kasan_reuse_owners_same();  // All41 owners, including shadow, exactly.

    KREQUIRE_TRUE(span.finish());
    for (size_t page = 0; page < KASAN_VMAP_PAGES; ++page)
        KEXPECT_EQ(mm::virt::translate(mm::virt::get_kernel_pagemap(), WARM_ADDRESS + page * mm::paging::PAGE_SIZE),
                   mm::virt::PADDR_INVALID);
    KREQUIRE_TRUE(collect_comparable());
    // Only this span's16 explicit SELFTEST frames were returned. Every other
    // owner, including KASAN_SHADOW, must still equal the actual warm baseline.
    for (size_t owner = 0; owner < mm::PHYSICAL_PAGE_OWNER_COUNT; ++owner) {
        auto const& EXPECTED = owner == OWNER ? warm_owners_before[OWNER] : kasan_reuse_baseline.scanned.owners[owner];
        KEXPECT_EQ(scanned.scanned.owners[owner].pages, EXPECTED.pages);
        KEXPECT_EQ(scanned.scanned.owners[owner].objects, EXPECTED.objects);
        for (size_t node = 0; node < scanned.node_count; ++node) {
            auto const& NODE_EXPECTED = owner == OWNER ? owner_baseline[node] : kasan_reuse_baseline.nodes[node].owners[owner];
            KEXPECT_EQ(scanned.nodes[node].owners[owner].pages, NODE_EXPECTED.pages);
            KEXPECT_EQ(scanned.nodes[node].owners[owner].objects, NODE_EXPECTED.objects);
        }
    }
}
#endif

#endif
