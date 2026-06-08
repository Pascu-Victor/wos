#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <platform/asm/cpu.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/page_alloc.hpp>
#include <platform/mm/paging.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/virt.hpp>
#include <platform/smt/smt.hpp>
#include <span>
#include <test/ktest.hpp>

namespace addr = ker::mod::mm::addr;
namespace cpu = ker::mod::cpu;
namespace phys = ker::mod::mm::phys;
namespace paging = ker::mod::mm::paging;
namespace smt = ker::mod::smt;
namespace virt = ker::mod::mm::virt;
namespace mm = ker::mod::mm;

namespace {

constexpr uintptr_t PAGE_ALIGNMENT_MASK = 0xFFFULL;
constexpr uint64_t NON_RAM_TEST_PHYS = (1ULL << 52) - paging::PAGE_SIZE;

auto is_page_aligned(const void* ptr) -> bool { return (reinterpret_cast<uintptr_t>(ptr) & PAGE_ALIGNMENT_MASK) == 0; }

struct TestUserTree {
    paging::PageTable* root = nullptr;
    paging::PageTable* pml3 = nullptr;
    paging::PageTable* pml2 = nullptr;
    paging::PageTable* pml1 = nullptr;
};

auto phys_addr_of(const void* page) -> uint64_t {
    return reinterpret_cast<uint64_t>(addr::get_phys_pointer(reinterpret_cast<addr::vaddr_t>(page)));
}

auto entry_for_page(const void* page, uint64_t flags = paging::page_types::USER) -> paging::PageTableEntry {
    return paging::create_page_table_entry(phys_addr_of(page), flags);
}

auto entry_for_phys(uint64_t phys_addr, uint64_t flags = paging::page_types::USER) -> paging::PageTableEntry {
    return paging::create_page_table_entry(phys_addr, flags);
}

auto alloc_test_page_table() -> paging::PageTable* {
    auto* table = static_cast<paging::PageTable*>(phys::page_alloc());
    if (table == nullptr) {
        return nullptr;
    }
    if (!phys::page_mark_kind(table, mm::PageKind::PAGE_TABLE)) {
        phys::page_free(table);
        return nullptr;
    }
    std::memset(table, 0, paging::PAGE_SIZE);
    return table;
}

template <typename T>
void free_live_page(T*& page) {
    if (page != nullptr && phys::page_ref_get(page) != 0U) {
        phys::page_free(page);
    }
    page = nullptr;
}

void cleanup_test_user_tree(TestUserTree& tree) {
    free_live_page(tree.pml1);
    free_live_page(tree.pml2);
    free_live_page(tree.pml3);
    free_live_page(tree.root);
}

auto init_test_user_tree(TestUserTree& tree) -> bool {
    tree.root = alloc_test_page_table();
    tree.pml3 = alloc_test_page_table();
    tree.pml2 = alloc_test_page_table();
    tree.pml1 = alloc_test_page_table();
    if (tree.root == nullptr || tree.pml3 == nullptr || tree.pml2 == nullptr || tree.pml1 == nullptr) {
        cleanup_test_user_tree(tree);
        return false;
    }

    tree.root->entries.at(0) = entry_for_page(tree.pml3);
    tree.pml3->entries.at(0) = entry_for_page(tree.pml2);
    tree.pml2->entries.at(0) = entry_for_page(tree.pml1);
    return true;
}

auto destroy_stats_cpu() -> uint64_t {
    if (smt::has_cpu_data()) {
        return cpu::current_cpu();
    }
    return 0;
}

auto destroy_stats_delta(const virt::DestroyUserSpaceStats& before, const virt::DestroyUserSpaceStats& after)
    -> virt::DestroyUserSpaceStats {
    return {
        .calls = after.calls - before.calls,
        .collect_frames_us_total = after.collect_frames_us_total - before.collect_frames_us_total,
        .collect_frames_us_max = after.collect_frames_us_max - before.collect_frames_us_max,
        .free_data_us_total = after.free_data_us_total - before.free_data_us_total,
        .free_data_us_max = after.free_data_us_max - before.free_data_us_max,
        .free_pt_us_total = after.free_pt_us_total - before.free_pt_us_total,
        .free_pt_us_max = after.free_pt_us_max - before.free_pt_us_max,
        .tlb_flush_us_total = after.tlb_flush_us_total - before.tlb_flush_us_total,
        .tlb_flush_us_max = after.tlb_flush_us_max - before.tlb_flush_us_max,
        .data_leaf_entries_visited = after.data_leaf_entries_visited - before.data_leaf_entries_visited,
        .data_pages_ref_decremented = after.data_pages_ref_decremented - before.data_pages_ref_decremented,
        .data_pages_freed = after.data_pages_freed - before.data_pages_freed,
        .page_table_pages_ref_decremented = after.page_table_pages_ref_decremented - before.page_table_pages_ref_decremented,
        .page_table_pages_freed = after.page_table_pages_freed - before.page_table_pages_freed,
        .skipped_huge_pages = after.skipped_huge_pages - before.skipped_huge_pages,
        .skipped_unknown_frames = after.skipped_unknown_frames - before.skipped_unknown_frames,
        .skipped_slab_alloc_frames = after.skipped_slab_alloc_frames - before.skipped_slab_alloc_frames,
        .skipped_medium_alloc_frames = after.skipped_medium_alloc_frames - before.skipped_medium_alloc_frames,
        .skipped_kmalloc_large_alloc_frames = after.skipped_kmalloc_large_alloc_frames - before.skipped_kmalloc_large_alloc_frames,
        .skipped_page_table_aliases = after.skipped_page_table_aliases - before.skipped_page_table_aliases,
        .skipped_corrupt_entries = after.skipped_corrupt_entries - before.skipped_corrupt_entries,
        .magic_unknown_probe_reads = after.magic_unknown_probe_reads - before.magic_unknown_probe_reads,
        .magic_unknown_slab_hits = after.magic_unknown_slab_hits - before.magic_unknown_slab_hits,
        .magic_unknown_medium_hits = after.magic_unknown_medium_hits - before.magic_unknown_medium_hits,
        .magic_unknown_kmalloc_large_hits = after.magic_unknown_kmalloc_large_hits - before.magic_unknown_kmalloc_large_hits,
    };
}

auto destroy_user_space_and_get_delta(paging::PageTable* root) -> virt::DestroyUserSpaceStats {
    uint64_t const CPU_NO = destroy_stats_cpu();
    virt::DestroyUserSpaceStats const BEFORE = virt::get_destroy_user_space_stats(CPU_NO);
    virt::destroy_user_space(root, 0, "mm_ktest", "negative-contract");
    virt::DestroyUserSpaceStats const AFTER = virt::get_destroy_user_space_stats(CPU_NO);
    return destroy_stats_delta(BEFORE, AFTER);
}

auto alloc_stats_snapshot() -> phys::AllocStatsSnapshot {
    phys::AllocStatsSnapshot snapshot{};
    phys::get_alloc_stats_snapshot(snapshot);
    return snapshot;
}

auto increasing_delta(uint64_t before, uint64_t after) -> uint64_t { return after >= before ? after - before : ~0ULL; }

auto decreasing_delta(uint64_t before, uint64_t after) -> uint64_t { return before >= after ? before - after : ~0ULL; }

void expect_alloc_stats_coherent(const phys::AllocStatsSnapshot& snapshot) {
    KEXPECT_TRUE(snapshot.total_allocated_pages >= snapshot.total_freed_pages);
    KEXPECT_TRUE(snapshot.total_allocated_bytes >= snapshot.total_freed_bytes);
    KEXPECT_EQ(snapshot.live_allocated_pages, snapshot.total_allocated_pages - snapshot.total_freed_pages);
    KEXPECT_EQ(snapshot.live_allocated_bytes, snapshot.total_allocated_bytes - snapshot.total_freed_bytes);
    KEXPECT_EQ(snapshot.current_free_pages, snapshot.free_mem_bytes / paging::PAGE_SIZE);
    KEXPECT_EQ(snapshot.current_free_pages + snapshot.live_allocated_pages, snapshot.total_mem_bytes / paging::PAGE_SIZE);
}

}  // namespace

// ---------------------------------------------------------------------------
// Basic page alloc/free round-trip
// ---------------------------------------------------------------------------

KTEST(MM, PageAllocFree) {
    void* page = phys::page_alloc();
    KREQUIRE_NE(page, nullptr);
    // Page address must be page-aligned (4 KiB)
    KEXPECT_EQ(is_page_aligned(page), true);
    phys::page_free(page);
}

KTEST(MM, PageAllocWriteReadback) {
    auto* page = static_cast<uint64_t*>(phys::page_alloc());
    KREQUIRE_NE(page, nullptr);
    page[0] = 0xDEADBEEFCAFEBABEULL;
    page[1] = 0x1234567890ABCDEFULL;
    KEXPECT_EQ(page[0], 0xDEADBEEFCAFEBABEULL);
    KEXPECT_EQ(page[1], 0x1234567890ABCDEFULL);
    phys::page_free(page);
}

KTEST(MM, PagemapPoolReusesZeroedRootPage) {
    virt::PageTablePoolStatsSnapshot before{};
    virt::get_page_table_pool_stats_snapshot(before);

    auto* first = virt::create_pagemap();
    KREQUIRE_NE(first, nullptr);
    first->entries.at(7).present = 1;
    first->entries.at(7).writable = 1;
    first->entries.at(7).frame = 0x12345;
    virt::release_pagemap(first);

    auto* second = virt::create_pagemap();
    KREQUIRE_NE(second, nullptr);

    paging::PageTableEntry zero_entry{};
    bool table_zero = true;
    for (auto const& entry : second->entries) {
        if (std::memcmp(&entry, &zero_entry, sizeof(entry)) != 0) {
            table_zero = false;
            break;
        }
    }
    KEXPECT_TRUE(table_zero);

    virt::PageTablePoolStatsSnapshot after{};
    virt::get_page_table_pool_stats_snapshot(after);
    KEXPECT_TRUE(after.alloc_hits >= before.alloc_hits + 1);
    KEXPECT_TRUE(after.releases >= before.releases + 1);

    virt::release_pagemap(second);
}

KTEST(MM, OwnedFrameTrackingMapUnmapPrivateNormalPage) {
    constexpr uint64_t TEST_VADDR = 0x40000000ULL;

    virt::OwnedFrameStatsSnapshot before{};
    virt::get_owned_frame_stats_snapshot(before);

    auto* root = virt::create_pagemap();
    KREQUIRE_NE(root, nullptr);

    void* page = phys::page_alloc(paging::PAGE_SIZE, "owned_frame_ktest");
    if (page == nullptr) {
        virt::release_pagemap(root);
        KREQUIRE_NE(page, nullptr);
    }

    virt::map_page(root, TEST_VADDR, phys_addr_of(page), paging::page_types::USER);

    virt::OwnedFrameStatsSnapshot after_map{};
    virt::get_owned_frame_stats_snapshot(after_map);
    KEXPECT_TRUE(after_map.track_added >= before.track_added + 1);
    KEXPECT_TRUE(after_map.entries >= before.entries + 1);

    virt::unmap_page(root, TEST_VADDR);

    virt::OwnedFrameStatsSnapshot after_unmap{};
    virt::get_owned_frame_stats_snapshot(after_unmap);
    KEXPECT_TRUE(after_unmap.untrack_removed >= after_map.untrack_removed + 1);

    virt::destroy_user_space(root, 0, "mm_ktest", "owned-frame");
    virt::release_pagemap(root);
}

KTEST(MM, MultiplePageAllocFree) {
    constexpr size_t PAGE_COUNT = 8;
    std::array<void*, PAGE_COUNT> pages{};
    for (auto& page : pages) {
        page = phys::page_alloc();
        KEXPECT_NE(page, nullptr);
    }
    // All pages must be distinct
    for (const auto* first = pages.cbegin(); first != pages.cend(); ++first) {
        for (const auto* second = std::next(first); second != pages.cend(); ++second) {
            KEXPECT_NE(*first, *second);
        }
    }
    for (auto* page : pages) {
        if (page != nullptr) {
            phys::page_free(page);
        }
    }
}

KTEST(MM, AllocStatsScalarLargeFreeDrift) {
    constexpr uint64_t REQUESTED_PAGES = 3;
    constexpr uint64_t ACCOUNTED_PAGES = 4;
    constexpr uint64_t ACCOUNTED_BYTES = ACCOUNTED_PAGES * paging::PAGE_SIZE;

    phys::AllocStatsSnapshot const BEFORE = alloc_stats_snapshot();
    expect_alloc_stats_coherent(BEFORE);

    void* mem = phys::page_alloc(REQUESTED_PAGES * paging::PAGE_SIZE);
    KREQUIRE_NE(mem, nullptr);

    phys::AllocStatsSnapshot const AFTER_ALLOC = alloc_stats_snapshot();
    expect_alloc_stats_coherent(AFTER_ALLOC);
    KEXPECT_EQ(increasing_delta(BEFORE.total_allocated_pages, AFTER_ALLOC.total_allocated_pages), ACCOUNTED_PAGES);
    KEXPECT_EQ(increasing_delta(BEFORE.total_allocated_bytes, AFTER_ALLOC.total_allocated_bytes), ACCOUNTED_BYTES);
    KEXPECT_EQ(increasing_delta(BEFORE.alloc_count, AFTER_ALLOC.alloc_count), 1U);
    KEXPECT_EQ(increasing_delta(BEFORE.total_freed_pages, AFTER_ALLOC.total_freed_pages), 0U);
    KEXPECT_EQ(increasing_delta(BEFORE.free_count, AFTER_ALLOC.free_count), 0U);
    KEXPECT_EQ(increasing_delta(BEFORE.live_allocated_pages, AFTER_ALLOC.live_allocated_pages), ACCOUNTED_PAGES);
    KEXPECT_EQ(decreasing_delta(BEFORE.current_free_pages, AFTER_ALLOC.current_free_pages), ACCOUNTED_PAGES);

    phys::page_free(mem);

    phys::AllocStatsSnapshot const AFTER_FREE = alloc_stats_snapshot();
    expect_alloc_stats_coherent(AFTER_FREE);
    KEXPECT_EQ(increasing_delta(AFTER_ALLOC.total_allocated_pages, AFTER_FREE.total_allocated_pages), 0U);
    KEXPECT_EQ(increasing_delta(AFTER_ALLOC.alloc_count, AFTER_FREE.alloc_count), 0U);
    KEXPECT_EQ(increasing_delta(AFTER_ALLOC.total_freed_pages, AFTER_FREE.total_freed_pages), ACCOUNTED_PAGES);
    KEXPECT_EQ(increasing_delta(AFTER_ALLOC.total_freed_bytes, AFTER_FREE.total_freed_bytes), ACCOUNTED_BYTES);
    KEXPECT_EQ(increasing_delta(AFTER_ALLOC.free_count, AFTER_FREE.free_count), 1U);
    KEXPECT_EQ(decreasing_delta(AFTER_ALLOC.live_allocated_pages, AFTER_FREE.live_allocated_pages), ACCOUNTED_PAGES);
    KEXPECT_EQ(increasing_delta(AFTER_ALLOC.current_free_pages, AFTER_FREE.current_free_pages), ACCOUNTED_PAGES);

    KEXPECT_EQ(increasing_delta(BEFORE.total_allocated_pages, AFTER_FREE.total_allocated_pages), ACCOUNTED_PAGES);
    KEXPECT_EQ(increasing_delta(BEFORE.total_freed_pages, AFTER_FREE.total_freed_pages), ACCOUNTED_PAGES);
    KEXPECT_EQ(increasing_delta(BEFORE.alloc_count, AFTER_FREE.alloc_count), 1U);
    KEXPECT_EQ(increasing_delta(BEFORE.free_count, AFTER_FREE.free_count), 1U);
    KEXPECT_EQ(AFTER_FREE.live_allocated_pages, BEFORE.live_allocated_pages);
    KEXPECT_EQ(AFTER_FREE.current_free_pages, BEFORE.current_free_pages);
}

// ---------------------------------------------------------------------------
// Refcount: a page starts at refcount 1 after pageAlloc.
// page_ref_inc bumps it; page_ref_dec decrements and returns new count.
// ---------------------------------------------------------------------------

KTEST(MM, RefCountBasic) {
    void* page = phys::page_alloc();
    KREQUIRE_NE(page, nullptr);
    // Initial refcount is 1
    KEXPECT_EQ(phys::page_ref_get(page), 1U);
    // Bump to 2
    phys::page_ref_inc(page);
    KEXPECT_EQ(phys::page_ref_get(page), 2U);
    // Drop back to 1
    uint32_t const REMAINING = phys::page_ref_dec(page);
    KEXPECT_EQ(REMAINING, 1U);
    // Final free
    phys::page_free(page);
}

KTEST(MM, RefCountDecWithLookupHint) {
    void* page = phys::page_alloc();
    KREQUIRE_NE(page, nullptr);

    phys::page_ref_inc(page);
    phys::PageLookupHint hint{};
    KEXPECT_EQ(phys::page_ref_get(page, &hint), 2U);

    KEXPECT_EQ(phys::page_ref_dec(page, &hint), 1U);
    KEXPECT_EQ(phys::page_ref_dec(page, &hint), 0U);
    KEXPECT_EQ(phys::page_ref_get(page, &hint), 0U);
}

KTEST(MM, LookupHintOwnerMustMatchAllocatorDomain) {
    void* regular_page = phys::page_alloc();
    KREQUIRE_NE(regular_page, nullptr);

    phys::PageLookupHint hint{};
    KEXPECT_EQ(phys::page_ref_get(regular_page, &hint), 1U);
    KEXPECT_EQ(hint.owner, phys::PageLookupOwner::REGULAR_ZONE);

    hint.owner = phys::PageLookupOwner::HUGE_ZONE;
    KEXPECT_EQ(phys::page_ref_get(regular_page, &hint), 1U);
    KEXPECT_EQ(hint.owner, phys::PageLookupOwner::REGULAR_ZONE);

    void* huge_page = phys::page_alloc_huge(paging::PAGE_SIZE);
    if (huge_page == nullptr) {
        phys::page_free(regular_page);
        return;
    }

    KEXPECT_EQ(phys::page_ref_get(huge_page, &hint), 1U);
    KEXPECT_EQ(hint.owner, phys::PageLookupOwner::HUGE_ZONE);

    hint.owner = phys::PageLookupOwner::REGULAR_ZONE;
    KEXPECT_EQ(phys::page_ref_get(huge_page, &hint), 1U);
    KEXPECT_EQ(hint.owner, phys::PageLookupOwner::HUGE_ZONE);

    KEXPECT_EQ(phys::page_kind_get(regular_page, &hint), mm::PageKind::NORMAL);
    KEXPECT_EQ(hint.owner, phys::PageLookupOwner::REGULAR_ZONE);

    phys::page_free(huge_page);
    phys::page_free(regular_page);
}

KTEST(MM, RefCountBatchFinalFreeContiguousRun) {
    constexpr size_t PAGE_COUNT = 4;
    auto* base = static_cast<uint8_t*>(phys::page_alloc(paging::PAGE_SIZE * PAGE_COUNT));
    KREQUIRE_NE(base, nullptr);
    KREQUIRE_TRUE(phys::page_split_to_order0(base));

    std::array<void*, PAGE_COUNT> pages{};
    for (size_t i = 0; i < PAGE_COUNT; ++i) {
        pages.at(i) = base + (i * paging::PAGE_SIZE);
        KEXPECT_EQ(phys::page_ref_get(pages.at(i)), 1U);
    }

    phys::PageRefBatchStats const STATS = phys::page_ref_dec_batch(std::span<void* const>{pages.data(), pages.size()});
    KEXPECT_EQ(STATS.refs_decremented, static_cast<uint64_t>(PAGE_COUNT));
    KEXPECT_EQ(STATS.pages_freed, static_cast<uint64_t>(PAGE_COUNT));

    for (void* page : pages) {
        KEXPECT_EQ(phys::page_ref_get(page), 0U);
    }
}

KTEST(MM, RefCountBatchFinalFreeWithLookupHint) {
    constexpr size_t PAGE_COUNT = 4;
    auto* base = static_cast<uint8_t*>(phys::page_alloc(paging::PAGE_SIZE * PAGE_COUNT));
    KREQUIRE_NE(base, nullptr);
    KREQUIRE_TRUE(phys::page_split_to_order0(base));

    std::array<void*, PAGE_COUNT> pages{};
    phys::PageLookupHint hint{};
    for (size_t i = 0; i < PAGE_COUNT; ++i) {
        pages.at(i) = base + (i * paging::PAGE_SIZE);
        KEXPECT_EQ(phys::page_ref_get(pages.at(i), &hint), 1U);
    }

    phys::PageRefBatchStats const STATS = phys::page_ref_dec_batch(std::span<void* const>{pages.data(), pages.size()}, &hint);
    KEXPECT_EQ(STATS.refs_decremented, static_cast<uint64_t>(PAGE_COUNT));
    KEXPECT_EQ(STATS.pages_freed, static_cast<uint64_t>(PAGE_COUNT));

    for (void* page : pages) {
        KEXPECT_EQ(phys::page_ref_get(page, &hint), 0U);
    }
}

KTEST(MM, AllocStatsBatchRefDecFreeDrift) {
    constexpr size_t PAGE_COUNT = 4;
    constexpr uint64_t ACCOUNTED_PAGES = static_cast<uint64_t>(PAGE_COUNT);
    constexpr uint64_t ACCOUNTED_BYTES = ACCOUNTED_PAGES * paging::PAGE_SIZE;

    phys::AllocStatsSnapshot const BEFORE = alloc_stats_snapshot();
    expect_alloc_stats_coherent(BEFORE);

    auto* base = static_cast<uint8_t*>(phys::page_alloc(paging::PAGE_SIZE * PAGE_COUNT));
    KREQUIRE_NE(base, nullptr);

    phys::AllocStatsSnapshot const AFTER_ALLOC = alloc_stats_snapshot();
    expect_alloc_stats_coherent(AFTER_ALLOC);
    if (!KEXPECT_TRUE(phys::page_split_to_order0(base))) {
        phys::page_free(base);
        return;
    }

    std::array<void*, PAGE_COUNT> pages{};
    for (size_t i = 0; i < PAGE_COUNT; ++i) {
        pages.at(i) = base + (i * paging::PAGE_SIZE);
        KEXPECT_EQ(phys::page_ref_get(pages.at(i)), 1U);
    }

    KEXPECT_EQ(increasing_delta(BEFORE.total_allocated_pages, AFTER_ALLOC.total_allocated_pages), ACCOUNTED_PAGES);
    KEXPECT_EQ(increasing_delta(BEFORE.total_allocated_bytes, AFTER_ALLOC.total_allocated_bytes), ACCOUNTED_BYTES);
    KEXPECT_EQ(increasing_delta(BEFORE.alloc_count, AFTER_ALLOC.alloc_count), 1U);
    KEXPECT_EQ(increasing_delta(BEFORE.total_freed_pages, AFTER_ALLOC.total_freed_pages), 0U);
    KEXPECT_EQ(increasing_delta(BEFORE.free_count, AFTER_ALLOC.free_count), 0U);
    KEXPECT_EQ(increasing_delta(BEFORE.live_allocated_pages, AFTER_ALLOC.live_allocated_pages), ACCOUNTED_PAGES);
    KEXPECT_EQ(decreasing_delta(BEFORE.current_free_pages, AFTER_ALLOC.current_free_pages), ACCOUNTED_PAGES);

    phys::PageRefBatchStats const STATS = phys::page_ref_dec_batch(std::span<void* const>{pages.data(), pages.size()});

    phys::AllocStatsSnapshot const AFTER_BATCH = alloc_stats_snapshot();
    expect_alloc_stats_coherent(AFTER_BATCH);
    KEXPECT_EQ(STATS.refs_decremented, ACCOUNTED_PAGES);
    KEXPECT_EQ(STATS.pages_freed, ACCOUNTED_PAGES);
    KEXPECT_EQ(increasing_delta(AFTER_ALLOC.total_allocated_pages, AFTER_BATCH.total_allocated_pages), 0U);
    KEXPECT_EQ(increasing_delta(AFTER_ALLOC.alloc_count, AFTER_BATCH.alloc_count), 0U);
    KEXPECT_EQ(increasing_delta(AFTER_ALLOC.total_freed_pages, AFTER_BATCH.total_freed_pages), ACCOUNTED_PAGES);
    KEXPECT_EQ(increasing_delta(AFTER_ALLOC.total_freed_bytes, AFTER_BATCH.total_freed_bytes), ACCOUNTED_BYTES);
    KEXPECT_EQ(increasing_delta(AFTER_ALLOC.free_count, AFTER_BATCH.free_count), ACCOUNTED_PAGES);
    KEXPECT_EQ(decreasing_delta(AFTER_ALLOC.live_allocated_pages, AFTER_BATCH.live_allocated_pages), ACCOUNTED_PAGES);
    KEXPECT_EQ(increasing_delta(AFTER_ALLOC.current_free_pages, AFTER_BATCH.current_free_pages), ACCOUNTED_PAGES);

    KEXPECT_EQ(increasing_delta(BEFORE.total_allocated_pages, AFTER_BATCH.total_allocated_pages), ACCOUNTED_PAGES);
    KEXPECT_EQ(increasing_delta(BEFORE.total_freed_pages, AFTER_BATCH.total_freed_pages), ACCOUNTED_PAGES);
    KEXPECT_EQ(increasing_delta(BEFORE.alloc_count, AFTER_BATCH.alloc_count), 1U);
    KEXPECT_EQ(increasing_delta(BEFORE.free_count, AFTER_BATCH.free_count), ACCOUNTED_PAGES);
    KEXPECT_EQ(AFTER_BATCH.live_allocated_pages, BEFORE.live_allocated_pages);
    KEXPECT_EQ(AFTER_BATCH.current_free_pages, BEFORE.current_free_pages);

    for (void* page : pages) {
        KEXPECT_EQ(phys::page_ref_get(page), 0U);
        KEXPECT_EQ(phys::page_kind_get(page), mm::PageKind::FREE);
        if (phys::page_kind_get(page) != mm::PageKind::FREE) {
            phys::page_free(page);
        }
    }
}

KTEST(MM, PageKindTracksSplitBatchFree) {
    constexpr size_t PAGE_COUNT = 4;
    auto* base = static_cast<uint8_t*>(phys::page_alloc(paging::PAGE_SIZE * PAGE_COUNT));
    KREQUIRE_NE(base, nullptr);

    KREQUIRE_TRUE(phys::page_mark_kind(base, mm::PageKind::PAGE_TABLE));

    std::array<void*, PAGE_COUNT> pages{};
    for (size_t i = 0; i < PAGE_COUNT; ++i) {
        pages.at(i) = base + (i * paging::PAGE_SIZE);
        KEXPECT_EQ(phys::page_kind_get(pages.at(i)), mm::PageKind::PAGE_TABLE);
    }

    KREQUIRE_TRUE(phys::page_split_to_order0(base));
    for (void* page : pages) {
        KEXPECT_EQ(phys::page_kind_get(page), mm::PageKind::PAGE_TABLE);
        KEXPECT_EQ(phys::page_ref_get(page), 1U);
    }

    phys::PageRefBatchStats const STATS = phys::page_ref_dec_batch(std::span<void* const>{pages.data(), pages.size()});
    KEXPECT_EQ(STATS.refs_decremented, static_cast<uint64_t>(PAGE_COUNT));
    KEXPECT_EQ(STATS.pages_freed, static_cast<uint64_t>(PAGE_COUNT));

    for (void* page : pages) {
        KEXPECT_EQ(phys::page_kind_get(page), mm::PageKind::FREE);
        KEXPECT_EQ(phys::page_ref_get(page), 0U);
    }
}

KTEST(MM, UnsplitLargeAllocationContinuationIsNotLeafFreeable) {
    constexpr size_t PAGE_COUNT = 4;
    auto* base = static_cast<uint8_t*>(phys::page_alloc(paging::PAGE_SIZE * PAGE_COUNT));
    KREQUIRE_NE(base, nullptr);
    KREQUIRE_TRUE(phys::page_mark_kind(base, mm::PageKind::PAGE_TABLE));

    void* continuation_page = base + paging::PAGE_SIZE;
    KEXPECT_EQ(phys::page_kind_get(base), mm::PageKind::PAGE_TABLE);
    KEXPECT_EQ(phys::page_kind_get(continuation_page), mm::PageKind::PAGE_TABLE);
    KEXPECT_EQ(phys::page_ref_get(base), 1U);
    KEXPECT_EQ(phys::page_ref_get(continuation_page), 1U);

    std::array<void*, 1> pages{continuation_page};
    phys::PageRefBatchStats const STATS = phys::page_ref_dec_batch(std::span<void* const>{pages.data(), pages.size()});
    KEXPECT_EQ(STATS.refs_decremented, 1U);
    KEXPECT_EQ(STATS.pages_freed, 0U);

    KEXPECT_EQ(phys::page_ref_get(base), 1U);
    KEXPECT_EQ(phys::page_ref_get(continuation_page), 0U);
    KEXPECT_EQ(phys::page_kind_get(base), mm::PageKind::PAGE_TABLE);
    KEXPECT_EQ(phys::page_kind_get(continuation_page), mm::PageKind::PAGE_TABLE);

    phys::page_free(base);
}

KTEST(MM, UnsplitLargeAllocationHeadIsNotLeafFreeable) {
    constexpr size_t PAGE_COUNT = 4;
    auto* base = static_cast<uint8_t*>(phys::page_alloc(paging::PAGE_SIZE * PAGE_COUNT));
    KREQUIRE_NE(base, nullptr);
    KREQUIRE_TRUE(phys::page_mark_kind(base, mm::PageKind::PAGE_TABLE));

    std::array<void*, 1> pages{base};
    phys::PageRefBatchStats const STATS = phys::page_ref_dec_batch(std::span<void* const>{pages.data(), pages.size()});
    KEXPECT_EQ(STATS.refs_decremented, 1U);
    KEXPECT_EQ(STATS.pages_freed, 0U);

    KEXPECT_EQ(phys::page_ref_get(base), 0U);
    for (size_t i = 1; i < PAGE_COUNT; ++i) {
        void* page = base + (i * paging::PAGE_SIZE);
        KEXPECT_EQ(phys::page_ref_get(page), 1U);
        KEXPECT_EQ(phys::page_kind_get(page), mm::PageKind::PAGE_TABLE);
    }

    phys::page_free(base);
}

KTEST(MM, SplitLargeAllocationMakesAllLeavesIndependentlyFreeable) {
    constexpr size_t PAGE_COUNT = 8;
    auto* base = static_cast<uint8_t*>(phys::page_alloc(paging::PAGE_SIZE * PAGE_COUNT));
    KREQUIRE_NE(base, nullptr);
    KREQUIRE_TRUE(phys::page_mark_kind(base, mm::PageKind::PAGE_TABLE));

    std::array<void*, PAGE_COUNT> pages{};
    for (size_t i = 0; i < PAGE_COUNT; ++i) {
        pages.at(i) = base + (i * paging::PAGE_SIZE);
        KEXPECT_EQ(phys::page_kind_get(pages.at(i)), mm::PageKind::PAGE_TABLE);
        KEXPECT_EQ(phys::page_ref_get(pages.at(i)), 1U);
    }

    KREQUIRE_TRUE(phys::page_split_to_order0(base));
    KEXPECT_TRUE(phys::page_split_to_order0(base));

    for (void* page : pages) {
        KEXPECT_EQ(phys::page_kind_get(page), mm::PageKind::PAGE_TABLE);
        KEXPECT_EQ(phys::page_ref_get(page), 1U);
    }

    phys::PageRefBatchStats const STATS = phys::page_ref_dec_batch(std::span<void* const>{pages.data(), pages.size()});
    KEXPECT_EQ(STATS.refs_decremented, static_cast<uint64_t>(PAGE_COUNT));
    KEXPECT_EQ(STATS.pages_freed, static_cast<uint64_t>(PAGE_COUNT));

    for (void* page : pages) {
        KEXPECT_EQ(phys::page_kind_get(page), mm::PageKind::FREE);
        KEXPECT_EQ(phys::page_ref_get(page), 0U);
    }
}

// ---------------------------------------------------------------------------
// Negative reclaim contracts: protected or ambiguous frames must not be
// reclaimed as ordinary user data.
// ---------------------------------------------------------------------------

KTEST(MM, DestroyUserSpaceSkipsProtectedLeafFrames) {
    TestUserTree tree{};
    KREQUIRE_TRUE(init_test_user_tree(tree));

    void* medium_page = phys::page_alloc();
    void* large_page = phys::page_alloc();
    void* unknown_page = phys::page_alloc();
    if (!KEXPECT_NE(medium_page, nullptr) || !KEXPECT_NE(large_page, nullptr) || !KEXPECT_NE(unknown_page, nullptr)) {
        free_live_page(medium_page);
        free_live_page(large_page);
        free_live_page(unknown_page);
        cleanup_test_user_tree(tree);
        return;
    }

    if (!KEXPECT_TRUE(phys::page_mark_kind(medium_page, mm::PageKind::MEDIUM)) ||
        !KEXPECT_TRUE(phys::page_mark_kind(large_page, mm::PageKind::KMALLOC_LARGE)) ||
        !KEXPECT_TRUE(phys::page_mark_kind(unknown_page, mm::PageKind::UNKNOWN))) {
        free_live_page(medium_page);
        free_live_page(large_page);
        free_live_page(unknown_page);
        cleanup_test_user_tree(tree);
        return;
    }

    tree.pml1->entries.at(0) = entry_for_page(medium_page);
    tree.pml1->entries.at(1) = entry_for_page(large_page);
    tree.pml1->entries.at(2) = entry_for_page(unknown_page);

    virt::DestroyUserSpaceStats const DELTA = destroy_user_space_and_get_delta(tree.root);

    KEXPECT_EQ(DELTA.calls, 1U);
    KEXPECT_EQ(DELTA.data_leaf_entries_visited, 3U);
    KEXPECT_EQ(DELTA.data_pages_ref_decremented, 0U);
    KEXPECT_EQ(DELTA.data_pages_freed, 0U);
    KEXPECT_EQ(DELTA.page_table_pages_freed, 3U);
    KEXPECT_EQ(DELTA.skipped_unknown_frames, 1U);
    KEXPECT_EQ(DELTA.skipped_slab_alloc_frames, 0U);
    KEXPECT_EQ(DELTA.skipped_medium_alloc_frames, 1U);
    KEXPECT_EQ(DELTA.skipped_kmalloc_large_alloc_frames, 1U);
    KEXPECT_EQ(DELTA.skipped_page_table_aliases, 0U);
    KEXPECT_EQ(DELTA.skipped_corrupt_entries, 0U);

    KEXPECT_EQ(phys::page_ref_get(medium_page), 1U);
    KEXPECT_EQ(phys::page_ref_get(large_page), 1U);
    KEXPECT_EQ(phys::page_ref_get(unknown_page), 1U);
    KEXPECT_EQ(phys::page_ref_get(tree.pml1), 0U);
    KEXPECT_EQ(phys::page_ref_get(tree.pml2), 0U);
    KEXPECT_EQ(phys::page_ref_get(tree.pml3), 0U);
    KEXPECT_EQ(phys::page_ref_get(tree.root), 1U);
    KEXPECT_EQ(tree.root->entries.at(0).present, 0U);

    cleanup_test_user_tree(tree);
    free_live_page(medium_page);
    free_live_page(large_page);
    free_live_page(unknown_page);
}

KTEST(MM, DestroyUserSpaceUnknownMagicProbeIsDiagnosticOnly) {
    constexpr uint64_t MEDIUM_ALLOC_MAGIC = 0xCAFEBABE87654321ULL;

    TestUserTree tree{};
    KREQUIRE_TRUE(init_test_user_tree(tree));

    void* unknown_page = phys::page_alloc();
    if (!KEXPECT_NE(unknown_page, nullptr)) {
        cleanup_test_user_tree(tree);
        return;
    }

    auto* words = static_cast<uint64_t*>(unknown_page);
    words[2] = MEDIUM_ALLOC_MAGIC;
    if (!KEXPECT_TRUE(phys::page_mark_kind(unknown_page, mm::PageKind::UNKNOWN))) {
        words[2] = 0;
        free_live_page(unknown_page);
        cleanup_test_user_tree(tree);
        return;
    }

    tree.pml1->entries.at(0) = entry_for_page(unknown_page);

    virt::DestroyUserSpaceStats const DELTA = destroy_user_space_and_get_delta(tree.root);

    KEXPECT_EQ(DELTA.calls, 1U);
    KEXPECT_EQ(DELTA.data_leaf_entries_visited, 1U);
    KEXPECT_EQ(DELTA.data_pages_ref_decremented, 0U);
    KEXPECT_EQ(DELTA.data_pages_freed, 0U);
    KEXPECT_EQ(DELTA.page_table_pages_freed, 3U);
#ifdef WOS_MM_RECLAIM_MAGIC_PROBES
    KEXPECT_EQ(DELTA.skipped_unknown_frames, 0U);
    KEXPECT_EQ(DELTA.skipped_medium_alloc_frames, 1U);
    KEXPECT_EQ(DELTA.magic_unknown_probe_reads, 1U);
    KEXPECT_EQ(DELTA.magic_unknown_medium_hits, 1U);
#else
    KEXPECT_EQ(DELTA.skipped_unknown_frames, 1U);
    KEXPECT_EQ(DELTA.skipped_medium_alloc_frames, 0U);
    KEXPECT_EQ(DELTA.magic_unknown_probe_reads, 0U);
    KEXPECT_EQ(DELTA.magic_unknown_medium_hits, 0U);
#endif
    KEXPECT_EQ(DELTA.skipped_slab_alloc_frames, 0U);
    KEXPECT_EQ(DELTA.skipped_kmalloc_large_alloc_frames, 0U);
    KEXPECT_EQ(DELTA.skipped_page_table_aliases, 0U);
    KEXPECT_EQ(DELTA.skipped_corrupt_entries, 0U);

    KEXPECT_EQ(phys::page_ref_get(unknown_page), 1U);
    KEXPECT_EQ(phys::page_ref_get(tree.pml1), 0U);
    KEXPECT_EQ(phys::page_ref_get(tree.pml2), 0U);
    KEXPECT_EQ(phys::page_ref_get(tree.pml3), 0U);
    KEXPECT_EQ(phys::page_ref_get(tree.root), 1U);
    KEXPECT_EQ(tree.root->entries.at(0).present, 0U);

    // This is synthetic magic in an UNKNOWN page, not a real medium allocation.
    // Clear it before returning the page so direct page_free guards stay armed.
    words[2] = 0;
    cleanup_test_user_tree(tree);
    free_live_page(unknown_page);
}

KTEST(MM, DestroyUserSpaceSkipsSlabUnknownAndCorruptNextLevelFrames) {
    auto* root = alloc_test_page_table();
    KREQUIRE_NE(root, nullptr);

    void* slab_page = phys::page_alloc();
    void* normal_page = phys::page_alloc();
    if (!KEXPECT_NE(slab_page, nullptr) || !KEXPECT_NE(normal_page, nullptr)) {
        free_live_page(slab_page);
        free_live_page(normal_page);
        free_live_page(root);
        return;
    }

    if (!KEXPECT_TRUE(phys::page_mark_kind(slab_page, mm::PageKind::SLAB))) {
        free_live_page(slab_page);
        free_live_page(normal_page);
        free_live_page(root);
        return;
    }
    root->entries.at(0) = entry_for_page(slab_page);
    root->entries.at(1) = entry_for_page(normal_page);
    root->entries.at(2) = entry_for_phys(NON_RAM_TEST_PHYS);

    virt::DestroyUserSpaceStats const DELTA = destroy_user_space_and_get_delta(root);

    KEXPECT_EQ(DELTA.calls, 1U);
    KEXPECT_EQ(DELTA.data_leaf_entries_visited, 0U);
    KEXPECT_EQ(DELTA.data_pages_ref_decremented, 0U);
    KEXPECT_EQ(DELTA.data_pages_freed, 0U);
    KEXPECT_EQ(DELTA.page_table_pages_freed, 0U);
    KEXPECT_EQ(DELTA.skipped_unknown_frames, 2U);
    KEXPECT_EQ(DELTA.skipped_slab_alloc_frames, 2U);
    KEXPECT_EQ(DELTA.skipped_medium_alloc_frames, 0U);
    KEXPECT_EQ(DELTA.skipped_kmalloc_large_alloc_frames, 0U);
    KEXPECT_EQ(DELTA.skipped_page_table_aliases, 0U);
    KEXPECT_EQ(DELTA.skipped_corrupt_entries, 2U);

    KEXPECT_EQ(phys::page_ref_get(slab_page), 1U);
    KEXPECT_EQ(phys::page_ref_get(normal_page), 1U);
    KEXPECT_EQ(root->entries.at(0).present, 0U);
    KEXPECT_EQ(root->entries.at(1).present, 0U);
    KEXPECT_EQ(root->entries.at(2).present, 0U);

    free_live_page(root);
    free_live_page(slab_page);
    free_live_page(normal_page);
}

KTEST(MM, DestroyUserSpaceKeepsPageTableAliasOutOfDataRefdec) {
    TestUserTree tree{};
    KREQUIRE_TRUE(init_test_user_tree(tree));

    tree.pml1->entries.at(7) = entry_for_page(tree.pml1);

    virt::DestroyUserSpaceStats const DELTA = destroy_user_space_and_get_delta(tree.root);

    KEXPECT_EQ(DELTA.calls, 1U);
    KEXPECT_EQ(DELTA.data_leaf_entries_visited, 1U);
    KEXPECT_EQ(DELTA.data_pages_ref_decremented, 0U);
    KEXPECT_EQ(DELTA.data_pages_freed, 0U);
    KEXPECT_EQ(DELTA.page_table_pages_freed, 3U);
    KEXPECT_EQ(DELTA.skipped_unknown_frames, 0U);
    KEXPECT_EQ(DELTA.skipped_slab_alloc_frames, 0U);
    KEXPECT_EQ(DELTA.skipped_medium_alloc_frames, 0U);
    KEXPECT_EQ(DELTA.skipped_kmalloc_large_alloc_frames, 0U);
    KEXPECT_EQ(DELTA.skipped_page_table_aliases, 1U);
    KEXPECT_EQ(DELTA.skipped_corrupt_entries, 0U);

    KEXPECT_EQ(phys::page_ref_get(tree.pml1), 0U);
    KEXPECT_EQ(phys::page_ref_get(tree.pml2), 0U);
    KEXPECT_EQ(phys::page_ref_get(tree.pml3), 0U);
    KEXPECT_EQ(phys::page_ref_get(tree.root), 1U);
    KEXPECT_EQ(tree.root->entries.at(0).present, 0U);

    cleanup_test_user_tree(tree);
}

KTEST(MM, RefCountBatchMixedFinalAndNonFinal) {
    constexpr size_t PAGE_COUNT = 4;
    std::array<void*, PAGE_COUNT> pages{};
    for (auto& page : pages) {
        page = phys::page_alloc();
        if (!KEXPECT_NE(page, nullptr)) {
            for (void* allocated_page : pages) {
                if (allocated_page != nullptr) {
                    phys::page_free(allocated_page);
                }
            }
            return;
        }
    }

    phys::page_ref_inc(pages.at(0));
    phys::page_ref_inc(pages.at(2));
    phys::page_ref_inc(pages.at(2));

    phys::PageRefBatchStats const STATS = phys::page_ref_dec_batch(std::span<void* const>{pages.data(), pages.size()});
    KEXPECT_EQ(STATS.refs_decremented, static_cast<uint64_t>(PAGE_COUNT));
    KEXPECT_EQ(STATS.pages_freed, 2U);

    KEXPECT_EQ(phys::page_ref_get(pages.at(0)), 1U);
    KEXPECT_EQ(phys::page_ref_get(pages.at(1)), 0U);
    KEXPECT_EQ(phys::page_ref_get(pages.at(2)), 2U);
    KEXPECT_EQ(phys::page_ref_get(pages.at(3)), 0U);
    KEXPECT_EQ(phys::page_kind_get(pages.at(1)), mm::PageKind::FREE);
    KEXPECT_EQ(phys::page_kind_get(pages.at(3)), mm::PageKind::FREE);

    KEXPECT_EQ(phys::page_ref_dec(pages.at(0)), 0U);
    KEXPECT_EQ(phys::page_ref_dec(pages.at(2)), 1U);
    KEXPECT_EQ(phys::page_ref_dec(pages.at(2)), 0U);
}

KTEST(MM, RefCountBatchFlushesMoreThanInternalCap) {
    constexpr size_t PAGE_COUNT = 130;
    std::array<void*, PAGE_COUNT> pages{};
    for (auto& page : pages) {
        page = phys::page_alloc();
        if (!KEXPECT_NE(page, nullptr)) {
            for (void* allocated_page : pages) {
                if (allocated_page != nullptr) {
                    phys::page_free(allocated_page);
                }
            }
            return;
        }
    }

    phys::PageRefBatchStats const STATS = phys::page_ref_dec_batch(std::span<void* const>{pages.data(), pages.size()});
    KEXPECT_EQ(STATS.refs_decremented, static_cast<uint64_t>(PAGE_COUNT));
    KEXPECT_EQ(STATS.pages_freed, static_cast<uint64_t>(PAGE_COUNT));

    for (void* page : pages) {
        KEXPECT_EQ(phys::page_ref_get(page), 0U);
        KEXPECT_EQ(phys::page_kind_get(page), mm::PageKind::FREE);
    }
}

KTEST(MM, RefCountBatchDuplicateAndNullEntries) {
    void* duplicate_page = phys::page_alloc();
    void* single_page = phys::page_alloc();
    if (!KEXPECT_NE(duplicate_page, nullptr) || !KEXPECT_NE(single_page, nullptr)) {
        if (duplicate_page != nullptr) {
            phys::page_free(duplicate_page);
        }
        if (single_page != nullptr) {
            phys::page_free(single_page);
        }
        return;
    }

    phys::page_ref_inc(duplicate_page);
    std::array<void*, 5> pages{duplicate_page, nullptr, duplicate_page, single_page, nullptr};

    phys::PageRefBatchStats const STATS = phys::page_ref_dec_batch(std::span<void* const>{pages.data(), pages.size()});
    KEXPECT_EQ(STATS.refs_decremented, 3U);
    KEXPECT_EQ(STATS.pages_freed, 2U);
    KEXPECT_EQ(phys::page_ref_get(duplicate_page), 0U);
    KEXPECT_EQ(phys::page_ref_get(single_page), 0U);
    KEXPECT_EQ(phys::page_kind_get(duplicate_page), mm::PageKind::FREE);
    KEXPECT_EQ(phys::page_kind_get(single_page), mm::PageKind::FREE);
}

// ---------------------------------------------------------------------------
// Large allocation (multi-page): pageAlloc(size) where size > PAGE_SIZE
// ---------------------------------------------------------------------------

KTEST(MM, LargePageAlloc) {
    constexpr uint64_t TWO_PAGES = 8192;
    void* mem = phys::page_alloc(TWO_PAGES);
    KREQUIRE_NE(mem, nullptr);
    KEXPECT_EQ(is_page_aligned(mem), true);
    phys::page_free(mem);
}

KTEST(MM, PerCpuPageCacheRevivalOrder0FreeResetsMetadata) {
    void* page = phys::page_alloc();
    KREQUIRE_NE(page, nullptr);
    KEXPECT_EQ(phys::page_ref_get(page), 1U);
    KEXPECT_EQ(phys::page_kind_get(page), mm::PageKind::NORMAL);
    KREQUIRE_TRUE(phys::page_mark_kind(page, mm::PageKind::PAGE_TABLE));

    phys::page_free(page);
    KEXPECT_EQ(phys::page_ref_get(page), 0U);
    KEXPECT_EQ(phys::page_kind_get(page), mm::PageKind::FREE);

    void* fresh_page = phys::page_alloc();
    KREQUIRE_NE(fresh_page, nullptr);
    KEXPECT_EQ(phys::page_ref_get(fresh_page), 1U);
    KEXPECT_EQ(phys::page_kind_get(fresh_page), mm::PageKind::NORMAL);
    phys::page_free(fresh_page);
}
