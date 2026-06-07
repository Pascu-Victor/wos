#include <array>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <platform/mm/page_alloc.hpp>
#include <platform/mm/paging.hpp>
#include <platform/mm/phys.hpp>
#include <span>
#include <test/ktest.hpp>

namespace phys = ker::mod::mm::phys;
namespace paging = ker::mod::mm::paging;
namespace mm = ker::mod::mm;

namespace {

constexpr uintptr_t PAGE_ALIGNMENT_MASK = 0xFFFULL;

auto is_page_aligned(const void* ptr) -> bool { return (reinterpret_cast<uintptr_t>(ptr) & PAGE_ALIGNMENT_MASK) == 0; }

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
