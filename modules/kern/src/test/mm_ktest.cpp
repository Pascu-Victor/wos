#include <array>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <platform/mm/paging.hpp>
#include <platform/mm/phys.hpp>
#include <span>
#include <test/ktest.hpp>

namespace phys = ker::mod::mm::phys;
namespace paging = ker::mod::mm::paging;

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
