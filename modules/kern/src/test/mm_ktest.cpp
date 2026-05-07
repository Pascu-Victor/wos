#include <cstdint>
#include <platform/mm/phys.hpp>
#include <test/ktest.hpp>

namespace phys = ker::mod::mm::phys;

// ---------------------------------------------------------------------------
// Basic page alloc/free round-trip
// ---------------------------------------------------------------------------

KTEST(MM, PageAllocFree) {
    void* page = phys::pageAlloc();
    KREQUIRE_NE(page, nullptr);
    // Page address must be page-aligned (4 KiB)
    KEXPECT_EQ(reinterpret_cast<uintptr_t>(page) & 0xFFFULL, 0ULL);
    phys::pageFree(page);
}

KTEST(MM, PageAllocWriteReadback) {
    auto* page = static_cast<uint64_t*>(phys::pageAlloc());
    KREQUIRE_NE(page, nullptr);
    page[0] = 0xDEADBEEFCAFEBABEULL;
    page[1] = 0x1234567890ABCDEFULL;
    KEXPECT_EQ(page[0], 0xDEADBEEFCAFEBABEULL);
    KEXPECT_EQ(page[1], 0x1234567890ABCDEFULL);
    phys::pageFree(page);
}

KTEST(MM, MultiplePageAllocFree) {
    constexpr int N = 8;
    void* pages[N] = {};
    for (int i = 0; i < N; ++i) {
        pages[i] = phys::pageAlloc();
        KEXPECT_NE(pages[i], nullptr);
    }
    // All pages must be distinct
    for (int i = 0; i < N; ++i) {
        for (int j = i + 1; j < N; ++j) {
            KEXPECT_NE(pages[i], pages[j]);
        }
    }
    for (int i = 0; i < N; ++i) {
        if (pages[i] != nullptr) {
            phys::pageFree(pages[i]);
        }
    }
}

// ---------------------------------------------------------------------------
// Refcount: a page starts at refcount 1 after pageAlloc.
// pageRefInc bumps it; pageRefDec decrements and returns new count.
// ---------------------------------------------------------------------------

KTEST(MM, RefCountBasic) {
    void* page = phys::pageAlloc();
    KREQUIRE_NE(page, nullptr);
    // Initial refcount is 1
    KEXPECT_EQ(phys::pageRefGet(page), 1U);
    // Bump to 2
    phys::pageRefInc(page);
    KEXPECT_EQ(phys::pageRefGet(page), 2U);
    // Drop back to 1
    uint32_t remaining = phys::pageRefDec(page);
    KEXPECT_EQ(remaining, 1U);
    // Final free
    phys::pageFree(page);
}

// ---------------------------------------------------------------------------
// Large allocation (multi-page): pageAlloc(size) where size > PAGE_SIZE
// ---------------------------------------------------------------------------

KTEST(MM, LargePageAlloc) {
    constexpr uint64_t TWO_PAGES = 8192;
    void* mem = phys::pageAlloc(TWO_PAGES);
    KREQUIRE_NE(mem, nullptr);
    KEXPECT_EQ(reinterpret_cast<uintptr_t>(mem) & 0xFFFULL, 0ULL);
    phys::pageFree(mem);
}
