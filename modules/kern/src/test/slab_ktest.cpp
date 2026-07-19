#include <cstddef>
#include <cstdint>
#include <platform/mm/dyn/kmalloc.hpp>
#include <platform/mm/virt.hpp>
#include <test/ktest.hpp>

namespace km = ker::mod::mm::dyn::kmalloc;
namespace virt = ker::mod::mm::virt;

// ---------------------------------------------------------------------------
// Basic alloc/free
// ---------------------------------------------------------------------------

KTEST(Slab, AllocFreeSmall) {
    void* p = km::malloc(16);
    KREQUIRE_NE(p, nullptr);
    km::free(p);
}

KTEST(Slab, AllocFreeMid) {
    void* p = km::malloc(64);
    KREQUIRE_NE(p, nullptr);
    km::free(p);
}

KTEST(Slab, AllocFreeLarge) {
    void* p = km::malloc(0x800);
    KREQUIRE_NE(p, nullptr);
    km::free(p);
}

KTEST(Slab, MultiMegabyteAllocationUsesVmap) {
    constexpr size_t SIZE = 0x35DF00;
    auto* data = static_cast<uint8_t*>(km::malloc(SIZE));
    KREQUIRE_NE(data, nullptr);
    KEXPECT_TRUE(virt::kernel_vmap_contains(data));
    for (size_t offset = 0; offset < SIZE; offset += 4096) {
        data[offset] = static_cast<uint8_t>(offset / 4096);
    }
    KEXPECT_EQ(data[0], 0U);
    KEXPECT_EQ(data[(SIZE - 1) & ~size_t{4095}], static_cast<uint8_t>(((SIZE - 1) & ~size_t{4095}) / 4096));
    km::free(data);
    virt::drain_kernel_vmap_frees();
}

// ---------------------------------------------------------------------------
// All canonical slab size classes
// ---------------------------------------------------------------------------

KTEST(Slab, SizeClasses) {
    constexpr size_t SIZES[] = {0x10, 0x20, 0x40, 0x80, 0x100, 0x200, 0x300, 0x400, 0x800};
    for (size_t const SZ : SIZES) {
        void* p = km::malloc(SZ);
        KEXPECT_NE(p, nullptr);
        if (p != nullptr) {
            km::free(p);
        }
    }
}

// ---------------------------------------------------------------------------
// Write and read back through allocated memory
// ---------------------------------------------------------------------------

KTEST(Slab, WriteReadback) {
    auto* buf = static_cast<uint8_t*>(km::malloc(64));
    KREQUIRE_NE(buf, nullptr);
    for (int i = 0; i < 64; ++i) {
        buf[i] = static_cast<uint8_t>(i);
    }
    for (int i = 0; i < 64; ++i) {
        KEXPECT_EQ(buf[i], static_cast<uint8_t>(i));
    }
    km::free(buf);
}

// ---------------------------------------------------------------------------
// Multiple sequential alloc/free cycles — exercises the free-list path
// ---------------------------------------------------------------------------

KTEST(Slab, MultipleAllocFree) {
    constexpr int N = 32;
    // NOLINTNEXTLINE(misc-const-correctness)
    void* ptrs[N] = {};
    for (int i = 0; i < N; ++i) {
        ptrs[i] = km::malloc(64);
        KEXPECT_NE(ptrs[i], nullptr);
    }
    for (int i = 0; i < N; ++i) {
        if (ptrs[i] != nullptr) {
            km::free(ptrs[i]);
        }
    }
}

// ---------------------------------------------------------------------------
// Typed allocation helper (malloc<T>)
// ---------------------------------------------------------------------------

struct TestObj {
    uint32_t a;
    uint32_t b;
};

KTEST(Slab, TypedAlloc) {
    auto* obj = km::malloc<TestObj>();
    KREQUIRE_NE(obj, nullptr);
    obj->a = 0xDEAD;
    obj->b = 0xBEEF;
    KEXPECT_EQ(obj->a, 0xDEADU);
    KEXPECT_EQ(obj->b, 0xBEEFU);
    km::free(obj);
}

// ---------------------------------------------------------------------------
// Allocate enough objects to force a second slab page
// ---------------------------------------------------------------------------

KTEST(Slab, SlabExpansion) {
    // Force slab expansion by allocating more objects than fit in one slab page.
    // A 4096-byte slab page for size=0x80 holds roughly 40 objects.
    // Allocate 64 to guarantee crossing into a second slab.
    constexpr int N = 64;
    // NOLINTNEXTLINE(misc-const-correctness)
    void* ptrs[N] = {};
    for (int i = 0; i < N; ++i) {
        ptrs[i] = km::malloc(0x80);
        KEXPECT_NE(ptrs[i], nullptr);
    }
    for (int i = 0; i < N; ++i) {
        if (ptrs[i] != nullptr) {
            km::free(ptrs[i]);
        }
    }
}
