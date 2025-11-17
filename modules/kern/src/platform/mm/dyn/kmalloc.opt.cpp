#include "kmalloc.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <mod/io/serial/serial.hpp>

#include "minimalist_malloc/mini_malloc.hpp"
#include "platform/mm/paging.hpp"
#include "platform/mm/phys.hpp"
#include "platform/sys/spinlock.hpp"

namespace ker::mod::mm::dyn::kmalloc {
// static tlsf_t tlsf = nullptr;
static ker::mod::sys::Spinlock* kmallocLock;

// Threshold for large allocations (2KB)
constexpr uint64_t LARGE_ALLOC_THRESHOLD = 0x800;

// Track large allocations made directly via pageAlloc
struct LargeAllocationTracker {
    void* virt_addr;
    uint64_t size;
    bool in_use;
};

constexpr size_t MAX_TRACKED_ALLOCS = 8192;
static std::array<LargeAllocationTracker, MAX_TRACKED_ALLOCS> trackedAllocs;
static bool trackedAllocsInitialized = false;

static void initTracker() {
    if (!trackedAllocsInitialized) {
        for (size_t i = 0; i < MAX_TRACKED_ALLOCS; ++i) {
            trackedAllocs[i].virt_addr = nullptr;
            trackedAllocs[i].size = 0;
            trackedAllocs[i].in_use = false;
        }
        trackedAllocsInitialized = true;
    }
}

static void trackAllocation(void* ptr, uint64_t size) {
    for (size_t i = 0; i < MAX_TRACKED_ALLOCS; ++i) {
        if (!trackedAllocs[i].in_use) {
            trackedAllocs[i].virt_addr = ptr;
            trackedAllocs[i].size = size;
            trackedAllocs[i].in_use = true;
            return;
        }
    }
    ker::mod::io::serial::write("kmalloc: tracker is full, unable to track large allocation at ");
    ker::mod::io::serial::writeHex((uint64_t)ptr);
    ker::mod::io::serial::write("\n");
}

static auto untrackAllocation(void* ptr, uint64_t& outSize) -> bool {
    for (size_t i = 0; i < MAX_TRACKED_ALLOCS; ++i) {
        if (trackedAllocs[i].in_use && trackedAllocs[i].virt_addr == ptr) {
            outSize = trackedAllocs[i].size;
            trackedAllocs[i].virt_addr = nullptr;
            trackedAllocs[i].size = 0;
            trackedAllocs[i].in_use = false;
            return true;
        }
    }
    return false;
}

void init() {
    initTracker();
    mini_malloc_init();
    kmallocLock = (ker::mod::sys::Spinlock*)mini_malloc(sizeof(ker::mod::sys::Spinlock));
}

void* malloc(uint64_t size) {
    // For large allocations, use direct page allocation
    if (size >= LARGE_ALLOC_THRESHOLD) {
        // Round up to page size (4KB)
        uint64_t page_size = ker::mod::mm::paging::PAGE_SIZE;
        uint64_t alloc_size = (size + page_size - 1) & ~(page_size - 1);
#ifdef DEBUG_KMALLOC
        ker::mod::io::serial::write("kmalloc: Large allocation (");
        ker::mod::io::serial::writeHex(size);
        ker::mod::io::serial::write(" bytes), using pageAlloc (");
        ker::mod::io::serial::writeHex(alloc_size);
        ker::mod::io::serial::write(" bytes)\n");
#endif
        // Allocate physical pages (pageAlloc returns virtual address already)
        void* virt_ptr = phys::pageAlloc(alloc_size);
        if (virt_ptr == nullptr) {
#ifdef DEBUG_KMALLOC
            ker::mod::io::serial::write("kmalloc: pageAlloc failed for large allocation\n");
#endif
            return nullptr;
        }
#ifdef DEBUG_KMALLOC
        ker::mod::io::serial::write("kmalloc: Large allocation successful, virt=");
        ker::mod::io::serial::writeHex((uint64_t)virt_ptr);
        ker::mod::io::serial::write("\n");
#endif

        // Track the allocation so we can free it later
        trackAllocation(virt_ptr, alloc_size);

        return virt_ptr;
    }

    // For small allocations, use mini_malloc
    kmallocLock->lock();
    void* ptr = mini_malloc(size);
    kmallocLock->unlock();
    return ptr;
}

void* realloc(void* ptr, int sz) {
    kmallocLock->lock();
    void* newPtr = mini_malloc(sz);
    if (newPtr) [[likely]]
        memcpy(newPtr, ptr, sz);
    kmallocLock->unlock();
    return newPtr;
}

void* calloc(int sz) {
    kmallocLock->lock();
    void* ptr = mini_malloc(sz);
    kmallocLock->unlock();
    if (ptr) [[likely]]
        memset(ptr, 0, sz);
    return ptr;
}

void free(void* ptr) {
    if (ptr == nullptr) {
        return;
    }

    kmallocLock->lock();

    // First check if this is a large allocation tracked by kmalloc
    uint64_t trackedSize = 0;
    if (untrackAllocation(ptr, trackedSize)) {
#ifdef DEBUG_KMALLOC
        // It's a large allocation we directly allocated via pageAlloc
        ker::mod::io::serial::write("kmalloc: Freeing large allocation at ");
        ker::mod::io::serial::writeHex((uint64_t)ptr);
        ker::mod::io::serial::write(" (");
        ker::mod::io::serial::writeHex(trackedSize);
        ker::mod::io::serial::write(" bytes)\n");
#endif
        kmallocLock->unlock();
        phys::pageFree(ptr);
        return;
    }

    // Otherwise, try to free via mini_malloc
    // mini_malloc handles both its own large allocations and slab allocations
    mini_free(ptr);

    kmallocLock->unlock();
}

}  // namespace ker::mod::mm::dyn::kmalloc

auto operator new(size_t sz) -> void* { return ker::mod::mm::dyn::kmalloc::malloc(sz); }

auto operator new[](size_t sz) -> void* { return ker::mod::mm::dyn::kmalloc::malloc(sz); }

// void operator delete(void* ptr) noexcept {
//     ker::mod::mm::dyn::kmalloc::kfree(ptr);
// }

void operator delete(void* ptr, size_t size) noexcept {
    (void)size;
    ker::mod::mm::dyn::kmalloc::free(ptr);
}

// void operator delete[](void* ptr) noexcept {
//     ker::mod::mm::dyn::kmalloc::kfree(ptr);
// }

void operator delete[](void* ptr, size_t size) noexcept {
    (void)size;
    ker::mod::mm::dyn::kmalloc::free(ptr);
}

void operator delete(void* ptr) noexcept { ker::mod::mm::dyn::kmalloc::free(ptr); }

void operator delete[](void* ptr) noexcept { ker::mod::mm::dyn::kmalloc::free(ptr); }
