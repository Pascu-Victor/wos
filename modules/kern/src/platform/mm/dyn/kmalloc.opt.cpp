#include "kmalloc.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
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

// Dynamic tracking array - starts at MIN capacity and grows/shrinks as needed
constexpr size_t MIN_TRACKED_ALLOCS = 8192;
constexpr size_t GROW_THRESHOLD_PERCENT = 75;    // Grow when 75% full
constexpr size_t SHRINK_THRESHOLD_PERCENT = 25;  // Shrink when 25% full (but not below MIN)

static LargeAllocationTracker* trackedAllocs = nullptr;
static size_t trackedAllocsCapacity = 0;
static size_t trackedAllocsCount = 0;  // Number of in_use entries
static bool trackedAllocsInitialized = false;

// Calculate size in bytes for a given capacity
static auto trackerArraySize(size_t capacity) -> uint64_t {
    uint64_t size = capacity * sizeof(LargeAllocationTracker);
    uint64_t page_size = ker::mod::mm::paging::PAGE_SIZE;
    return (size + page_size - 1) & ~(page_size - 1);  // Round up to page size
}

static void initTracker() {
    if (!trackedAllocsInitialized) {
        uint64_t allocSize = trackerArraySize(MIN_TRACKED_ALLOCS);
        trackedAllocs = static_cast<LargeAllocationTracker*>(phys::pageAlloc(allocSize));
        if (trackedAllocs == nullptr) {
            ker::mod::io::serial::write("kmalloc: FATAL - failed to allocate initial tracker array\n");
            return;
        }
        trackedAllocsCapacity = MIN_TRACKED_ALLOCS;
        trackedAllocsCount = 0;
        for (size_t i = 0; i < trackedAllocsCapacity; ++i) {
            trackedAllocs[i].virt_addr = nullptr;
            trackedAllocs[i].size = 0;
            trackedAllocs[i].in_use = false;
        }
        trackedAllocsInitialized = true;
    }
}

// Resize the tracker array (grow or shrink)
// Returns true on success, false on failure
static auto resizeTrackerArray(size_t newCapacity) -> bool {
    if (newCapacity < MIN_TRACKED_ALLOCS) {
        newCapacity = MIN_TRACKED_ALLOCS;
    }
    if (newCapacity == trackedAllocsCapacity) {
        return true;  // No change needed
    }

    uint64_t newAllocSize = trackerArraySize(newCapacity);
    auto* newArray = static_cast<LargeAllocationTracker*>(phys::pageAlloc(newAllocSize));
    if (newArray == nullptr) {
        return false;  // Allocation failed
    }

    // Initialize new array
    for (size_t i = 0; i < newCapacity; ++i) {
        newArray[i].virt_addr = nullptr;
        newArray[i].size = 0;
        newArray[i].in_use = false;
    }

    // Copy existing entries to new array (compact them)
    size_t newIndex = 0;
    for (size_t i = 0; i < trackedAllocsCapacity && newIndex < newCapacity; ++i) {
        if (trackedAllocs[i].in_use) {
            newArray[newIndex] = trackedAllocs[i];
            newIndex++;
        }
    }

    // Free old array
    phys::pageFree(trackedAllocs);

    trackedAllocs = newArray;
    trackedAllocsCapacity = newCapacity;
    // trackedAllocsCount stays the same

    return true;
}

// Check if we need to grow the array
static void maybeGrowTracker() {
    if (trackedAllocsCapacity == 0) {
        return;
    }

    size_t threshold = (trackedAllocsCapacity * GROW_THRESHOLD_PERCENT) / 100;
    if (trackedAllocsCount >= threshold) {
        size_t newCapacity = trackedAllocsCapacity * 2;
        if (!resizeTrackerArray(newCapacity)) {
            ker::mod::io::serial::write("kmalloc: WARNING - failed to grow tracker array\n");
        }
    }
}

// Check if we should shrink the array
static void maybeShrinkTracker() {
    if (trackedAllocsCapacity <= MIN_TRACKED_ALLOCS) {
        return;
    }

    size_t threshold = (trackedAllocsCapacity * SHRINK_THRESHOLD_PERCENT) / 100;
    if (trackedAllocsCount <= threshold) {
        size_t newCapacity = trackedAllocsCapacity / 2;
        newCapacity = std::max(newCapacity, MIN_TRACKED_ALLOCS);
        if (newCapacity < trackedAllocsCapacity) {
            resizeTrackerArray(newCapacity);  // Shrink failure is not critical
        }
    }
}

static void trackAllocation(void* ptr, uint64_t size) {
    // Check if we need to grow before adding
    maybeGrowTracker();

    for (size_t i = 0; i < trackedAllocsCapacity; ++i) {
        if (!trackedAllocs[i].in_use) {
            trackedAllocs[i].virt_addr = ptr;
            trackedAllocs[i].size = size;
            trackedAllocs[i].in_use = true;
            trackedAllocsCount++;
            return;
        }
    }
    ker::mod::io::serial::write("kmalloc: tracker is full, unable to track large allocation at ");
    ker::mod::io::serial::writeHex((uint64_t)ptr);
    ker::mod::io::serial::write("\n");
    mm::phys::dumpPageAllocationsOOM();
}

static auto untrackAllocation(void* ptr, uint64_t& outSize) -> bool {
    for (size_t i = 0; i < trackedAllocsCapacity; ++i) {
        if (trackedAllocs[i].in_use && trackedAllocs[i].virt_addr == ptr) {
            outSize = trackedAllocs[i].size;
            trackedAllocs[i].virt_addr = nullptr;
            trackedAllocs[i].size = 0;
            trackedAllocs[i].in_use = false;
            trackedAllocsCount--;

            // Check if we should shrink after removing
            maybeShrinkTracker();
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

// Helper to find tracked allocation without removing it
static auto findTrackedAllocation(void* ptr, uint64_t& outSize) -> bool {
    for (size_t i = 0; i < trackedAllocsCapacity; ++i) {
        if (trackedAllocs[i].in_use && trackedAllocs[i].virt_addr == ptr) {
            outSize = trackedAllocs[i].size;
            return true;
        }
    }
    return false;
}

// Helper to update tracked allocation pointer and size
static void updateTrackedAllocation(void* oldPtr, void* newPtr, uint64_t newSize) {
    for (size_t i = 0; i < trackedAllocsCapacity; ++i) {
        if (trackedAllocs[i].in_use && trackedAllocs[i].virt_addr == oldPtr) {
            trackedAllocs[i].virt_addr = newPtr;
            trackedAllocs[i].size = newSize;
            return;
        }
    }
}

auto realloc(void* ptr, int sz) -> void* {
    if (ptr == nullptr) {
        return malloc(static_cast<uint64_t>(sz));
    }

    if (sz <= 0) {
        free(ptr);
        return nullptr;
    }

    uint64_t newSize = static_cast<uint64_t>(sz);

    // Check if this is a tracked large allocation
    uint64_t oldSize = 0;
    if (findTrackedAllocation(ptr, oldSize)) {
        // It's a large allocation - handle with pageAlloc directly
        uint64_t page_size = ker::mod::mm::paging::PAGE_SIZE;
        uint64_t newAllocSize = (newSize + page_size - 1) & ~(page_size - 1);

        // If the new size fits in the current allocation and is the same page count, return same pointer
        if (newAllocSize == oldSize) {
            return ptr;
        }

        // Need to reallocate (grow or shrink): allocate new pages directly
        void* newPtr = phys::pageAlloc(newAllocSize);
        if (newPtr == nullptr) {
            return nullptr;  // Allocation failed, original ptr still valid
        }

        // Copy old data to new location (copy the smaller of old/new sizes)
        uint64_t copySize = (oldSize < newSize) ? oldSize : newSize;
        memcpy(newPtr, ptr, copySize);

        // Update tracking: change the entry to point to new allocation
        updateTrackedAllocation(ptr, newPtr, newAllocSize);

        // Free old pages
        phys::pageFree(ptr);

        return newPtr;
    }

    // Check if new size should be a large allocation
    if (newSize >= LARGE_ALLOC_THRESHOLD) {
        // Transitioning from small (mini_malloc) to large (pageAlloc)
        uint64_t page_size = ker::mod::mm::paging::PAGE_SIZE;
        uint64_t newAllocSize = (newSize + page_size - 1) & ~(page_size - 1);

        void* newPtr = phys::pageAlloc(newAllocSize);
        if (newPtr == nullptr) {
            return nullptr;
        }

        // Copy old data - we don't know the old size, so copy up to newSize
        // This is safe as long as the caller knows what they're doing
        memcpy(newPtr, ptr, newSize);

        // Track the new large allocation
        trackAllocation(newPtr, newAllocSize);

        // Free the old small allocation
        kmallocLock->lock();
        mini_free(ptr);
        kmallocLock->unlock();

        return newPtr;
    }

    // Small allocation staying small - use mini_malloc
    kmallocLock->lock();
    void* newPtr = mini_malloc(newSize);
    if (newPtr != nullptr) [[likely]] {
        // Copy old data
        memcpy(newPtr, ptr, newSize);
        mini_free(ptr);
    }
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
