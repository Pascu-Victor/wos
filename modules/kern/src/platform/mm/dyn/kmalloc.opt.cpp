#include "kmalloc.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <mod/io/serial/serial.hpp>
#include <platform/acpi/apic/apic.hpp>
#include <platform/asm/cpu.hpp>
#include <platform/smt/smt.hpp>

#include "minimalist_malloc/mini_malloc.hpp"
#include "platform/dbg/dbg.hpp"
#include "platform/mm/paging.hpp"
#include "platform/mm/phys.hpp"
#include "platform/sys/spinlock.hpp"

namespace ker::mod::mm::dyn::kmalloc {

static constexpr int NUM_SLAB_CLASSES = 9;
static constexpr int MAGAZINE_CAPACITY = 32;

// Per-CPU magazine cache — Linux SLUB pattern.
// Fast path: pop/push from per-CPU magazine with IRQs disabled (no lock).
// Slow path: fall through to mini_malloc/mini_free which hold per-size-class slab_lock.
struct PerCpuAllocator {
    bool initialized{false};
    void* magazine[NUM_SLAB_CLASSES][MAGAZINE_CAPACITY]{};
    uint8_t mag_count[NUM_SLAB_CLASSES]{};

    PerCpuAllocator() = default;
};

static PerCpuAllocator* perCpuAllocators = nullptr;
static size_t numCpus = 0;
static std::atomic<bool> perCpuReady{false};  // Set after per-CPU structures are initialized

// Safe CPU ID getter - falls back to APIC ID during early boot
static inline uint64_t getCurrentCpuId() {
    if (perCpuReady.load(std::memory_order_acquire)) {
        return cpu::currentCpu();
    }
    // Early boot: use APIC ID
    uint32_t apicId = apic::getApicId();
    if (numCpus > 0) {
        uint64_t cpuIdx = smt::getCpuIndexFromApicId(apicId);
        return cpuIdx;
    }
    return 0;  // BSP during very early init
}

// Allocation size boundaries:
// 0x1 - 0x800: mini_malloc (slab allocator)
// 0x801 - 0xFFFF: medium allocations (regular pageAlloc with tracking)
// 0x10000+: large allocations (pageAllocHuge with tracking)
constexpr uint64_t SLAB_MAX_SIZE = 0x800;     // 2KB - maximum size mini_malloc handles
constexpr uint64_t MEDIUM_MAX_SIZE = 0xFFFF;  // ~64KB - maximum size for medium allocations

// Medium allocation header for sizes 0x801 - 0xFFFF
struct MediumAllocationHeader {
    MediumAllocationHeader* next;
    uint64_t size;   // Total allocation size including header
    uint64_t magic;  // For validation
    // Data follows immediately after this header
};

constexpr uint64_t MEDIUM_ALLOC_MAGIC = 0xCAFEBABE87654321ULL;

// Large allocation header for sizes >= 0x10000
struct LargeAllocationHeader {
    LargeAllocationHeader* next;
    uint64_t size;
    uint64_t magic;  // For validation
    // Data follows immediately after this header
};

constexpr uint64_t LARGE_ALLOC_MAGIC = 0xDEADBEEF12345678ULL;

static MediumAllocationHeader* mediumAllocList = nullptr;
static sys::Spinlock mediumAllocLock;

static LargeAllocationHeader* largeAllocList = nullptr;
static sys::Spinlock largeAllocLock;

void init() {
    // Initialize per-CPU allocators
    numCpus = smt::getEarlyCpuCount();

    mini_malloc_init();

    // Allocate per-CPU allocator structures using mini_malloc
    perCpuAllocators = static_cast<PerCpuAllocator*>(mini_malloc(sizeof(PerCpuAllocator) * numCpus));
    if (perCpuAllocators != nullptr) {
        for (size_t i = 0; i < numCpus; i++) {
            new (&perCpuAllocators[i]) PerCpuAllocator();
            perCpuAllocators[i].initialized = true;
        }
    }
}

void enablePerCpuAllocations() { perCpuReady.store(true, std::memory_order_release); }

void dumpTrackedAllocations() {
    mediumAllocLock.lock();
    uint64_t mediumTotalBytes = 0;
    uint64_t mediumCount = 0;
    ker::mod::io::serial::write("kmalloc: Medium allocations (0x801-0xFFFF):\n");

    for (MediumAllocationHeader* curr = mediumAllocList; curr != nullptr; curr = curr->next) {
        if (curr->magic == MEDIUM_ALLOC_MAGIC) {
            mediumCount++;
            mediumTotalBytes += curr->size;
            ker::mod::io::serial::write("  addr=0x");
            ker::mod::io::serial::writeHex((uint64_t)(curr + 1));
            ker::mod::io::serial::write(" size=");
            ker::mod::io::serial::writeHex(curr->size);
            ker::mod::io::serial::write("\n");
        }
    }
    mediumAllocLock.unlock();

    ker::mod::io::serial::write("  medium_total: ");
    ker::mod::io::serial::writeHex(mediumCount);
    ker::mod::io::serial::write(" entries, ");
    ker::mod::io::serial::writeHex(mediumTotalBytes);
    ker::mod::io::serial::write(" bytes\n");

    largeAllocLock.lock();
    uint64_t largeTotalBytes = 0;
    uint64_t largeCount = 0;
    ker::mod::io::serial::write("kmalloc: Large allocations (>=0x10000):\n");

    for (LargeAllocationHeader* curr = largeAllocList; curr != nullptr; curr = curr->next) {
        if (curr->magic == LARGE_ALLOC_MAGIC) {
            largeCount++;
            largeTotalBytes += curr->size;
            ker::mod::io::serial::write("  addr=0x");
            ker::mod::io::serial::writeHex((uint64_t)(curr + 1));  // Data starts after header
            ker::mod::io::serial::write(" size=");
            ker::mod::io::serial::writeHex(curr->size);
            ker::mod::io::serial::write("\n");
        }
    }

    ker::mod::io::serial::write("  large_total: ");
    ker::mod::io::serial::writeHex(largeCount);
    ker::mod::io::serial::write(" entries, ");
    ker::mod::io::serial::writeHex(largeTotalBytes);
    ker::mod::io::serial::write(" bytes\n");
    largeAllocLock.unlock();
}

void getTrackedAllocTotals(uint64_t& outCount, uint64_t& outBytes) {
    mediumAllocLock.lock();
    outCount = 0;
    outBytes = 0;

    for (MediumAllocationHeader* curr = mediumAllocList; curr != nullptr; curr = curr->next) {
        if (curr->magic == MEDIUM_ALLOC_MAGIC) {
            outCount++;
            outBytes += curr->size;
        }
    }
    mediumAllocLock.unlock();

    largeAllocLock.lock();
    for (LargeAllocationHeader* curr = largeAllocList; curr != nullptr; curr = curr->next) {
        if (curr->magic == LARGE_ALLOC_MAGIC) {
            outCount++;
            outBytes += curr->size;
        }
    }
    largeAllocLock.unlock();
}

static int size_to_slab_idx(uint64_t size) {
    if (size <= 0x10)  return 0;
    if (size <= 0x20)  return 1;
    if (size <= 0x40)  return 2;
    if (size <= 0x80)  return 3;
    if (size <= 0x100) return 4;
    if (size <= 0x200) return 5;
    if (size <= 0x300) return 6;
    if (size <= 0x400) return 7;
    if (size <= 0x800) return 8;
    return -1;
}

static int slab_size_to_idx(size_t slab_size) {
    switch (slab_size) {
        case 0x10:  return 0;
        case 0x20:  return 1;
        case 0x40:  return 2;
        case 0x80:  return 3;
        case 0x100: return 4;
        case 0x200: return 5;
        case 0x300: return 6;
        case 0x400: return 7;
        case 0x800: return 8;
        default:    return -1;
    }
}

void* malloc(uint64_t size) {
    if (size == 0) {
        return nullptr;
    }

    // Tier 1: Small allocations (0x1 - 0x800) - magazine fast path, slab slow path
    if (size <= SLAB_MAX_SIZE) {
        int idx = size_to_slab_idx(size);
        if (idx >= 0 && perCpuAllocators != nullptr && perCpuReady.load(std::memory_order_acquire)) {
            uint64_t flags;
            asm volatile("pushfq; popq %0; cli" : "=r"(flags));
            uint64_t cpuId = getCurrentCpuId();
            auto& cpu = perCpuAllocators[cpuId];
            if (cpu.initialized && cpu.mag_count[idx] > 0) {
                void* ptr = cpu.magazine[idx][--cpu.mag_count[idx]];
                if (flags & 0x200) asm volatile("sti");
                return ptr;
            }
            if (flags & 0x200) asm volatile("sti");
        }
        // Slow path: mini_malloc acquires per-size-class slab_lock internally
        return mini_malloc(size);
    }

    // Tier 2: Medium allocations (0x801 - 0xFFFF) - use regular pageAlloc with tracking
    if (size <= MEDIUM_MAX_SIZE) {
        uint64_t page_size = ker::mod::mm::paging::PAGE_SIZE;
        uint64_t total_size = size + sizeof(MediumAllocationHeader);
        uint64_t alloc_size = (total_size + page_size - 1) & ~(page_size - 1);

#ifdef DEBUG_KMALLOC
        ker::mod::io::serial::write("kmalloc: Medium allocation (");
        ker::mod::io::serial::writeHex(size);
        ker::mod::io::serial::write(" bytes), using pageAlloc (");
        ker::mod::io::serial::writeHex(alloc_size);
        ker::mod::io::serial::write(" bytes)\n");
#endif

        void* alloc_ptr = phys::pageAlloc(alloc_size);
        if (alloc_ptr == nullptr) {
#ifdef DEBUG_KMALLOC
            ker::mod::io::serial::write("kmalloc: pageAlloc failed for medium allocation\n");
#endif
            return nullptr;
        }

        // Set up header with tracking info
        auto* header = static_cast<MediumAllocationHeader*>(alloc_ptr);
        header->size = alloc_size;
        header->magic = MEDIUM_ALLOC_MAGIC;

        // Add to linked list
        mediumAllocLock.lock();
        header->next = mediumAllocList;
        mediumAllocList = header;
        mediumAllocLock.unlock();

        // Return pointer to data (after header)
        return static_cast<void*>(header + 1);
    }

    // Tier 3: Large allocations (>= 0x10000) - use pageAllocHuge with tracking
    uint64_t page_size = ker::mod::mm::paging::PAGE_SIZE;
    uint64_t total_size = size + sizeof(LargeAllocationHeader);
    uint64_t alloc_size = (total_size + page_size - 1) & ~(page_size - 1);

#ifdef DEBUG_KMALLOC
    ker::mod::io::serial::write("kmalloc: Large allocation (");
    ker::mod::io::serial::writeHex(size);
    ker::mod::io::serial::write(" bytes), using pageAllocHuge (");
    ker::mod::io::serial::writeHex(alloc_size);
    ker::mod::io::serial::write(" bytes)\n");
#endif

    // Allocate from huge page zone
    void* alloc_ptr = phys::pageAllocHuge(alloc_size);
    if (alloc_ptr == nullptr) {
        // Fallback to regular pageAlloc if huge zone is full
        alloc_ptr = phys::pageAlloc(alloc_size);
        if (alloc_ptr == nullptr) {
#ifdef DEBUG_KMALLOC
            ker::mod::io::serial::write("kmalloc: pageAlloc failed for large allocation\n");
#endif
            return nullptr;
        }
    }

    // Set up header with tracking info
    auto* header = static_cast<LargeAllocationHeader*>(alloc_ptr);
    header->size = alloc_size;
    header->magic = LARGE_ALLOC_MAGIC;

    // Add to linked list
    largeAllocLock.lock();
    header->next = largeAllocList;
    largeAllocList = header;
    largeAllocLock.unlock();

    // Return pointer to data (after header)
    return static_cast<void*>(header + 1);
}

// Helper to find and remove from medium alloc list
static auto findAndRemoveMediumAlloc(void* dataPtr, uint64_t& outSize) -> bool {
    // dataPtr points to the data, header is just before it
    auto* header = static_cast<MediumAllocationHeader*>(dataPtr) - 1;

    // Validate magic
    if (header->magic != MEDIUM_ALLOC_MAGIC) {
        return false;
    }

    mediumAllocLock.lock();

    // Remove from linked list
    MediumAllocationHeader** prev = &mediumAllocList;
    for (MediumAllocationHeader* curr = mediumAllocList; curr != nullptr; prev = &curr->next, curr = curr->next) {
        if (curr == header) {
            *prev = curr->next;
            outSize = curr->size;
            mediumAllocLock.unlock();
            return true;
        }
    }

    mediumAllocLock.unlock();
    return false;
}

// Helper to find and remove from large alloc list
static auto findAndRemoveLargeAlloc(void* dataPtr, uint64_t& outSize) -> bool {
    // dataPtr points to the data, header is just before it
    auto* header = static_cast<LargeAllocationHeader*>(dataPtr) - 1;

    // Validate magic
    if (header->magic != LARGE_ALLOC_MAGIC) {
        return false;
    }

    largeAllocLock.lock();

    // Remove from linked list
    LargeAllocationHeader** prev = &largeAllocList;
    for (LargeAllocationHeader* curr = largeAllocList; curr != nullptr; prev = &curr->next, curr = curr->next) {
        if (curr == header) {
            *prev = curr->next;
            outSize = curr->size;
            largeAllocLock.unlock();
            return true;
        }
    }

    largeAllocLock.unlock();
    return false;
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

    // Determine the type of the existing allocation by checking headers
    auto* potentialLargeHeader = static_cast<LargeAllocationHeader*>(ptr) - 1;
    auto* potentialMediumHeader = static_cast<MediumAllocationHeader*>(ptr) - 1;

    // Case 1: Current allocation is LARGE (>= 0x10000)
    if (potentialLargeHeader->magic == LARGE_ALLOC_MAGIC) {
        uint64_t oldSize = potentialLargeHeader->size - sizeof(LargeAllocationHeader);

        // Staying in large range?
        if (newSize > MEDIUM_MAX_SIZE) {
            uint64_t page_size = ker::mod::mm::paging::PAGE_SIZE;
            uint64_t newAllocSize = (newSize + sizeof(LargeAllocationHeader) + page_size - 1) & ~(page_size - 1);

            // If the new size fits in the current allocation, return same pointer
            if (newAllocSize == potentialLargeHeader->size) {
                return ptr;
            }

            // Need to reallocate - allocate new, copy, free old
            void* newAlloc = phys::pageAllocHuge(newAllocSize);
            if (newAlloc == nullptr) {
                newAlloc = phys::pageAlloc(newAllocSize);
                if (newAlloc == nullptr) {
                    return nullptr;
                }
            }

            auto* newHeader = static_cast<LargeAllocationHeader*>(newAlloc);
            newHeader->size = newAllocSize;
            newHeader->magic = LARGE_ALLOC_MAGIC;

            uint64_t copySize = (oldSize < newSize) ? oldSize : newSize;
            memcpy(newHeader + 1, ptr, copySize);

            // Update linked list
            largeAllocLock.lock();
            LargeAllocationHeader** prev = &largeAllocList;
            for (LargeAllocationHeader* curr = largeAllocList; curr != nullptr; prev = &curr->next, curr = curr->next) {
                if (curr == potentialLargeHeader) {
                    *prev = curr->next;
                    break;
                }
            }
            newHeader->next = largeAllocList;
            largeAllocList = newHeader;
            largeAllocLock.unlock();

            phys::pageFree(potentialLargeHeader);
            return static_cast<void*>(newHeader + 1);
        }

        // Transitioning from large to medium or small
        void* newPtr = malloc(newSize);
        if (newPtr != nullptr) {
            uint64_t copySize = (oldSize < newSize) ? oldSize : newSize;
            memcpy(newPtr, ptr, copySize);
            free(ptr);
        }
        return newPtr;
    }

    // Case 2: Current allocation is MEDIUM (0x801 - 0xFFFF)
    if (potentialMediumHeader->magic == MEDIUM_ALLOC_MAGIC) {
        uint64_t oldSize = potentialMediumHeader->size - sizeof(MediumAllocationHeader);

        // Staying in medium range?
        if (newSize > SLAB_MAX_SIZE && newSize <= MEDIUM_MAX_SIZE) {
            uint64_t page_size = ker::mod::mm::paging::PAGE_SIZE;
            uint64_t newAllocSize = (newSize + sizeof(MediumAllocationHeader) + page_size - 1) & ~(page_size - 1);

            // If the new size fits in the current allocation, return same pointer
            if (newAllocSize == potentialMediumHeader->size) {
                return ptr;
            }

            // Need to reallocate - allocate new, copy, free old
            void* newAlloc = phys::pageAlloc(newAllocSize);
            if (newAlloc == nullptr) {
                return nullptr;
            }

            auto* newHeader = static_cast<MediumAllocationHeader*>(newAlloc);
            newHeader->size = newAllocSize;
            newHeader->magic = MEDIUM_ALLOC_MAGIC;

            uint64_t copySize = (oldSize < newSize) ? oldSize : newSize;
            memcpy(newHeader + 1, ptr, copySize);

            // Update linked list
            mediumAllocLock.lock();
            MediumAllocationHeader** prev = &mediumAllocList;
            for (MediumAllocationHeader* curr = mediumAllocList; curr != nullptr; prev = &curr->next, curr = curr->next) {
                if (curr == potentialMediumHeader) {
                    *prev = curr->next;
                    break;
                }
            }
            newHeader->next = mediumAllocList;
            mediumAllocList = newHeader;
            mediumAllocLock.unlock();

            phys::pageFree(potentialMediumHeader);
            return static_cast<void*>(newHeader + 1);
        }

        // Transitioning from medium to large or small
        void* newPtr = malloc(newSize);
        if (newPtr != nullptr) {
            uint64_t copySize = (oldSize < newSize) ? oldSize : newSize;
            memcpy(newPtr, ptr, copySize);
            free(ptr);
        }
        return newPtr;
    }

    // Case 3: Current allocation is SMALL (<= 0x800) - from mini_malloc
    // We don't know the exact old size, so we'll allocate new and copy what we can

    // Staying in small range?
    if (newSize <= SLAB_MAX_SIZE) {
        void* newPtr = malloc(newSize);
        if (newPtr != nullptr && newPtr != ptr) {
            memcpy(newPtr, ptr, newSize);
            free(ptr);
        }
        return newPtr;
    }

    // Transitioning from small to medium or large
    void* newPtr = malloc(newSize);
    if (newPtr != nullptr) {
        // We don't know the old size, but it's at most SLAB_MAX_SIZE
        // Copy up to newSize (safe because old allocation is at least as large as requested)
        uint64_t copySize = (SLAB_MAX_SIZE < newSize) ? SLAB_MAX_SIZE : newSize;
        memcpy(newPtr, ptr, copySize);
        free(ptr);
    }
    return newPtr;
}

void* calloc(int sz) {
    if (sz <= 0) {
        return nullptr;
    }

    void* ptr = malloc(static_cast<uint64_t>(sz));
    if (ptr) [[likely]] {
        memset(ptr, 0, sz);
    }
    return ptr;
}

auto calloc(size_t nmemb, size_t size) -> void* {
    if (nmemb == 0 || size == 0) {
        return nullptr;
    }

    // Check for overflow
    if (nmemb > SIZE_MAX / size) {
        return nullptr;
    }

    size_t total = nmemb * size;
    void* ptr = malloc(total);
    if (ptr) [[likely]] {
        memset(ptr, 0, total);
    }
    return ptr;
}

void free(void* ptr) {
    if (ptr == nullptr) {
        return;
    }

    // Validate ptr is in a reasonable range
    uintptr_t ptrVal = reinterpret_cast<uintptr_t>(ptr);
    bool inHHDM = (ptrVal >= 0xffff800000000000ULL && ptrVal < 0xffff900000000000ULL);
    bool inKernelStatic = (ptrVal >= 0xffffffff80000000ULL && ptrVal < 0xffffffffc0000000ULL);
    if (!inHHDM && !inKernelStatic) {
        ker::mod::dbg::log("kmalloc::free: caller=%p freeing ptr=%p outside valid kernel range", __builtin_return_address(0), ptr);
        return;
    }

    // Check if this is a large allocation (>= 0x10000) by examining the header
    auto* potentialLargeHeader = static_cast<LargeAllocationHeader*>(ptr) - 1;
    if (potentialLargeHeader->magic == LARGE_ALLOC_MAGIC) {
        uint64_t size = 0;
        if (findAndRemoveLargeAlloc(ptr, size)) {
#ifdef DEBUG_KMALLOC
            ker::mod::io::serial::write("kmalloc: Freeing large allocation at ");
            ker::mod::io::serial::writeHex((uint64_t)ptr);
            ker::mod::io::serial::write(" (");
            ker::mod::io::serial::writeHex(size);
            ker::mod::io::serial::write(" bytes)\n");
#endif
            phys::pageFree(potentialLargeHeader);  // Free from header, not data ptr
            return;
        }
    }

    // Check if this is a medium allocation (0x801 - 0xFFFF) by examining the header
    auto* potentialMediumHeader = static_cast<MediumAllocationHeader*>(ptr) - 1;
    if (potentialMediumHeader->magic == MEDIUM_ALLOC_MAGIC) {
        uint64_t size = 0;
        if (findAndRemoveMediumAlloc(ptr, size)) {
#ifdef DEBUG_KMALLOC
            ker::mod::io::serial::write("kmalloc: Freeing medium allocation at ");
            ker::mod::io::serial::writeHex((uint64_t)ptr);
            ker::mod::io::serial::write(" (");
            ker::mod::io::serial::writeHex(size);
            ker::mod::io::serial::write(" bytes)\n");
#endif
            phys::pageFree(potentialMediumHeader);  // Free from header, not data ptr
            return;
        }
    }

    // Otherwise, it's a small allocation (<= 0x800) - magazine fast path, slab slow path
    size_t slab_sz = mini_get_slab_size(ptr);
    int idx = (slab_sz > 0) ? slab_size_to_idx(slab_sz) : -1;

    if (idx >= 0 && perCpuAllocators != nullptr && perCpuReady.load(std::memory_order_acquire)) {
        uint64_t flags;
        asm volatile("pushfq; popq %0; cli" : "=r"(flags));
        uint64_t cpuId = getCurrentCpuId();
        auto& cpu = perCpuAllocators[cpuId];
        if (cpu.initialized) {
            if (cpu.mag_count[idx] < MAGAZINE_CAPACITY) {
                cpu.magazine[idx][cpu.mag_count[idx]++] = ptr;
                if (flags & 0x200) asm volatile("sti");
                return;
            }
            // Magazine full: flush all entries to slab, push new ptr, then drain
            uint8_t cnt = cpu.mag_count[idx];
            void* batch[MAGAZINE_CAPACITY];
            for (uint8_t i = 0; i < cnt; i++) {
                batch[i] = cpu.magazine[idx][i];
            }
            cpu.mag_count[idx] = 0;
            cpu.magazine[idx][cpu.mag_count[idx]++] = ptr;
            if (flags & 0x200) asm volatile("sti");
            for (uint8_t i = 0; i < cnt; i++) {
                mini_free(batch[i]);
            }
            return;
        }
        if (flags & 0x200) asm volatile("sti");
    }

    // Slow path: mini_free acquires per-size-class slab_lock internally
    mini_free(ptr);
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
