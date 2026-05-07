#include "kmalloc.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <mod/io/serial/serial.hpp>
#include <new>
#include <platform/acpi/apic/apic.hpp>
#include <platform/asm/cpu.hpp>
#include <platform/smt/smt.hpp>

#include "minimalist_malloc/mini_malloc.hpp"
#include "platform/dbg/dbg.hpp"
#include "platform/mm/paging.hpp"
#include "platform/mm/phys.hpp"
#include "platform/sys/spinlock.hpp"

#ifdef WOS_KASAN
#include <sanitizer/kasan.hpp>
#endif

namespace ker::mod::mm::dyn::kmalloc {

static constexpr int NUM_SLAB_CLASSES = 9;
static constexpr int MAGAZINE_CAPACITY = 32;

// Per-CPU magazine cache - Linux SLUB pattern.
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
        uint64_t cpuIdx = smt::get_cpu_index_from_apic_id(apicId);
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
struct alignas(16) MediumAllocationHeader {
    MediumAllocationHeader* next;
    uint64_t size;   // Total allocation size including header
    uint64_t magic;  // For validation
#if defined(WOS_KASAN) || defined(WOS_KUBSAN)
    uint32_t debug_idx;  // Index into s_alloc_debug (0 = none)
    uint32_t _pad;
#else
    uint64_t _pad;  // Pad to 32 bytes so (header + 1) is 16-byte aligned
#endif
    // Data follows immediately after this header
};

constexpr uint64_t MEDIUM_ALLOC_MAGIC = 0xCAFEBABE87654321ULL;

// Large allocation header for sizes >= 0x10000
struct alignas(16) LargeAllocationHeader {
    LargeAllocationHeader* next;
    uint64_t size;
    uint64_t magic;  // For validation
#if defined(WOS_KASAN) || defined(WOS_KUBSAN)
    uint32_t debug_idx;  // Index into s_alloc_debug (0 = none)
    uint32_t _pad;
#else
    uint64_t _pad;  // Pad to 32 bytes so (header + 1) is 16-byte aligned
#endif
    // Data follows immediately after this header
};

constexpr uint64_t LARGE_ALLOC_MAGIC = 0xDEADBEEF12345678ULL;

static_assert(sizeof(MediumAllocationHeader) == sizeof(LargeAllocationHeader), "allocation header sizes must match");
static_assert(sizeof(MediumAllocationHeader) == sizeof(void*) + (3 * sizeof(uint64_t)),
              "MediumAllocationHeader must be 32 bytes (no accidental padding)");

static MediumAllocationHeader* mediumAllocList = nullptr;
static sys::Spinlock mediumAllocLock;

static LargeAllocationHeader* largeAllocList = nullptr;
static sys::Spinlock largeAllocLock;

#if defined(WOS_KASAN) || defined(WOS_KUBSAN)
// Per-allocation debug side-table.  Stored in .bss so it is always available during OOM.
// Index 0 is a sentinel meaning "no info".  The counter never wraps back — once full,
// new allocations silently get index 0.  64 KB total; zero runtime cost in release builds.
struct AllocDebugInfo {
    uintptr_t caller;  // return address captured at the kmalloc call site
    const char* tag;   // compile-time string (type name or "::new"), may be null
};
static constexpr uint32_t ALLOC_DEBUG_NONE = 0;
static constexpr size_t ALLOC_DEBUG_MAX = 4096;
__attribute__((section(".bss"))) static std::array<AllocDebugInfo, ALLOC_DEBUG_MAX> s_alloc_debug{};
static std::atomic<uint32_t> s_alloc_debug_next{1};

static auto register_alloc_debug(uintptr_t caller, const char* tag) -> uint32_t {
    uint32_t slot = s_alloc_debug_next.fetch_add(1, std::memory_order_relaxed);
    if (slot >= ALLOC_DEBUG_MAX) {
        return ALLOC_DEBUG_NONE;
    }
    s_alloc_debug[slot] = {.caller = caller, .tag = tag};
    return slot;
}
#endif

void init() {
    // Initialize per-CPU allocators
    numCpus = smt::get_early_cpu_count();

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
    uint64_t mediumLockFlags = mediumAllocLock.lock_irqsave();
    uint64_t mediumTotalBytes = 0;
    uint64_t mediumCount = 0;
    ker::mod::io::serial::write("kmalloc: Medium allocations (0x801-0xFFFF):\n");

    for (MediumAllocationHeader* curr = mediumAllocList; curr != nullptr; curr = curr->next) {
        if (curr->magic == MEDIUM_ALLOC_MAGIC) {
            mediumCount++;
            mediumTotalBytes += curr->size;
            ker::mod::io::serial::write("  addr=0x");
            ker::mod::io::serial::writeHex((uint64_t)(curr + 1));
            ker::mod::io::serial::write(" size=0x");
            ker::mod::io::serial::writeHex(curr->size);
#if defined(WOS_KASAN) || defined(WOS_KUBSAN)
            if (curr->debug_idx != ALLOC_DEBUG_NONE && curr->debug_idx < ALLOC_DEBUG_MAX) {
                const auto& d = s_alloc_debug[curr->debug_idx];
                if (d.caller != 0) {
                    ker::mod::io::serial::write(" caller=0x");
                    ker::mod::io::serial::writeHex(d.caller);
                }
                if (d.tag != nullptr) {
                    ker::mod::io::serial::write(" tag=");
                    ker::mod::io::serial::write(d.tag);
                }
            }
#endif
            ker::mod::io::serial::write("\n");
        }
    }
    mediumAllocLock.unlock_irqrestore(mediumLockFlags);

    ker::mod::io::serial::write("  medium_total: 0x");
    ker::mod::io::serial::writeHex(mediumCount);
    ker::mod::io::serial::write(" entries, 0x");
    ker::mod::io::serial::writeHex(mediumTotalBytes);
    ker::mod::io::serial::write(" bytes\n");

    uint64_t largeLockFlags = largeAllocLock.lock_irqsave();
    uint64_t largeTotalBytes = 0;
    uint64_t largeCount = 0;
    ker::mod::io::serial::write("kmalloc: Large allocations (>=0x10000):\n");

    for (LargeAllocationHeader* curr = largeAllocList; curr != nullptr; curr = curr->next) {
        if (curr->magic == LARGE_ALLOC_MAGIC) {
            largeCount++;
            largeTotalBytes += curr->size;
            ker::mod::io::serial::write("  addr=0x");
            ker::mod::io::serial::writeHex((uint64_t)(curr + 1));  // Data starts after header
            ker::mod::io::serial::write(" size=0x");
            ker::mod::io::serial::writeHex(curr->size);
#if defined(WOS_KASAN) || defined(WOS_KUBSAN)
            if (curr->debug_idx != ALLOC_DEBUG_NONE && curr->debug_idx < ALLOC_DEBUG_MAX) {
                const auto& d = s_alloc_debug[curr->debug_idx];
                if (d.caller != 0) {
                    ker::mod::io::serial::write(" caller=0x");
                    ker::mod::io::serial::writeHex(d.caller);
                }
                if (d.tag != nullptr) {
                    ker::mod::io::serial::write(" tag=");
                    ker::mod::io::serial::write(d.tag);
                }
            }
#endif
            ker::mod::io::serial::write("\n");
        }
    }

    ker::mod::io::serial::write("  large_total: 0x");
    ker::mod::io::serial::writeHex(largeCount);
    ker::mod::io::serial::write(" entries, 0x");
    ker::mod::io::serial::writeHex(largeTotalBytes);
    ker::mod::io::serial::write(" bytes\n");
    largeAllocLock.unlock_irqrestore(largeLockFlags);

#if defined(WOS_KASAN) || defined(WOS_KUBSAN)
    ker::mod::io::serial::write("kmalloc: Slab live allocations with debug info (KASAN/KUBSAN):\n");
    mini_iter_live_debug_slots(nullptr, [](void* /*ud*/, const void* ptr, size_t sz, uint32_t dbg_idx) -> void {
        if (dbg_idx == ALLOC_DEBUG_NONE || dbg_idx >= ALLOC_DEBUG_MAX) {
            return;
        }
        const auto& d = s_alloc_debug[dbg_idx];
        if (d.caller == 0) {
            return;
        }
        ker::mod::io::serial::write("  addr=0x");
        ker::mod::io::serial::writeHex((uint64_t)ptr);
        ker::mod::io::serial::write(" sz=0x");
        ker::mod::io::serial::writeHex(sz);
        ker::mod::io::serial::write(" caller=0x");
        ker::mod::io::serial::writeHex(d.caller);
        if (d.tag != nullptr) {
            ker::mod::io::serial::write(" tag=");
            ker::mod::io::serial::write(d.tag);
        }
        ker::mod::io::serial::write("\n");
    });
#endif
}

void getTrackedAllocTotals(uint64_t& outCount, uint64_t& outBytes) {
    outCount = 0;
    outBytes = 0;

    uint64_t mediumLockFlags = mediumAllocLock.lock_irqsave();
    for (MediumAllocationHeader* curr = mediumAllocList; curr != nullptr; curr = curr->next) {
        if (curr->magic == MEDIUM_ALLOC_MAGIC) {
            outCount++;
            outBytes += curr->size;
        }
    }
    mediumAllocLock.unlock_irqrestore(mediumLockFlags);

    uint64_t largeLockFlags = largeAllocLock.lock_irqsave();
    for (LargeAllocationHeader* curr = largeAllocList; curr != nullptr; curr = curr->next) {
        if (curr->magic == LARGE_ALLOC_MAGIC) {
            outCount++;
            outBytes += curr->size;
        }
    }
    largeAllocLock.unlock_irqrestore(largeLockFlags);
}

static auto size_to_slab_idx(uint64_t size) -> int {
    if (size <= 0x10) {
        return 0;
    }
    if (size <= 0x20) {
        return 1;
    }
    if (size <= 0x40) {
        return 2;
    }
    if (size <= 0x80) {
        return 3;
    }
    if (size <= 0x100) {
        return 4;
    }
    if (size <= 0x200) {
        return 5;
    }
    if (size <= 0x300) {
        return 6;
    }
    if (size <= 0x400) {
        return 7;
    }
    if (size <= 0x800) {
        return 8;
    }
    return -1;
}

static auto slab_size_to_idx(size_t slab_size) -> int {
    switch (slab_size) {
        case 0x10:
            return 0;
        case 0x20:
            return 1;
        case 0x40:
            return 2;
        case 0x80:
            return 3;
        case 0x100:
            return 4;
        case 0x200:
            return 5;
        case 0x300:
            return 6;
        case 0x400:
            return 7;
        case 0x800:
            return 8;
        default:
            return -1;
    }
}

static auto malloc_impl(uint64_t size, uintptr_t caller, const char* tag) -> void* {
    if (size == 0) {
        return nullptr;
    }

    // Tier 1: Small allocations (0x1 - 0x800) - magazine fast path, slab slow path
    if (size <= SLAB_MAX_SIZE) {
        int idx = size_to_slab_idx(size);
        void* ptr = nullptr;
        if (idx >= 0 && perCpuAllocators != nullptr && perCpuReady.load(std::memory_order_acquire)) {
            uint64_t flags;
            asm volatile("pushfq; popq %0; cli" : "=r"(flags));
            uint64_t cpuId = getCurrentCpuId();
            auto& cpu = perCpuAllocators[cpuId];
            if (cpu.initialized && cpu.mag_count[idx] > 0) {
                ptr = cpu.magazine[idx][--cpu.mag_count[idx]];
                if (flags & 0x200) asm volatile("sti");
            } else {
                if (flags & 0x200) asm volatile("sti");
            }
        }
        // Slow path: mini_malloc acquires per-size-class slab_lock internally
        if (ptr == nullptr) {
            ptr = mini_malloc(size);
        }
#ifdef WOS_KASAN
        if (ptr != nullptr) {
            kasan::unpoison_range(ptr, size);
        }
#endif
#if defined(WOS_KASAN) || defined(WOS_KUBSAN)
        if (ptr != nullptr) {
            // Store debug_idx in the lower 32 bits of _align_pad (the 8 bytes before user data).
            *reinterpret_cast<uint32_t*>(static_cast<uint8_t*>(ptr) - sizeof(uintptr_t)) = register_alloc_debug(caller, tag);
        }
#endif
        return ptr;
    }

    // Tier 2: Medium allocations (0x801 - 0xFFFF) - use regular pageAlloc with tracking
    if (size <= MEDIUM_MAX_SIZE) {
        uint64_t page_size = ker::mod::mm::paging::PAGE_SIZE;
        uint64_t total_size = size + sizeof(MediumAllocationHeader);
        uint64_t alloc_size = (total_size + page_size - 1) & ~(page_size - 1);

#ifdef DEBUG_KMALLOC
        ker::mod::io::serial::write("kmalloc: Medium allocation (0x");
        ker::mod::io::serial::writeHex(size);
        ker::mod::io::serial::write(" bytes), using pageAlloc (0x");
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

        // Add to linked list and set magic under the same lock.
        // Magic must be set while holding the lock so that any concurrent free() that
        // sees magic set is guaranteed to find the entry in the list (no TOCTOU window).
        // lock_irqsave: kmalloc::free runs from the timer ISR via gc_expired_tasks, so
        // a non-IRQ caller holding mediumAllocLock would deadlock against the ISR.
        uint64_t lockFlags = mediumAllocLock.lock_irqsave();
        header->magic = MEDIUM_ALLOC_MAGIC;
        header->next = mediumAllocList;
        mediumAllocList = header;
        mediumAllocLock.unlock_irqrestore(lockFlags);

        // Return pointer to data (after header)
        void* data = static_cast<void*>(header + 1);
#ifdef WOS_KASAN
        // Mark the header as poisoned (internal), user data as accessible.
        kasan::poison_range(static_cast<void*>(header), sizeof(MediumAllocationHeader), kasan::SHADOW_HEAP_LREDZONE);
        kasan::unpoison_range(data, size);
#endif
#if defined(WOS_KASAN) || defined(WOS_KUBSAN)
        header->debug_idx = register_alloc_debug(caller, tag);
#endif
        return data;
    }

    // Tier 3: Large allocations (>= 0x10000) - use pageAllocHuge with tracking
    uint64_t page_size = ker::mod::mm::paging::PAGE_SIZE;
    uint64_t total_size = size + sizeof(LargeAllocationHeader);
    uint64_t alloc_size = (total_size + page_size - 1) & ~(page_size - 1);

#ifdef DEBUG_KMALLOC
    ker::mod::io::serial::write("kmalloc: Large allocation (0x");
    ker::mod::io::serial::writeHex(size);
    ker::mod::io::serial::write(" bytes), using pageAllocHuge (0x");
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

    // Add to linked list and set magic under the same lock.
    // Magic must be set while holding the lock so that any concurrent free() that
    // sees magic set is guaranteed to find the entry in the list (no TOCTOU window).
    // lock_irqsave: see matching comment on the medium-tier insert above.
    uint64_t lockFlags = largeAllocLock.lock_irqsave();
    header->magic = LARGE_ALLOC_MAGIC;
    header->next = largeAllocList;
    largeAllocList = header;
    largeAllocLock.unlock_irqrestore(lockFlags);

    // Return pointer to data (after header)
    void* data = static_cast<void*>(header + 1);
#ifdef WOS_KASAN
    kasan::poison_range(static_cast<void*>(header), sizeof(LargeAllocationHeader), kasan::SHADOW_HEAP_LREDZONE);
    kasan::unpoison_range(data, size);
#endif
#if defined(WOS_KASAN) || defined(WOS_KUBSAN)
    header->debug_idx = register_alloc_debug(caller, tag);
#endif
    return data;
}

auto malloc(uint64_t size) -> void* {
#if defined(WOS_KASAN) || defined(WOS_KUBSAN)
    return malloc_impl(size, (uintptr_t)__builtin_return_address(0), nullptr);
#else
    return malloc_impl(size, 0, nullptr);
#endif
}

auto malloc_tagged(uint64_t size, uintptr_t caller, const char* tag) -> void* { return malloc_impl(size, caller, tag); }

// Tri-state result for the medium/large free helpers.
//   NotTracked: header magic does not match this tier; caller should try the next tier.
//   Freed:      header was found in the tracker list, spliced out, and magic cleared.
//   DoubleFree: header magic matched but the entry is not in the tracker list — genuine
//               double-free or list corruption. Caller should panic.
enum class TrackedFreeResult : uint8_t { NotTracked, Freed, DoubleFree };

// Find, splice, and clear-magic for a medium-tier allocation, all under one
// acquisition of mediumAllocLock. The magic read, list walk, and magic clear
// must be atomic w.r.t. other free()/malloc() callers — otherwise a concurrent
// free of the same pointer can race past the magic check after the entry was
// removed but before magic was zeroed, and panic spuriously.
static auto tryFreeMediumAlloc(void* dataPtr, uint64_t& outSize) -> TrackedFreeResult {
    auto* header = static_cast<MediumAllocationHeader*>(dataPtr) - 1;

    uint64_t flags = mediumAllocLock.lock_irqsave();
    if (header->magic != MEDIUM_ALLOC_MAGIC) {
        mediumAllocLock.unlock_irqrestore(flags);
        return TrackedFreeResult::NotTracked;
    }

    MediumAllocationHeader** prev = &mediumAllocList;
    for (MediumAllocationHeader* curr = mediumAllocList; curr != nullptr; prev = &curr->next, curr = curr->next) {
        if (curr == header) {
            *prev = curr->next;
            outSize = curr->size;
            // Clear magic while still holding the lock so a concurrent free()
            // observes either (magic set AND in list) or (magic clear). The
            // (magic set AND not in list) state is no longer reachable except
            // via real corruption / double-free.
            header->magic = 0;
            mediumAllocLock.unlock_irqrestore(flags);
            return TrackedFreeResult::Freed;
        }
    }

    // Still holding the lock — dump diagnostic info before unlocking.
    // Keeping the lock ensures the list is stable during the dump.

    // Helper lambda to print debug_idx info for a node.
    auto printDebugInfo = [](const MediumAllocationHeader* node) {
#if defined(WOS_KASAN) || defined(WOS_KUBSAN)
        if (node->debug_idx != ALLOC_DEBUG_NONE && node->debug_idx < ALLOC_DEBUG_MAX) {
            const auto& d = s_alloc_debug[node->debug_idx];
            if (d.caller != 0) {
                ker::mod::io::serial::write(" caller=0x");
                ker::mod::io::serial::writeHex(d.caller);
            }
            if (d.tag != nullptr) {
                ker::mod::io::serial::write(" tag=");
                ker::mod::io::serial::write(d.tag);
            }
        }
#else
        (void)node;
#endif
    };

    ker::mod::io::serial::write("kmalloc: DoubleFree chain dump (target=0x");
    ker::mod::io::serial::writeHex(reinterpret_cast<uint64_t>(header));
    ker::mod::io::serial::write(" size=0x");
    ker::mod::io::serial::writeHex(header->size);
    printDebugInfo(header);
    ker::mod::io::serial::write("):\n");

    uint32_t n = 0;
    MediumAllocationHeader* lastNode = nullptr;
    bool foundCorrupt = false;
    for (MediumAllocationHeader* c = mediumAllocList; c != nullptr && n < 8192; c = c->next, ++n) {
        if (c->magic != MEDIUM_ALLOC_MAGIC) {
            ker::mod::io::serial::write("  BAD node=0x");
            ker::mod::io::serial::writeHex(reinterpret_cast<uint64_t>(c));
            ker::mod::io::serial::write(" size=0x");
            ker::mod::io::serial::writeHex(c->size);
            ker::mod::io::serial::write(" magic=0x");
            ker::mod::io::serial::writeHex(c->magic);
            ker::mod::io::serial::write(" (prev had this as next)\n");
            foundCorrupt = true;
            break;
        }
        MediumAllocationHeader* nxt = c->next;
        if (nxt != nullptr && nxt->magic != MEDIUM_ALLOC_MAGIC) {
            ker::mod::io::serial::write("  CORRUPT node=0x");
            ker::mod::io::serial::writeHex(reinterpret_cast<uint64_t>(c));
            ker::mod::io::serial::write(" size=0x");
            ker::mod::io::serial::writeHex(c->size);
            printDebugInfo(c);
            ker::mod::io::serial::write(" ->next=0x");
            ker::mod::io::serial::writeHex(reinterpret_cast<uint64_t>(nxt));
            ker::mod::io::serial::write(" (next_magic=0x");
            ker::mod::io::serial::writeHex(nxt->magic);
            ker::mod::io::serial::write(")\n");
            const auto* data = reinterpret_cast<const uint64_t*>(c + 1);
            ker::mod::io::serial::write("  CORRUPT node data[0..3]: 0x");
            ker::mod::io::serial::writeHex(data[0]);
            ker::mod::io::serial::write(" 0x");
            ker::mod::io::serial::writeHex(data[1]);
            ker::mod::io::serial::write(" 0x");
            ker::mod::io::serial::writeHex(data[2]);
            ker::mod::io::serial::write(" 0x");
            ker::mod::io::serial::writeHex(data[3]);
            ker::mod::io::serial::write("\n");
            foundCorrupt = true;
            break;
        }
        lastNode = c;
    }
    if (!foundCorrupt && lastNode != nullptr) {
        // Chain ended without finding target — the predecessor of target had its
        // ->next overwritten with null (or some other valid node, skipping target).
        ker::mod::io::serial::write("  TRUNCATED: last valid node=0x");
        ker::mod::io::serial::writeHex(reinterpret_cast<uint64_t>(lastNode));
        ker::mod::io::serial::write(" size=0x");
        ker::mod::io::serial::writeHex(lastNode->size);
        printDebugInfo(lastNode);
        ker::mod::io::serial::write(" ->next=0x0\n");
        const auto* data = reinterpret_cast<const uint64_t*>(lastNode + 1);
        ker::mod::io::serial::write("  TRUNCATED node data[0..7]: 0x");
        ker::mod::io::serial::writeHex(data[0]);
        ker::mod::io::serial::write(" 0x");
        ker::mod::io::serial::writeHex(data[1]);
        ker::mod::io::serial::write(" 0x");
        ker::mod::io::serial::writeHex(data[2]);
        ker::mod::io::serial::write(" 0x");
        ker::mod::io::serial::writeHex(data[3]);
        ker::mod::io::serial::write(" 0x");
        ker::mod::io::serial::writeHex(data[4]);
        ker::mod::io::serial::write(" 0x");
        ker::mod::io::serial::writeHex(data[5]);
        ker::mod::io::serial::write(" 0x");
        ker::mod::io::serial::writeHex(data[6]);
        ker::mod::io::serial::write(" 0x");
        ker::mod::io::serial::writeHex(data[7]);
        ker::mod::io::serial::write("\n");
    }
    ker::mod::io::serial::write("kmalloc: DoubleFree chain dump done (");
    ker::mod::io::serial::writeHex(n);
    ker::mod::io::serial::write(" nodes walked)\n");

    mediumAllocLock.unlock_irqrestore(flags);

    return TrackedFreeResult::DoubleFree;
}

static auto tryFreeLargeAlloc(void* dataPtr, uint64_t& outSize) -> TrackedFreeResult {
    auto* header = static_cast<LargeAllocationHeader*>(dataPtr) - 1;

    uint64_t flags = largeAllocLock.lock_irqsave();
    if (header->magic != LARGE_ALLOC_MAGIC) {
        largeAllocLock.unlock_irqrestore(flags);
        return TrackedFreeResult::NotTracked;
    }

    LargeAllocationHeader** prev = &largeAllocList;
    for (LargeAllocationHeader* curr = largeAllocList; curr != nullptr; prev = &curr->next, curr = curr->next) {
        if (curr == header) {
            *prev = curr->next;
            outSize = curr->size;
            header->magic = 0;
            largeAllocLock.unlock_irqrestore(flags);
            return TrackedFreeResult::Freed;
        }
    }

    largeAllocLock.unlock_irqrestore(flags);
    return TrackedFreeResult::DoubleFree;
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

            uint64_t copySize = (oldSize < newSize) ? oldSize : newSize;
            memcpy(newHeader + 1, ptr, copySize);

            // Update linked list; set magic under the lock alongside insertion
            // so there is no window where magic is set but the entry is not yet in the list.
            // Clear the old header's magic inside the same critical section so a
            // concurrent free observes either (old in list) or (old fully gone).
            uint64_t lockFlags = largeAllocLock.lock_irqsave();
            newHeader->magic = LARGE_ALLOC_MAGIC;
            LargeAllocationHeader** prev = &largeAllocList;
            for (LargeAllocationHeader* curr = largeAllocList; curr != nullptr; prev = &curr->next, curr = curr->next) {
                if (curr == potentialLargeHeader) {
                    *prev = curr->next;
                    break;
                }
            }
            newHeader->next = largeAllocList;
            largeAllocList = newHeader;
            potentialLargeHeader->magic = 0;
            largeAllocLock.unlock_irqrestore(lockFlags);
#if defined(WOS_KASAN) || defined(WOS_KUBSAN)
            newHeader->debug_idx = ALLOC_DEBUG_NONE;
#endif
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

            uint64_t copySize = (oldSize < newSize) ? oldSize : newSize;
            memcpy(newHeader + 1, ptr, copySize);

            // Update linked list; set magic under the lock alongside insertion
            // so there is no window where magic is set but the entry is not yet in the list.
            // Clear the old header's magic inside the same critical section so a
            // concurrent free observes either (old in list) or (old fully gone).
            uint64_t lockFlags = mediumAllocLock.lock_irqsave();
            newHeader->magic = MEDIUM_ALLOC_MAGIC;
            MediumAllocationHeader** prev = &mediumAllocList;
            for (MediumAllocationHeader* curr = mediumAllocList; curr != nullptr; prev = &curr->next, curr = curr->next) {
                if (curr == potentialMediumHeader) {
                    *prev = curr->next;
                    break;
                }
            }
            newHeader->next = mediumAllocList;
            mediumAllocList = newHeader;
            potentialMediumHeader->magic = 0;
            mediumAllocLock.unlock_irqrestore(lockFlags);
#if defined(WOS_KASAN) || defined(WOS_KUBSAN)
            newHeader->debug_idx = ALLOC_DEBUG_NONE;
#endif
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
    auto ptr_val = reinterpret_cast<uintptr_t>(ptr);
    const bool IN_HHDM = (ptr_val >= 0xffff800000000000ULL && ptr_val < 0xffff900000000000ULL);
    const bool IN_KERNEL_STATIC = (ptr_val >= 0xffffffff80000000ULL && ptr_val < 0xffffffffc0000000ULL);
    if (!IN_HHDM && !IN_KERNEL_STATIC) {
        ker::mod::dbg::log("kmalloc::free: caller=%p freeing ptr=%p outside valid kernel range", __builtin_return_address(0), ptr);
        return;
    }

    // Try the large-allocation tier (>= 0x10000). The helper takes the lock,
    // re-checks magic, walks the list, and clears magic atomically so no other
    // free()/malloc() caller can observe (magic set AND not in list) during a
    // concurrent free of the same pointer.
    auto* potential_large_header = static_cast<LargeAllocationHeader*>(ptr) - 1;
    {
        uint64_t size = 0;
        TrackedFreeResult r = tryFreeLargeAlloc(ptr, size);
        if (r == TrackedFreeResult::Freed) {
#ifdef DEBUG_KMALLOC
            ker::mod::io::serial::write("kmalloc: Freeing large allocation at 0x");
            ker::mod::io::serial::writeHex((uint64_t)ptr);
            ker::mod::io::serial::write(" (0x");
            ker::mod::io::serial::writeHex(size);
            ker::mod::io::serial::write(" bytes)\n");
#endif
#ifdef WOS_KASAN
            // Poison entire allocation (header + user data) as freed.
            kasan::poison_range(static_cast<void*>(potential_large_header), size, kasan::SHADOW_HEAP_FREED);
#endif
            phys::pageFree(potential_large_header);  // Free from header, not data ptr
            return;
        }
        if (r == TrackedFreeResult::DoubleFree) {
            ker::mod::dbg::panic_handler("kmalloc: Double-free or corrupted large allocation detected");
        }
    }

    // Try the medium-allocation tier (0x801 - 0xFFFF).
    auto* potentialMediumHeader = static_cast<MediumAllocationHeader*>(ptr) - 1;
    {
        uint64_t size = 0;
        TrackedFreeResult r = tryFreeMediumAlloc(ptr, size);
        if (r == TrackedFreeResult::Freed) {
#ifdef DEBUG_KMALLOC
            ker::mod::io::serial::write("kmalloc: Freeing medium allocation at 0x");
            ker::mod::io::serial::writeHex((uint64_t)ptr);
            ker::mod::io::serial::write(" (0x");
            ker::mod::io::serial::writeHex(size);
            ker::mod::io::serial::write(" bytes)\n");
#endif
#ifdef WOS_KASAN
            kasan::poison_range(static_cast<void*>(potentialMediumHeader), size, kasan::SHADOW_HEAP_FREED);
#endif
            phys::pageFree(potentialMediumHeader);  // Free from header, not data ptr
            return;
        }
        if (r == TrackedFreeResult::DoubleFree) {
            ker::mod::dbg::panic_handler("kmalloc: Double-free or corrupted medium allocation detected");
        }
    }

    // Otherwise, it's a small allocation (<= 0x800) - magazine fast path, slab slow path
    size_t slab_sz = mini_get_slab_size(ptr);
    int idx = (slab_sz > 0) ? slab_size_to_idx(slab_sz) : -1;

#ifdef WOS_KASAN
    // Poison the slab chunk as freed before returning it to the magazine/slab.
    if (slab_sz > 0) {
        kasan::poison_range(ptr, slab_sz, kasan::SHADOW_HEAP_FREED);
    }
#endif

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

auto operator new(size_t sz) -> void* {
#if defined(WOS_KASAN) || defined(WOS_KUBSAN)
    return ker::mod::mm::dyn::kmalloc::malloc_tagged(sz, (uintptr_t)__builtin_return_address(0), "::new");
#else
    return ker::mod::mm::dyn::kmalloc::malloc(sz);
#endif
}

auto operator new[](size_t sz) -> void* {
#if defined(WOS_KASAN) || defined(WOS_KUBSAN)
    return ker::mod::mm::dyn::kmalloc::malloc_tagged(sz, (uintptr_t)__builtin_return_address(0), "::new[]");
#else
    return ker::mod::mm::dyn::kmalloc::malloc(sz);
#endif
}

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

namespace std {
extern const nothrow_t nothrow{};
}

auto operator new(size_t size, std::nothrow_t const& /*tag*/) noexcept -> void* { return ker::mod::mm::dyn::kmalloc::malloc(size); }

auto operator new[](size_t size, std::nothrow_t const& /*tag*/) noexcept -> void* { return ker::mod::mm::dyn::kmalloc::malloc(size); }
