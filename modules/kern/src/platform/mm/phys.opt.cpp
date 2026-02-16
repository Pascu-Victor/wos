#include "phys.hpp"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <mod/io/serial/serial.hpp>
#include <platform/acpi/apic/apic.hpp>
#include <platform/asm/cpu.hpp>
#include <platform/mm/mm.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/smt/smt.hpp>

#include "limine.h"
#include "page_alloc.hpp"
#include "platform/mm/addr.hpp"
#include "platform/mm/paging.hpp"
#include "platform/mm/virt.hpp"
#include "platform/sys/spinlock.hpp"
#include "util/hcf.hpp"

namespace {
// Forward declaration - we'll get kernel pagemap physical address once during init
static uint64_t kernelCr3 = 0;
}  // anonymous namespace

namespace ker::mod::mm::phys {

namespace {
// Per-CPU page cache for reducing lock contention
struct PerCpuPageCache {
    static constexpr size_t CACHE_SIZE = 16;  // Pages per CPU cache
    void* pages[CACHE_SIZE];
    size_t count;
    sys::Spinlock lock;  // Fine-grained per-CPU lock

    PerCpuPageCache() : count(0) {
        for (size_t i = 0; i < CACHE_SIZE; i++) {
            pages[i] = nullptr;
        }
    }
};

// Debug spinlock for memlock — records holder CR3, CPU, and caller RIP
struct TrackedSpinlock {
    std::atomic<bool> locked{false};
    volatile uint64_t holder_cr3 = 0;
    volatile uint64_t holder_cpu = 0;
    volatile uint64_t holder_rip = 0;

    auto lock_irq() -> uint64_t {
        // Save RFLAGS and disable interrupts before acquiring
        uint64_t flags = 0;
        asm volatile("pushfq; popq %0" : "=r"(flags));
        asm volatile("cli");

        while (locked.exchange(true, std::memory_order_acquire)) {
            while (locked.load(std::memory_order_relaxed)) {
                asm volatile("pause");
            }
        }
        // Record who holds the lock
        uint64_t cr3;
        asm volatile("mov %%cr3, %0" : "=r"(cr3));
        holder_cr3 = cr3;
        holder_rip = reinterpret_cast<uint64_t>(__builtin_return_address(0));
        holder_cpu = apic::getApicId();
        return flags;
    }

    void unlock_irq(uint64_t flags) {
        holder_cr3 = 0;
        holder_cpu = 0;
        holder_rip = 0;
        locked.store(false, std::memory_order_release);

        // Restore interrupt state
        if ((flags & 0x200) != 0) {
            asm volatile("sti");
        }
    }
};

TrackedSpinlock memlock;  // Global lock for zone list and large allocations
__attribute__((section(".data"))) paging::PageZone* zones = nullptr;
__attribute__((section(".data"))) paging::PageZone* hugePageZone = nullptr;  // Dedicated zone for huge allocations

// Per-CPU caches (initialized in init())
static PerCpuPageCache* perCpuCaches = nullptr;
static size_t numCpus = 0;
static std::atomic<bool> perCpuReady{false};  // Set after per-CPU structures are initialized

// Per-CPU cache deferred initialization info
static uint64_t perCpuCachesPhysBase = 0;
static uint64_t perCpuCachesSize = 0;

// Statistics counters
static uint64_t mainHeapSize = 0;

// Huge page zone deferred initialization info
static uint64_t hugePageBase = 0;
static uint64_t hugePageSize = 0;

// Allocation tracking counters (now atomic for multi-CPU safety)
static std::atomic<uint64_t> totalAllocatedBytes{0};
static std::atomic<uint64_t> totalFreedBytes{0};
static std::atomic<uint64_t> allocCount{0};
static std::atomic<uint64_t> freeCount{0};

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
}  // namespace

void dumpAllocStats() {
    io::serial::write("Physical alloc stats: allocated=");
    io::serial::writeHex(totalAllocatedBytes.load());
    io::serial::write(" freed=");
    io::serial::writeHex(totalFreedBytes.load());
    io::serial::write(" delta=");
    io::serial::writeHex(totalAllocatedBytes.load() - totalFreedBytes.load());
    io::serial::write(" allocCount=");
    io::serial::writeHex(allocCount.load());
    io::serial::write(" freeCount=");
    io::serial::writeHex(freeCount.load());
    io::serial::write("\n");
}

uint64_t get_free_mem_bytes() { return mainHeapSize - (totalAllocatedBytes.load() - totalFreedBytes.load()); }

auto getZones() -> paging::PageZone* { return zones; }
auto getHugePageZone() -> paging::PageZone* { return hugePageZone; }

namespace {
// Forward declaration
auto findFreeBlock(uint64_t size) -> void*;

auto initPageZone(uint64_t base, uint64_t len, int zoneNum) -> paging::PageZone* {
    auto* zone = (paging::PageZone*)base;

    base = PAGE_ALIGN_UP(base + sizeof(paging::PageZone));
    len -= paging::PAGE_SIZE;

    zone->name = "Physical Memory";
    zone->zoneNum = zoneNum;

    // Initialise the page allocator; metadata is embedded at the start of the region.
    auto* allocator = reinterpret_cast<PageAllocator*>(base);
    allocator->init(base, len);
    zone->allocator = allocator;
    zone->start = base;
    zone->len = (uint64_t)allocator->getUsablePages() * paging::PAGE_SIZE;
    zone->pageCount = allocator->getUsablePages();

    return zone;
}

auto initHugePageZone(uint64_t base, uint64_t len) -> paging::PageZone* {
    // Allocate zone structure from regular memory (which is already mapped)
    // Don't use the huge page region itself for metadata since it's not mapped yet
    auto* zone = (paging::PageZone*)findFreeBlock(paging::PAGE_SIZE);
    if (zone == nullptr) {
        return nullptr;  // OOM
    }

    // Allocate PageAllocator structure from regular memory too
    auto* allocator = reinterpret_cast<PageAllocator*>(findFreeBlock(paging::PAGE_SIZE));
    if (allocator == nullptr) {
        return nullptr;  // OOM
    }

    // Convert huge page region base to virtual address for initialization
    auto virt_base = reinterpret_cast<uint64_t>(addr::getVirtPointer(base));

    zone->name = "Huge Pages";
    zone->zoneNum = 9999;  // Special zone number for huge pages

    // Initialize allocator with the huge page region (all virtual addresses now)
    allocator->init(virt_base, len);
    zone->allocator = allocator;
    zone->start = virt_base;
    zone->len = (uint64_t)allocator->getUsablePages() * paging::PAGE_SIZE;
    zone->pageCount = allocator->getUsablePages();
    zone->next = nullptr;

    return zone;
}

auto findFreeBlock(uint64_t size) -> void* {
    for (paging::PageZone* zone = zones; zone != nullptr; zone = zone->next) {
        if (zone->len < size) {
            continue;
        }

        void* const block = zone->allocator->alloc(size);
        if (block == nullptr) {
            [[unlikely]] continue;
        }
        return block;
    }

    return nullptr;
}

auto findFreeBlockHuge(uint64_t size) -> void* {
    if (hugePageZone == nullptr || hugePageZone->len < size) {
        return nullptr;
    }

    return hugePageZone->allocator->alloc(size);
}

}  // namespace

void init(limine_memmap_response* memmapResponse) {
    if (memmapResponse == nullptr) {
        // TODO: logging
        hcf();
    }
    limine_memmap_response memmap = *(memmapResponse);

    // Initialize per-CPU caches
    numCpus = smt::getCoreCount();
    if (numCpus == 0) numCpus = 1;  // Fallback to single CPU

    // Reserve per-CPU caches region (will be mapped and initialized after virt::initPagemap)
    // We'll allocate these from the first usable memory region
    perCpuCachesSize = PAGE_ALIGN_UP(sizeof(PerCpuPageCache) * numCpus);
    for (size_t i = 0; i < memmap.entry_count; i++) {
        if (memmap.entries[i]->type == LIMINE_MEMMAP_USABLE && memmap.entries[i]->length >= perCpuCachesSize + paging::PAGE_SIZE) {
            // Save the physical address for later mapping
            perCpuCachesPhysBase = memmap.entries[i]->base;
            // Carve it out from the memory map
            memmap.entries[i]->base += perCpuCachesSize;
            memmap.entries[i]->length -= perCpuCachesSize;
            break;
        }
    }

    if (perCpuCachesPhysBase == 0) {
        hcf();  // Can't allocate per-CPU caches
    }

    // Find the largest usable region for huge pages (reserve ~10% or largest region > 128MB)
    limine_memmap_entry* hugePageEntry = nullptr;
    uint64_t largestSize = 0;
    size_t hugePageIdx = 0;

    for (size_t i = 0; i < memmap.entry_count; i++) {
        if (memmap.entries[i]->type == LIMINE_MEMMAP_USABLE && memmap.entries[i]->length > largestSize) {
            largestSize = memmap.entries[i]->length;
            hugePageEntry = memmap.entries[i];
            hugePageIdx = i;
        }
    }

    paging::PageZone* zones_tail = nullptr;
    size_t zoneNum = 0;

    for (size_t i = 0; i < memmap.entry_count; i++) {
        if (memmap.entries[i]->type != LIMINE_MEMMAP_USABLE || memmap.entries[i]->length == paging::PAGE_SIZE) {
            continue;
        }

        // If this is the huge page entry and it's large enough, split it
        if (i == hugePageIdx && largestSize > 128 * 1024 * 1024) {
            uint64_t hugeSz = largestSize / 4;  // 25% for huge pages
            hugeSz = std::max<uint64_t>(hugeSz, static_cast<const unsigned long>(16 * 1024 * 1024));
            hugeSz = PAGE_ALIGN_UP(hugeSz);

            // Save huge page region info for later initialization (after virt::initPagemap)
            hugePageBase = memmap.entries[i]->base + memmap.entries[i]->length - hugeSz;
            hugePageSize = hugeSz;

            // Reduce the main zone size
            memmap.entries[i]->length -= hugeSz;
        }

        mainHeapSize += memmap.entries[i]->length;

        paging::PageZone* zone =
            initPageZone((uint64_t)addr::getVirtPointer(memmap.entries[i]->base), memmap.entries[i]->length, zoneNum++);

        if (zones_tail == nullptr) {
            zones = zone;  // set the head
        } else {
            zones_tail->next = zone;
        }
        zones_tail = zone;
    }

    if (zones_tail == nullptr) {
        hcf();  // no usable memory???
    }

    zones_tail->next = nullptr;
}

void setKernelCr3(uint64_t cr3) {
    kernelCr3 = cr3;

    // Re-initialize the tracked memlock after pagemap switch so that
    // stale CR3/RIP values from boot-time Limine pagemaps are cleared.
    memlock.locked.store(false, std::memory_order_release);
    memlock.holder_cr3 = 0;
    memlock.holder_cpu = 0;
    memlock.holder_rip = 0;
}

void initHugePageZoneDeferred() {
    // Map and initialize per-CPU caches first
    if (perCpuCachesSize > 0 && perCpuCachesPhysBase != 0) {
        io::serial::write("Mapping per-CPU caches: base=0x");
        io::serial::writeHex(perCpuCachesPhysBase);
        io::serial::write(" size=0x");
        io::serial::writeHex(perCpuCachesSize);
        io::serial::write("\n");

        for (uint64_t offset = 0; offset < perCpuCachesSize; offset += paging::PAGE_SIZE) {
            uint64_t phys = perCpuCachesPhysBase + offset;
            auto virt = (uint64_t)addr::getVirtPointer(phys);
            virt::mapToKernelPageTable(virt, phys, paging::pageTypes::KERNEL);
        }

        io::serial::write("Per-CPU caches mapped, initializing structures\n");

        // Now we can safely access the memory
        void* cacheMemory = addr::getVirtPointer(perCpuCachesPhysBase);
        perCpuCaches = static_cast<PerCpuPageCache*>(cacheMemory);
        for (size_t i = 0; i < numCpus; i++) {
            new (&perCpuCaches[i]) PerCpuPageCache();  // Placement new
        }

        io::serial::write("Per-CPU caches initialized for ");
        io::serial::writeHex(numCpus);
        io::serial::write(" CPUs\n");
    }

    // Initialize the huge page zone after virt::initPagemap() has set up the kernel page map
    // This ensures the zone metadata is in mapped memory
    if (hugePageSize > 0 && hugePageBase != 0) {
        io::serial::write("Mapping huge page region: base=0x");
        io::serial::writeHex(hugePageBase);
        io::serial::write(" size=0x");
        io::serial::writeHex(hugePageSize);
        io::serial::write("\n");

        for (uint64_t offset = 0; offset < hugePageSize; offset += paging::PAGE_SIZE) {
            uint64_t phys = hugePageBase + offset;
            auto virt = (uint64_t)addr::getVirtPointer(phys);
            virt::mapToKernelPageTable(virt, phys, paging::pageTypes::KERNEL);
        }

        io::serial::write("Huge page region mapped, initializing zone\n");

        uint64_t flags = memlock.lock_irq();
        // Pass physical addresses - initHugePageZone will convert to virtual
        hugePageZone = initHugePageZone(hugePageBase, hugePageSize);
        memlock.unlock_irq(flags);

        if (hugePageZone != nullptr) {
            io::serial::write("Huge page zone initialized: base=0x");
            io::serial::writeHex(hugePageBase);
            io::serial::write(" size=0x");
            io::serial::writeHex(hugePageSize);
            io::serial::write(" usable=");
            io::serial::writeHex(hugePageZone->len);
            io::serial::write("\n");
        } else {
            io::serial::write("WARNING: Failed to initialize huge page zone\n");
        }
    }
}

void enablePerCpuAllocations() { perCpuReady.store(true, std::memory_order_release); }

auto pageAlloc(uint64_t size) -> void* {
    // Try per-CPU cache first for single-page allocations
    if (size == paging::PAGE_SIZE && perCpuCaches != nullptr && perCpuReady.load(std::memory_order_acquire)) {
        uint64_t cpuId = getCurrentCpuId();
        if (cpuId < numCpus) {
            PerCpuPageCache& cache = perCpuCaches[cpuId];
            cache.lock.lock();

            if (cache.count > 0) {
                // Fast path: pop from cache
                void* page = cache.pages[--cache.count];
                cache.lock.unlock();

                totalAllocatedBytes.fetch_add(size, std::memory_order_relaxed);
                allocCount.fetch_add(1, std::memory_order_relaxed);

                // Zero the page
                uint64_t savedCr3 = 0;
                if (kernelCr3 != 0) {
                    uint64_t currentCr3 = rdcr3();
                    if (currentCr3 != kernelCr3) {
                        savedCr3 = currentCr3;
                        wrcr3(kernelCr3);
                    }
                }
                memset(page, 0, size);
                if (savedCr3 != 0) {
                    wrcr3(savedCr3);
                }

                return page;
            }
            cache.lock.unlock();
        }
    }

    // Slow path: allocate from zones
    uint64_t flags = memlock.lock_irq();
    void* block = findFreeBlock(size);
    memlock.unlock_irq(flags);

    if (block == nullptr) {
        // OOM condition - dump allocation info for debugging
        io::serial::write("OOM: pageAlloc failed for size ");
        io::serial::writeHex(size);
        io::serial::write(" bytes\n");
        dumpPageAllocationsOOM();
        return nullptr;
    }

    totalAllocatedBytes.fetch_add(size, std::memory_order_relaxed);
    allocCount.fetch_add(1, std::memory_order_relaxed);

    // Validate the returned address is in a reasonable HHDM range
    auto blockAddr = (uint64_t)block;
    uint64_t hhdmBase = 0xffff800000000000ULL;
    uint64_t hhdmEnd = 0xffff808000000000ULL;  // ~512GB max physical
    if (blockAddr < hhdmBase || blockAddr >= hhdmEnd) {
        io::serial::write("FATAL: pageAlloc returned invalid HHDM addr: ");
        io::serial::writeHex(blockAddr);
        io::serial::write("\n");
        hcf();
    }

    // Zero outside the lock — the block is exclusively ours now
    uint64_t savedCr3 = 0;
    if (kernelCr3 != 0) {
        uint64_t currentCr3 = rdcr3();
        if (currentCr3 != kernelCr3) {
            savedCr3 = currentCr3;
            wrcr3(kernelCr3);
        }
    }

    memset(block, 0, size);

    if (savedCr3 != 0) {
        wrcr3(savedCr3);
    }

    return block;
}

auto pageAllocHuge(uint64_t size) -> void* {
    // Allocate from dedicated huge page zone
    uint64_t flags = memlock.lock_irq();
    void* block = findFreeBlockHuge(size);
    memlock.unlock_irq(flags);

    if (block == nullptr) {
        io::serial::write("OOM: pageAllocHuge failed for size ");
        io::serial::writeHex(size);
        io::serial::write(" bytes\n");
        return nullptr;
    }

    totalAllocatedBytes.fetch_add(size, std::memory_order_relaxed);
    allocCount.fetch_add(1, std::memory_order_relaxed);

    // Zero outside the lock — the block is exclusively ours now
    uint64_t savedCr3 = 0;
    if (kernelCr3 != 0) {
        uint64_t currentCr3 = rdcr3();
        if (currentCr3 != kernelCr3) {
            savedCr3 = currentCr3;
            wrcr3(kernelCr3);
        }
    }

    memset(block, 0, size);

    if (savedCr3 != 0) {
        wrcr3(savedCr3);
    }

    return block;
}

void pageFree(void* page) {
    // Try to return single pages to per-CPU cache
    if (perCpuCaches != nullptr && perCpuReady.load(std::memory_order_acquire)) {
        uint64_t cpuId = getCurrentCpuId();
        if (cpuId < numCpus) {
            PerCpuPageCache& cache = perCpuCaches[cpuId];
            cache.lock.lock();

            if (cache.count < PerCpuPageCache::CACHE_SIZE) {
                // Fast path: push to cache
                cache.pages[cache.count++] = page;
                cache.lock.unlock();

                freeCount.fetch_add(1, std::memory_order_relaxed);
                totalFreedBytes.fetch_add(paging::PAGE_SIZE, std::memory_order_relaxed);
                return;
            }
            cache.lock.unlock();
        }
    }

    // Slow path: return to zone
    uint64_t flags = memlock.lock_irq();

    // Try huge page zone first
    if (hugePageZone != nullptr) {
        if ((uint64_t)page >= hugePageZone->start && (uint64_t)page < hugePageZone->start + hugePageZone->len) {
            if (hugePageZone->allocator != nullptr) {
                hugePageZone->allocator->free(page);
                freeCount.fetch_add(1, std::memory_order_relaxed);
                totalFreedBytes.fetch_add(paging::PAGE_SIZE, std::memory_order_relaxed);
            }
            memlock.unlock_irq(flags);
            return;
        }
    }

    // Check regular zones
    for (paging::PageZone* zone = zones; zone != nullptr; zone = zone->next) {
        if ((uint64_t)page < zone->start || (uint64_t)page >= zone->start + zone->len) {
            continue;
        }

        if (zone->allocator != nullptr) {
            zone->allocator->free(page);
            freeCount.fetch_add(1, std::memory_order_relaxed);
            totalFreedBytes.fetch_add(paging::PAGE_SIZE, std::memory_order_relaxed);
        }
        break;
    }

    memlock.unlock_irq(flags);
}

void dumpMiniMallocStats() { ::mini_dump_stats(); }

void dumpKmallocTrackedAllocs() { ker::mod::mm::dyn::kmalloc::dumpTrackedAllocations(); }

}  // namespace ker::mod::mm::phys
