#include "phys.hpp"

#include <extern/limine.h>

#include <algorithm>
#include <array>
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
#include <sanitizer/kasan.hpp>
#include <string_view>

#include "minimalist_malloc/mini_malloc.hpp"
#include "page_alloc.hpp"
#include "platform/asm/tlb.hpp"
#include "platform/dbg/dbg.hpp"
#include "platform/mm/addr.hpp"
#include "platform/mm/dyn/kmalloc.hpp"
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
    void* pages[CACHE_SIZE]{};
    size_t count{};
    sys::Spinlock lock;  // Fine-grained per-CPU lock

    PerCpuPageCache() {
        for (auto& page : pages) {
            page = nullptr;
        }
    }
};

// Debug spinlock for memlock - records holder CR3, CPU, and caller RIP
__attribute__((section(".data"))) paging::PageZone* zones = nullptr;
__attribute__((section(".data"))) paging::PageZone* hugePageZone = nullptr;  // Dedicated zone for huge allocations

// Per-CPU caches (initialized in init())
PerCpuPageCache* per_cpu_caches = nullptr;
size_t num_cpus = 0;
std::atomic<bool> per_cpu_ready{false};  // Set after per-CPU structures are initialized

// The cache stores only bare 4 KiB pages, but pageFree() does not receive the
// allocation size. Caching the base of a multi-page allocation leaves the buddy
// metadata tagged with the original larger order and can later free/reuse pages
// that are still mapped elsewhere. Keep the cache disabled until frees carry
// enough size/order information to prove an allocation is order-0.
constexpr bool USE_PER_CPU_PAGE_CACHE = false;

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
        holder_cpu = per_cpu_ready.load(std::memory_order_acquire) ? cpu::currentCpu() : apic::getApicId();
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

// Per-CPU cache deferred initialization info
uint64_t per_cpu_caches_phys_base = 0;
uint64_t per_cpu_caches_size = 0;

// Statistics counters
uint64_t main_heap_size = 0;

// Huge page zone deferred initialization info
uint64_t huge_page_base = 0;
uint64_t huge_page_size = 0;

// Allocation tracking counters (now atomic for multi-CPU safety)
std::atomic<uint64_t> total_allocated_bytes{0};
std::atomic<uint64_t> total_freed_bytes{0};
std::atomic<uint64_t> alloc_count{0};
std::atomic<uint64_t> free_count{0};

// Per-caller page allocation histogram.
// Indexed by return address so OOM output shows which subsystem consumed memory.
struct CallerStat {
    uint64_t caller;  // return address (0 = empty)
    uint64_t pages;   // cumulative pages allocated from this site
};
constexpr size_t CALLER_STAT_SLOTS = 64;
__attribute__((section(".bss"))) CallerStat caller_stats[CALLER_STAT_SLOTS];
std::atomic<bool> caller_stat_lock{false};

void record_page_alloc_caller(void* caller_addr, uint64_t num_pages) {
    if (caller_addr == nullptr) {
        return;
    }
    bool expected = false;
    while (!caller_stat_lock.compare_exchange_weak(expected, true, std::memory_order_acquire, std::memory_order_relaxed)) {
        expected = false;
        asm volatile("pause");
    }
    auto caller = reinterpret_cast<uint64_t>(caller_addr);
    size_t start = (caller >> 3) % CALLER_STAT_SLOTS;
    for (size_t i = 0; i < CALLER_STAT_SLOTS; i++) {
        size_t idx = (start + i) % CALLER_STAT_SLOTS;
        if (caller_stats[idx].caller == caller) {
            caller_stats[idx].pages += num_pages;
            caller_stat_lock.store(false, std::memory_order_release);
            return;
        }
        if (caller_stats[idx].caller == 0) {
            caller_stats[idx].caller = caller;
            caller_stats[idx].pages = num_pages;
            caller_stat_lock.store(false, std::memory_order_release);
            return;
        }
    }
    // Table full - silently drop; 64 slots covers all known call sites.
    caller_stat_lock.store(false, std::memory_order_release);
}

}  // namespace

void dumpAllocStats() {
    io::serial::write("Physical alloc stats: allocated=");
    io::serial::writeHex(total_allocated_bytes.load());
    io::serial::write(" freed=");
    io::serial::writeHex(total_freed_bytes.load());
    io::serial::write(" delta=");
    io::serial::writeHex(total_allocated_bytes.load() - total_freed_bytes.load());
    io::serial::write(" allocCount=");
    io::serial::writeHex(alloc_count.load());
    io::serial::write(" freeCount=");
    io::serial::writeHex(free_count.load());
    io::serial::write("\n");
}

auto get_free_mem_bytes() -> uint64_t { return main_heap_size - (total_allocated_bytes.load() - total_freed_bytes.load()); }

auto get_total_mem_bytes() -> uint64_t { return main_heap_size; }

void dump_caller_page_stats() {
    io::serial::write("Physical page alloc by caller (cumulative, sorted by pages desc):\n");

    // Copy table under lock so we get a stable snapshot
    constexpr uint64_t BYTES_PER_KB = 1024;
    std::array<CallerStat, CALLER_STAT_SLOTS> snapshot{};
    bool expected = false;
    while (!caller_stat_lock.compare_exchange_weak(expected, true, std::memory_order_acquire, std::memory_order_relaxed)) {
        expected = false;
        asm volatile("pause");
    }
    for (size_t i = 0; i < CALLER_STAT_SLOTS; i++) {
        snapshot[i] = caller_stats[i];
    }
    caller_stat_lock.store(false, std::memory_order_release);

    // Selection-sort descending by pages (64 entries - fine for OOM context)
    for (size_t i = 0; i < CALLER_STAT_SLOTS - 1; i++) {
        size_t max_idx = i;
        for (size_t j = i + 1; j < CALLER_STAT_SLOTS; j++) {
            if (snapshot[j].pages > snapshot[max_idx].pages) {
                max_idx = j;
            }
        }
        if (max_idx != i) {
            CallerStat tmp = snapshot[i];
            snapshot[i] = snapshot[max_idx];
            snapshot[max_idx] = tmp;
        }
    }

    for (size_t i = 0; i < CALLER_STAT_SLOTS; i++) {
        if (snapshot[i].caller == 0 || snapshot[i].pages == 0) {
            break;
        }
        io::serial::write("  0x");
        io::serial::writeHex(snapshot[i].caller);
        io::serial::write(": ");
        io::serial::write(snapshot[i].pages);
        io::serial::write(" pages (");
        io::serial::write(snapshot[i].pages * paging::PAGE_SIZE / BYTES_PER_KB);
        io::serial::write(" KB)\n");
    }
}

auto getZones() -> paging::PageZone* { return zones; }
auto getHugePageZone() -> paging::PageZone* { return hugePageZone; }

namespace {
// Forward declaration
auto find_free_block(uint64_t size) -> void*;

auto init_page_zone(uint64_t base, uint64_t len, int zone_num) -> paging::PageZone* {
    auto* zone = (paging::PageZone*)base;

    base = PAGE_ALIGN_UP(base + sizeof(paging::PageZone));
    len -= paging::PAGE_SIZE;

    zone->name = "Physical Memory";
    zone->zoneNum = zone_num;

    // Initialise the page allocator; metadata is embedded at the start of the region.
    auto* allocator = reinterpret_cast<PageAllocator*>(base);
    allocator->init(base, len);
    zone->allocator = allocator;
    zone->start = base;
    zone->len = (uint64_t)allocator->getUsablePages() * paging::PAGE_SIZE;
    zone->pageCount = allocator->getUsablePages();

    return zone;
}

auto init_huge_page_zone(uint64_t base, uint64_t len) -> paging::PageZone* {
    // Allocate zone structure from regular memory (which is already mapped)
    // Don't use the huge page region itself for metadata since it's not mapped yet
    auto* zone = (paging::PageZone*)find_free_block(paging::PAGE_SIZE);
    if (zone == nullptr) {
        return nullptr;  // OOM
    }

    // Allocate PageAllocator structure from regular memory too
    auto* allocator = reinterpret_cast<PageAllocator*>(find_free_block(paging::PAGE_SIZE));
    if (allocator == nullptr) {
        return nullptr;  // OOM
    }

    // Convert huge page region base to virtual address for initialization
    auto virt_base = reinterpret_cast<uint64_t>(addr::get_virt_pointer(base));

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

auto find_free_block(uint64_t size) -> void* {
    for (paging::PageZone* zone = zones; zone != nullptr; zone = zone->next) {
        if (zone->len < size) {
            continue;
        }

        void* const BLOCK = zone->allocator->alloc(size);
        if (BLOCK == nullptr) {
            [[unlikely]] continue;
        }
        return BLOCK;
    }

    return nullptr;
}

auto find_free_block_huge(uint64_t size) -> void* {
    if (hugePageZone == nullptr || hugePageZone->len < size) {
        return nullptr;
    }

    return hugePageZone->allocator->alloc(size);
}

}  // namespace

void init(limine_memmap_response* memmap_response) {
    if (memmap_response == nullptr) {
        // TODO: logging
        hcf();
    }
    limine_memmap_response memmap = *(memmap_response);

    // Initialize per-CPU caches
    num_cpus = smt::get_core_count();
    if (num_cpus == 0) {
        num_cpus = 1;  // Fallback to single CPU
    }

    // Reserve per-CPU caches region (will be mapped and initialized after virt::initPagemap)
    // We'll allocate these from the first usable memory region
    per_cpu_caches_size = PAGE_ALIGN_UP(sizeof(PerCpuPageCache) * num_cpus);
    for (size_t i = 0; i < memmap.entry_count; i++) {
        if (memmap.entries[i]->type == LIMINE_MEMMAP_USABLE && memmap.entries[i]->length >= per_cpu_caches_size + paging::PAGE_SIZE) {
            // Save the physical address for later mapping
            per_cpu_caches_phys_base = memmap.entries[i]->base;
            // Carve it out from the memory map
            memmap.entries[i]->base += per_cpu_caches_size;
            memmap.entries[i]->length -= per_cpu_caches_size;
            break;
        }
    }

    if (per_cpu_caches_phys_base == 0) {
        hcf();  // Can't allocate per-CPU caches
    }

    // Find the largest usable region for huge pages (reserve ~10% or largest region > 128MB)
    limine_memmap_entry* huge_page_entry = nullptr;
    uint64_t largest_size = 0;
    size_t huge_page_idx = 0;

    for (size_t i = 0; i < memmap.entry_count; i++) {
        if (memmap.entries[i]->type == LIMINE_MEMMAP_USABLE && memmap.entries[i]->length > largest_size) {
            largest_size = memmap.entries[i]->length;
            huge_page_entry = memmap.entries[i];
            huge_page_idx = i;
        }
    }

    paging::PageZone* zones_tail = nullptr;
    size_t zone_num = 0;

    for (size_t i = 0; i < memmap.entry_count; i++) {
        if (memmap.entries[i]->type != LIMINE_MEMMAP_USABLE || memmap.entries[i]->length == paging::PAGE_SIZE) {
            continue;
        }

        // If this is the huge page entry and it's large enough, split it
        if (i == huge_page_idx && largest_size > 128 * 1024 * 1024) {
            uint64_t huge_sz = largest_size / 4;  // 25% for huge pages
            huge_sz = std::max<uint64_t>(huge_sz, static_cast<const unsigned long>(16 * 1024 * 1024));
            huge_sz = PAGE_ALIGN_UP(huge_sz);

            // Save huge page region info for later initialization (after virt::initPagemap)
            huge_page_base = memmap.entries[i]->base + memmap.entries[i]->length - huge_sz;
            huge_page_size = huge_sz;

            // Reduce the main zone size
            memmap.entries[i]->length -= huge_sz;
        }

        main_heap_size += memmap.entries[i]->length;

        paging::PageZone* zone =
            init_page_zone((uint64_t)addr::get_virt_pointer(memmap.entries[i]->base), memmap.entries[i]->length, zone_num++);

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
    if (per_cpu_caches_size > 0 && per_cpu_caches_phys_base != 0) {
        mod::dbg::log("Mapping per-CPU caches: base=0x%016x size=0x%016x", per_cpu_caches_phys_base, per_cpu_caches_size);

        for (uint64_t offset = 0; offset < per_cpu_caches_size; offset += paging::PAGE_SIZE) {
            uint64_t phys = per_cpu_caches_phys_base + offset;
            auto virt = (uint64_t)addr::get_virt_pointer(phys);
            virt::mapToKernelPageTable(virt, phys, paging::pageTypes::KERNEL);
        }

        mod::dbg::log("Per-CPU caches mapped, initializing structures");

        // Now we can safely access the memory
        void* cache_memory = addr::get_virt_pointer(per_cpu_caches_phys_base);
        per_cpu_caches = static_cast<PerCpuPageCache*>(cache_memory);
        for (size_t i = 0; i < num_cpus; i++) {
            new (&per_cpu_caches[i]) PerCpuPageCache();  // Placement new
        }

        mod::dbg::log("Per-CPU caches initialized for %zu CPUs", num_cpus);
    }

    // Initialize the huge page zone after virt::initPagemap() has set up the kernel page map
    // This ensures the zone metadata is in mapped memory
    if (huge_page_size > 0 && huge_page_base != 0) {
        mod::dbg::log("Mapping huge page region: base=0x%016x size=0x%016x", huge_page_base, huge_page_size);

        for (uint64_t offset = 0; offset < huge_page_size; offset += paging::PAGE_SIZE) {
            uint64_t phys = huge_page_base + offset;
            auto virt = (uint64_t)addr::get_virt_pointer(phys);
            virt::mapToKernelPageTable(virt, phys, paging::pageTypes::KERNEL);
        }

        mod::dbg::log("Huge page region mapped, initializing zone");

        uint64_t flags = memlock.lock_irq();
        // Pass physical addresses - initHugePageZone will convert to virtual
        hugePageZone = init_huge_page_zone(huge_page_base, huge_page_size);
        memlock.unlock_irq(flags);

        if (hugePageZone != nullptr) {
            mod::dbg::log("Huge page zone initialized: start=0x%016x len=0x%016x pages=%zu", hugePageZone->start, hugePageZone->len,
                          hugePageZone->pageCount);
        } else {
            mod::dbg::log("Failed to initialize huge page zone");
        }
    }
}

void enablePerCpuAllocations() {
    per_cpu_ready.store(true, std::memory_order_release);
    cpu::notifyPerCpuReady();
}

auto pageAlloc(uint64_t size, std::string_view name) -> void* {
    void* caller_addr = __builtin_return_address(0);
    uint64_t num_pages = (size + paging::PAGE_SIZE - 1) / paging::PAGE_SIZE;

    // Try per-CPU cache first for single-page allocations
    if (USE_PER_CPU_PAGE_CACHE && size == paging::PAGE_SIZE && per_cpu_caches != nullptr && per_cpu_ready.load(std::memory_order_acquire)) {
        uint64_t cpu_id = cpu::getCurrentCpuIdSafe();
        if (cpu_id < num_cpus) {
            PerCpuPageCache& cache = per_cpu_caches[cpu_id];
            cache.lock.lock();

            if (cache.count > 0) {
                // Fast path: pop from cache
                void* page = cache.pages[--cache.count];
                cache.lock.unlock();

                total_allocated_bytes.fetch_add(size, std::memory_order_relaxed);
                alloc_count.fetch_add(1, std::memory_order_relaxed);

                // Double-alloc sentinel: if the page still holds a live slab header
                // it was freed to this cache while still in the slab chain.
                constexpr uint32_t SLAB_MAGIC_CACHE = 0x8CBEEFC8;
                if (*reinterpret_cast<const volatile uint32_t*>(page) == SLAB_MAGIC_CACHE) {
                    mod::dbg::log("DETECT: pageAlloc (cache) returning live slab page – double-alloc trap! virt=%p", page);
                    hcf();
                }

                // Zero the page
                uint64_t saved_cr3 = 0;
                if (kernelCr3 != 0) {
                    uint64_t current_cr3 = rdcr3();
                    if (current_cr3 != kernelCr3) {
                        saved_cr3 = current_cr3;
                        wrcr3(kernelCr3);
                    }
                }
                memset(page, 0, size);
#ifdef WOS_KASAN
                if (kasan::is_enabled() && !kasan::in_shadow_fault()) {
                    kasan::unpoison_range(page, size);
                }
#endif
                if (saved_cr3 != 0) {
                    wrcr3(saved_cr3);
                }

                record_page_alloc_caller(caller_addr, num_pages);
                return page;
            }
            cache.lock.unlock();
        }
    }

    // Slow path: allocate from zones
    uint64_t flags = memlock.lock_irq();
    void* block = find_free_block(size);
    memlock.unlock_irq(flags);

    if (block == nullptr) {
        // OOM condition - dump allocation info for debugging
        io::serial::write("OOM: pageAlloc failed for size ");
        io::serial::writeHex(size);
        io::serial::write(" bytes\n");
        io::serial::write("Allocation site: 0x");
        io::serial::writeHex((uint64_t)caller_addr);
        io::serial::write(" (");
        io::serial::write(name.data(), name.size());
        io::serial::write(")\n");
        dumpPageAllocationsOOM();
        return nullptr;
    }

    total_allocated_bytes.fetch_add(size, std::memory_order_relaxed);
    alloc_count.fetch_add(1, std::memory_order_relaxed);

    // Validate the returned address is in a reasonable HHDM range
    auto block_addr = (uint64_t)block;
    constexpr uint64_t HHDM_BASE = 0xffff800000000000ULL;
    constexpr uint64_t HHDM_END = 0xffff808000000000ULL;  // ~512GB max physical
    if (block_addr < HHDM_BASE || block_addr >= HHDM_END) {
        io::serial::write("FATAL: pageAlloc returned invalid HHDM addr: ");
        io::serial::writeHex(block_addr);
        io::serial::write("\n");
        hcf();
    }

    // Zero outside the lock - the block is exclusively ours now
    // Double-alloc sentinel (buddy path): if the page still holds a live slab
    // header it was freed to the buddy while still referenced by the slab chain.
    constexpr uint32_t SLAB_MAGIC_BUDDY = 0x8CBEEFC8;
    if (*reinterpret_cast<const volatile uint32_t*>(block) == SLAB_MAGIC_BUDDY) {
        mod::dbg::log("DETECT: pageAlloc (buddy) returning live slab page – double-alloc trap! virt=%p", block);
        hcf();
    }

    // Zero outside the lock - the block is exclusively ours now
    uint64_t saved_cr3 = 0;
    if (kernelCr3 != 0) {
        uint64_t current_cr3 = rdcr3();
        if (current_cr3 != kernelCr3) {
            saved_cr3 = current_cr3;
            wrcr3(kernelCr3);
        }
    }

    memset(block, 0, size);
#ifdef WOS_KASAN
    if (kasan::is_enabled() && !kasan::in_shadow_fault()) {
        kasan::unpoison_range(block, size);
    }
#endif

    if (saved_cr3 != 0) {
        wrcr3(saved_cr3);
    }

    record_page_alloc_caller(caller_addr, num_pages);
    return block;
}

auto pageAllocHuge(uint64_t size) -> void* {
    void* caller_addr = __builtin_return_address(0);
    uint64_t num_pages = (size + paging::PAGE_SIZE - 1) / paging::PAGE_SIZE;

    // Allocate from dedicated huge page zone
    uint64_t flags = memlock.lock_irq();
    void* block = find_free_block_huge(size);
    memlock.unlock_irq(flags);

    if (block == nullptr) {
        io::serial::write("OOM: pageAllocHuge failed for size 0x");
        io::serial::writeHex(size);
        io::serial::write(" bytes\n");
        return nullptr;
    }

    total_allocated_bytes.fetch_add(size, std::memory_order_relaxed);
    alloc_count.fetch_add(1, std::memory_order_relaxed);

    // Zero outside the lock - the block is exclusively ours now
    uint64_t saved_cr3 = 0;
    if (kernelCr3 != 0) {
        uint64_t current_cr3 = rdcr3();
        if (current_cr3 != kernelCr3) {
            saved_cr3 = current_cr3;
            wrcr3(kernelCr3);
        }
    }

    memset(block, 0, size);
#ifdef WOS_KASAN
    if (kasan::is_enabled() && !kasan::in_shadow_fault()) {
        kasan::unpoison_range(block, size);
    }
#endif

    if (saved_cr3 != 0) {
        wrcr3(saved_cr3);
    }

    record_page_alloc_caller(caller_addr, num_pages);
    return block;
}

void pageFree(void* page) {
    // Try to return single pages to per-CPU cache
    if (USE_PER_CPU_PAGE_CACHE && per_cpu_caches != nullptr && per_cpu_ready.load(std::memory_order_acquire)) {
        uint64_t cpu_id = cpu::getCurrentCpuIdSafe();
        if (cpu_id < num_cpus) {
            PerCpuPageCache& cache = per_cpu_caches[cpu_id];
            cache.lock.lock();

            if (cache.count < PerCpuPageCache::CACHE_SIZE) {
                // Fast path: push to cache
                cache.pages[cache.count++] = page;
                cache.lock.unlock();

                free_count.fetch_add(1, std::memory_order_relaxed);
                total_freed_bytes.fetch_add(paging::PAGE_SIZE, std::memory_order_relaxed);
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
                free_count.fetch_add(1, std::memory_order_relaxed);
                total_freed_bytes.fetch_add(paging::PAGE_SIZE, std::memory_order_relaxed);
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
            free_count.fetch_add(1, std::memory_order_relaxed);
            total_freed_bytes.fetch_add(paging::PAGE_SIZE, std::memory_order_relaxed);
        }
        break;
    }

    memlock.unlock_irq(flags);
}

auto pageSplitToOrder0(void* page) -> bool {
    if (page == nullptr) {
        return false;
    }

    uint64_t flags = memlock.lock_irq();

    if (hugePageZone != nullptr) {
        if ((uint64_t)page >= hugePageZone->start && (uint64_t)page < hugePageZone->start + hugePageZone->len) {
            bool ok = hugePageZone->allocator != nullptr && hugePageZone->allocator->splitAllocatedBlockToOrder0(page);
            memlock.unlock_irq(flags);
            return ok;
        }
    }

    for (paging::PageZone* zone = zones; zone != nullptr; zone = zone->next) {
        if ((uint64_t)page < zone->start || (uint64_t)page >= zone->start + zone->len) {
            continue;
        }

        bool ok = zone->allocator != nullptr && zone->allocator->splitAllocatedBlockToOrder0(page);
        memlock.unlock_irq(flags);
        return ok;
    }

    memlock.unlock_irq(flags);
    return false;
}

// --- Frame reference counting helpers ---

namespace {
// Find the PageAllocator and page index for a given HHDM pointer.
// Returns true if found, populating allocator and pageIdx.
auto find_allocator_for_page(void* page, PageAllocator*& out_alloc, uint32_t& out_idx) -> bool {
    auto addr = (uint64_t)page;
    // Check regular zones first
    for (paging::PageZone* zone = zones; zone != nullptr; zone = zone->next) {
        if (addr >= zone->start && addr < zone->start + zone->len && zone->allocator != nullptr) {
            out_alloc = zone->allocator;
            out_idx = static_cast<uint32_t>((addr - zone->allocator->base) / paging::PAGE_SIZE);
            if (out_idx < out_alloc->totalPages) {
                return true;
            }
        }
    }
    // Check huge page zone
    if (hugePageZone != nullptr && addr >= hugePageZone->start && addr < hugePageZone->start + hugePageZone->len &&
        hugePageZone->allocator != nullptr) {
        out_alloc = hugePageZone->allocator;
        out_idx = static_cast<uint32_t>((addr - hugePageZone->allocator->base) / paging::PAGE_SIZE);
        if (out_idx < out_alloc->totalPages) {
            return true;
        }
    }
    return false;
}
}  // namespace

void pageRefInc(void* page) {
    if (page == nullptr) {
        return;
    }
    PageAllocator* alloc = nullptr;
    uint32_t idx = 0;
    uint64_t flags = memlock.lock_irq();
    if (find_allocator_for_page(page, alloc, idx)) {
        alloc->pageRefcounts[idx]++;
    }
    memlock.unlock_irq(flags);
}

auto pageRefDec(void* page) -> uint32_t {
    if (page == nullptr) {
        return 0;
    }
    PageAllocator* alloc = nullptr;
    uint32_t idx = 0;
    uint64_t flags = memlock.lock_irq();
    if (find_allocator_for_page(page, alloc, idx)) {
        uint32_t new_ref = alloc->pageRefcounts[idx];
        if (new_ref > 0) {
            new_ref = --alloc->pageRefcounts[idx];
            if (new_ref == 0) {  // UAF sentinel: a live slab page must never reach refcount 0 via pageRefDec.
                // If the first 4 bytes match the slab magic the page is still in the slab
                // chain – some corrupt PTE is being incorrectly treated as a user data page.
                constexpr uint32_t SLAB_MAGIC = 0x8CBEEFC8;
                if (*reinterpret_cast<const volatile uint32_t*>(page) == SLAB_MAGIC) {
                    memlock.unlock_irq(flags);
                    mod::dbg::log("DETECT: pageRefDec freeing live slab page – UAF trap! virt=%p", page);
                    hcf();
                }

                // Refcount reached zero - free the page
                alloc->free(page);
                free_count.fetch_add(1, std::memory_order_relaxed);
                total_freed_bytes.fetch_add(paging::PAGE_SIZE, std::memory_order_relaxed);
            }
        }
        // If new_ref was already 0 (page already freed), do NOT call alloc->free again.
        memlock.unlock_irq(flags);
        return new_ref;
    }
    memlock.unlock_irq(flags);
    return 0;
}

auto pageRefGet(void* page) -> uint32_t {
    if (page == nullptr) {
        return 0;
    }
    PageAllocator* alloc = nullptr;
    uint32_t idx = 0;
    uint64_t flags = memlock.lock_irq();
    uint32_t ref = 0;
    if (find_allocator_for_page(page, alloc, idx)) {
        ref = alloc->pageRefcounts[idx];
    }
    memlock.unlock_irq(flags);
    return ref;
}

void dumpMiniMallocStats() { ::mini_dump_stats(); }

void dumpKmallocTrackedAllocs() { ker::mod::mm::dyn::kmalloc::dumpTrackedAllocations(); }

}  // namespace ker::mod::mm::phys
