#include "kmalloc.hpp"

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
#include "minimalist_malloc/slab_allocator.hpp"
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

namespace {
// Per-CPU magazine cache - Linux SLUB pattern.
// Fast path: pop/push from per-CPU magazine with IRQs disabled (no lock).
// Slow path: fall through to mini_malloc/mini_free which hold per-size-class slab_lock.
struct PerCpuAllocator {
    bool initialized{false};
    std::array<std::array<void*, MAGAZINE_CAPACITY>, NUM_SLAB_CLASSES> magazine{};
    std::array<uint8_t, NUM_SLAB_CLASSES> mag_count{};

    PerCpuAllocator() = default;
};

PerCpuAllocator* per_cpu_allocators = nullptr;
size_t num_cpus = 0;
std::atomic<bool> per_cpu_ready{false};  // Set after per-CPU structures are initialized

// Safe CPU ID getter - falls back to APIC ID during early boot
inline auto get_current_cpu_id() -> uint64_t {
    if (per_cpu_ready.load(std::memory_order_acquire)) {
        return cpu::current_cpu();
    }
    // Early boot: use APIC ID
    uint32_t const APIC_ID = apic::get_apic_id();
    if (num_cpus > 0) {
        uint64_t const CPU_IDX = smt::get_cpu_index_from_apic_id(APIC_ID);
        return CPU_IDX;
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
struct alignas(MEMORY_ALIGNMENT) MediumAllocationHeader {
    MediumAllocationHeader* next;
    uint64_t size;   // Total allocation size including header
    uint64_t magic;  // For validation
#if defined(WOS_KASAN) || defined(WOS_KUBSAN)
    uint32_t debug_idx;  // Index into s_alloc_debug (0 = none)
    uint32_t pad;
#else
    uint64_t _pad;  // Pad to 32 bytes so (header + 1) is 16-byte aligned
#endif
    // Data follows immediately after this header
};

constexpr uint64_t MEDIUM_ALLOC_MAGIC = 0xCAFEBABE87654321ULL;

// Large allocation header for sizes >= 0x10000
struct alignas(MEMORY_ALIGNMENT) LargeAllocationHeader {
    LargeAllocationHeader* next;
    uint64_t size;
    uint64_t magic;  // For validation
#if defined(WOS_KASAN) || defined(WOS_KUBSAN)
    uint32_t debug_idx;  // Index into s_alloc_debug (0 = none)
    uint32_t pad;
#else
    uint64_t _pad;  // Pad to 32 bytes so (header + 1) is 16-byte aligned
#endif
    // Data follows immediately after this header
};

constexpr uint64_t LARGE_ALLOC_MAGIC = 0xDEADBEEF12345678ULL;

static_assert(sizeof(MediumAllocationHeader) == sizeof(LargeAllocationHeader), "allocation header sizes must match");
static_assert(sizeof(MediumAllocationHeader) == sizeof(void*) + (3 * sizeof(uint64_t)),
              "MediumAllocationHeader must be 32 bytes (no accidental padding)");

MediumAllocationHeader* medium_alloc_list = nullptr;
sys::Spinlock medium_alloc_lock;

LargeAllocationHeader* large_alloc_list = nullptr;
sys::Spinlock large_alloc_lock;

#if defined(WOS_KASAN) || defined(WOS_KUBSAN)
// Per-allocation debug side-table.  Stored in .bss so it is always available during OOM.
// Index 0 is a sentinel meaning "no info".  The counter never wraps back — once full,
// new allocations silently get index 0.  64 KB total; zero runtime cost in release builds.
struct AllocDebugInfo {
    uintptr_t caller;  // return address captured at the kmalloc call site
    const char* tag;   // compile-time string (type name or "::new"), may be null
};
constexpr uint32_t ALLOC_DEBUG_NONE = 0;
constexpr size_t ALLOC_DEBUG_MAX = 4096;
__attribute__((section(".bss"))) std::array<AllocDebugInfo, ALLOC_DEBUG_MAX> s_alloc_debug{};
std::atomic<uint32_t> s_alloc_debug_next{1};

auto register_alloc_debug(uintptr_t caller, const char* tag) -> uint32_t {
    uint32_t const SLOT = s_alloc_debug_next.fetch_add(1, std::memory_order_relaxed);
    if (SLOT >= ALLOC_DEBUG_MAX) {
        return ALLOC_DEBUG_NONE;
    }
    s_alloc_debug[SLOT] = {.caller = caller, .tag = tag};
    return SLOT;
}
#endif
}  // namespace

void init() {
    // Initialize per-CPU allocators
    num_cpus = smt::get_early_cpu_count();

    mini_malloc::mini_malloc_init();

    // Allocate per-CPU allocator structures using mini_malloc
    per_cpu_allocators = static_cast<PerCpuAllocator*>(mini_malloc::mini_malloc(sizeof(PerCpuAllocator) * num_cpus));
    if (per_cpu_allocators != nullptr) {
        for (size_t i = 0; i < num_cpus; i++) {
            new (&per_cpu_allocators[i]) PerCpuAllocator();
            per_cpu_allocators[i].initialized = true;
        }
    }
}

void enable_per_cpu_allocations() { per_cpu_ready.store(true, std::memory_order_release); }

void dump_tracked_allocations() {
    uint64_t const MEDIUM_LOCK_FLAGS = medium_alloc_lock.lock_irqsave();
    uint64_t medium_total_bytes = 0;
    uint64_t medium_count = 0;
    ker::mod::io::serial::write("kmalloc: Medium allocations (0x801-0xFFFF):\n");

    for (MediumAllocationHeader const* curr = medium_alloc_list; curr != nullptr; curr = curr->next) {
        if (curr->magic == MEDIUM_ALLOC_MAGIC) {
            medium_count++;
            medium_total_bytes += curr->size;
            ker::mod::io::serial::write("  addr=0x");
            ker::mod::io::serial::write_hex((uint64_t)(curr + 1));
            ker::mod::io::serial::write(" size=0x");
            ker::mod::io::serial::write_hex(curr->size);
#if defined(WOS_KASAN) || defined(WOS_KUBSAN)
            if (curr->debug_idx != ALLOC_DEBUG_NONE && curr->debug_idx < ALLOC_DEBUG_MAX) {
                const auto& d = s_alloc_debug[curr->debug_idx];
                if (d.caller != 0) {
                    ker::mod::io::serial::write(" caller=0x");
                    ker::mod::io::serial::write_hex(d.caller);
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
    medium_alloc_lock.unlock_irqrestore(MEDIUM_LOCK_FLAGS);

    ker::mod::io::serial::write("  medium_total: 0x");
    ker::mod::io::serial::write_hex(medium_count);
    ker::mod::io::serial::write(" entries, 0x");
    ker::mod::io::serial::write_hex(medium_total_bytes);
    ker::mod::io::serial::write(" bytes\n");

    uint64_t const LARGE_LOCK_FLAGS = large_alloc_lock.lock_irqsave();
    uint64_t large_total_bytes = 0;
    uint64_t large_count = 0;
    ker::mod::io::serial::write("kmalloc: Large allocations (>=0x10000):\n");

    for (LargeAllocationHeader const* curr = large_alloc_list; curr != nullptr; curr = curr->next) {
        if (curr->magic == LARGE_ALLOC_MAGIC) {
            large_count++;
            large_total_bytes += curr->size;
            ker::mod::io::serial::write("  addr=0x");
            ker::mod::io::serial::write_hex((uint64_t)(curr + 1));  // Data starts after header
            ker::mod::io::serial::write(" size=0x");
            ker::mod::io::serial::write_hex(curr->size);
#if defined(WOS_KASAN) || defined(WOS_KUBSAN)
            if (curr->debug_idx != ALLOC_DEBUG_NONE && curr->debug_idx < ALLOC_DEBUG_MAX) {
                const auto& d = s_alloc_debug[curr->debug_idx];
                if (d.caller != 0) {
                    ker::mod::io::serial::write(" caller=0x");
                    ker::mod::io::serial::write_hex(d.caller);
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
    ker::mod::io::serial::write_hex(large_count);
    ker::mod::io::serial::write(" entries, 0x");
    ker::mod::io::serial::write_hex(large_total_bytes);
    ker::mod::io::serial::write(" bytes\n");
    large_alloc_lock.unlock_irqrestore(LARGE_LOCK_FLAGS);

#if defined(WOS_KASAN) || defined(WOS_KUBSAN)
    ker::mod::io::serial::write("kmalloc: Slab live allocations with debug info (KASAN/KUBSAN):\n");
    mini_malloc::mini_iter_live_debug_slots(nullptr, [](void* /*ud*/, const void* ptr, size_t sz, uint32_t dbg_idx) -> void {
        if (dbg_idx == ALLOC_DEBUG_NONE || dbg_idx >= ALLOC_DEBUG_MAX) {
            return;
        }
        const auto& d = s_alloc_debug[dbg_idx];
        if (d.caller == 0) {
            return;
        }
        ker::mod::io::serial::write("  addr=0x");
        ker::mod::io::serial::write_hex((uint64_t)ptr);
        ker::mod::io::serial::write(" sz=0x");
        ker::mod::io::serial::write_hex(sz);
        ker::mod::io::serial::write(" caller=0x");
        ker::mod::io::serial::write_hex(d.caller);
        if (d.tag != nullptr) {
            ker::mod::io::serial::write(" tag=");
            ker::mod::io::serial::write(d.tag);
        }
        ker::mod::io::serial::write("\n");
    });
#endif
}

void get_tracked_alloc_totals(uint64_t& out_count, uint64_t& out_bytes) {
    out_count = 0;
    out_bytes = 0;

    uint64_t const MEDIUM_LOCK_FLAGS = medium_alloc_lock.lock_irqsave();
    for (MediumAllocationHeader const* curr = medium_alloc_list; curr != nullptr; curr = curr->next) {
        if (curr->magic == MEDIUM_ALLOC_MAGIC) {
            out_count++;
            out_bytes += curr->size;
        }
    }
    medium_alloc_lock.unlock_irqrestore(MEDIUM_LOCK_FLAGS);

    uint64_t const LARGE_LOCK_FLAGS = large_alloc_lock.lock_irqsave();
    for (LargeAllocationHeader const* curr = large_alloc_list; curr != nullptr; curr = curr->next) {
        if (curr->magic == LARGE_ALLOC_MAGIC) {
            out_count++;
            out_bytes += curr->size;
        }
    }
    large_alloc_lock.unlock_irqrestore(LARGE_LOCK_FLAGS);
}
namespace {
enum class SlabClass : int8_t {
    SLAB_0X10 = 0,
    SLAB_0X20 = 1,
    SLAB_0X40 = 2,
    SLAB_0X80 = 3,
    SLAB_0X100 = 4,
    SLAB_0X200 = 5,
    SLAB_0X300 = 6,
    SLAB_0X400 = 7,
    SLAB_0X800 = 8,
    SLAB_INVALID = -1
};

auto size_to_slab_idx(uint64_t size) -> SlabClass {
    if (size <= mini_malloc::SLAB_SIZE10) {
        return SlabClass::SLAB_0X10;
    }
    if (size <= mini_malloc::SLAB_SIZE20) {
        return SlabClass::SLAB_0X20;
    }
    if (size <= mini_malloc::SLAB_SIZE40) {
        return SlabClass::SLAB_0X40;
    }
    if (size <= mini_malloc::SLAB_SIZE80) {
        return SlabClass::SLAB_0X80;
    }
    if (size <= mini_malloc::SLAB_SIZE100) {
        return SlabClass::SLAB_0X100;
    }
    if (size <= mini_malloc::SLAB_SIZE200) {
        return SlabClass::SLAB_0X200;
    }
    if (size <= mini_malloc::SLAB_SIZE300) {
        return SlabClass::SLAB_0X300;
    }
    if (size <= mini_malloc::SLAB_SIZE400) {
        return SlabClass::SLAB_0X400;
    }
    if (size <= mini_malloc::SLAB_SIZE800) {
        return SlabClass::SLAB_0X800;
    }
    return SlabClass::SLAB_INVALID;
}

auto slab_size_to_idx(size_t slab_size) -> SlabClass {
    switch (slab_size) {
        case mini_malloc::SLAB_SIZE10:
            return SlabClass::SLAB_0X10;
        case mini_malloc::SLAB_SIZE20:
            return SlabClass::SLAB_0X20;
        case mini_malloc::SLAB_SIZE40:
            return SlabClass::SLAB_0X40;
        case mini_malloc::SLAB_SIZE80:
            return SlabClass::SLAB_0X80;
        case mini_malloc::SLAB_SIZE100:
            return SlabClass::SLAB_0X100;
        case mini_malloc::SLAB_SIZE200:
            return SlabClass::SLAB_0X200;
        case mini_malloc::SLAB_SIZE300:
            return SlabClass::SLAB_0X300;
        case mini_malloc::SLAB_SIZE400:
            return SlabClass::SLAB_0X400;
        case mini_malloc::SLAB_SIZE800:
            return SlabClass::SLAB_0X800;
        default:
            return SlabClass::SLAB_INVALID;
    }
}

auto malloc_impl(uint64_t size, uintptr_t caller, const char* tag) -> void* {
    if (size == 0) {
        return nullptr;
    }

    // Tier 1: Small allocations (0x1 - 0x800) - magazine fast path, slab slow path
    if (size <= SLAB_MAX_SIZE) {
        SlabClass const IDX = size_to_slab_idx(size);
        void* ptr = nullptr;
        if (IDX != SlabClass::SLAB_INVALID && per_cpu_allocators != nullptr && per_cpu_ready.load(std::memory_order_acquire)) {
            // NOLINTNEXTLINE(misc-const-correctness)
            uint64_t flags = 0;
            asm volatile("pushfq; popq %0; cli" : "=r"(flags));
            uint64_t const CPU_ID = get_current_cpu_id();
            auto& cpu = per_cpu_allocators[CPU_ID];
            if (cpu.initialized && cpu.mag_count[static_cast<int>(IDX)] > 0) {
                ptr = cpu.magazine[static_cast<int>(IDX)][--cpu.mag_count[static_cast<int>(IDX)]];
                if ((flags & cpu::GATE_IF_MASK) != 0U) {
                    asm volatile("sti");
                }
            } else {
                if ((flags & cpu::GATE_IF_MASK) != 0U) {
                    asm volatile("sti");
                }
            }
        }
        // Slow path: mini_malloc acquires per-size-class slab_lock internally
        if (ptr == nullptr) {
            ptr = mini_malloc::mini_malloc(size);
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
        uint64_t const PAGE_SIZE = ker::mod::mm::paging::PAGE_SIZE;
        uint64_t const TOTAL_SIZE = size + sizeof(MediumAllocationHeader);
        uint64_t const ALLOC_SIZE = (TOTAL_SIZE + PAGE_SIZE - 1) & ~(PAGE_SIZE - 1);

#ifdef DEBUG_KMALLOC
        ker::mod::io::serial::write("kmalloc: Medium allocation (0x");
        ker::mod::io::serial::writeHex(size);
        ker::mod::io::serial::write(" bytes), using pageAlloc (0x");
        ker::mod::io::serial::writeHex(alloc_size);
        ker::mod::io::serial::write(" bytes)\n");
#endif

        void* alloc_ptr = phys::page_alloc(ALLOC_SIZE);
        if (alloc_ptr == nullptr) {
#ifdef DEBUG_KMALLOC
            ker::mod::io::serial::write("kmalloc: pageAlloc failed for medium allocation\n");
#endif
            return nullptr;
        }

        // Set up header with tracking info
        auto* header = static_cast<MediumAllocationHeader*>(alloc_ptr);
        header->size = ALLOC_SIZE;

        // Add to linked list and set magic under the same lock.
        // Magic must be set while holding the lock so that any concurrent free() that
        // sees magic set is guaranteed to find the entry in the list (no TOCTOU window).
        // lock_irqsave: kmalloc::free runs from the timer ISR via gc_expired_tasks, so
        // a non-IRQ caller holding mediumAllocLock would deadlock against the ISR.
        uint64_t const LOCK_FLAGS = medium_alloc_lock.lock_irqsave();
        header->magic = MEDIUM_ALLOC_MAGIC;
        header->next = medium_alloc_list;
        medium_alloc_list = header;
        medium_alloc_lock.unlock_irqrestore(LOCK_FLAGS);

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
    uint64_t const PAGE_SIZE = ker::mod::mm::paging::PAGE_SIZE;
    uint64_t const TOTAL_SIZE = size + sizeof(LargeAllocationHeader);
    uint64_t const ALLOC_SIZE = (TOTAL_SIZE + PAGE_SIZE - 1) & ~(PAGE_SIZE - 1);

#ifdef DEBUG_KMALLOC
    ker::mod::io::serial::write("kmalloc: Large allocation (0x");
    ker::mod::io::serial::writeHex(size);
    ker::mod::io::serial::write(" bytes), using pageAllocHuge (0x");
    ker::mod::io::serial::writeHex(alloc_size);
    ker::mod::io::serial::write(" bytes)\n");
#endif

    // Allocate from huge page zone
    void* alloc_ptr = phys::page_alloc_huge(ALLOC_SIZE);
    if (alloc_ptr == nullptr) {
        // Fallback to regular pageAlloc if huge zone is full
        alloc_ptr = phys::page_alloc(ALLOC_SIZE);
        if (alloc_ptr == nullptr) {
#ifdef DEBUG_KMALLOC
            ker::mod::io::serial::write("kmalloc: pageAlloc failed for large allocation\n");
#endif
            return nullptr;
        }
    }

    // Set up header with tracking info
    auto* header = static_cast<LargeAllocationHeader*>(alloc_ptr);
    header->size = ALLOC_SIZE;

    // Add to linked list and set magic under the same lock.
    // Magic must be set while holding the lock so that any concurrent free() that
    // sees magic set is guaranteed to find the entry in the list (no TOCTOU window).
    // lock_irqsave: see matching comment on the medium-tier insert above.
    uint64_t const LOCK_FLAGS = large_alloc_lock.lock_irqsave();
    header->magic = LARGE_ALLOC_MAGIC;
    header->next = large_alloc_list;
    large_alloc_list = header;
    large_alloc_lock.unlock_irqrestore(LOCK_FLAGS);

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

// Tri-state result for the medium/large free helpers.
//   NotTracked: header magic does not match this tier; caller should try the next tier.
//   Freed:      header was found in the tracker list, spliced out, and magic cleared.
//   DoubleFree: header magic matched but the entry is not in the tracker list — genuine
//               double-free or list corruption. Caller should panic.
enum class TrackedFreeResult : uint8_t { NOT_TRACKED, FREED, DOUBLE_FREE };

// Find, splice, and clear-magic for a medium-tier allocation, all under one
// acquisition of mediumAllocLock. The magic read, list walk, and magic clear
// must be atomic w.r.t. other free()/malloc() callers — otherwise a concurrent
// free of the same pointer can race past the magic check after the entry was
// removed but before magic was zeroed, and panic spuriously.
auto try_free_medium_alloc(void* data_ptr, uint64_t& out_size) -> TrackedFreeResult {
    auto* header = static_cast<MediumAllocationHeader*>(data_ptr) - 1;

    uint64_t const FLAGS = medium_alloc_lock.lock_irqsave();
    if (header->magic != MEDIUM_ALLOC_MAGIC) {
        medium_alloc_lock.unlock_irqrestore(FLAGS);
        return TrackedFreeResult::NOT_TRACKED;
    }

    MediumAllocationHeader** prev = &medium_alloc_list;
    for (MediumAllocationHeader* curr = medium_alloc_list; curr != nullptr; prev = &curr->next, curr = curr->next) {
        if (curr == header) {
            *prev = curr->next;
            out_size = curr->size;
            // Clear magic while still holding the lock so a concurrent free()
            // observes either (magic set AND in list) or (magic clear). The
            // (magic set AND not in list) state is no longer reachable except
            // via real corruption / double-free.
            header->magic = 0;
            medium_alloc_lock.unlock_irqrestore(FLAGS);
            return TrackedFreeResult::FREED;
        }
    }

    // Still holding the lock — dump diagnostic info before unlocking.
    // Keeping the lock ensures the list is stable during the dump.

    // Helper lambda to print debug_idx info for a node.
    auto print_debug_info = [](const MediumAllocationHeader* node) {
#if defined(WOS_KASAN) || defined(WOS_KUBSAN)
        if (node->debug_idx != ALLOC_DEBUG_NONE && node->debug_idx < ALLOC_DEBUG_MAX) {
            const auto& d = s_alloc_debug[node->debug_idx];
            if (d.caller != 0) {
                ker::mod::io::serial::write(" caller=0x");
                ker::mod::io::serial::write_hex(d.caller);
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
    ker::mod::io::serial::write_hex(reinterpret_cast<uint64_t>(header));
    ker::mod::io::serial::write(" size=0x");
    ker::mod::io::serial::write_hex(header->size);
    print_debug_info(header);
    ker::mod::io::serial::write("):\n");

    uint32_t n = 0;
    MediumAllocationHeader const* last_node = nullptr;
    bool found_corrupt = false;
    for (MediumAllocationHeader const* c = medium_alloc_list; c != nullptr && n < 8192; c = c->next, ++n) {
        if (c->magic != MEDIUM_ALLOC_MAGIC) {
            ker::mod::io::serial::write("  BAD node=0x");
            ker::mod::io::serial::write_hex(reinterpret_cast<uint64_t>(c));
            ker::mod::io::serial::write(" size=0x");
            ker::mod::io::serial::write_hex(c->size);
            ker::mod::io::serial::write(" magic=0x");
            ker::mod::io::serial::write_hex(c->magic);
            ker::mod::io::serial::write(" (prev had this as next)\n");
            found_corrupt = true;
            break;
        }
        MediumAllocationHeader const* nxt = c->next;
        if (nxt != nullptr && nxt->magic != MEDIUM_ALLOC_MAGIC) {
            ker::mod::io::serial::write("  CORRUPT node=0x");
            ker::mod::io::serial::write_hex(reinterpret_cast<uint64_t>(c));
            ker::mod::io::serial::write(" size=0x");
            ker::mod::io::serial::write_hex(c->size);
            print_debug_info(c);
            ker::mod::io::serial::write(" ->next=0x");
            ker::mod::io::serial::write_hex(reinterpret_cast<uint64_t>(nxt));
            ker::mod::io::serial::write(" (next_magic=0x");
            ker::mod::io::serial::write_hex(nxt->magic);
            ker::mod::io::serial::write(")\n");
            const auto* data = reinterpret_cast<const uint64_t*>(c + 1);
            ker::mod::io::serial::write("  CORRUPT node data[0..3]: 0x");
            ker::mod::io::serial::write_hex(data[0]);
            ker::mod::io::serial::write(" 0x");
            ker::mod::io::serial::write_hex(data[1]);
            ker::mod::io::serial::write(" 0x");
            ker::mod::io::serial::write_hex(data[2]);
            ker::mod::io::serial::write(" 0x");
            ker::mod::io::serial::write_hex(data[3]);
            ker::mod::io::serial::write("\n");
            found_corrupt = true;
            break;
        }
        last_node = c;
    }
    if (!found_corrupt && last_node != nullptr) {
        // Chain ended without finding target — the predecessor of target had its
        // ->next overwritten with null (or some other valid node, skipping target).
        ker::mod::io::serial::write("  TRUNCATED: last valid node=0x");
        ker::mod::io::serial::write_hex(reinterpret_cast<uint64_t>(last_node));
        ker::mod::io::serial::write(" size=0x");
        ker::mod::io::serial::write_hex(last_node->size);
        print_debug_info(last_node);
        ker::mod::io::serial::write(" ->next=0x0\n");
        const auto* data = reinterpret_cast<const uint64_t*>(last_node + 1);
        ker::mod::io::serial::write("  TRUNCATED node data[0..7]: 0x");
        ker::mod::io::serial::write_hex(data[0]);
        ker::mod::io::serial::write(" 0x");
        ker::mod::io::serial::write_hex(data[1]);
        ker::mod::io::serial::write(" 0x");
        ker::mod::io::serial::write_hex(data[2]);
        ker::mod::io::serial::write(" 0x");
        ker::mod::io::serial::write_hex(data[3]);
        ker::mod::io::serial::write(" 0x");
        ker::mod::io::serial::write_hex(data[4]);
        ker::mod::io::serial::write(" 0x");
        ker::mod::io::serial::write_hex(data[5]);
        ker::mod::io::serial::write(" 0x");
        ker::mod::io::serial::write_hex(data[6]);
        ker::mod::io::serial::write(" 0x");
        ker::mod::io::serial::write_hex(data[7]);
        ker::mod::io::serial::write("\n");
    }
    ker::mod::io::serial::write("kmalloc: DoubleFree chain dump done (");
    ker::mod::io::serial::write_hex(n);
    ker::mod::io::serial::write(" nodes walked)\n");

    medium_alloc_lock.unlock_irqrestore(FLAGS);

    return TrackedFreeResult::DOUBLE_FREE;
}

auto try_free_large_alloc(void* data_ptr, uint64_t& out_size) -> TrackedFreeResult {
    auto* header = static_cast<LargeAllocationHeader*>(data_ptr) - 1;

    uint64_t const FLAGS = large_alloc_lock.lock_irqsave();
    if (header->magic != LARGE_ALLOC_MAGIC) {
        large_alloc_lock.unlock_irqrestore(FLAGS);
        return TrackedFreeResult::NOT_TRACKED;
    }

    LargeAllocationHeader** prev = &large_alloc_list;
    for (LargeAllocationHeader* curr = large_alloc_list; curr != nullptr; prev = &curr->next, curr = curr->next) {
        if (curr == header) {
            *prev = curr->next;
            out_size = curr->size;
            header->magic = 0;
            large_alloc_lock.unlock_irqrestore(FLAGS);
            return TrackedFreeResult::FREED;
        }
    }

    large_alloc_lock.unlock_irqrestore(FLAGS);
    return TrackedFreeResult::DOUBLE_FREE;
}
}  // namespace

auto malloc(uint64_t size) -> void* {
#if defined(WOS_KASAN) || defined(WOS_KUBSAN)
    return malloc_impl(size, (uintptr_t)__builtin_return_address(0), nullptr);
#else
    return malloc_impl(size, 0, nullptr);
#endif
}

auto malloc_tagged(uint64_t size, uintptr_t caller, const char* tag) -> void* { return malloc_impl(size, caller, tag); }

auto realloc(void* ptr, size_t size) -> void* {
    if (ptr == nullptr) {
        return malloc(static_cast<uint64_t>(size));
    }

    if (size <= 0) {
        free(ptr);
        return nullptr;
    }

    auto const NEW_SIZE = static_cast<uint64_t>(size);

    // Determine the type of the existing allocation by checking headers
    auto* potential_large_header = static_cast<LargeAllocationHeader*>(ptr) - 1;
    auto* potential_medium_header = static_cast<MediumAllocationHeader*>(ptr) - 1;

    // Case 1: Current allocation is LARGE (>= 0x10000)
    if (potential_large_header->magic == LARGE_ALLOC_MAGIC) {
        uint64_t const OLD_SIZE = potential_large_header->size - sizeof(LargeAllocationHeader);

        // Staying in large range?
        if (NEW_SIZE > MEDIUM_MAX_SIZE) {
            uint64_t const PAGE_SIZE = ker::mod::mm::paging::PAGE_SIZE;
            uint64_t const NEW_ALLOC_SIZE = (NEW_SIZE + sizeof(LargeAllocationHeader) + PAGE_SIZE - 1) & ~(PAGE_SIZE - 1);

            // If the new size fits in the current allocation, return same pointer
            if (NEW_ALLOC_SIZE == potential_large_header->size) {
                return ptr;
            }

            // Need to reallocate - allocate new, copy, free old
            void* new_alloc = phys::page_alloc_huge(NEW_ALLOC_SIZE);
            if (new_alloc == nullptr) {
                new_alloc = phys::page_alloc(NEW_ALLOC_SIZE);
                if (new_alloc == nullptr) {
                    return nullptr;
                }
            }

            auto* new_header = static_cast<LargeAllocationHeader*>(new_alloc);
            new_header->size = NEW_ALLOC_SIZE;

            uint64_t const COPY_SIZE = (OLD_SIZE < NEW_SIZE) ? OLD_SIZE : NEW_SIZE;
            memcpy(new_header + 1, ptr, COPY_SIZE);

            // Update linked list; set magic under the lock alongside insertion
            // so there is no window where magic is set but the entry is not yet in the list.
            // Clear the old header's magic inside the same critical section so a
            // concurrent free observes either (old in list) or (old fully gone).
            uint64_t const LOCK_FLAGS = large_alloc_lock.lock_irqsave();
            new_header->magic = LARGE_ALLOC_MAGIC;
            LargeAllocationHeader** prev = &large_alloc_list;
            for (LargeAllocationHeader* curr = large_alloc_list; curr != nullptr; prev = &curr->next, curr = curr->next) {
                if (curr == potential_large_header) {
                    *prev = curr->next;
                    break;
                }
            }
            new_header->next = large_alloc_list;
            large_alloc_list = new_header;
            potential_large_header->magic = 0;
            large_alloc_lock.unlock_irqrestore(LOCK_FLAGS);
#if defined(WOS_KASAN) || defined(WOS_KUBSAN)
            new_header->debug_idx = ALLOC_DEBUG_NONE;
#endif
            phys::page_free(potential_large_header);
            return static_cast<void*>(new_header + 1);
        }

        // Transitioning from large to medium or small
        void* new_ptr = malloc(NEW_SIZE);
        if (new_ptr != nullptr) {
            uint64_t const COPY_SIZE = (OLD_SIZE < NEW_SIZE) ? OLD_SIZE : NEW_SIZE;
            memcpy(new_ptr, ptr, COPY_SIZE);
            free(ptr);
        }
        return new_ptr;
    }

    // Case 2: Current allocation is MEDIUM (0x801 - 0xFFFF)
    if (potential_medium_header->magic == MEDIUM_ALLOC_MAGIC) {
        uint64_t const OLD_SIZE = potential_medium_header->size - sizeof(MediumAllocationHeader);

        // Staying in medium range?
        if (NEW_SIZE > SLAB_MAX_SIZE && NEW_SIZE <= MEDIUM_MAX_SIZE) {
            uint64_t const PAGE_SIZE = ker::mod::mm::paging::PAGE_SIZE;
            uint64_t const NEW_ALLOC_SIZE = (NEW_SIZE + sizeof(MediumAllocationHeader) + PAGE_SIZE - 1) & ~(PAGE_SIZE - 1);

            // If the new size fits in the current allocation, return same pointer
            if (NEW_ALLOC_SIZE == potential_medium_header->size) {
                return ptr;
            }

            // Need to reallocate - allocate new, copy, free old
            void* new_alloc = phys::page_alloc(NEW_ALLOC_SIZE);
            if (new_alloc == nullptr) {
                return nullptr;
            }

            auto* new_header = static_cast<MediumAllocationHeader*>(new_alloc);
            new_header->size = NEW_ALLOC_SIZE;

            uint64_t const COPY_SIZE = (OLD_SIZE < NEW_SIZE) ? OLD_SIZE : NEW_SIZE;
            memcpy(new_header + 1, ptr, COPY_SIZE);

            // Update linked list; set magic under the lock alongside insertion
            // so there is no window where magic is set but the entry is not yet in the list.
            // Clear the old header's magic inside the same critical section so a
            // concurrent free observes either (old in list) or (old fully gone).
            uint64_t const LOCK_FLAGS = medium_alloc_lock.lock_irqsave();
            new_header->magic = MEDIUM_ALLOC_MAGIC;
            MediumAllocationHeader** prev = &medium_alloc_list;
            for (MediumAllocationHeader* curr = medium_alloc_list; curr != nullptr; prev = &curr->next, curr = curr->next) {
                if (curr == potential_medium_header) {
                    *prev = curr->next;
                    break;
                }
            }
            new_header->next = medium_alloc_list;
            medium_alloc_list = new_header;
            potential_medium_header->magic = 0;
            medium_alloc_lock.unlock_irqrestore(LOCK_FLAGS);
#if defined(WOS_KASAN) || defined(WOS_KUBSAN)
            new_header->debug_idx = ALLOC_DEBUG_NONE;
#endif
            phys::page_free(potential_medium_header);
            return static_cast<void*>(new_header + 1);
        }

        // Transitioning from medium to large or small
        void* new_ptr = malloc(NEW_SIZE);
        if (new_ptr != nullptr) {
            uint64_t const COPY_SIZE = (OLD_SIZE < NEW_SIZE) ? OLD_SIZE : NEW_SIZE;
            memcpy(new_ptr, ptr, COPY_SIZE);
            free(ptr);
        }
        return new_ptr;
    }

    // Case 3: Current allocation is SMALL (<= 0x800) - from mini_malloc
    // We don't know the exact old size, so we'll allocate new and copy what we can

    // Staying in small range?
    if (NEW_SIZE <= SLAB_MAX_SIZE) {
        void* new_ptr = malloc(NEW_SIZE);
        if (new_ptr != nullptr && new_ptr != ptr) {
            memcpy(new_ptr, ptr, NEW_SIZE);
            free(ptr);
        }
        return new_ptr;
    }

    // Transitioning from small to medium or large
    void* new_ptr = malloc(NEW_SIZE);
    if (new_ptr != nullptr) {
        // We don't know the old size, but it's at most SLAB_MAX_SIZE
        // Copy up to newSize (safe because old allocation is at least as large as requested)
        uint64_t const COPY_SIZE = (SLAB_MAX_SIZE < NEW_SIZE) ? SLAB_MAX_SIZE : NEW_SIZE;
        memcpy(new_ptr, ptr, COPY_SIZE);
        free(ptr);
    }
    return new_ptr;
}

auto calloc(size_t nmemb, size_t size) -> void* {
    if (nmemb == 0 || size == 0) {
        return nullptr;
    }

    // Check for overflow
    if (nmemb > SIZE_MAX / size) {
        return nullptr;
    }

    size_t const TOTAL = nmemb * size;
    void* ptr = malloc(TOTAL);
    if (ptr != nullptr) [[likely]] {
        memset(ptr, 0, TOTAL);
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
        TrackedFreeResult const R = try_free_large_alloc(ptr, size);
        if (R == TrackedFreeResult::FREED) {
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
            phys::page_free(potential_large_header);  // Free from header, not data ptr
            return;
        }
        if (R == TrackedFreeResult::DOUBLE_FREE) {
            ker::mod::dbg::panic_handler("kmalloc: Double-free or corrupted large allocation detected");
        }
    }

    // Try the medium-allocation tier (0x801 - 0xFFFF).
    auto* potential_medium_header = static_cast<MediumAllocationHeader*>(ptr) - 1;
    {
        uint64_t size = 0;
        TrackedFreeResult const R = try_free_medium_alloc(ptr, size);
        if (R == TrackedFreeResult::FREED) {
#ifdef DEBUG_KMALLOC
            ker::mod::io::serial::write("kmalloc: Freeing medium allocation at 0x");
            ker::mod::io::serial::writeHex((uint64_t)ptr);
            ker::mod::io::serial::write(" (0x");
            ker::mod::io::serial::writeHex(size);
            ker::mod::io::serial::write(" bytes)\n");
#endif
#ifdef WOS_KASAN
            kasan::poison_range(static_cast<void*>(potential_medium_header), size, kasan::SHADOW_HEAP_FREED);
#endif
            phys::page_free(potential_medium_header);  // Free from header, not data ptr
            return;
        }
        if (R == TrackedFreeResult::DOUBLE_FREE) {
            ker::mod::dbg::panic_handler("kmalloc: Double-free or corrupted medium allocation detected");
        }
    }

    // Otherwise, it's a small allocation (<= 0x800) - magazine fast path, slab slow path
    size_t const SLAB_SZ = mini_malloc::mini_get_slab_size(ptr);
    int const IDX = (SLAB_SZ > 0) ? static_cast<int>(slab_size_to_idx(SLAB_SZ)) : -1;

#ifdef WOS_KASAN
    // Poison the slab chunk as freed before returning it to the magazine/slab.
    if (SLAB_SZ > 0) {
        kasan::poison_range(ptr, SLAB_SZ, kasan::SHADOW_HEAP_FREED);
    }
#endif

    if (IDX >= 0 && per_cpu_allocators != nullptr && per_cpu_ready.load(std::memory_order_acquire)) {
        // NOLINTNEXTLINE(misc-const-correctness)
        uint64_t flags = 0;
        asm volatile("pushfq; popq %0; cli" : "=r"(flags));
        uint64_t const CPU_ID = get_current_cpu_id();
        auto& cpu = per_cpu_allocators[CPU_ID];
        if (cpu.initialized) {
            if (cpu.mag_count[IDX] < MAGAZINE_CAPACITY) {
                cpu.magazine[IDX][cpu.mag_count[IDX]++] = ptr;
                if ((flags & 0x200) != 0U) {
                    asm volatile("sti");
                }
                return;
            }
            // Magazine full: flush all entries to slab, push new ptr, then drain
            uint8_t const CNT = cpu.mag_count[IDX];
            std::array<void*, MAGAZINE_CAPACITY> batch{};
            for (uint8_t i = 0; i < CNT; i++) {
                batch[i] = cpu.magazine[IDX][i];
            }
            cpu.mag_count[IDX] = 0;
            cpu.magazine[IDX][cpu.mag_count[IDX]++] = ptr;
            if ((flags & cpu::GATE_IF_MASK) != 0U) {
                asm volatile("sti");
            }
            for (uint8_t i = 0; i < CNT; i++) {
                mini_malloc::mini_free(batch[i]);
            }
            return;
        }
        if ((flags & cpu::GATE_IF_MASK) != 0U) {
            asm volatile("sti");
        }
    }

    // Slow path: mini_free acquires per-size-class slab_lock internally
    mini_malloc::mini_free(ptr);
}

}  // namespace ker::mod::mm::dyn::kmalloc

auto operator new(std::size_t sz) -> void* {
#if defined(WOS_KASAN) || defined(WOS_KUBSAN)
    return ker::mod::mm::dyn::kmalloc::malloc_tagged(sz, (uintptr_t)__builtin_return_address(0), "::new");
#else
    return ker::mod::mm::dyn::kmalloc::malloc(sz);
#endif
}

auto operator new[](std::size_t sz) -> void* {
#if defined(WOS_KASAN) || defined(WOS_KUBSAN)
    return ker::mod::mm::dyn::kmalloc::malloc_tagged(sz, (uintptr_t)__builtin_return_address(0), "::new[]");
#else
    return ker::mod::mm::dyn::kmalloc::malloc(sz);
#endif
}

void operator delete(void* ptr, std::size_t size) noexcept {
    (void)size;
    ker::mod::mm::dyn::kmalloc::free(ptr);
}

void operator delete[](void* ptr, std::size_t size) noexcept {
    (void)size;
    ker::mod::mm::dyn::kmalloc::free(ptr);
}

void operator delete(void* ptr) noexcept { ker::mod::mm::dyn::kmalloc::free(ptr); }

void operator delete[](void* ptr) noexcept { ker::mod::mm::dyn::kmalloc::free(ptr); }

namespace std {
extern const nothrow_t nothrow{};
}

auto operator new(std::size_t size, std::nothrow_t const& /*tag*/) noexcept -> void* { return ker::mod::mm::dyn::kmalloc::malloc(size); }

auto operator new[](std::size_t size, std::nothrow_t const& /*tag*/) noexcept -> void* { return ker::mod::mm::dyn::kmalloc::malloc(size); }
