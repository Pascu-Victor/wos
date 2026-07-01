#include "kmalloc.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <new>
#include <platform/acpi/apic/apic.hpp>
#include <platform/asm/cpu.hpp>
#include <platform/smt/smt.hpp>

#include "minimalist_malloc/mini_malloc.hpp"
#include "minimalist_malloc/slab_allocator.hpp"
#include "platform/dbg/dbg.hpp"
#include "platform/mm/page_alloc.hpp"
#include "platform/mm/paging.hpp"
#include "platform/mm/phys.hpp"
#include "platform/sys/spinlock.hpp"

#ifdef WOS_KASAN
#include <sanitizer/kasan.hpp>
#endif

namespace ker::mod::mm::dyn::kmalloc {
namespace emergency_serial = ker::mod::dbg::emergency_serial_unlocked;

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
constexpr uint64_t MAX_BUDDY_ALLOCATION_BYTES = (uint64_t{1} << PageAllocator::MAX_ORDER) * ker::mod::mm::paging::PAGE_SIZE;

#ifdef WOS_KMALLOC_DEBUG_INFO
struct AllocDebugInfo;
#endif

// Medium allocation header for sizes 0x801 - 0xFFFF
struct alignas(MEMORY_ALIGNMENT) MediumAllocationHeader {
    MediumAllocationHeader* next;
    uint64_t size;   // Total allocation size including header
    uint64_t magic;  // For validation
#ifdef WOS_KMALLOC_DEBUG_INFO
    AllocDebugInfo* debug_info;
#else
    uint64_t pad;  // Pad to 32 bytes so (header + 1) is 16-byte aligned
#endif
    // Data follows immediately after this header
};

constexpr uint64_t MEDIUM_ALLOC_MAGIC = 0xCAFEBABE87654321ULL;

// Large allocation header for sizes >= 0x10000
struct alignas(MEMORY_ALIGNMENT) LargeAllocationHeader {
    LargeAllocationHeader* next;
    uint64_t size;
    uint64_t magic;  // For validation
#ifdef WOS_KMALLOC_DEBUG_INFO
    AllocDebugInfo* debug_info;
#else
    uint64_t pad;  // Pad to 32 bytes so (header + 1) is 16-byte aligned
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

#ifdef WOS_KMALLOC_DEBUG_INFO
struct AllocDebugInfo {
    uintptr_t caller;  // return address captured at the kmalloc call site
    const char* tag;   // compile-time string (type name or "::new"), may be null
    uint32_t generation;
    AllocDebugInfo* next_free;
};

struct AllocDebugBlock {
    AllocDebugBlock* next;
    uint32_t capacity;
};

constexpr size_t ALLOC_DEBUG_BLOCK_BYTES = ker::mod::mm::paging::PAGE_SIZE;
constexpr uint32_t ALLOC_DEBUG_BLOCK_ENTRIES =
    static_cast<uint32_t>((ALLOC_DEBUG_BLOCK_BYTES - sizeof(AllocDebugBlock)) / sizeof(AllocDebugInfo));
static_assert(ALLOC_DEBUG_BLOCK_ENTRIES > 0, "kmalloc debug block must fit at least one entry");

sys::Spinlock s_alloc_debug_lock;
AllocDebugBlock* s_alloc_debug_blocks = nullptr;
AllocDebugInfo* s_alloc_debug_free_list = nullptr;
std::atomic<uint32_t> s_alloc_debug_generation{1};
std::atomic<bool> s_alloc_debug_enabled{true};
std::atomic<uint64_t> s_alloc_debug_block_count{0};
std::atomic<uint64_t> s_alloc_debug_capacity{0};
std::atomic<uint64_t> s_alloc_debug_active{0};
std::atomic<uint64_t> s_alloc_debug_high_water{0};
std::atomic<uint64_t> s_alloc_debug_dropped{0};

auto current_debug_generation() -> uint32_t { return s_alloc_debug_generation.load(std::memory_order_acquire); }

auto debug_block_entries(AllocDebugBlock* block) -> AllocDebugInfo* { return reinterpret_cast<AllocDebugInfo*>(block + 1); }

void update_debug_high_water(uint64_t active) {
    uint64_t observed = s_alloc_debug_high_water.load(std::memory_order_relaxed);
    while (active > observed && !s_alloc_debug_high_water.compare_exchange_weak(observed, active, std::memory_order_relaxed)) {
    }
}

void push_debug_block_locked(AllocDebugBlock* block) {
    block->capacity = ALLOC_DEBUG_BLOCK_ENTRIES;
    block->next = s_alloc_debug_blocks;
    s_alloc_debug_blocks = block;

    auto* entries = debug_block_entries(block);
    for (uint32_t i = 0; i < ALLOC_DEBUG_BLOCK_ENTRIES; ++i) {
        entries[i] = AllocDebugInfo{.caller = 0, .tag = nullptr, .generation = 0, .next_free = s_alloc_debug_free_list};
        s_alloc_debug_free_list = &entries[i];
    }

    s_alloc_debug_block_count.fetch_add(1, std::memory_order_relaxed);
    s_alloc_debug_capacity.fetch_add(ALLOC_DEBUG_BLOCK_ENTRIES, std::memory_order_relaxed);
}

auto pop_debug_slot_locked() -> AllocDebugInfo* {
    AllocDebugInfo* info = s_alloc_debug_free_list;
    if (info != nullptr) {
        s_alloc_debug_free_list = info->next_free;
        info->next_free = nullptr;
    }
    return info;
}

auto allocate_debug_block() -> AllocDebugBlock* {
    return static_cast<AllocDebugBlock*>(phys::page_alloc_with_reclaim(ALLOC_DEBUG_BLOCK_BYTES, "kmalloc_debug"));
}

auto register_alloc_debug(uintptr_t caller, const char* tag) -> AllocDebugInfo* {
    if (!s_alloc_debug_enabled.load(std::memory_order_relaxed)) {
        return nullptr;
    }

    AllocDebugInfo* info = nullptr;
    uint64_t flags = s_alloc_debug_lock.lock_irqsave();
    info = pop_debug_slot_locked();
    s_alloc_debug_lock.unlock_irqrestore(flags);

    if (info == nullptr) {
        AllocDebugBlock* block = allocate_debug_block();
        if (block == nullptr) {
            s_alloc_debug_dropped.fetch_add(1, std::memory_order_relaxed);
            return nullptr;
        }

        flags = s_alloc_debug_lock.lock_irqsave();
        push_debug_block_locked(block);
        info = pop_debug_slot_locked();
        s_alloc_debug_lock.unlock_irqrestore(flags);
    }

    if (info == nullptr) {
        s_alloc_debug_dropped.fetch_add(1, std::memory_order_relaxed);
        return nullptr;
    }

    info->caller = caller;
    info->tag = tag;
    info->generation = current_debug_generation();
    uint64_t const ACTIVE = s_alloc_debug_active.fetch_add(1, std::memory_order_relaxed) + 1;
    update_debug_high_water(ACTIVE);
    return info;
}

void release_alloc_debug(AllocDebugInfo* info) {
    if (info == nullptr) {
        return;
    }

    uint64_t const FLAGS = s_alloc_debug_lock.lock_irqsave();
    info->caller = 0;
    info->tag = nullptr;
    info->generation = 0;
    info->next_free = s_alloc_debug_free_list;
    s_alloc_debug_free_list = info;
    s_alloc_debug_lock.unlock_irqrestore(FLAGS);

    uint64_t active = s_alloc_debug_active.load(std::memory_order_relaxed);
    while (active != 0 &&
           !s_alloc_debug_active.compare_exchange_weak(active, active - 1, std::memory_order_relaxed, std::memory_order_relaxed)) {
    }
}

auto debug_ref_has_current_generation(const AllocDebugInfo* info, uint64_t& caller, const char*& tag) -> bool {
    if (info == nullptr || info->caller == 0 || info->generation != current_debug_generation()) {
        return false;
    }
    caller = info->caller;
    tag = info->tag;
    return true;
}

auto debug_ref_has_any_generation(const AllocDebugInfo* info, uint64_t& caller, const char*& tag) -> bool {
    if (info == nullptr || info->caller == 0) {
        return false;
    }
    caller = info->caller;
    tag = info->tag;
    return true;
}

auto slab_debug_slot(void* ptr) -> AllocDebugInfo** {
    return reinterpret_cast<AllocDebugInfo**>(static_cast<uint8_t*>(ptr) - sizeof(uintptr_t));
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
    emergency_serial::write("kmalloc: Medium allocations (0x801-0xFFFF):\n");

    for (MediumAllocationHeader const* curr = medium_alloc_list; curr != nullptr; curr = curr->next) {
        if (curr->magic == MEDIUM_ALLOC_MAGIC) {
            medium_count++;
            medium_total_bytes += curr->size;
            emergency_serial::write("  addr=0x");
            emergency_serial::write_hex((uint64_t)(curr + 1));
            emergency_serial::write(" size=0x");
            emergency_serial::write_hex(curr->size);
#ifdef WOS_KMALLOC_DEBUG_INFO
            uint64_t caller = 0;
            const char* tag = nullptr;
            if (debug_ref_has_any_generation(curr->debug_info, caller, tag)) {
                emergency_serial::write(" caller=0x");
                emergency_serial::write_hex(caller);
                if (tag != nullptr) {
                    emergency_serial::write(" tag=");
                    emergency_serial::write(tag);
                }
            }
#endif
            emergency_serial::write("\n");
        }
    }
    medium_alloc_lock.unlock_irqrestore(MEDIUM_LOCK_FLAGS);

    emergency_serial::write("  medium_total: 0x");
    emergency_serial::write_hex(medium_count);
    emergency_serial::write(" entries, 0x");
    emergency_serial::write_hex(medium_total_bytes);
    emergency_serial::write(" bytes\n");

    uint64_t const LARGE_LOCK_FLAGS = large_alloc_lock.lock_irqsave();
    uint64_t large_total_bytes = 0;
    uint64_t large_count = 0;
    emergency_serial::write("kmalloc: Large allocations (>=0x10000):\n");

    for (LargeAllocationHeader const* curr = large_alloc_list; curr != nullptr; curr = curr->next) {
        if (curr->magic == LARGE_ALLOC_MAGIC) {
            large_count++;
            large_total_bytes += curr->size;
            emergency_serial::write("  addr=0x");
            emergency_serial::write_hex((uint64_t)(curr + 1));  // Data starts after header
            emergency_serial::write(" size=0x");
            emergency_serial::write_hex(curr->size);
#ifdef WOS_KMALLOC_DEBUG_INFO
            uint64_t caller = 0;
            const char* tag = nullptr;
            if (debug_ref_has_any_generation(curr->debug_info, caller, tag)) {
                emergency_serial::write(" caller=0x");
                emergency_serial::write_hex(caller);
                if (tag != nullptr) {
                    emergency_serial::write(" tag=");
                    emergency_serial::write(tag);
                }
            }
#endif
            emergency_serial::write("\n");
        }
    }

    emergency_serial::write("  large_total: 0x");
    emergency_serial::write_hex(large_count);
    emergency_serial::write(" entries, 0x");
    emergency_serial::write_hex(large_total_bytes);
    emergency_serial::write(" bytes\n");
    large_alloc_lock.unlock_irqrestore(LARGE_LOCK_FLAGS);

#ifdef WOS_KMALLOC_DEBUG_INFO
    emergency_serial::write("kmalloc: Slab live allocations with debug info:\n");
    mini_malloc::mini_iter_live_debug_slots(nullptr, [](void* /*ud*/, const void* ptr, size_t sz, uintptr_t debug_ref) -> void {
        uint64_t caller = 0;
        const char* tag = nullptr;
        if (!debug_ref_has_any_generation(reinterpret_cast<const AllocDebugInfo*>(debug_ref), caller, tag)) {
            return;
        }
        emergency_serial::write("  addr=0x");
        emergency_serial::write_hex((uint64_t)ptr);
        emergency_serial::write(" sz=0x");
        emergency_serial::write_hex(sz);
        emergency_serial::write(" caller=0x");
        emergency_serial::write_hex(caller);
        if (tag != nullptr) {
            emergency_serial::write(" tag=");
            emergency_serial::write(tag);
        }
        emergency_serial::write("\n");
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

void get_tracked_alloc_breakdown(KmallocTrackedTotals& out) {
    out = {};

    uint64_t const MEDIUM_LOCK_FLAGS = medium_alloc_lock.lock_irqsave();
    for (MediumAllocationHeader const* curr = medium_alloc_list; curr != nullptr; curr = curr->next) {
        if (curr->magic == MEDIUM_ALLOC_MAGIC) {
            out.medium_count++;
            out.medium_bytes += curr->size;
        }
    }
    medium_alloc_lock.unlock_irqrestore(MEDIUM_LOCK_FLAGS);

    uint64_t const LARGE_LOCK_FLAGS = large_alloc_lock.lock_irqsave();
    for (LargeAllocationHeader const* curr = large_alloc_list; curr != nullptr; curr = curr->next) {
        if (curr->magic == LARGE_ALLOC_MAGIC) {
            out.large_count++;
            out.large_bytes += curr->size;
        }
    }
    large_alloc_lock.unlock_irqrestore(LARGE_LOCK_FLAGS);
}

auto snapshot_live_allocs(KmallocLiveAlloc* out, size_t max_rows, size_t& total_rows) -> size_t {
    total_rows = 0;
    size_t rows = 0;

    uint64_t const LARGE_LOCK_FLAGS = large_alloc_lock.lock_irqsave();
    for (LargeAllocationHeader const* curr = large_alloc_list; curr != nullptr; curr = curr->next) {
        if (curr->magic != LARGE_ALLOC_MAGIC) {
            continue;
        }
        if (out != nullptr && rows < max_rows) {
            uint64_t caller = 0;
            const char* tag = nullptr;
#ifdef WOS_KMALLOC_DEBUG_INFO
            bool const HAS_DEBUG = debug_ref_has_current_generation(curr->debug_info, caller, tag);
#else
            bool const HAS_DEBUG = false;
#endif
            out[rows++] = KmallocLiveAlloc{.tier = "large",
                                           .addr = reinterpret_cast<uint64_t>(curr + 1),
                                           .size = curr->size,
                                           .caller = caller,
                                           .tag = tag,
                                           .has_debug = HAS_DEBUG};
        }
        total_rows++;
    }
    large_alloc_lock.unlock_irqrestore(LARGE_LOCK_FLAGS);

    uint64_t const MEDIUM_LOCK_FLAGS = medium_alloc_lock.lock_irqsave();
    for (MediumAllocationHeader const* curr = medium_alloc_list; curr != nullptr; curr = curr->next) {
        if (curr->magic != MEDIUM_ALLOC_MAGIC) {
            continue;
        }
        if (out != nullptr && rows < max_rows) {
            uint64_t caller = 0;
            const char* tag = nullptr;
#ifdef WOS_KMALLOC_DEBUG_INFO
            bool const HAS_DEBUG = debug_ref_has_current_generation(curr->debug_info, caller, tag);
#else
            bool const HAS_DEBUG = false;
#endif
            out[rows++] = KmallocLiveAlloc{.tier = "medium",
                                           .addr = reinterpret_cast<uint64_t>(curr + 1),
                                           .size = curr->size,
                                           .caller = caller,
                                           .tag = tag,
                                           .has_debug = HAS_DEBUG};
        }
        total_rows++;
    }
    medium_alloc_lock.unlock_irqrestore(MEDIUM_LOCK_FLAGS);
    return rows;
}

namespace {

auto tag_equals(const char* lhs, const char* rhs) -> bool {
    if (lhs == rhs) {
        return true;
    }
    if (lhs == nullptr || rhs == nullptr) {
        return false;
    }
    return std::strcmp(lhs, rhs) == 0;
}

void add_caller_stat(KmallocCallerStat* out, size_t max_rows, size_t& rows, size_t& overflow_rows, const char* tier, uint64_t size,
                     bool has_debug, uint64_t caller, const char* tag) {
    if (!has_debug) {
        caller = 0;
        tag = nullptr;
    }

    for (size_t i = 0; i < rows; ++i) {
        auto& row = out[i];  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        if (row.has_debug == has_debug && row.caller == caller && row.tier == tier && tag_equals(row.tag, tag)) {
            row.count++;
            row.bytes += size;
            return;
        }
    }

    if (rows < max_rows) {
        out[rows++] = KmallocCallerStat{.tier = tier, .caller = caller, .tag = tag, .count = 1, .bytes = size, .has_debug = has_debug};
        return;
    }
    overflow_rows++;
}

}  // namespace

auto snapshot_caller_stats(KmallocCallerStat* out, size_t max_rows, size_t& total_rows) -> size_t {
    total_rows = 0;
    if (out == nullptr || max_rows == 0) {
        uint64_t count = 0;
        uint64_t bytes = 0;
        get_tracked_alloc_totals(count, bytes);
        total_rows = static_cast<size_t>(count);
        return 0;
    }

    size_t rows = 0;
    size_t overflow_rows = 0;

    uint64_t const LARGE_LOCK_FLAGS = large_alloc_lock.lock_irqsave();
    for (LargeAllocationHeader const* curr = large_alloc_list; curr != nullptr; curr = curr->next) {
        if (curr->magic != LARGE_ALLOC_MAGIC) {
            continue;
        }
        uint64_t caller = 0;
        const char* tag = nullptr;
#ifdef WOS_KMALLOC_DEBUG_INFO
        bool const HAS_DEBUG = debug_ref_has_current_generation(curr->debug_info, caller, tag);
#else
        bool const HAS_DEBUG = false;
#endif
        add_caller_stat(out, max_rows, rows, overflow_rows, "large", curr->size, HAS_DEBUG, caller, tag);
    }
    large_alloc_lock.unlock_irqrestore(LARGE_LOCK_FLAGS);

    uint64_t const MEDIUM_LOCK_FLAGS = medium_alloc_lock.lock_irqsave();
    for (MediumAllocationHeader const* curr = medium_alloc_list; curr != nullptr; curr = curr->next) {
        if (curr->magic != MEDIUM_ALLOC_MAGIC) {
            continue;
        }
        uint64_t caller = 0;
        const char* tag = nullptr;
#ifdef WOS_KMALLOC_DEBUG_INFO
        bool const HAS_DEBUG = debug_ref_has_current_generation(curr->debug_info, caller, tag);
#else
        bool const HAS_DEBUG = false;
#endif
        add_caller_stat(out, max_rows, rows, overflow_rows, "medium", curr->size, HAS_DEBUG, caller, tag);
    }
    medium_alloc_lock.unlock_irqrestore(MEDIUM_LOCK_FLAGS);

    std::sort(out, out + rows, [](const KmallocCallerStat& lhs, const KmallocCallerStat& rhs) { return lhs.bytes > rhs.bytes; });
    total_rows = rows + overflow_rows;
    return rows;
}

auto debug_info_available() -> bool {
#ifdef WOS_KMALLOC_DEBUG_INFO
    return true;
#else
    return false;
#endif
}

auto debug_info_enabled() -> bool {
#ifdef WOS_KMALLOC_DEBUG_INFO
    return s_alloc_debug_enabled.load(std::memory_order_acquire);
#else
    return false;
#endif
}

void debug_info_set_enabled(bool enabled) {
#ifdef WOS_KMALLOC_DEBUG_INFO
    s_alloc_debug_enabled.store(enabled, std::memory_order_release);
#else
    (void)enabled;
#endif
}

void debug_info_reset() {
#ifdef WOS_KMALLOC_DEBUG_INFO
    s_alloc_debug_generation.fetch_add(1, std::memory_order_acq_rel);
#endif
}

auto debug_info_generation() -> uint64_t {
#ifdef WOS_KMALLOC_DEBUG_INFO
    return s_alloc_debug_generation.load(std::memory_order_acquire);
#else
    return 0;
#endif
}

auto debug_info_default_enabled() -> bool {
#ifdef WOS_KMALLOC_DEBUG_INFO
    return true;
#else
    return false;
#endif
}

auto debug_info_stats() -> KmallocDebugStats {
#ifdef WOS_KMALLOC_DEBUG_INFO
    uint64_t const BLOCKS = s_alloc_debug_block_count.load(std::memory_order_acquire);
    uint64_t const CAPACITY = s_alloc_debug_capacity.load(std::memory_order_acquire);
    uint64_t const ACTIVE = s_alloc_debug_active.load(std::memory_order_acquire);
    return KmallocDebugStats{.block_count = BLOCKS,
                             .block_bytes = BLOCKS * ALLOC_DEBUG_BLOCK_BYTES,
                             .capacity = CAPACITY,
                             .active = ACTIVE,
                             .high_water = s_alloc_debug_high_water.load(std::memory_order_acquire),
                             .dropped = s_alloc_debug_dropped.load(std::memory_order_acquire)};
#else
    return {};
#endif
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

auto alloc_medium_backing(uint64_t size) -> void* { return phys::page_alloc_with_reclaim(size, "kmalloc_medium"); }

auto alloc_large_backing(uint64_t size) -> void* {
    void* alloc_ptr = phys::page_alloc_huge(size);
    if (alloc_ptr != nullptr) {
        return alloc_ptr;
    }
    return phys::page_alloc_with_reclaim(size, "kmalloc_large");
}

auto checked_page_rounded_alloc_size(uint64_t payload_size, uint64_t header_size, uint64_t& out_rounded_size) -> bool {
    out_rounded_size = 0;
    if (payload_size > UINT64_MAX - header_size) {
        return false;
    }

    uint64_t const TOTAL_SIZE = payload_size + header_size;
    uint64_t const PAGE_SIZE = ker::mod::mm::paging::PAGE_SIZE;
    uint64_t const PAGES = (TOTAL_SIZE / PAGE_SIZE) + ((TOTAL_SIZE % PAGE_SIZE) != 0 ? 1 : 0);
    if (PAGES == 0 || PAGES > UINT64_MAX / PAGE_SIZE) {
        return false;
    }

    uint64_t const ALIGNED_SIZE = PAGES * PAGE_SIZE;
    if (ALIGNED_SIZE > MAX_BUDDY_ALLOCATION_BYTES) {
        return false;
    }

    out_rounded_size = ALIGNED_SIZE;
    return true;
}

auto malloc_impl(uint64_t size, uintptr_t caller, const char* tag) -> void* {
#ifndef WOS_KMALLOC_DEBUG_INFO
    (void)caller;
    (void)tag;
#endif
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
#ifdef WOS_KMALLOC_DEBUG_INFO
        if (ptr != nullptr) {
            auto** debug_slot = slab_debug_slot(ptr);
            *debug_slot = nullptr;
            *debug_slot = register_alloc_debug(caller, tag);
        }
#endif
        return ptr;
    }

    // Tier 2: Medium allocations (0x801 - 0xFFFF) - use regular pageAlloc with tracking
    if (size <= MEDIUM_MAX_SIZE) {
        uint64_t rounded_size = 0;
        if (!checked_page_rounded_alloc_size(size, sizeof(MediumAllocationHeader), rounded_size)) {
            return nullptr;
        }

#ifdef DEBUG_KMALLOC
        emergency_serial::write("kmalloc: Medium allocation (0x");
        emergency_serial::write_hex(size);
        emergency_serial::write(" bytes), using pageAlloc (0x");
        emergency_serial::write_hex(rounded_size);
        emergency_serial::write(" bytes)\n");
#endif

        void* alloc_ptr = alloc_medium_backing(rounded_size);
        if (alloc_ptr == nullptr) {
#ifdef DEBUG_KMALLOC
            emergency_serial::write("kmalloc: pageAlloc failed for medium allocation\n");
#endif
            return nullptr;
        }
        (void)phys::page_mark_kind(alloc_ptr, PageKind::MEDIUM);

        // Set up header with tracking info
        auto* header = static_cast<MediumAllocationHeader*>(alloc_ptr);
        header->size = rounded_size;
#ifdef WOS_KMALLOC_DEBUG_INFO
        header->debug_info = register_alloc_debug(caller, tag);
#endif

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
        return data;
    }

    // Tier 3: Large allocations (>= 0x10000) - use pageAllocHuge with tracking
    uint64_t rounded_size = 0;
    if (!checked_page_rounded_alloc_size(size, sizeof(LargeAllocationHeader), rounded_size)) {
        return nullptr;
    }

#ifdef DEBUG_KMALLOC
    emergency_serial::write("kmalloc: Large allocation (0x");
    emergency_serial::write_hex(size);
    emergency_serial::write(" bytes), using pageAllocHuge (0x");
    emergency_serial::write_hex(rounded_size);
    emergency_serial::write(" bytes)\n");
#endif

    // Allocate from huge page zone first; if it is full, the regular-zone
    // fallback may yield to scheduler GC when the caller is preemptible.
    void* alloc_ptr = alloc_large_backing(rounded_size);
    if (alloc_ptr == nullptr) {
#ifdef DEBUG_KMALLOC
        emergency_serial::write("kmalloc: pageAlloc failed for large allocation\n");
#endif
        return nullptr;
    }
    (void)phys::page_mark_kind(alloc_ptr, PageKind::KMALLOC_LARGE);

    // Set up header with tracking info
    auto* header = static_cast<LargeAllocationHeader*>(alloc_ptr);
    header->size = rounded_size;
#ifdef WOS_KMALLOC_DEBUG_INFO
    header->debug_info = register_alloc_debug(caller, tag);
#endif

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
#ifdef WOS_KMALLOC_DEBUG_INFO
            release_alloc_debug(header->debug_info);
            header->debug_info = nullptr;
#endif
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

    // Helper lambda to print debug info for a node.
    auto print_debug_info = [](const MediumAllocationHeader* node) {
#ifdef WOS_KMALLOC_DEBUG_INFO
        uint64_t caller = 0;
        const char* tag = nullptr;
        if (debug_ref_has_any_generation(node->debug_info, caller, tag)) {
            emergency_serial::write(" caller=0x");
            emergency_serial::write_hex(caller);
            if (tag != nullptr) {
                emergency_serial::write(" tag=");
                emergency_serial::write(tag);
            }
        }
#else
        (void)node;
#endif
    };

    emergency_serial::write("kmalloc: DoubleFree chain dump (target=0x");
    emergency_serial::write_hex(reinterpret_cast<uint64_t>(header));
    emergency_serial::write(" size=0x");
    emergency_serial::write_hex(header->size);
    print_debug_info(header);
    emergency_serial::write("):\n");

    uint32_t n = 0;
    MediumAllocationHeader const* last_node = nullptr;
    bool found_corrupt = false;
    for (MediumAllocationHeader const* c = medium_alloc_list; c != nullptr && n < 8192; c = c->next, ++n) {
        if (c->magic != MEDIUM_ALLOC_MAGIC) {
            emergency_serial::write("  BAD node=0x");
            emergency_serial::write_hex(reinterpret_cast<uint64_t>(c));
            emergency_serial::write(" size=0x");
            emergency_serial::write_hex(c->size);
            emergency_serial::write(" magic=0x");
            emergency_serial::write_hex(c->magic);
            emergency_serial::write(" (prev had this as next)\n");
            found_corrupt = true;
            break;
        }
        MediumAllocationHeader const* nxt = c->next;
        if (nxt != nullptr && nxt->magic != MEDIUM_ALLOC_MAGIC) {
            emergency_serial::write("  CORRUPT node=0x");
            emergency_serial::write_hex(reinterpret_cast<uint64_t>(c));
            emergency_serial::write(" size=0x");
            emergency_serial::write_hex(c->size);
            print_debug_info(c);
            emergency_serial::write(" ->next=0x");
            emergency_serial::write_hex(reinterpret_cast<uint64_t>(nxt));
            emergency_serial::write(" (next_magic=0x");
            emergency_serial::write_hex(nxt->magic);
            emergency_serial::write(")\n");
            const auto* data = reinterpret_cast<const uint64_t*>(c + 1);
            emergency_serial::write("  CORRUPT node data[0..3]: 0x");
            emergency_serial::write_hex(data[0]);
            emergency_serial::write(" 0x");
            emergency_serial::write_hex(data[1]);
            emergency_serial::write(" 0x");
            emergency_serial::write_hex(data[2]);
            emergency_serial::write(" 0x");
            emergency_serial::write_hex(data[3]);
            emergency_serial::write("\n");
            found_corrupt = true;
            break;
        }
        last_node = c;
    }
    if (!found_corrupt && last_node != nullptr) {
        // Chain ended without finding target — the predecessor of target had its
        // ->next overwritten with null (or some other valid node, skipping target).
        emergency_serial::write("  TRUNCATED: last valid node=0x");
        emergency_serial::write_hex(reinterpret_cast<uint64_t>(last_node));
        emergency_serial::write(" size=0x");
        emergency_serial::write_hex(last_node->size);
        print_debug_info(last_node);
        emergency_serial::write(" ->next=0x0\n");
        const auto* data = reinterpret_cast<const uint64_t*>(last_node + 1);
        emergency_serial::write("  TRUNCATED node data[0..7]: 0x");
        emergency_serial::write_hex(data[0]);
        emergency_serial::write(" 0x");
        emergency_serial::write_hex(data[1]);
        emergency_serial::write(" 0x");
        emergency_serial::write_hex(data[2]);
        emergency_serial::write(" 0x");
        emergency_serial::write_hex(data[3]);
        emergency_serial::write(" 0x");
        emergency_serial::write_hex(data[4]);
        emergency_serial::write(" 0x");
        emergency_serial::write_hex(data[5]);
        emergency_serial::write(" 0x");
        emergency_serial::write_hex(data[6]);
        emergency_serial::write(" 0x");
        emergency_serial::write_hex(data[7]);
        emergency_serial::write("\n");
    }
    emergency_serial::write("kmalloc: DoubleFree chain dump done (");
    emergency_serial::write_hex(n);
    emergency_serial::write(" nodes walked)\n");

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
#ifdef WOS_KMALLOC_DEBUG_INFO
            release_alloc_debug(header->debug_info);
            header->debug_info = nullptr;
#endif
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
#ifdef WOS_KMALLOC_DEBUG_INFO
    return malloc_impl(size, (uintptr_t)__builtin_return_address(0), nullptr);
#else
    return malloc_impl(size, 0, nullptr);
#endif
}

#ifdef WOS_KMALLOC_DEBUG_INFO
auto malloc_tagged(uint64_t size, uintptr_t caller, const char* tag) -> void* { return malloc_impl(size, caller, tag); }
#endif

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
            uint64_t new_rounded_size = 0;
            if (!checked_page_rounded_alloc_size(NEW_SIZE, sizeof(LargeAllocationHeader), new_rounded_size)) {
                return nullptr;
            }

            // If the new size fits in the current allocation, return same pointer
            if (new_rounded_size == potential_large_header->size) {
                return ptr;
            }

            // Need to reallocate - allocate new, copy, free old
            void* new_alloc = alloc_large_backing(new_rounded_size);
            if (new_alloc == nullptr) {
                return nullptr;
            }
            (void)phys::page_mark_kind(new_alloc, PageKind::KMALLOC_LARGE);

            auto* new_header = static_cast<LargeAllocationHeader*>(new_alloc);
            new_header->size = new_rounded_size;
#ifdef WOS_KMALLOC_DEBUG_INFO
            new_header->debug_info = nullptr;
#endif

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
#ifdef WOS_KMALLOC_DEBUG_INFO
            release_alloc_debug(potential_large_header->debug_info);
            potential_large_header->debug_info = nullptr;
#endif
            potential_large_header->magic = 0;
            large_alloc_lock.unlock_irqrestore(LOCK_FLAGS);
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
            uint64_t new_rounded_size = 0;
            if (!checked_page_rounded_alloc_size(NEW_SIZE, sizeof(MediumAllocationHeader), new_rounded_size)) {
                return nullptr;
            }

            // If the new size fits in the current allocation, return same pointer
            if (new_rounded_size == potential_medium_header->size) {
                return ptr;
            }

            // Need to reallocate - allocate new, copy, free old
            void* new_alloc = alloc_medium_backing(new_rounded_size);
            if (new_alloc == nullptr) {
                return nullptr;
            }
            (void)phys::page_mark_kind(new_alloc, PageKind::MEDIUM);

            auto* new_header = static_cast<MediumAllocationHeader*>(new_alloc);
            new_header->size = new_rounded_size;
#ifdef WOS_KMALLOC_DEBUG_INFO
            new_header->debug_info = nullptr;
#endif

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
#ifdef WOS_KMALLOC_DEBUG_INFO
            release_alloc_debug(potential_medium_header->debug_info);
            potential_medium_header->debug_info = nullptr;
#endif
            potential_medium_header->magic = 0;
            medium_alloc_lock.unlock_irqrestore(LOCK_FLAGS);
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
            emergency_serial::write("kmalloc: Freeing large allocation at 0x");
            emergency_serial::write_hex((uint64_t)ptr);
            emergency_serial::write(" (0x");
            emergency_serial::write_hex(size);
            emergency_serial::write(" bytes)\n");
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
            emergency_serial::write("kmalloc: Freeing medium allocation at 0x");
            emergency_serial::write_hex((uint64_t)ptr);
            emergency_serial::write(" (0x");
            emergency_serial::write_hex(size);
            emergency_serial::write(" bytes)\n");
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

#ifdef WOS_KMALLOC_DEBUG_INFO
    if (SLAB_SZ > 0) {
        auto** debug_slot = slab_debug_slot(ptr);
        release_alloc_debug(*debug_slot);
        *debug_slot = nullptr;
    }
#endif

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
#ifdef WOS_KMALLOC_DEBUG_INFO
    return ker::mod::mm::dyn::kmalloc::malloc_tagged(sz, (uintptr_t)__builtin_return_address(0), "::new");
#else
    return ker::mod::mm::dyn::kmalloc::malloc(sz);
#endif
}

auto operator new[](std::size_t sz) -> void* {
#ifdef WOS_KMALLOC_DEBUG_INFO
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
