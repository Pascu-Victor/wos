// Finite address/allocator/ref/TLB endpoints for unchanged native map bodies.
// All permission predicates call the same actual MMRuntime as the real driver.
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

#include <csignal>
#ifdef WOS_SELFTEST
#define WOS_MMIO_WRITER_RESTORE_SELFTEST 1
#undef WOS_SELFTEST
#endif
namespace mmio_writer_host {
using namespace ker::mod::mm;
using namespace ker::mod::mm::paging;
using vaddr_t = uint64_t;
using paddr_t = uint64_t;
namespace mmio = ker::mod::mm::mmio;
using log = ker::mod::dbg::logger<"writer-host">;
inline constexpr uint64_t LARGE_PAGE_2M_BYTES = 2 * 1024 * 1024;
inline constexpr uint64_t PTE_FRAME_MASK = 0x000ffffffffff000ULL;
inline std::atomic<bool> typed_mmio_guards{};
inline PageTable* kernel_pagemap{};
inline int effects{-1};
inline size_t allocations{}, untracks{}, tracks{}, refs{}, tlbs{};
void effect(char kind) {
    if (effects != -1) {
        if (::write(effects, &kind, 1) != 1) _exit(91);
    }
}
struct Lock {
    auto lock_irqsave() -> uint64_t { return mmio::detail::platform::lock_pin(); }
    void unlock_irqrestore(uint64_t flags) { mmio::detail::platform::unlock_pin(flags); }
} user_mapping_pin_lock;
inline std::mutex cow_mutex;
inline unsigned cow_depth{};
struct CowLock {
    auto lock_irqsave() -> uint64_t {
        assert(cow_depth == 0 && mmio_host::pin_depth == 0 && pci_cold_host::config_depth == 0);
        uint64_t const flags = runtime_ports::flags;
        runtime_ports::flags &= ~runtime_ports::IF;
        cow_mutex.lock();
        ++cow_depth;
        return flags;
    }
    void unlock_irqrestore(uint64_t flags) {
        assert(cow_depth == 1 && mmio_host::pin_depth == 0);
        --cow_depth;
        cow_mutex.unlock();
        runtime_ports::flags = flags;
    }
} cow_pte_lock;
// Explicit absent-swap endpoint for the actual ordinary unmap body. No swap
// registry or active mapping is fabricated; every nonnull operation aborts.
enum class AnonymousSwapRuntimeState : uint8_t { PAGEOUT, SWAPPED, PAGEIN, FAILED };
struct AnonymousSwapObject {};
struct AnonymousSwapMapping {
    bool cancelled{}, pagein_queued{};
    AnonymousSwapRuntimeState state{AnonymousSwapRuntimeState::PAGEOUT};
};
struct {
    std::atomic<uint64_t> swapped_pages{};
} anonymous_swap_stats;
auto find_anonymous_swap_mapping_locked(PageTable*, uint64_t) -> AnonymousSwapMapping* {
    assert(cow_depth == 1 && mmio_host::pin_depth == 1);
    return nullptr;
}
void remove_anonymous_swap_mapping_locked(AnonymousSwapMapping*) { std::abort(); }
auto detach_anonymous_swap_object_locked(AnonymousSwapMapping*) -> AnonymousSwapObject* { std::abort(); }
void release_anonymous_swap_object(AnonymousSwapObject* object) {
    assert(object == nullptr && cow_depth == 0 && mmio_host::pin_depth == 0);
}
namespace addr {
auto get_hhdm_offset() -> uint64_t { return 0xffff800000000000ULL; }
auto get_virt_pointer(uint64_t physical) -> uint64_t* {
    auto const found = mmio_host::tables.find(physical);
    assert(found != mmio_host::tables.end());
    return reinterpret_cast<uint64_t*>(found->second);
}
auto get_phys_pointer(uint64_t address) -> uint64_t* {
    return reinterpret_cast<uint64_t*>(mmio_host::physical.at(reinterpret_cast<PageTable*>(address)));
}
}  // namespace addr
namespace dbg {
[[noreturn]] void panic_handler(const char*) { __builtin_trap(); }
}  // namespace dbg
[[noreturn]] void hcf() { __builtin_trap(); }
auto rdcr3() -> uint64_t { return 0; }
void wrcr3(uint64_t) {
    ++tlbs;
    effect('T');
}
auto alloc_zeroed_page_table() -> PageTable* {
    assert(mmio_host::pin_depth == 0);
    ++allocations;
    effect('A');
    return mmio_host::allocate_table();
}
struct PageTableReleaseOrigin {
    void* caller;
    uint64_t owner;
};
auto try_release_page_table_to_pool(PageTable*, PageTableReleaseOrigin) -> bool {
    effect('F');
    return false;
}
namespace phys {
void page_free(void*) {
    effect('F');
    __builtin_trap();
}
}  // namespace phys
void owned_frame_untrack_leaf(PageTable*, uint64_t, const PageTableEntry&) {
    ++untracks;
    effect('U');
}
void owned_frame_track_private_mapping(PageTable*, uint64_t, uint64_t, uint64_t) {
    ++tracks;
    effect('K');
}
void invalidate_local_tlb_if_current(PageTable*, uint64_t, bool) {
    ++tlbs;
    effect('T');
}
void shootdown_remote_user_pagemap(PageTable*, uint64_t, bool) {
    ++tlbs;
    effect('T');
}
void flush_pagemap_after_update(PageTable*, uint64_t, bool) {
    ++tlbs;
    effect('T');
}
void drop_present_leaf_ref(const PageTableEntry&) {
    ++refs;
    effect('R');
}
auto is_reserved_leaf(const PageTableEntry& entry) -> bool;
struct Range {
    uint64_t start{}, end{};
};
struct PageMapBatch;
void flush_page_map_batch(PageMapBatch*);
auto advance_page_table(PageTable*, size_t, uint64_t, bool* = nullptr) -> PageTable*;
