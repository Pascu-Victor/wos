#pragma once

// Finite effects for the unchanged actual ancestor/paging implementation.
// No endpoint issues a cache/MMIO capability or installs a rival page table.
#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <mutex>
#include <new>
#include <platform/mm/paging.hpp>
#include <utility>
#include <vector>

namespace ker::mod::mm::virt {
using namespace ker::mod::mm::paging;
using paddr_t = uint64_t;
using vaddr_t = uint64_t;
extern std::atomic<bool> typed_mmio_guards;
// ACTUAL_RELEASE_ORIGIN
auto advance_page_table(PageTable*, size_t, uint64_t, bool* = nullptr) -> PageTable*;
}  // namespace ker::mod::mm::virt
namespace ancestor_host {
using PageTable = ker::mod::mm::paging::PageTable;
struct Page {
    PageTable* table{};
    uint64_t physical{};
    bool pooled{}, freed{};
};
struct State;
inline State* active{};
struct State {
    std::vector<Page> pages;
    bool irq = true, incoming_irq = true, pool_accept = true, fail_allocation{};
    unsigned pin_depth{}, allocations{}, pool_calls{}, free_calls{}, violations{};
    unsigned outside_child_reads{}, child_reads{}, outside_publish_addresses{}, publish_addresses{};
    unsigned topology_calls{}, topology_outside_pin{}, topology_while_untyped{}, policy_calls{}, flushes{};
    uint64_t cr3 = 0x1000;
    PageTable *root{}, *outer_spare{}, *rival{}, *descendant{};
    std::function<void()> on_allocation;
    explicit State(bool typed = false, bool initial_irq = true) : irq(initial_irq), incoming_irq(initial_irq) {
        if (active != nullptr) std::abort();
        active = this;
        ker::mod::mm::virt::typed_mmio_guards.store(typed);
        root = make_table();
    }
    ~State() {
        for (auto& page : pages)
            if (!page.freed) std::free(page.table);
        active = nullptr;
    }
    auto make_table() -> PageTable* {
        void* memory = std::aligned_alloc(4096, 4096);
        if (memory == nullptr) std::abort();
        auto* table = new (memory) PageTable{};
        pages.push_back({table, 0x100000 + pages.size() * 4096, false, false});
        return table;
    }
    auto record(const PageTable* table) -> Page& {
        for (auto& page : pages)
            if (page.table == table) return page;
        std::abort();
    }
    auto physical(const PageTable* table) -> uint64_t { return record(table).physical; }
};
inline auto state() -> State& {
    if (active == nullptr) std::abort();
    return *active;
}
struct Halt {};
struct Panic {};
class Pin {
   public:
    auto lock_irqsave() -> uint64_t {
        auto& s = state();
        mutex_.lock();
        auto const before = s.irq;
        if (s.pin_depth != 0) ++s.violations;
        ++s.pin_depth;
        s.irq = false;
        return before;
    }
    void unlock_irqrestore(uint64_t before) {
        auto& s = state();
        if (s.pin_depth == 0) {
            ++s.violations;
            return;
        }
        --s.pin_depth;
        s.irq = before != 0;
        mutex_.unlock();
    }

   private:
    std::recursive_mutex mutex_;
};
inline auto entry_raw(const ker::mod::mm::paging::PageTableEntry& entry) -> uint64_t {
    uint64_t raw{};
    std::memcpy(&raw, &entry, sizeof(raw));
    return raw;
}
}  // namespace ancestor_host
namespace ker::mod::mm::addr {
inline auto get_virt_pointer(uint64_t physical) -> void* {
    auto& s = ancestor_host::state();
    ++s.child_reads;
    if (s.pin_depth != 1) ++s.outside_child_reads;
    for (auto& page : s.pages)
        if (page.physical == physical && !page.freed) return page.table;
    std::abort();
}
inline auto get_phys_pointer(uint64_t address) -> void* {
    auto& s = ancestor_host::state();
    ++s.publish_addresses;
    if (s.pin_depth != 1) ++s.outside_publish_addresses;
    return reinterpret_cast<void*>(s.physical(reinterpret_cast<ker::mod::mm::paging::PageTable*>(address)));
}
}  // namespace ker::mod::mm::addr
namespace ker::mod::mm::mmio::detail {
inline auto generic_table_allowed(const ker::mod::mm::paging::PageTable* table) -> bool {
    auto& s = ancestor_host::state();
    ++s.policy_calls;
    return !s.record(table).freed;
}
inline void note_topology_publication_locked() {
    auto& s = ancestor_host::state();
    ++s.topology_calls;
    if (s.pin_depth != 1) ++s.topology_outside_pin;
    if (!ker::mod::mm::virt::typed_mmio_guards.load()) ++s.topology_while_untyped;
}
}  // namespace ker::mod::mm::mmio::detail
namespace ker::mod::mm::phys {
inline void page_free(void* pointer) {
    auto& s = ancestor_host::state();
    if (s.pin_depth != 0 || s.irq != s.incoming_irq) ++s.violations;
    auto& page = s.record(static_cast<ancestor_host::PageTable*>(pointer));
    if (page.freed || page.pooled) std::abort();
    page.freed = true;
    ++s.free_calls;
    std::free(pointer);
}
}  // namespace ker::mod::mm::phys
namespace ker::mod::dbg {
[[noreturn]] inline void panic_handler(const char*) {
    auto const& s = ancestor_host::state();
    std::fprintf(stderr, "ancestor panic pin=%u irq=%u\n", s.pin_depth, s.irq ? 1 : 0);
    throw ancestor_host::Panic{};
}
}  // namespace ker::mod::dbg
namespace ker::mod::mm::virt {
inline ancestor_host::Pin user_mapping_pin_lock;
struct log {
    template <class... T>
    static void critical(const char*, T...) {}
};
[[noreturn]] inline void hcf() { throw ancestor_host::Halt{}; }
inline auto rdcr3() -> uint64_t { return ancestor_host::state().cr3; }
inline void wrcr3(uint64_t value) {
    auto& s = ancestor_host::state();
    if (s.pin_depth != 0 || s.irq != s.incoming_irq || value != s.cr3) ++s.violations;
    ++s.flushes;
}
inline auto alloc_zeroed_page_table() -> PageTable* {
    auto& s = ancestor_host::state();
    if (s.pin_depth != 0 || s.irq != s.incoming_irq) ++s.violations;
    ++s.allocations;
    if (s.fail_allocation) return nullptr;
    auto* table = s.make_table();
    auto hook = std::exchange(s.on_allocation, {});
    if (hook) {
        s.outer_spare = table;
        hook();
    }
    return table;
}
inline auto try_release_page_table_to_pool(PageTable* table, PageTableReleaseOrigin) -> bool {
    auto& s = ancestor_host::state();
    if (s.pin_depth != 0 || s.irq != s.incoming_irq) ++s.violations;
    ++s.pool_calls;
    if (s.pool_accept) s.record(table).pooled = true;
    return s.pool_accept;
}
}  // namespace ker::mod::mm::virt
