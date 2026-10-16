// Host physical/HHDM, source-exclusive, TLB and fresh-tracker endpoints only.
// The extracted full page-table copier and actual NUMA factory are unchanged.
#include <algorithm>
#include <array>
#include <bit>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <platform/acpi/numa_boot.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/numa_alloc.hpp>
#include <platform/mm/reclaim.hpp>
#include <platform/sched/scheduler.hpp>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>
#define CHECK(x)                                                                                       \
    do {                                                                                               \
        if (!(x)) throw std::runtime_error(std::string("line ") + std::to_string(__LINE__) + ": " #x); \
    } while (false)
namespace host {
namespace mm = ker::mod::mm;
namespace numa = mm::numa;
constexpr uint64_t PAGE = 4096, FRAME = 0x000ffffffffff000ULL;
struct Block {
    uint8_t* data;
    uint64_t physical;
    unsigned refs;
    uint16_t node;
};
std::map<uint64_t, Block> blocks;
std::set<void*> tables;
std::set<void*> registered;
ker::mod::acpi::numa::Topology topology;
bool topology_ready = true, eager = true;
unsigned exclusive = 0, flushes = 0, swap_clones = 0, legacy_allocations = 0, range_allocations = 0, cpu_reads = 0, preempt = 0;
uint64_t cpu = 0;
std::array<uint64_t, 64> next{};
std::array<unsigned, 64> capacity{};
std::map<uint64_t, std::array<uint8_t, PAGE>> expected;
void reset(unsigned nodes = 2) {
    CHECK(blocks.empty() && tables.empty() && registered.empty() && exclusive == 0);
    topology = {};
    topology.node_count = topology.cpu_count = static_cast<uint16_t>(nodes);
    topology.memory_range_count = nodes;
    next.fill(0);
    capacity.fill(100);
    expected.clear();
    for (unsigned n = 0; n < nodes; ++n) {
        topology.cpu_nodes[n] = static_cast<uint16_t>(n);
        topology.nodes[n].usable_bytes = 16 * 1024 * 1024;
        topology.memory[n] = {0x10000000ULL + n * 0x10000000ULL, 16 * 1024 * 1024, static_cast<uint16_t>(n), 0};
        for (unsigned j = 0; j < nodes; ++j) topology.distances[n][j] = n == j ? 10 : 20;
    }
    topology_ready = eager = true;
    flushes = swap_clones = legacy_allocations = range_allocations = cpu_reads = preempt = 0;
    cpu = 0;
}
auto allocate(unsigned node) -> void* {
    CHECK(node < topology.node_count);
    if (capacity[node] == 0) return nullptr;
    --capacity[node];
    auto* p = static_cast<uint8_t*>(std::aligned_alloc(PAGE, PAGE));
    CHECK(p);
    uint64_t pa = topology.memory[node].base + PAGE * (next[node]++);
    CHECK(pa + PAGE <= topology.memory[node].base + topology.memory[node].length);
    std::memset(p, 0xa5, PAGE);
    CHECK(blocks.emplace(pa, Block{p, pa, 1, static_cast<uint16_t>(node)}).second);
    return p;
}
auto by_pointer(const void* p) -> Block& {
    for (auto& [pa, b] : blocks)
        if (b.data == p) return b;
    throw std::runtime_error("unknown owned data pointer");
}
auto table() -> mm::paging::PageTable* {
    auto* p = std::aligned_alloc(PAGE, PAGE);
    CHECK(p);
    std::memset(p, 0, PAGE);
    CHECK(tables.insert(p).second);
    return static_cast<mm::paging::PageTable*>(p);
}
auto raw(const mm::paging::PageTableEntry& entry) -> uint64_t { return std::bit_cast<uint64_t>(entry); }
auto entry(uint64_t value) -> mm::paging::PageTableEntry { return std::bit_cast<mm::paging::PageTableEntry>(value); }
auto data(uint64_t pa) -> uint8_t* {
    auto it = blocks.find(pa);
    CHECK(it != blocks.end());
    return it->second.data;
}
void drop(void* p) {
    auto& b = by_pointer(p);
    CHECK(b.refs > 0);
    if (--b.refs == 0) {
        auto pa = b.physical;
        std::free(b.data);
        blocks.erase(pa);
    }
}
auto leaf(mm::paging::PageTable* root, uint64_t va, bool create = false) -> mm::paging::PageTableEntry* {
    auto* t = root;
    for (unsigned level = 4; level > 1; --level) {
        auto& e = t->entries[(va >> (12 + 9 * (level - 1))) & 511];
        if (!e.present) {
            if (!create) return nullptr;
            auto* next = table();
            e = entry(reinterpret_cast<uint64_t>(next) | 7);
        }
        CHECK(!e.pagesize);
        t = reinterpret_cast<mm::paging::PageTable*>(static_cast<uint64_t>(e.frame) << 12);
    }
    return &t->entries[(va >> 12) & 511];
}
void destroy(mm::paging::PageTable* t, unsigned level = 4) {
    for (auto& e : t->entries) {
        if (!e.present) continue;
        CHECK(!e.pagesize || level == 1);
        uint64_t pa = static_cast<uint64_t>(e.frame) << 12;
        if (level == 1)
            drop(data(pa));
        else
            destroy(reinterpret_cast<mm::paging::PageTable*>(pa), level - 1);
    }
    CHECK(tables.erase(t) == 1);
    std::free(t);
}
struct Root {
    mm::paging::PageTable* value = table();
    Root() { registered.insert(value); }
    ~Root() noexcept(false) {
        CHECK(registered.erase(value) == 1);
        destroy(value);
    }
    void source(uint64_t va, unsigned node, uint64_t flags = 7) {
        auto* p = static_cast<uint8_t*>(allocate(node));
        CHECK(p);
        for (size_t i = 0; i < PAGE; ++i) p[i] = static_cast<uint8_t>((i * 31 + va / PAGE) & 255);
        auto& e = *leaf(value, va, true);
        CHECK(raw(e) == 0);
        e = entry(by_pointer(p).physical | flags);
        std::array<uint8_t, PAGE> bytes;
        std::copy(p, p + PAGE, bytes.begin());
        expected[va] = bytes;
    }
};
void copied(Root& src, Root& dst, uint64_t va, unsigned node) {
    auto* a = leaf(src.value, va);
    auto* b = leaf(dst.value, va);
    CHECK(a && b && b->present);
    auto ap = static_cast<uint64_t>(a->frame) << 12, bp = static_cast<uint64_t>(b->frame) << 12;
    CHECK(ap != bp);
    CHECK(blocks.at(ap).refs == 1 && blocks.at(bp).refs == 1 && blocks.at(bp).node == node);
    CHECK(std::memcmp(data(ap), data(bp), PAGE) == 0);
    CHECK((raw(*b) & (mm::paging::PAGE_COW | mm::paging::PAGE_SHARED)) == 0);
    CHECK(b->writable);
}
}  // namespace host
namespace ker::mod::acpi::numa {
auto boot_topology() noexcept -> const Topology* { return host::topology_ready ? &host::topology : nullptr; }
}  // namespace ker::mod::acpi::numa
namespace ker::mod::cpu {
auto current_cpu() -> uint64_t {
    CHECK(host::preempt > 0);
    ++host::cpu_reads;
    return host::cpu;
}
}  // namespace ker::mod::cpu
namespace ker::mod::sched {
void preempt_disable() { ++host::preempt; }
void preempt_enable() {
    CHECK(host::preempt > 0);
    --host::preempt;
}
}  // namespace ker::mod::sched
namespace ker::mod::mm::addr {
auto get_virt_pointer(vaddr_t pa) -> vaddr_t* {
    if (host::tables.contains(reinterpret_cast<void*>(pa))) return reinterpret_cast<vaddr_t*>(pa);
    return reinterpret_cast<vaddr_t*>(host::data(pa));
}
auto get_phys_pointer(paddr_t p) -> paddr_t* {
    if (host::tables.contains(reinterpret_cast<void*>(p))) return reinterpret_cast<paddr_t*>(p);
    return reinterpret_cast<paddr_t*>(host::by_pointer(reinterpret_cast<void*>(p)).physical);
}
}  // namespace ker::mod::mm::addr
namespace ker::mod::mm::phys {
auto page_alloc_may_fail(PhysicalPageOwner, uint64_t bytes, std::string_view) -> void* {
    CHECK(bytes == 4096);
    auto* p = host::allocate(0);
    if (p) std::memset(p, 0, 4096);
    return p;
}
auto page_alloc_full_overwrite_page_may_fail(PhysicalPageOwner, std::string_view) -> void* { return host::allocate(0); }
auto page_alloc_with_reclaim_may_fail(PhysicalPageOwner owner, uint64_t bytes, std::string_view site, uint32_t) -> void* {
    return page_alloc_may_fail(owner, bytes, site);
}
auto page_alloc_full_overwrite_page_with_reclaim_may_fail(PhysicalPageOwner, std::string_view, uint32_t) -> void* {
    ++host::legacy_allocations;
    return host::allocate(0);
}
auto page_alloc_order0_span_in_range_may_fail(PhysicalPageOwner owner, uint64_t first, uint64_t end, uint32_t pages, uint64_t alignment,
                                              PageInitialization init) -> void* {
    CHECK(owner == PhysicalPageOwner::USER_PRIVATE_MAPPING && pages == 1 && alignment == 4096 &&
          init == PageInitialization::FULL_OVERWRITE);
    CHECK(host::exclusive > 0);
    ++host::range_allocations;
    for (unsigned n = 0; n < host::topology.node_count; ++n)
        if (first == host::topology.memory[n].base && end == first + host::topology.memory[n].length) return host::allocate(n);
    throw std::runtime_error("wrong physical range");
}
auto page_ref_dec(void* p) -> uint32_t {
    auto refs = host::by_pointer(p).refs;
    host::drop(p);
    return refs - 1;
}
auto page_ref_try_inc(void* p, PageLookupHint*) -> bool {
    ++host::by_pointer(p).refs;
    return true;
}
auto page_ref_get(const void* p, PageLookupHint*) -> uint32_t { return host::by_pointer(p).refs; }
void page_free(void* p) {
    if (host::tables.erase(p)) {
        std::free(p);
        return;
    }
    host::drop(p);
}
}  // namespace ker::mod::mm::phys
namespace ker::mod::mm::reclaim {
auto notify_order0_node_failure(uint16_t) noexcept -> NodeReclaimStatus { return NodeReclaimStatus::NONE; }
}  // namespace ker::mod::mm::reclaim
namespace ker::mod::mm::virt {
using namespace ker::mod::mm::paging;
using namespace ker::mod::mm::addr;
constexpr uint64_t PTE_FRAME_MASK = host::FRAME;
PageTable* kernel_pagemap = reinterpret_cast<PageTable*>(0x9000);
struct UserPagemapExclusiveScope {
    explicit UserPagemapExclusiveScope(PageTable*) { CHECK(host::exclusive++ == 0); }
    ~UserPagemapExclusiveScope() noexcept(false) { CHECK(--host::exclusive == 0); }
};
void selftest_require_mutable_pagemap_root(PageTable* p, const char*) { CHECK(host::registered.contains(p)); }
auto capture_user_root_incarnation(PageTable* p, uint64_t& empty) noexcept -> bool {
    if (empty || !host::registered.contains(p)) return false;
    empty = 1;
    return true;
}
auto fork_eager_copy_enabled() -> bool { return host::eager; }
auto pte_raw(const PageTableEntry& e) -> uint64_t { return host::raw(e); }
auto pte_from_raw(uint64_t x) -> PageTableEntry { return host::entry(x); }
auto entry_at(PageTable* p, size_t n) -> PageTableEntry& { return p->entries[n]; }
auto alloc_zeroed_page_table() -> PageTable* { return host::table(); }
void flush_pagemap_after_update(PageTable*, uint64_t, bool) {
    CHECK(host::exclusive > 0);
    ++host::flushes;
}
auto clone_anonymous_swap_mappings(PageTable*, PageTable*) -> bool {
    ++host::swap_clones;
    return true;
}
void owned_frame_untrack_mapping(PageTable*, uint64_t, uint64_t) {}
void owned_frame_track_fresh_normal_mapping(PageTable* root, uint64_t va, uint64_t pa, uint64_t) {
    CHECK(host::exclusive > 0);
    auto* e = host::leaf(root, va);
    CHECK(e && e->present && (static_cast<uint64_t>(e->frame) << 12) == pa);
    CHECK(host::blocks.at(pa).refs == 1);
    CHECK(std::memcmp(host::data(pa), host::expected.at(va).data(), 4096) == 0);
}
auto is_watched_mmap_vaddr(uint64_t) -> bool { return false; }
struct log {
    template <class... A>
    static void warn(const char*, A...) {};
};
// ACTUAL_EXTRACTED_COPIER_INSERTION
}  // namespace ker::mod::mm::virt
// ACTUAL_FACTORY_INSERTION
namespace host {
namespace virt = mm::virt;
using Context = numa::AllocationContext;
using Result = numa::AllocationResult;
constexpr uint64_t VA = 0x6f1fe000;
void tests() {
    reset();
    {
        Root a, b;
        for (unsigned i = 0; i < 3; ++i) a.source(VA + i * PAGE, 0);
        Context c{{2, numa::Mode::BIND}, 0, 0};
        Result r;
        CHECK(virt::selftest_deep_copy_user_pagemap_eager(a.value, b.value, c, r, UINT64_MAX));
        for (unsigned i = 0; i < 3; ++i) copied(a, b, VA + i * PAGE, 1);
        CHECK(r.span.empty() && range_allocations == 3 && legacy_allocations == 0 && swap_clones == 1);
        CHECK(c.policy == numa::Policy(2, numa::Mode::BIND));
    }
    CHECK(blocks.empty());
    reset();
    {
        Root a, b;
        for (unsigned i = 0; i < 5; ++i) a.source(VA + i * PAGE, 0);
        Context c{{3, numa::Mode::INTERLEAVE}, 0, 999};
        Result r;
        CHECK(virt::selftest_deep_copy_user_pagemap_eager(a.value, b.value, c, r, UINT64_MAX));
        for (unsigned i = 0; i < 5; ++i) copied(a, b, VA + i * PAGE, static_cast<unsigned>((VA / PAGE + i) % 2));
        CHECK(c.virtual_page_index == (VA / PAGE + 4));
    }
    for (unsigned fail : {0U, 1U, 3U}) {
        reset();
        {
            Root a, b;
            for (unsigned i = 0; i < 4; ++i) a.source(VA + i * PAGE, 0);
            Context c{{2, numa::Mode::BIND}, 0, 0};
            Result r;
            CHECK(!virt::selftest_deep_copy_user_pagemap_eager(a.value, b.value, c, r, fail));
            for (unsigned i = 0; i < fail; ++i) copied(a, b, VA + i * PAGE, 1);
            auto* e = leaf(b.value, VA + fail * PAGE);
            CHECK(!e || !e->present);
            CHECK(r.span.empty() && range_allocations == fail && exclusive == 0);
        }
        CHECK(blocks.empty());
    }
    reset();
    {
        Root a, b;
        for (unsigned i = 0; i < 4; ++i) a.source(VA + i * PAGE, 0);
        capacity[1] = 2;
        Context c{{2, numa::Mode::BIND}, 0, 0};
        Result r;
        CHECK(!virt::selftest_deep_copy_user_pagemap_eager(a.value, b.value, c, r, UINT64_MAX));
        for (unsigned i = 0; i < 2; ++i) copied(a, b, VA + i * PAGE, 1);
        CHECK(range_allocations == 3 && legacy_allocations == 0 && r.span.empty() && r.error == numa::AllocationError::EXHAUSTED);
    }
    CHECK(blocks.empty());
    reset();
    {
        Root a, b;
        a.source(VA, 0);
        Context c;
        numa::Policy policy{2, numa::Mode::BIND};
        CHECK(numa::capture_allocation_context(policy, c) == numa::AllocationError::NONE);
        auto captured = c;
        policy = {};
        cpu = 1;
        Result r;
        CHECK(virt::selftest_deep_copy_user_pagemap_eager(a.value, b.value, c, r, UINT64_MAX));
        copied(a, b, VA, 1);
        CHECK(c.policy == captured.policy && c.local_node == captured.local_node && cpu_reads == 1);
    }
    reset();
    eager = false;
    {
        Root a, b;
        a.source(VA, 0);
        a.source(VA + PAGE, 0, mm::paging::PAGE_PRESENT | mm::paging::PAGE_USER | mm::paging::PAGE_NX);
        *leaf(a.value, VA + 2 * PAGE, true) = entry(mm::paging::PAGE_RESERVED);
        CHECK(virt::deep_copy_user_pagemap_cow(a.value, b.value));
        CHECK(raw(*leaf(a.value, VA)) == raw(*leaf(b.value, VA)));
        CHECK((raw(*leaf(a.value, VA)) & mm::paging::PAGE_COW) && !leaf(a.value, VA)->writable);
        CHECK(blocks.at(static_cast<uint64_t>(leaf(a.value, VA)->frame) << 12).refs == 2);
        CHECK(raw(*leaf(b.value, VA + 2 * PAGE)) == mm::paging::PAGE_RESERVED && legacy_allocations == 0 && range_allocations == 0);
    }
    reset(1);
    {
        Root a, b;
        a.source(VA, 0);
        Context c{};
        c.local_node = 0;
        Result r;
        CHECK(virt::deep_copy_user_pagemap_cow(a.value, b.value, c, r));
        copied(a, b, VA, 0);
        CHECK(legacy_allocations == 1 && range_allocations == 0 && r.span.empty());
    }
    reset();
    {
        Root a, b;
        a.source(VA, 0);
        Context c{{0, numa::Mode::BIND}, 0, 0};
        Result r;
        auto nt = tables.size();
        CHECK(!virt::selftest_deep_copy_user_pagemap_eager(a.value, b.value, c, r, UINT64_MAX));
        CHECK(tables.size() == nt && range_allocations == 0);
        c = {{2, numa::Mode::BIND}, 0, 0};
        CHECK(!virt::selftest_deep_copy_user_pagemap_eager(a.value, a.value, c, r, UINT64_MAX));
        topology_ready = false;
        CHECK(!virt::deep_copy_user_pagemap_cow(a.value, b.value, c, r));
        topology_ready = true;
        // Actual factory owns this output before entry; refusal cannot consume it
        // or mutate destination tables. The Host factory endpoint uses a local
        // scope counter, independently of the copier under test.
        exclusive = 1;
        numa::try_allocate_pages_into(r, c, 1, PAGE, mm::PhysicalPageOwner::USER_PRIVATE_MAPPING,
                                      mm::phys::PageInitialization::FULL_OVERWRITE, numa::ReclaimMode::NEVER, "owned-input");
        exclusive = 0;
        CHECK(!r.span.empty());
        auto* owned = r.span.data();
        nt = tables.size();
        CHECK(!virt::selftest_deep_copy_user_pagemap_eager(a.value, b.value, c, r, UINT64_MAX));
        CHECK(r.span.data() == owned && tables.size() == nt && range_allocations == 1);
    }
    for (unsigned level : {3U, 2U}) {
        reset();
        {
            Root a, b;
            auto* t = a.value;
            for (unsigned l = 4; l > level; --l) {
                auto* n = table();
                t->entries[0] = entry(reinterpret_cast<uint64_t>(n) | 7);
                t = n;
            }
            t->entries[0] = entry(0x70000000ULL | 0x87);
            Context c{{2, numa::Mode::BIND}, 0, 0};
            Result r;
            CHECK(!virt::selftest_deep_copy_user_pagemap_eager(a.value, b.value, c, r, UINT64_MAX));
            CHECK(range_allocations == 0 && r.span.empty());
            t->entries[0] = {};
        }
    }
    CHECK(blocks.empty() && tables.empty() && registered.empty() && exclusive == 0);
    std::puts("PASS actual copier+factory: bytes/ref1/VA-interleave/strict-prefix/context/legacy/PS-refusal");
}
}  // namespace host
int main() {
    try {
        host::tests();
        return 0;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "FAIL %s\n", e.what());
        return 1;
    }
}
