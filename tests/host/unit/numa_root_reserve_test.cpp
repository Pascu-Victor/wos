#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <new>
#include <platform/acpi/numa_boot.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/numa_alloc.hpp>
#include <platform/mm/numa_root_reserve.hpp>
#include <platform/mm/phys.hpp>
#include <platform/sys/spinlock.hpp>
#include <vector>

// Defined only by the private harness's exact-body module copy, never by the
// production candidate API. A separate process tests malformed private state.
void host_corrupt_root_reserve_member_index();

// Actual PageAllocator, numa_alloc.cpp, new reserve.cpp and exact extracted
// physical helper are compiled. Host seams below replace native IRQ/CR3, zone
// discovery/bridge, topology publication and refdec/global accounting only.
namespace host {
namespace mm = ker::mod::mm;
namespace rs = mm::numa::root_reserve;
namespace nt = ker::mod::acpi::numa;
constexpr uint64_t PAGE = mm::paging::PAGE_SIZE;
constexpr auto TABLE = mm::PhysicalPageOwner::PAGE_TABLE;
constexpr auto POOL = mm::PhysicalPageOwner::PAGE_TABLE_POOL_RESERVE;
uint64_t checks{};
void require(bool value, const char* description) {
    ++checks;
    if (!value) {
        std::fprintf(stderr, "FAIL %s\n", description);
        std::abort();
    }
}
struct Arena {
    void* storage{};
    mm::PageAllocator* allocator{};
    uint64_t physical{};
    size_t bytes{};
    Arena(uint64_t first, size_t length) : physical(first), bytes(length) {
        storage = std::aligned_alloc(PAGE, length);
        require(storage != nullptr, "Host storage");
        std::memset(storage, 0, length);
        allocator = ::new (storage) mm::PageAllocator{};
        allocator->init(reinterpret_cast<uint64_t>(storage), length);
    }
    ~Arena() { std::free(storage); }
    auto contains(uint64_t pointer) const -> bool { return pointer >= allocator->base && pointer - allocator->base < bytes; }
    auto index(const void* pointer) const -> uint32_t {
        return static_cast<uint32_t>((reinterpret_cast<uint64_t>(pointer) - allocator->base) / PAGE);
    }
    auto copy() const -> std::vector<std::byte> {
        std::vector<std::byte> result(bytes);
        std::memcpy(result.data(), storage, bytes);
        return result;
    }
};
std::array<Arena*, 2> arenas{};
nt::Topology topology{};
bool topology_ready{};
auto arena_for(const void* pointer) -> Arena* {
    for (auto* arena : arenas)
        if (arena != nullptr && arena->contains(reinterpret_cast<uint64_t>(pointer))) return arena;
    return nullptr;
}
void require_no_locks() {
    require(ker::mod::sys::host_spinlocks_held == 0, "reserve lock not held across backend/zero");
    for (auto* arena : arenas)
        if (arena != nullptr) require(!arena->allocator->lock_held.load(), "allocator lock not held");
}
auto allocate(Arena& arena, uint64_t bytes, mm::PhysicalPageOwner owner = TABLE) -> void* {
    auto const FLAGS = arena.allocator->lock_irq();
    auto* result = arena.allocator->alloc(bytes, owner, 0x12345678);
    arena.allocator->unlock_irq(FLAGS);
    return result;
}
void release(Arena& arena, void* pointer, uint64_t bytes) {
    auto const FLAGS = arena.allocator->lock_irq();
    require(arena.allocator->free(pointer) == bytes, "real allocator release");
    arena.allocator->unlock_irq(FLAGS);
}
auto stats() -> rs::Snapshot {
    rs::Snapshot result;
    rs::snapshot(result);
    return result;
}
uint32_t zeros{};
void zero(mm::paging::PageTable* table) {
    require_no_locks();
    auto const SNAPSHOT = stats();
    auto* arena = arena_for(table);
    require(arena != nullptr, "zero exact member arena");
    auto const NODE = static_cast<size_t>(arena == arenas[0] ? 0 : 1);
    require(SNAPSHOT.nodes[NODE].transfers == 1, "RETURNING remains counted across zero");
    require(arena->allocator->page_owners[arena->index(table)] == static_cast<uint8_t>(TABLE), "zero before POOL retag");
    std::memset(table, 0, PAGE);
    ++zeros;
}
void invariant(const rs::Snapshot& snapshot) {
    require(snapshot.published && snapshot.healthy, "published healthy reserve");
    uint32_t members = 0;
    for (size_t i = 0; i < snapshot.node_count; ++i) {
        auto const& node = snapshot.nodes[i];
        require(node.capacity == node.available + node.loaned + node.transfers, "capacity identity");
        require(node.capacity + node.shortfall == rs::PAGES_PER_NODE, "shortfall identity");
        members += node.capacity;
    }
    require(members == snapshot.members, "member identity");
}
}  // namespace host

namespace ker::mod::acpi::numa {
auto boot_topology() noexcept -> const Topology* { return host::topology_ready ? &host::topology : nullptr; }
}  // namespace ker::mod::acpi::numa
namespace ker::mod::mm::addr {
auto get_phys_pointer(uint64_t pointer) -> uint64_t* {
    auto* arena = host::arena_for(reinterpret_cast<void*>(pointer));
    host::require(arena != nullptr, "physical address belongs to real arena");
    return reinterpret_cast<uint64_t*>(arena->physical + pointer - arena->allocator->base);
}
}  // namespace ker::mod::mm::addr
namespace ker::mod::mm::phys {
auto find_allocator_for_page(void* page, PageAllocator*& allocator, uint32_t& index) -> bool {
    host::require_no_locks();
    auto* arena = host::arena_for(page);
    if (arena == nullptr) return false;
    allocator = arena->allocator;
    index = arena->index(page);
    return true;
}
auto page_alloc_order0_span_in_range_may_fail(PhysicalPageOwner owner, uint64_t first, uint64_t end, uint32_t pages, uint64_t alignment,
                                              PageInitialization initialization) -> void* {
    host::require_no_locks();
    for (auto* arena : host::arenas) {
        auto const FLAGS = arena->allocator->lock_irq();
        auto* result = arena->allocator->alloc_order0_span_in_range(arena->physical, first, end, pages, alignment, owner, 0xabcdef);
        arena->allocator->unlock_irq(FLAGS);
        if (result != nullptr) {
            if (initialization == PageInitialization::ZERO) std::memset(result, 0, pages * host::PAGE);
            return result;
        }
    }
    return nullptr;
}
auto page_alloc_may_fail(PhysicalPageOwner, uint64_t, std::string_view) -> void* {
    host::require(false, "BIND seeding must not take UMA LOCAL cache shortcut");
    return nullptr;
}
auto page_alloc_full_overwrite_page_may_fail(PhysicalPageOwner, std::string_view) -> void* {
    host::require(false, "BIND seeding must not take UMA LOCAL overwrite shortcut");
    return nullptr;
}
auto page_alloc_with_reclaim_may_fail(PhysicalPageOwner, uint64_t, std::string_view) -> void* {
    host::require(false, "BIND seeding must not take UMA LOCAL reclaim shortcut");
    return nullptr;
}
auto page_alloc_full_overwrite_page_with_reclaim_may_fail(PhysicalPageOwner, std::string_view) -> void* {
    host::require(false, "BIND seeding must not take UMA LOCAL overwrite reclaim shortcut");
    return nullptr;
}
auto page_mark_kind(void* page, PageKind kind) -> bool {
    host::require_no_locks();
    auto* arena = host::arena_for(page);
    if (arena == nullptr) return false;
    auto const FLAGS = arena->allocator->lock_irq();
    bool const RESULT = arena->allocator->mark_allocated_block_kind(page, kind);
    arena->allocator->unlock_irq(FLAGS);
    return RESULT;
}
auto page_ref_dec(void* page) -> uint32_t {
    host::require_no_locks();
    auto* arena = host::arena_for(page);
    host::require(arena != nullptr, "refdec arena");
    auto& ref = arena->allocator->page_refcounts[arena->index(page)];
    uint32_t const OLD = ref.fetch_sub(1);
    host::require(OLD != 0, "ref underflow");
    if (OLD == 1) host::release(*arena, page, host::PAGE);
    return OLD - 1;
}
}  // namespace ker::mod::mm::phys

// Link primitives for the actual copied-context factory. The runtime adapter
// extracts the exact current inline PreemptGuard; no scheduler is simulated.
namespace host {
uint32_t preempt_depth{};
}
namespace ker::mod::sched {
void preempt_disable() { ++host::preempt_depth; }
void preempt_enable() {
    host::require(host::preempt_depth != 0, "balanced Host preempt guard");
    --host::preempt_depth;
}
}  // namespace ker::mod::sched
namespace ker::mod::cpu {
auto current_cpu() -> uint64_t {
    host::require(host::preempt_depth != 0, "CPU primitive queried under real extracted guard");
    return 0;
}
}  // namespace ker::mod::cpu

int main(int argc, char** argv) {
    using namespace host;
    namespace phys = mm::phys;
    require(rs::initialize_after_topology() == rs::Error::NO_TOPOLOGY, "no fabricated early topology");
    require(rs::try_take(0).error == rs::Error::NOT_READY, "no early reserve");
    Arena large(0x40003000, 4 * 1024 * 1024);
    Arena small(0x50005000, 32 * PAGE);
    arenas = {&large, &small};
    auto* allocator = large.allocator;
    auto* page = allocate(large, PAGE);
    require(page != nullptr && phys::page_mark_kind(page, mm::PageKind::PAGE_TABLE), "real page-table allocation");
    auto const FREE = allocator->free_count;
    auto const INDEX = large.index(page);
    auto unchanged_failure = [&](void* pointer, mm::PhysicalPageOwner expected, mm::PhysicalPageOwner desired) {
        auto const BEFORE = large.copy();
        require(!phys::page_table_owner_transition_if_exclusive(pointer, expected, desired), "invalid transition rejected");
        require(std::memcmp(BEFORE.data(), large.storage, large.bytes) == 0, "invalid transition leaves complete arena unchanged");
    };
    unchanged_failure(nullptr, TABLE, POOL);
    unchanged_failure(static_cast<std::byte*>(page) + 1, TABLE, POOL);
    unchanged_failure(large.storage, TABLE, POOL);
    unchanged_failure(page, POOL, TABLE);
    unchanged_failure(page, mm::PhysicalPageOwner::INVALID, POOL);
    unchanged_failure(page, TABLE, mm::PhysicalPageOwner::USER_PRIVATE_MAPPING);
    allocator->page_refcounts[INDEX].store(2);
    unchanged_failure(page, TABLE, POOL);
    allocator->page_refcounts[INDEX].store(0);
    unchanged_failure(page, TABLE, POOL);
    allocator->page_refcounts[INDEX].store(1);
    require(phys::page_mark_kind(page, mm::PageKind::NORMAL), "normal kind");
    unchanged_failure(page, TABLE, POOL);
    require(phys::page_mark_kind(page, mm::PageKind::PAGE_TABLE), "restore page-table kind");
    allocator->page_flags[INDEX] |= 0x20;
    unchanged_failure(page, TABLE, POOL);
    allocator->page_flags[INDEX] = mm::PageAllocator::FLAG_ALLOC_HEAD;
    auto* order1 = allocate(large, 2 * PAGE);
    require(order1 != nullptr && phys::page_mark_kind(order1, mm::PageKind::PAGE_TABLE), "real order1");
    unchanged_failure(order1, TABLE, POOL);
    unchanged_failure(static_cast<std::byte*>(order1) + PAGE, TABLE, POOL);
    release(large, order1, 2 * PAGE);
    uint64_t const CALLER =
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
        allocator->page_callers[INDEX];
#else
        0;
#endif
    require(phys::page_table_owner_transition_if_exclusive(page, TABLE, POOL), "checked transfer to POOL");
    require(allocator->owner_pages[static_cast<size_t>(POOL)] == 1 && allocator->owner_objects[static_cast<size_t>(POOL)] == 1,
            "POOL actual page and object debt");
    require(phys::page_table_owner_transition_if_exclusive(page, POOL, POOL), "same-owner validation");
    require(phys::page_table_owner_transition_if_exclusive(page, POOL, TABLE), "checked transfer to TABLE");
    require(allocator->free_count == FREE && allocator->page_refcounts[INDEX].load() == 1, "retag does not free/add ref");
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
    require(allocator->page_callers[INDEX] == CALLER, "retag preserves allocation caller");
#else
    static_cast<void>(CALLER);
#endif
    release(large, page, PAGE);

    topology.node_count = topology.cpu_count = 3;
    topology.memory_range_count = 2;
    for (uint16_t node = 0; node < 3; ++node) {
        topology.cpu_nodes[node] = node;
        topology.nodes[node].cpus.set(node);
        for (uint16_t other = 0; other < 3; ++other) topology.distances[node][other] = node == other ? 10 : 20;
    }
    for (size_t i = 0; i < arenas.size(); ++i) {
        topology.memory[i] = {arenas[i]->physical, arenas[i]->bytes, static_cast<uint16_t>(i), 0};
        topology.nodes[i].usable_bytes = arenas[i]->bytes;
    }
    topology_ready = true;
    uint32_t const LARGE_FREE = allocator->free_count;
    uint32_t const SMALL_FREE = small.allocator->free_count;
    require(SMALL_FREE > 0 && SMALL_FREE < rs::PAGES_PER_NODE, "actual tiny-node capacity");
    require(rs::initialize_after_topology() == rs::Error::NONE, "actual BIND seeding");
    require(rs::initialize_after_topology() == rs::Error::ALREADY_INITIALIZED, "immutable membership");
    auto baseline = stats();
    invariant(baseline);
    require(baseline.nodes[0].capacity == 64 && baseline.nodes[1].capacity == SMALL_FREE && baseline.nodes[2].capacity == 0,
            "full tiny and memoryless capacity");
    require(allocator->free_count == LARGE_FREE - 64 && small.allocator->free_count == 0, "seeding consumes actual free frames once");
    require(rs::try_take(3).error == rs::Error::INVALID_NODE, "invalid node refusal");
    require(rs::try_take(2).error == rs::Error::EMPTY, "no memoryless fallback");
    if (argc == 2 && std::strcmp(argv[1], "--corrupt-member-index") == 0) {
        host_corrupt_root_reserve_member_index();
        auto const BEFORE = stats();
        auto result = rs::try_take(0);
        require(result.table == nullptr && result.error == rs::Error::CORRUPT, "invalid member index safely refused");
        auto const AFTER = stats();
        require(!AFTER.healthy && AFTER.nodes[0].available == BEFORE.nodes[0].available && AFTER.nodes[0].loaned == 0 &&
                    AFTER.nodes[0].transfers == 0,
                "invalid member index consumes no frame or capacity");
        std::printf("PASS malformed_member_index checks=%llu\n", static_cast<unsigned long long>(checks));
        return 0;
    }
    require(argc == 1, "known private Host invocation");
    auto* ordinary = allocate(large, PAGE);
    require(ordinary != nullptr, "ordinary PMM remains usable");
    require(rs::try_return(static_cast<mm::paging::PageTable*>(ordinary), zero) == rs::ReturnResult::NOT_MEMBER,
            "ordinary frame not adopted");
    require(stats().sequence == baseline.sequence, "ordinary allocation does not consume membership");
    release(large, ordinary, PAGE);
    std::array<mm::paging::PageTable*, 64> held{};
    for (size_t i = 0; i < held.size(); ++i) {
        auto taken = rs::try_take(0);
        require(taken.error == rs::Error::NONE && taken.table != nullptr, "node0 actual fallback");
        require(arena_for(taken.table) == &large, "member exact physical origin");
        auto const IDX = large.index(taken.table);
        require(allocator->page_owners[IDX] == static_cast<uint8_t>(TABLE) && allocator->page_refcounts[IDX].load() == 1,
                "loaned member sole TABLE owner");
        held[i] = taken.table;
        for (auto const& entry : taken.table->entries) require(entry.frame == 0 && !entry.present, "seeded zero page");
        std::memset(taken.table, 0xa5, PAGE);
    }
    require(rs::try_take(0).error == rs::Error::EMPTY && stats().nodes[1].available == SMALL_FREE,
            "empty requested node never borrows other reserve");
    for (auto* table : held) require(rs::try_return(table, zero) == rs::ReturnResult::RETURNED, "origin return");
    for (unsigned i = 0; i < 1000; ++i) {
        mm::numa::NodeId const NODE = static_cast<mm::numa::NodeId>(i % 2);
        auto taken = rs::try_take(NODE);
        require(taken.error == rs::Error::NONE, "1000 real frame borrow");
        require(reinterpret_cast<std::byte*>(taken.table)[123] == std::byte{}, "recycled page zero");
        std::memset(taken.table, 0x5a, PAGE);
        require(rs::try_return(taken.table, zero) == rs::ReturnResult::RETURNED, "1000 real frame return");
    }
    auto final = stats();
    invariant(final);
    require(final.sequence > baseline.sequence && final.members == baseline.members, "nonwrapping membership sequence");
    for (size_t i = 0; i < final.node_count; ++i)
        require(final.nodes[i].available == final.nodes[i].capacity && final.nodes[i].loaned == 0 && final.nodes[i].transfers == 0,
                "all actual frame debt returned");
    require(allocator->free_count == LARGE_FREE - 64 && small.allocator->free_count == 0, "borrow return never frees permanent capacity");
    require(allocator->owner_pages[static_cast<size_t>(TABLE)] == 0 && allocator->owner_objects[static_cast<size_t>(TABLE)] == 0 &&
                allocator->owner_pages[static_cast<size_t>(POOL)] == 64 && allocator->owner_objects[static_cast<size_t>(POOL)] == 64,
            "actual allocator owner closure");
    require(rs::try_return(held[0], zero) == rs::ReturnResult::CORRUPT, "duplicate return cannot fall through");
    auto rejected = stats();
    require(!rejected.healthy && rejected.members == final.members && rejected.nodes[0].available == 64,
            "corrupt return creates no capacity");
    std::printf("PASS actual_allocator_root_reserve checks=%llu cycles=1000 zeros=%u\n", static_cast<unsigned long long>(checks), zeros);
    return 0;
}
