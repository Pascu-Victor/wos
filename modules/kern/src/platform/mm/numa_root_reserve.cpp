#include "numa_root_reserve.hpp"

#include <algorithm>
#include <atomic>
#include <bit>
#include <cstddef>
#include <cstring>
#include <platform/acpi/numa_boot.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/numa_alloc.hpp>
#include <platform/mm/phys.hpp>
#include <platform/sys/spinlock.hpp>

namespace ker::mod::mm::numa::root_reserve {
namespace {
enum class Publication : uint8_t { EMPTY, SEEDING, READY, FAILED };
enum class MemberState : uint8_t { AVAILABLE, ACQUIRING, LOANED_ROOT, RETURNING };
struct Member {
    paging::PageTable* table{};
    NodeId origin{INVALID_NODE};
    uint8_t slot{};
    MemberState state{MemberState::AVAILABLE};
};
struct NodeState {
    NodeStats counts{};
    uint64_t available_mask{};
    std::array<uint16_t, PAGES_PER_NODE> members{};
};
struct Reserve {
    sys::Spinlock lock{};
    std::array<Member, MAX_MEMBERS> members{};
    std::array<NodeState, MAX_NODES> nodes{};
    uint64_t sequence{};
    uint32_t member_count{};
    uint16_t node_count{};
    bool healthy{true};
};
static_assert(sizeof(Reserve) + sizeof(Snapshot) <= 128 * 1024);
Reserve reserve{};                                         // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<Publication> publication{Publication::EMPTY};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

// Aggregate copies can lower to uninstrumented libc++ helpers. Read the exact
// initialized static representation on the exclusive BSP before any lock or
// publication, so diagnostic shadow checks run in an allocation-safe context.
[[gnu::noinline]] void prepare_state_shadow(const volatile uint8_t* bytes, size_t size) noexcept {
    for (size_t i = 0; i < size; ++i) static_cast<void>(bytes[i]);
}

auto address(const paging::PageTable* table) noexcept -> uintptr_t { return reinterpret_cast<uintptr_t>(table); }

// Only table/origin/slot are consulted without lock after release publication;
// these fields are immutable. State and counts always require the pool lock.
auto find_member(const paging::PageTable* table) noexcept -> Member* {
    uintptr_t const FRAME = address(table) & ~(paging::PAGE_SIZE - 1);
    uint32_t first = 0;
    uint32_t last = reserve.member_count;
    while (first < last) {
        uint32_t const MIDDLE = first + (last - first) / 2;
        if (address(reserve.members[MIDDLE].table) < FRAME)
            first = MIDDLE + 1;
        else
            last = MIDDLE;
    }
    return first < reserve.member_count && address(reserve.members[first].table) == FRAME ? &reserve.members[first] : nullptr;
}

auto advance_locked() noexcept -> bool {
    if (!reserve.healthy || reserve.sequence == UINT64_MAX) {
        reserve.healthy = false;
        return false;
    }
    ++reserve.sequence;
    return true;
}

void corrupt() noexcept {
    uint64_t const FLAGS = reserve.lock.lock_irqsave();
    if (reserve.sequence != UINT64_MAX) ++reserve.sequence;
    reserve.healthy = false;
    reserve.lock.unlock_irqrestore(FLAGS);
}

auto valid_node_locked(NodeId node) noexcept -> bool {
    auto const& state = reserve.nodes[node];
    auto const& counts = state.counts;
    return counts.capacity <= PAGES_PER_NODE && counts.available <= counts.capacity && counts.loaned <= counts.capacity &&
           counts.transfers <= counts.capacity && counts.available + counts.loaned + counts.transfers == counts.capacity &&
           counts.available == static_cast<uint32_t>(std::popcount(state.available_mask)) &&
           (counts.capacity == PAGES_PER_NODE || (state.available_mask >> counts.capacity) == 0);
}
}  // namespace

auto initialize_after_topology() noexcept -> Error {
    auto const* topology = acpi::numa::boot_topology();
    if (topology == nullptr || topology->node_count == 0 || topology->node_count > MAX_NODES) return Error::NO_TOPOLOGY;
    Publication expected = Publication::EMPTY;
    if (!publication.compare_exchange_strong(expected, Publication::SEEDING, std::memory_order_acq_rel, std::memory_order_acquire))
        return Error::ALREADY_INITIALIZED;
    // Initialize values first; these aggregate assignments alone do not prove
    // that an out-of-line provider materialized diagnostic shadow.
    for (auto& member : reserve.members) member = Member{};
    for (auto& node : reserve.nodes) node = NodeState{};
    reserve.member_count = 0;
    reserve.sequence = 0;
    reserve.healthy = true;
    prepare_state_shadow(reinterpret_cast<const volatile uint8_t*>(&reserve), sizeof(reserve));
    uint64_t const SHADOW_FLAGS = reserve.lock.lock_irqsave();
    reserve.lock.unlock_irqrestore(SHADOW_FLAGS);
    reserve.node_count = topology->node_count;
    for (NodeId node = 0; node < reserve.node_count; ++node) reserve.nodes[node].counts.shortfall = PAGES_PER_NODE;
    for (NodeId node = 0; node < reserve.node_count; ++node) {
        auto& counts = reserve.nodes[node].counts;
        for (uint32_t slot = 0; slot < PAGES_PER_NODE; ++slot) {
            AllocationContext const CONTEXT{{NodeMask{1} << node, Mode::BIND}, node, 0};
            auto allocated = try_allocate_pages(CONTEXT, 1, paging::PAGE_SIZE, PhysicalPageOwner::PAGE_TABLE_POOL_RESERVE,
                                                phys::PageInitialization::ZERO);
            if (allocated.span.empty()) {
                if (allocated.error == AllocationError::EXHAUSTED) break;
                reserve.healthy = false;
                counts.shortfall = PAGES_PER_NODE - counts.capacity;
                publication.store(Publication::FAILED, std::memory_order_release);
                return Error::CORRUPT;
            }
            auto* table = static_cast<paging::PageTable*>(allocated.span.data());
            bool duplicate = false;
            for (uint32_t i = 0; i < reserve.member_count; ++i) duplicate |= reserve.members[i].table == table;
            auto const PHYSICAL = reinterpret_cast<uint64_t>(addr::get_phys_pointer(address(table)));
            if (allocated.error != AllocationError::NONE || allocated.span.pages() != 1 || duplicate || allocated.actual != node ||
                (address(table) & (paging::PAGE_SIZE - 1)) != 0 || physical_node(PHYSICAL) != node ||
                !phys::page_mark_kind(table, PageKind::PAGE_TABLE) ||
                !phys::page_table_owner_transition_if_exclusive(table, PhysicalPageOwner::PAGE_TABLE_POOL_RESERVE,
                                                                PhysicalPageOwner::PAGE_TABLE_POOL_RESERVE)) {
                // A backend ownership invariant failed. In particular a duplicate
                // must not let RAII free an already adopted permanent member.
                static_cast<void>(allocated.span.release());
                reserve.healthy = false;
                counts.shortfall = PAGES_PER_NODE - counts.capacity;
                publication.store(Publication::FAILED, std::memory_order_release);
                return Error::CORRUPT;
            }
            reserve.members[reserve.member_count++] = {table, node, static_cast<uint8_t>(slot), MemberState::AVAILABLE};
            ++counts.capacity;
            ++counts.available;
            reserve.nodes[node].available_mask |= uint64_t{1} << slot;
            static_cast<void>(allocated.span.release());
        }
        counts.shortfall = PAGES_PER_NODE - counts.capacity;
    }
    std::sort(reserve.members.begin(), reserve.members.begin() + reserve.member_count,
              [](auto const& left, auto const& right) { return address(left.table) < address(right.table); });
    for (uint32_t i = 0; i < reserve.member_count; ++i) {
        auto const& member = reserve.members[i];
        reserve.nodes[member.origin].members[member.slot] = static_cast<uint16_t>(i);
    }
    reserve.sequence = 1;
    publication.store(Publication::READY, std::memory_order_release);
    return Error::NONE;
}

auto try_take(NodeId requested) noexcept -> TakeResult {
    auto const PUBLICATION = publication.load(std::memory_order_acquire);
    if (PUBLICATION == Publication::FAILED) return {nullptr, Error::CORRUPT};
    if (PUBLICATION != Publication::READY) return {nullptr, Error::NOT_READY};
    if (requested >= reserve.node_count) return {nullptr, Error::INVALID_NODE};
    uint64_t const FLAGS = reserve.lock.lock_irqsave();
    auto& node = reserve.nodes[requested];
    if (!reserve.healthy || !valid_node_locked(requested)) {
        reserve.healthy = false;
        reserve.lock.unlock_irqrestore(FLAGS);
        return {nullptr, Error::CORRUPT};
    }
    if (node.available_mask == 0) {
        reserve.lock.unlock_irqrestore(FLAGS);
        return {nullptr, Error::EMPTY};
    }
    auto const SLOT = static_cast<uint32_t>(std::countr_zero(node.available_mask));
    auto const INDEX = node.members[SLOT];
    if (INDEX >= reserve.member_count || INDEX >= MAX_MEMBERS) {
        reserve.healthy = false;
        reserve.lock.unlock_irqrestore(FLAGS);
        return {nullptr, Error::CORRUPT};
    }
    auto& member = reserve.members[INDEX];
    if (member.state != MemberState::AVAILABLE || member.origin != requested || member.slot != SLOT || !advance_locked()) {
        reserve.healthy = false;
        reserve.lock.unlock_irqrestore(FLAGS);
        return {nullptr, Error::CORRUPT};
    }
    member.state = MemberState::ACQUIRING;
    node.available_mask &= ~(uint64_t{1} << SLOT);
    --node.counts.available;
    ++node.counts.transfers;
    reserve.lock.unlock_irqrestore(FLAGS);

    if (!phys::page_table_owner_transition_if_exclusive(member.table, PhysicalPageOwner::PAGE_TABLE_POOL_RESERVE,
                                                        PhysicalPageOwner::PAGE_TABLE)) {
        corrupt();
        return {nullptr, Error::CORRUPT};
    }
    uint64_t const COMMIT_FLAGS = reserve.lock.lock_irqsave();
    if (member.state != MemberState::ACQUIRING || !valid_node_locked(requested) || !advance_locked()) {
        reserve.healthy = false;
        reserve.lock.unlock_irqrestore(COMMIT_FLAGS);
        return {nullptr, Error::CORRUPT};
    }
    member.state = MemberState::LOANED_ROOT;
    --node.counts.transfers;
    ++node.counts.loaned;
    reserve.lock.unlock_irqrestore(COMMIT_FLAGS);
    return {member.table, Error::NONE};
}

auto try_return(paging::PageTable* table, ZeroTable zero_table) noexcept -> ReturnResult {
    auto const PUBLICATION = publication.load(std::memory_order_acquire);
    if (PUBLICATION == Publication::EMPTY || PUBLICATION == Publication::SEEDING) return ReturnResult::NOT_MEMBER;
    if (PUBLICATION == Publication::FAILED) return ReturnResult::CORRUPT;
    auto* member = find_member(table);
    if (member == nullptr) return ReturnResult::NOT_MEMBER;
    if (table != member->table || zero_table == nullptr || PUBLICATION != Publication::READY) return ReturnResult::CORRUPT;
    uint64_t const FLAGS = reserve.lock.lock_irqsave();
    auto& node = reserve.nodes[member->origin];
    if (member->state != MemberState::LOANED_ROOT || !valid_node_locked(member->origin) || !advance_locked()) {
        reserve.healthy = false;
        reserve.lock.unlock_irqrestore(FLAGS);
        return ReturnResult::CORRUPT;
    }
    member->state = MemberState::RETURNING;
    --node.counts.loaned;
    ++node.counts.transfers;
    reserve.lock.unlock_irqrestore(FLAGS);

    if (!phys::page_table_owner_transition_if_exclusive(table, PhysicalPageOwner::PAGE_TABLE, PhysicalPageOwner::PAGE_TABLE)) {
        corrupt();
        return ReturnResult::CORRUPT;
    }
    zero_table(table);
    if (!phys::page_table_owner_transition_if_exclusive(table, PhysicalPageOwner::PAGE_TABLE, PhysicalPageOwner::PAGE_TABLE_POOL_RESERVE)) {
        corrupt();
        return ReturnResult::CORRUPT;
    }
    uint64_t const COMMIT_FLAGS = reserve.lock.lock_irqsave();
    if (member->state != MemberState::RETURNING || !valid_node_locked(member->origin) || !advance_locked()) {
        reserve.healthy = false;
        reserve.lock.unlock_irqrestore(COMMIT_FLAGS);
        return ReturnResult::CORRUPT;
    }
    member->state = MemberState::AVAILABLE;
    --node.counts.transfers;
    ++node.counts.available;
    node.available_mask |= uint64_t{1} << member->slot;
    reserve.lock.unlock_irqrestore(COMMIT_FLAGS);
    return ReturnResult::RETURNED;
}

void snapshot(Snapshot& out) noexcept {
    // libc++ memset may be an excluded out-of-line provider. Volatile stores
    // make the exact output's instrumentation execute before taking the lock.
    auto* bytes = reinterpret_cast<volatile uint8_t*>(&out);
    for (size_t i = 0; i < sizeof(out); ++i) bytes[i] = 0;
    auto const PUBLICATION = publication.load(std::memory_order_acquire);
    if (PUBLICATION == Publication::EMPTY || PUBLICATION == Publication::SEEDING) return;
    uint64_t const FLAGS = reserve.lock.lock_irqsave();
    out.sequence = reserve.sequence;
    out.members = reserve.member_count;
    out.node_count = reserve.node_count;
    out.published = PUBLICATION == Publication::READY;
    out.healthy = reserve.healthy;
    for (NodeId node = 0; node < reserve.node_count; ++node) out.nodes[node] = reserve.nodes[node].counts;
    reserve.lock.unlock_irqrestore(FLAGS);
}

}  // namespace ker::mod::mm::numa::root_reserve
