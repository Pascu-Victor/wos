#include "managed_huge.hpp"

#include <new>
#include <platform/acpi/numa_boot.hpp>
#include <platform/mm/addr.hpp>
#include <utility>

namespace ker::mod::mm::huge {
namespace {
phys::NodeCounterSnapshot bootstrap_counts{};
phys::NodeCounterWorkspace bootstrap_workspace{};
}  // namespace

auto initialize_after_topology() noexcept -> Error {
    if (!phys::node_counters_ready() || phys::snapshot_node_counters(bootstrap_counts, bootstrap_workspace) != NodeCounterError::NONE ||
        !bootstrap_counts.complete || bootstrap_counts.node_count == 0 || bootstrap_counts.node_count > MAX_NODES)
        return Error::NotReady;
    CapsInput input{};
    input.node_count = bootstrap_counts.node_count;
    uint64_t sum = 0;
    for (size_t node = 0; node < input.node_count; ++node) {
        input.usable[node] = bootstrap_counts.nodes[node].usable;
        if (UINT64_MAX - sum < input.usable[node]) return Error::Invalid;
        sum += input.usable[node];
    }
    if (sum != bootstrap_counts.global.usable) return Error::Invalid;
    return detail::initialize_caps(input);
}

void snapshot(Snapshot& output) noexcept { detail::snapshot(output); }

PreparedSpan::~PreparedSpan() { reset(); }
PreparedSpan::PreparedSpan(PreparedSpan&& other) noexcept
    : ticket_(std::exchange(other.ticket_, {})),
      span_(std::move(other.span_)),
      requested_(std::exchange(other.requested_, numa::INVALID_NODE)),
      actual_(std::exchange(other.actual_, numa::INVALID_NODE)) {}
auto PreparedSpan::operator=(PreparedSpan&& other) noexcept -> PreparedSpan& {
    if (this != &other) {
        reset();
        ticket_ = std::exchange(other.ticket_, {});
        span_ = std::move(other.span_);
        requested_ = std::exchange(other.requested_, numa::INVALID_NODE);
        actual_ = std::exchange(other.actual_, numa::INVALID_NODE);
    }
    return *this;
}
void PreparedSpan::reset() noexcept {
    // The actual owner returns every frame before the scarce capacity credit.
    span_.reset();
    auto const TICKET = std::exchange(ticket_, {});
    if (TICKET.generation != 0 && detail::cancel_leaf(TICKET) != Error::None) __builtin_trap();
    requested_ = actual_ = numa::INVALID_NODE;
}
auto PreparedSpan::prepare(RootKey root, uint64_t va, const numa::AllocationContext& context) noexcept -> Error {
    if (!empty() || !span_.empty() || !RegistryCore::valid_root(root) || !RegistryCore::valid_va(va) || context.virtual_page_index != 0)
        return Error::Invalid;
    auto const* topology = acpi::numa::boot_topology();
    if (topology == nullptr) return Error::NotReady;
    auto const selection = numa::select_nodes(context.policy, *topology, context.local_node, va / LEAF_BYTES);
    if (!selection.valid()) return Error::Invalid;
    Error last = Error::Exhausted;
    for (size_t index = 0; index < selection.count; ++index) {
        auto const NODE = selection.nodes[index];
        LeafHandle ticket{};
        Error const RESERVED = detail::reserve_leaf(root, va, NODE, ticket);
        if (RESERVED != Error::None) {
            if (RESERVED == Error::Capacity) {
                last = RESERVED;
                continue;
            }
            return RESERVED;
        }
        auto single = context;
        single.policy = {uint64_t{1} << NODE, numa::Mode::BIND};
        single.virtual_page_index = va / LEAF_BYTES;
        auto allocation = numa::try_allocate_pages(single, LEAF_PAGES, LEAF_BYTES, PhysicalPageOwner::USER_PRIVATE_MAPPING,
                                                   phys::PageInitialization::ZERO, numa::ReclaimMode::NEVER, "huge-prepared");
        Error result = allocation.span.empty() ? Error::Exhausted : Error::None;
        if (result == Error::None) {
            uint64_t const PHYSICAL =
                reinterpret_cast<uint64_t>(addr::get_phys_pointer(reinterpret_cast<uint64_t>(allocation.span.data())));
            if (allocation.span.pages() != LEAF_PAGES || allocation.actual != NODE || (PHYSICAL & (LEAF_BYTES - 1)) != 0)
                result = Error::Invalid;
            for (uint64_t page = 0; result == Error::None && page < LEAF_PAGES; ++page) {
                auto* frame = static_cast<uint8_t*>(allocation.span.data()) + page * PAGE_BYTES;
                if (phys::page_kind_get(frame) != PageKind::NORMAL || phys::page_ref_get(frame) != 1 ||
                    numa::physical_node(PHYSICAL + page * PAGE_BYTES) != NODE)
                    result = Error::Invalid;
            }
            if (result == Error::None && !detail::root_matches(root)) result = Error::Stale;
        }
        if (result == Error::None) {
            ticket_ = ticket;
            span_ = std::move(allocation.span);
            requested_ = selection.requested;
            actual_ = NODE;
            return Error::None;
        }
        allocation.span.reset();
        if (detail::cancel_leaf(ticket) != Error::None) __builtin_trap();
        if (result != Error::Exhausted) return result;
        last = result;
    }
    return last;
}

PreparedTables::~PreparedTables() { reset(); }
PreparedTables::PreparedTables(PreparedTables&& other) noexcept
    : ticket_(std::exchange(other.ticket_, {})), storage_(std::exchange(other.storage_, nullptr)) {}
auto PreparedTables::operator=(PreparedTables&& other) noexcept -> PreparedTables& {
    if (this != &other) {
        reset();
        ticket_ = std::exchange(other.ticket_, {});
        storage_ = std::exchange(other.storage_, nullptr);
    }
    return *this;
}
void PreparedTables::reset() noexcept {
    if (storage_ != nullptr) {
        for (uint16_t i = 0; i < storage_->table_count; ++i) detail::release_table(storage_->tables[i]);
        delete std::exchange(storage_, nullptr);
    }
    auto const TICKET = std::exchange(ticket_, {});
    if (TICKET.generation != 0 && detail::release_plan(TICKET) != Error::None) __builtin_trap();
}
auto PreparedTables::prepare(uint16_t count) noexcept -> Error {
    return prepare_impl(count
#ifdef WOS_SELFTEST
                        ,
                        UINT16_MAX
#endif
    );
}
auto PreparedTables::prepare_impl(uint16_t count
#ifdef WOS_SELFTEST
                                  ,
                                  uint16_t fail_before_table
#endif
                                  ) noexcept -> Error {
    if (!empty() || storage_ != nullptr || count == 0 || count > MAX_PLAN_TABLES) return Error::Invalid;
    PlanHandle ticket{};
    if (auto const error = detail::reserve_plan(count, ticket); error != Error::None) return error;
    ticket_ = ticket;
    storage_ = new (std::nothrow) SplitPlanStorage{};
    if (storage_ == nullptr) {
        reset();
        return Error::Exhausted;
    }
    for (uint16_t i = 0; i < count; ++i) {
#ifdef WOS_SELFTEST
        if (i == fail_before_table) {
            reset();
            return Error::Exhausted;
        }
#endif
        auto* table = detail::allocate_table();
        if (table == nullptr) {
            reset();
            return Error::Exhausted;
        }
        storage_->table_physical[storage_->table_count] =
            reinterpret_cast<uint64_t>(addr::get_phys_pointer(reinterpret_cast<uint64_t>(table)));
        storage_->tables[storage_->table_count++] = table;
    }
    return Error::None;
}
#ifdef WOS_SELFTEST
auto detail::PreparationAccess::prepare_tables_for_test(PreparedTables& owner, uint16_t count, uint16_t before) noexcept -> Error {
    return owner.prepare_impl(count, before);
}
#endif
auto detail::PreparationAccess::span_view(const PreparedSpan& owner, SpanPublicationView& empty) noexcept -> bool {
    if (empty.handle.generation != 0 || owner.ticket_.generation == 0 || owner.span_.empty() || owner.span_.pages() != LEAF_PAGES)
        return false;
    empty = {owner.ticket_, reinterpret_cast<uint64_t>(addr::get_phys_pointer(reinterpret_cast<uint64_t>(owner.span_.data()))),
             owner.requested_, owner.actual_};
    return true;
}
void detail::PreparationAccess::span_transferred_after_publish(PreparedSpan& owner) noexcept {
    if (owner.ticket_.generation == 0 || owner.span_.empty()) __builtin_trap();
    (void)owner.span_.release();
    owner.ticket_ = {};
    owner.requested_ = owner.actual_ = numa::INVALID_NODE;
}
auto detail::PreparationAccess::plan_storage(PreparedTables& owner) noexcept -> SplitPlanStorage* { return owner.storage_; }
void detail::PreparationAccess::tables_transferred_after_publish(PreparedTables& owner, uint64_t adopted_mask) noexcept {
    if (owner.storage_ == nullptr || owner.ticket_.generation == 0 ||
        (owner.storage_->table_count < 64 && (adopted_mask >> owner.storage_->table_count) != 0))
        __builtin_trap();
    for (uint16_t i = 0; i < owner.storage_->table_count; ++i)
        if ((adopted_mask & (uint64_t{1} << i)) != 0) {
            if (owner.storage_->tables[i] == nullptr) __builtin_trap();
            owner.storage_->tables[i] = nullptr;
        }
}
}  // namespace ker::mod::mm::huge
