#include "numa_alloc.hpp"

#include <platform/acpi/numa_boot.hpp>
#include <platform/asm/cpu.hpp>
#include <platform/mm/addr.hpp>
#include <platform/sched/scheduler.hpp>
#include <utility>

namespace ker::mod::mm::numa {
namespace {
auto node_for_address(const acpi::numa::Topology& topology, uint64_t physical_address) noexcept -> NodeId {
    for (size_t i = 0; i < topology.memory_range_count; ++i) {
        auto const& range = topology.memory[i];
        if (physical_address >= range.base && physical_address - range.base < range.length) return range.node;
    }
    return INVALID_NODE;
}
}  // namespace

OwnedSpan::~OwnedSpan() { reset(); }

OwnedSpan::OwnedSpan(OwnedSpan&& other) noexcept : data_(std::exchange(other.data_, nullptr)), pages_(std::exchange(other.pages_, 0)) {}

auto OwnedSpan::operator=(OwnedSpan&& other) noexcept -> OwnedSpan& {
    if (this != &other) {
        reset();
        data_ = std::exchange(other.data_, nullptr);
        pages_ = std::exchange(other.pages_, 0);
    }
    return *this;
}

auto OwnedSpan::release() noexcept -> void* {
    pages_ = 0;
    return std::exchange(data_, nullptr);
}

void OwnedSpan::reset() noexcept {
    auto* const DATA = static_cast<uint8_t*>(std::exchange(data_, nullptr));
    uint32_t const PAGES = std::exchange(pages_, 0);
    for (uint32_t i = 0; i < PAGES; ++i) phys::page_ref_dec(DATA + static_cast<uint64_t>(i) * paging::PAGE_SIZE);
}

auto capture_allocation_context(Policy copied_policy, AllocationContext& output) noexcept -> AllocationError {
    auto const* topology = acpi::numa::boot_topology();
    if (topology == nullptr || topology->node_count == 0 || topology->node_count > MAX_NODES || topology->cpu_count == 0 ||
        topology->cpu_count > topology->cpu_nodes.size())
        return AllocationError::NO_TOPOLOGY;
    if (!valid_policy(copied_policy, topology->node_count)) return AllocationError::INVALID_POLICY;
    NodeId local = INVALID_NODE;
    {
        sched::PreemptGuard const PREEMPT;
        uint64_t const CPU = cpu::current_cpu();
        if (CPU >= topology->cpu_count || CPU >= topology->cpu_nodes.size()) return AllocationError::INVALID_LOCAL_NODE;
        local = topology->cpu_nodes[CPU];
        if (local >= topology->node_count) return AllocationError::INVALID_LOCAL_NODE;
    }
    output.policy = copied_policy;
    output.local_node = local;
    output.virtual_page_index = 0;
    return AllocationError::NONE;
}

auto physical_node(uint64_t physical_address) noexcept -> NodeId {
    auto const* topology = acpi::numa::boot_topology();
    return topology != nullptr ? node_for_address(*topology, physical_address) : INVALID_NODE;
}

auto try_allocate_pages(const AllocationContext& context, uint32_t pages, uint64_t alignment_bytes, PhysicalPageOwner owner,
                        phys::PageInitialization initialization, ReclaimMode reclaim, const char* name) -> AllocationResult {
    AllocationResult result;
    // Validate every argument before the legacy UMA shortcut. In particular,
    // the old allocator requires a concrete owner rather than returning null.
    if ((pages != 1 && pages != 512) || alignment_bytes < paging::PAGE_SIZE || (alignment_bytes & (alignment_bytes - 1)) != 0 ||
        !physical_page_owner_is_valid(owner) || (reclaim != ReclaimMode::NEVER && reclaim != ReclaimMode::LEGACY_UMA) ||
        (initialization != phys::PageInitialization::ZERO && initialization != phys::PageInitialization::FULL_OVERWRITE)) {
        result.error = AllocationError::INVALID_ARGUMENT;
        return result;
    }
    auto const* topology = acpi::numa::boot_topology();
    if (topology == nullptr) return result;
    auto const SELECTION = select_nodes(context.policy, *topology, context.local_node, context.virtual_page_index);
    if (!SELECTION.valid()) {
        result.error =
            SELECTION.error == SelectionError::INVALID_LOCAL_NODE ? AllocationError::INVALID_LOCAL_NODE : AllocationError::INVALID_POLICY;
        return result;
    }
    result.requested = SELECTION.requested;
    result.error = AllocationError::EXHAUSTED;

    if (topology->node_count == 1 && context.policy.mode == Mode::LOCAL && pages == 1 && alignment_bytes == paging::PAGE_SIZE) {
        const char* const SITE = name != nullptr ? name : "numa-uma";
        void* const PAGE =
            reclaim == ReclaimMode::LEGACY_UMA
                ? (initialization == phys::PageInitialization::ZERO
                       ? phys::page_alloc_with_reclaim_may_fail(owner, paging::PAGE_SIZE, SITE)
                       : phys::page_alloc_full_overwrite_page_with_reclaim_may_fail(owner, SITE))
                : (initialization == phys::PageInitialization::ZERO ? phys::page_alloc_may_fail(owner, paging::PAGE_SIZE, SITE)
                                                                    : phys::page_alloc_full_overwrite_page_may_fail(owner, SITE));
        if (PAGE == nullptr) return result;
        result.span = OwnedSpan{PAGE, 1};
        auto const ADDRESS = reinterpret_cast<uint64_t>(addr::get_phys_pointer(reinterpret_cast<uint64_t>(PAGE)));
        result.actual = node_for_address(*topology, ADDRESS);
        if (result.actual == INVALID_NODE) {
            result.span.reset();
            result.error = AllocationError::INVALID_PLACEMENT;
            return result;
        }
        result.error = AllocationError::NONE;
        return result;
    }

    for (size_t selected = 0; selected < SELECTION.count; ++selected) {
        NodeId const NODE = SELECTION.nodes[selected];
        for (size_t i = 0; i < topology->memory_range_count; ++i) {
            auto const& range = topology->memory[i];
            if (range.node != NODE) continue;
            void* const SPAN = phys::page_alloc_order0_span_in_range_may_fail(owner, range.base, range.base + range.length, pages,
                                                                              alignment_bytes, initialization);
            if (SPAN == nullptr) continue;
            // The bridge proved the whole span lies inside this immutable
            // range. Each frame already owns one independently releasable ref.
            result.span = OwnedSpan{SPAN, pages};
            result.actual = NODE;
            result.error = AllocationError::NONE;
            return result;
        }
    }
    return result;
}

// Kept out of line even with LTO: an excluded TU must never become the caller
// owning the factory's instrumented hidden return storage.
[[gnu::noinline]] void try_allocate_pages_into(AllocationResult& output, const AllocationContext& context, uint32_t pages,
                                               uint64_t alignment_bytes, PhysicalPageOwner owner, phys::PageInitialization initialization,
                                               ReclaimMode reclaim, const char* name) {
    output = try_allocate_pages(context, pages, alignment_bytes, owner, initialization, reclaim, name);
}

}  // namespace ker::mod::mm::numa
