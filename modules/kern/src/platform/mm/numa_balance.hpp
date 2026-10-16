#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <platform/acpi/numa_topology.hpp>
#include <type_traits>

#include "phys.hpp"
#ifdef WOS_SELFTEST
#include <test/numa_lifecycle_mm_debt.hpp>
#endif

namespace ker::mod::mm::numa::balance {

inline constexpr size_t MAX_ZONES = 129;
inline constexpr size_t KIND_COUNT = static_cast<size_t>(PageKind::KMALLOC_LARGE) + 1;

enum class Error : uint8_t {
    NONE,
    NOT_COLLECTED,
    NO_TOPOLOGY,
    INVALID_INPUT,
    CAPACITY,
    FRAME_LIMIT,
    TOPOLOGY,
    ZONE,
    OVERLAP,
    COVERAGE,
    METADATA,
    COUNTERS,
    OVERFLOW,
    RECONCILIATION,
};

struct Result {
    Error error{Error::NOT_COLLECTED};
    uint32_t zone_index{UINT32_MAX};
    uint64_t physical{};
    uint8_t flags{};
    uint8_t kind{};
    uint8_t owner{};
    [[nodiscard]] auto valid() const noexcept -> bool { return error == Error::NONE; }
};

struct Counts {
    uint64_t managed_pages{};
    uint64_t free_pages{};
    uint64_t cached_free_pages{};  // Subset of free_pages; INVALID owner.
    uint64_t allocator_metadata_pages{};
    uint64_t zone_descriptor_pages{};
    uint64_t owner_pages{};
    std::array<phys::PhysicalOwnerSnapshot, PHYSICAL_PAGE_OWNER_COUNT> owners{};
    // Metadata tags, not payload validation or a kernel-object census.
    std::array<uint64_t, KIND_COUNT> kind_pages{};
    // Refcounts change outside allocator locks: these are sampled telemetry.
    uint64_t sampled_zero_ref_owned_pages{};
    uint64_t sampled_single_ref_owned_pages{};
    uint64_t sampled_shared_ref_owned_pages{};
};

struct Snapshot {
    bool complete{};
    Result result{};
    acpi::numa::Provenance provenance{};
    uint16_t node_count{};
    uint16_t zone_count{};
    uint64_t scanned_frames{};
    phys::PhysicalBalanceSnapshot global{};
    Counts scanned{};
    std::array<Counts, acpi::numa::MAX_NODES> nodes{};
};

// Explicit physical base permits the same scanner to inspect real allocators
// mapped at different Host/HHDM virtual addresses. No payload is dereferenced.
struct ZoneView {
    PageAllocator* allocator{};
    uint64_t physical_base{};
    uint64_t zone_id{};
    bool regular_descriptor{};
};

struct Workspace {
    std::array<ZoneView, MAX_ZONES> zones{};  // Actual lock acquisition order.
    std::array<uint16_t, MAX_ZONES> sorted{};
    std::array<uint64_t, MAX_ZONES> lock_flags{};
    std::array<uint64_t, acpi::numa::MAX_NODES> topology_pages{};
    phys::PhysicalBalanceSnapshot preliminary{};
    Counts zone_scanned{};
    uint64_t frame_count{};
    size_t zone_count{};
    size_t memory_cursor{};
    bool prepared{};
    bool scanned{};
};

static_assert(std::is_trivially_copyable_v<Snapshot> && std::is_trivially_copyable_v<Workspace>);
static_assert(sizeof(Snapshot) + sizeof(Workspace) <= 96 * 1024);

// Explicit, O(frames), IRQ-disabled diagnostic. Both buffers must be exclusive,
// distinct caller/BSS storage, never large automatic kernel-stack temporaries.
// No incoming allocator/cache/VM/scheduler locks, IRQ/panic/reclaim context or
// allocation hot path. max_frames bounds work, not blocking-lock wall time.
// All mutable global/node counts share one all-allocator-lock interval. Only
// boot-immutable peripheral reserves come from an earlier ordinary snapshot.
[[nodiscard]] auto snapshot(Snapshot& out, Workspace& scratch, uint64_t max_frames) noexcept -> Result;

#ifdef WOS_SELFTEST
// The caller owns exclusive nonaliasing named output/workspace/MM/scratch.
// MM/scratch must already be addressable, prepared by an instrumented caller.
// Preflight occurs before locks; the authoritative MM point is after the last
// physical lock. Both paths run the entire unchanged per-frame scanner.
[[nodiscard]] auto selftest_snapshot_with_mm(Snapshot& out, Workspace& scratch, ::ker::test::numa_lifecycle::MmDebtSnapshot& mm,
                                             ::ker::test::numa_lifecycle::MmDebtCaptureScratch& capture, uint64_t max_frames) noexcept
    -> Result;
#endif

}  // namespace ker::mod::mm::numa::balance
