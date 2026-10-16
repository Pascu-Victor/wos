#include "numa_balance.hpp"

#include <platform/acpi/numa_boot.hpp>
#include <platform/mm/addr.hpp>

#include "numa_balance_scan.hpp"

namespace ker::mod::mm::numa::balance {

static_assert(sizeof(paging::PageZone) <= paging::PAGE_SIZE);

auto snapshot(Snapshot& out, Workspace& scratch, uint64_t max_frames) noexcept -> Result {
    // In-place clears avoid huge aggregate temporaries even in O0 diagnostics.
    std::memset(&out, 0, sizeof(out));
    std::memset(&scratch, 0, sizeof(scratch));
    out.result.error = Error::NOT_COLLECTED;
    out.result.zone_index = UINT32_MAX;
    auto const* topology = acpi::numa::boot_topology();
    if (topology == nullptr) return detail::refuse(out, Error::NO_TOPOLOGY);
    uint64_t const HHDM = addr::get_hhdm_offset();
    size_t count = 0;
    for (auto const* zone = phys::get_zones(); zone != nullptr; zone = zone->next) {
        if (count == MAX_ZONES - 1) return detail::refuse(out, Error::CAPACITY);
        if (zone->allocator == nullptr || zone->start < HHDM || zone->start - HHDM < paging::PAGE_SIZE ||
            zone->allocator->base != zone->start || zone->len != static_cast<uint64_t>(zone->allocator->total_pages) * paging::PAGE_SIZE ||
            reinterpret_cast<uint64_t>(zone) != zone->start - paging::PAGE_SIZE)
            return detail::refuse(out, Error::ZONE, static_cast<uint32_t>(count));
        scratch.zones[count++] = {zone->allocator, zone->start - HHDM, zone->zone_num, true};
    }
    if (auto const* huge = phys::get_huge_page_zone(); huge != nullptr) {
        if (huge->allocator == nullptr || huge->start < HHDM || huge->allocator->base != huge->start ||
            huge->len != static_cast<uint64_t>(huge->allocator->total_pages) * paging::PAGE_SIZE)
            return detail::refuse(out, Error::ZONE, static_cast<uint32_t>(count));
        bool duplicate = false;
        for (size_t i = 0; i < count; ++i) duplicate |= scratch.zones[i].allocator == huge->allocator;
        if (!duplicate) {
            if (count == MAX_ZONES) return detail::refuse(out, Error::CAPACITY);
            scratch.zones[count++] = {huge->allocator, huge->start - HHDM, huge->zone_num, false};
        }
    }
    // Preparation preserves an input span in scratch.zones and clears only
    // other workspace fields. No large automatic aggregate is constructed.
    auto const prepared = detail::prepare_scan(*topology, std::span<const ZoneView>{scratch.zones.data(), count}, out, scratch, max_frames);
    if (!prepared.valid()) return prepared;

    phys::get_physical_balance_snapshot(scratch.preliminary);
    out.global.per_cpu_page_cache_reserve_pages = scratch.preliminary.per_cpu_page_cache_reserve_pages;
    out.global.per_cpu_page_cache_reserve_objects = scratch.preliminary.per_cpu_page_cache_reserve_objects;
    out.global.firmware_reserves = scratch.preliminary.firmware_reserves;

    for (size_t i = 0; i < scratch.zone_count; ++i) scratch.lock_flags[i] = scratch.zones[i].allocator->lock_irq();
    detail::scan_locked(*topology, out, scratch);
    for (size_t i = scratch.zone_count; i != 0; --i) scratch.zones[i - 1].allocator->unlock_irq(scratch.lock_flags[i - 1]);
    return detail::finish_unlocked(out, scratch);
}

#ifdef WOS_SELFTEST
auto selftest_snapshot_with_mm(Snapshot& out, Workspace& scratch, ::ker::test::numa_lifecycle::MmDebtSnapshot& mm,
                               ::ker::test::numa_lifecycle::MmDebtCaptureScratch& capture, uint64_t max_frames) noexcept -> Result {
    // In-place clears avoid huge aggregate temporaries even in O0 diagnostics.
    std::memset(&out, 0, sizeof(out));
    std::memset(&scratch, 0, sizeof(scratch));
    out.result.error = Error::NOT_COLLECTED;
    out.result.zone_index = UINT32_MAX;
    auto const* topology = acpi::numa::boot_topology();
    if (topology == nullptr) return detail::refuse(out, Error::NO_TOPOLOGY);
    uint64_t const HHDM = addr::get_hhdm_offset();
    size_t count = 0;
    for (auto const* zone = phys::get_zones(); zone != nullptr; zone = zone->next) {
        if (count == MAX_ZONES - 1) return detail::refuse(out, Error::CAPACITY);
        if (zone->allocator == nullptr || zone->start < HHDM || zone->start - HHDM < paging::PAGE_SIZE ||
            zone->allocator->base != zone->start || zone->len != static_cast<uint64_t>(zone->allocator->total_pages) * paging::PAGE_SIZE ||
            reinterpret_cast<uint64_t>(zone) != zone->start - paging::PAGE_SIZE)
            return detail::refuse(out, Error::ZONE, static_cast<uint32_t>(count));
        scratch.zones[count++] = {zone->allocator, zone->start - HHDM, zone->zone_num, true};
    }
    if (auto const* huge = phys::get_huge_page_zone(); huge != nullptr) {
        if (huge->allocator == nullptr || huge->start < HHDM || huge->allocator->base != huge->start ||
            huge->len != static_cast<uint64_t>(huge->allocator->total_pages) * paging::PAGE_SIZE)
            return detail::refuse(out, Error::ZONE, static_cast<uint32_t>(count));
        bool duplicate = false;
        for (size_t i = 0; i < count; ++i) duplicate |= scratch.zones[i].allocator == huge->allocator;
        if (!duplicate) {
            if (count == MAX_ZONES) return detail::refuse(out, Error::CAPACITY);
            scratch.zones[count++] = {huge->allocator, huge->start - HHDM, huge->zone_num, false};
        }
    }
    // Preparation preserves an input span in scratch.zones and clears only
    // other workspace fields. No large automatic aggregate is constructed.
    auto const prepared = detail::prepare_scan(*topology, std::span<const ZoneView>{scratch.zones.data(), count}, out, scratch, max_frames);
    if (!prepared.valid()) return prepared;

    phys::get_physical_balance_snapshot(scratch.preliminary);
    out.global.per_cpu_page_cache_reserve_pages = scratch.preliminary.per_cpu_page_cache_reserve_pages;
    out.global.per_cpu_page_cache_reserve_objects = scratch.preliminary.per_cpu_page_cache_reserve_objects;
    out.global.firmware_reserves = scratch.preliminary.firmware_reserves;

    // Ignored observation: exercise these same selected atomic/Epoch input and
    // output providers outside locks. Never a gate, retry or baseline.
    ::ker::test::numa_lifecycle::mm_debt::snapshot_addressable(mm, capture);
    for (size_t i = 0; i < scratch.zone_count; ++i) scratch.lock_flags[i] = scratch.zones[i].allocator->lock_irq();
    // Exactly one authoritative point. An unstable sample still runs the full
    // physical scan; waiting for MM writers here could deadlock on our locks.
    ::ker::test::numa_lifecycle::mm_debt::snapshot_addressable(mm, capture);
    detail::scan_locked(*topology, out, scratch);
    for (size_t i = scratch.zone_count; i != 0; --i) scratch.zones[i - 1].allocator->unlock_irq(scratch.lock_flags[i - 1]);
    return detail::finish_unlocked(out, scratch);
}
#endif

}  // namespace ker::mod::mm::numa::balance
