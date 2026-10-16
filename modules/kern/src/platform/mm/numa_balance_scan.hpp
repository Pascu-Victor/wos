#pragma once

#include <cstring>
#include <span>

#include "numa_balance.hpp"

namespace ker::mod::mm::numa::balance::detail {

inline auto refuse(Snapshot& out, Error error, uint32_t zone = UINT32_MAX, uint64_t physical = 0, uint8_t flags = 0, uint8_t kind = 0,
                   uint8_t owner = 0) noexcept -> Result {
    out.complete = false;
    out.result = {error, zone, physical, flags, kind, owner};
    return out.result;
}

inline auto add(uint64_t& destination, uint64_t value) noexcept -> bool {
    if (value > UINT64_MAX - destination) return false;
    destination += value;
    return true;
}

inline auto view_start(const ZoneView& view) noexcept -> uint64_t {
    return view.physical_base - (view.regular_descriptor ? paging::PAGE_SIZE : 0);
}

inline auto view_end(const ZoneView& view) noexcept -> uint64_t {
    return view.physical_base + static_cast<uint64_t>(view.allocator->total_pages) * paging::PAGE_SIZE;
}

inline auto metadata_contains(const PageAllocator& allocator, const void* pointer, uint64_t bytes, size_t alignment) noexcept -> bool {
    uint64_t const ADDRESS = reinterpret_cast<uint64_t>(pointer);
    uint64_t const METADATA_BYTES = static_cast<uint64_t>(allocator.metadata_pages) * paging::PAGE_SIZE;
    return ADDRESS >= allocator.base && ADDRESS % alignment == 0 && ADDRESS - allocator.base <= METADATA_BYTES &&
           bytes <= METADATA_BYTES - (ADDRESS - allocator.base);
}

// No locks are acquired, and no mutable allocator counters are read here.
// Validate all extents and the frame budget before even the preliminary
// ordinary physical snapshot is allowed to acquire allocator locks.
inline auto prepare_scan(const acpi::numa::Topology& topology, std::span<const ZoneView> zones, Snapshot& out, Workspace& scratch,
                         uint64_t max_frames) noexcept -> Result {
    std::memset(&out, 0, sizeof(out));
    // The collector supplies scratch.zones itself. Preserve that input span;
    // reset only scratch fields consumed before they are overwritten below.
    std::memset(scratch.topology_pages.data(), 0, sizeof(scratch.topology_pages));
    scratch.frame_count = 0;
    scratch.zone_count = 0;
    scratch.memory_cursor = 0;
    scratch.prepared = false;
    scratch.scanned = false;
    out.result.error = Error::NOT_COLLECTED;
    out.result.zone_index = UINT32_MAX;
    if (zones.empty()) return refuse(out, Error::INVALID_INPUT);
    if (zones.size() > MAX_ZONES) return refuse(out, Error::CAPACITY);
    if (topology.node_count == 0 || topology.node_count > acpi::numa::MAX_NODES || topology.memory_range_count == 0 ||
        topology.memory_range_count > acpi::numa::MAX_MEMORY_RANGES)
        return refuse(out, Error::TOPOLOGY);
    out.provenance = topology.provenance;
    out.node_count = topology.node_count;
    out.zone_count = static_cast<uint16_t>(zones.size());
    uint64_t previous_end = 0;
    uint64_t topology_frames = 0;
    for (size_t i = 0; i < topology.memory_range_count; ++i) {
        auto const& range = topology.memory[i];
        if (range.node >= topology.node_count || range.length == 0 || range.base % paging::PAGE_SIZE != 0 ||
            range.length % paging::PAGE_SIZE != 0 || range.length > UINT64_MAX - range.base || range.base < previous_end)
            return refuse(out, Error::TOPOLOGY);
        previous_end = range.base + range.length;
        uint64_t const PAGES = range.length / paging::PAGE_SIZE;
        if (!add(topology_frames, PAGES) || !add(scratch.topology_pages[range.node], PAGES)) return refuse(out, Error::OVERFLOW);
    }
    for (size_t node = 0; node < topology.node_count; ++node) {
        if (topology.nodes[node].usable_bytes % paging::PAGE_SIZE != 0 ||
            topology.nodes[node].usable_bytes / paging::PAGE_SIZE != scratch.topology_pages[node])
            return refuse(out, Error::TOPOLOGY);
    }
    size_t regular_count = 0;
    for (size_t i = 0; i < zones.size(); ++i) {
        auto const& view = zones[i];
        auto const* allocator = view.allocator;
        if (allocator == nullptr || allocator->base == 0 || allocator->base % paging::PAGE_SIZE != 0 ||
            view.physical_base % paging::PAGE_SIZE != 0 || allocator->total_pages == 0 ||
            allocator->metadata_pages > allocator->total_pages || allocator->page_flags == nullptr || allocator->page_kinds == nullptr ||
            allocator->page_owners == nullptr || allocator->page_refcounts == nullptr ||
            (view.regular_descriptor && view.physical_base < paging::PAGE_SIZE))
            return refuse(out, Error::ZONE, static_cast<uint32_t>(i));
        uint64_t const BYTES = static_cast<uint64_t>(allocator->total_pages) * paging::PAGE_SIZE;
        if (BYTES > UINT64_MAX - view.physical_base || BYTES > UINT64_MAX - allocator->base)
            return refuse(out, Error::ZONE, static_cast<uint32_t>(i));
        if (!metadata_contains(*allocator, allocator->page_flags, allocator->total_pages, alignof(uint8_t)) ||
            !metadata_contains(*allocator, allocator->page_kinds,
                               static_cast<uint64_t>(allocator->total_pages) * sizeof(std::atomic<uint8_t>),
                               alignof(std::atomic<uint8_t>)) ||
            !metadata_contains(*allocator, allocator->page_owners, allocator->total_pages, alignof(uint8_t)) ||
            !metadata_contains(*allocator, allocator->page_refcounts,
                               static_cast<uint64_t>(allocator->total_pages) * sizeof(std::atomic<uint32_t>),
                               alignof(std::atomic<uint32_t>)))
            return refuse(out, Error::ZONE, static_cast<uint32_t>(i));
#ifdef WOS_PHYS_ALLOC_CALLER_STATS
        if (!metadata_contains(*allocator, allocator->page_callers, static_cast<uint64_t>(allocator->total_pages) * sizeof(uint64_t),
                               alignof(uint64_t)))
            return refuse(out, Error::ZONE, static_cast<uint32_t>(i));
#endif
        for (size_t j = 0; j < i; ++j)
            if (zones[j].allocator == allocator) return refuse(out, Error::OVERLAP, static_cast<uint32_t>(i));
        scratch.zones[i] = view;
        scratch.sorted[i] = static_cast<uint16_t>(i);
        if (view.regular_descriptor && ++regular_count >= MAX_ZONES) return refuse(out, Error::CAPACITY);
        if (!add(scratch.frame_count, allocator->total_pages) || !add(scratch.frame_count, view.regular_descriptor ? 1 : 0))
            return refuse(out, Error::OVERFLOW);
    }
    scratch.zone_count = zones.size();
    if (max_frames == 0 || scratch.frame_count > max_frames) return refuse(out, Error::FRAME_LIMIT);
    // Fixed small insertion sort; no allocation and no large temporary.
    for (size_t i = 1; i < zones.size(); ++i) {
        uint16_t const INDEX = scratch.sorted[i];
        size_t position = i;
        while (position != 0 && view_start(scratch.zones[scratch.sorted[position - 1]]) > view_start(scratch.zones[INDEX])) {
            scratch.sorted[position] = scratch.sorted[position - 1];
            --position;
        }
        scratch.sorted[position] = INDEX;
    }
    size_t memory = 0;
    previous_end = 0;
    for (size_t i = 0; i < zones.size(); ++i) {
        uint16_t const INDEX = scratch.sorted[i];
        auto const& view = scratch.zones[INDEX];
        uint64_t cursor = view_start(view);
        uint64_t const END = view_end(view);
        if (i != 0 && cursor < previous_end) return refuse(out, Error::OVERLAP, INDEX, cursor);
        previous_end = END;
        while (cursor < END) {
            while (memory < topology.memory_range_count && topology.memory[memory].base + topology.memory[memory].length <= cursor)
                ++memory;
            if (memory == topology.memory_range_count || cursor < topology.memory[memory].base)
                return refuse(out, Error::COVERAGE, INDEX, cursor);
            uint64_t const RANGE_END = topology.memory[memory].base + topology.memory[memory].length;
            cursor = RANGE_END < END ? RANGE_END : END;
        }
    }
    if (scratch.frame_count != topology_frames) return refuse(out, Error::COVERAGE);
    scratch.prepared = true;
    return {Error::NONE};
}

inline auto node_for(const acpi::numa::Topology& topology, Workspace& scratch, uint64_t physical) noexcept -> uint16_t {
    while (scratch.memory_cursor < topology.memory_range_count &&
           topology.memory[scratch.memory_cursor].base + topology.memory[scratch.memory_cursor].length <= physical)
        ++scratch.memory_cursor;
    if (scratch.memory_cursor == topology.memory_range_count || physical < topology.memory[scratch.memory_cursor].base)
        return acpi::numa::INVALID_NODE;
    return topology.memory[scratch.memory_cursor].node;
}

enum class Category : uint8_t { DESCRIPTOR, METADATA, FREE, CACHED, OWNED };

// Bulk arithmetic follows the same category fields as count(). Every frame's
// raw metadata/ref sample is still validated before its run is accounted.
inline auto count_run(Counts& counts, Category category, uint8_t kind, uint8_t owner, uint64_t pages, uint64_t heads, uint64_t zero_refs,
                      uint64_t single_refs, uint64_t shared_refs) noexcept -> bool {
    if (!add(counts.managed_pages, pages)) return false;
    if (category == Category::DESCRIPTOR) return add(counts.zone_descriptor_pages, pages);
    if (!add(counts.kind_pages[kind], pages)) return false;
    if (category == Category::METADATA) return add(counts.allocator_metadata_pages, pages);
    if (category == Category::FREE || category == Category::CACHED)
        return add(counts.free_pages, pages) && (category != Category::CACHED || add(counts.cached_free_pages, pages));
    return add(counts.owner_pages, pages) && add(counts.owners[owner].pages, pages) && add(counts.owners[owner].objects, heads) &&
           add(counts.sampled_zero_ref_owned_pages, zero_refs) && add(counts.sampled_single_ref_owned_pages, single_refs) &&
           add(counts.sampled_shared_ref_owned_pages, shared_refs);
}

inline auto count(Counts& counts, Category category, uint8_t kind, uint8_t owner, uint32_t refs, bool head) noexcept -> bool {
    return count_run(counts, category, kind, owner, 1, head ? 1 : 0, refs == 0 ? 1 : 0, refs == 1 ? 1 : 0, refs > 1 ? 1 : 0);
}

inline auto account_run(Snapshot& out, Workspace& scratch, uint32_t zone, uint16_t node, uint64_t physical, Category category,
                        uint8_t flags, uint8_t kind, uint8_t owner, uint32_t pages, bool head, uint64_t zero_refs, uint64_t single_refs,
                        uint64_t shared_refs) noexcept -> bool {
    if (!count_run(out.scanned, category, kind, owner, pages, head ? 1 : 0, zero_refs, single_refs, shared_refs) ||
        !count_run(out.nodes[node], category, kind, owner, pages, head ? 1 : 0, zero_refs, single_refs, shared_refs) ||
        !count_run(scratch.zone_scanned, category, kind, owner, pages, head ? 1 : 0, zero_refs, single_refs, shared_refs) ||
        !add(out.scanned_frames, pages)) {
        refuse(out, Error::OVERFLOW, zone, physical, flags, kind, owner);
        return false;
    }
    return true;
}

inline auto page(const acpi::numa::Topology& topology, Snapshot& out, Workspace& scratch, uint32_t zone, uint64_t physical,
                 Category category, uint8_t flags = 0, uint8_t kind = 0, uint8_t owner = 0, uint32_t refs = 0, bool head = false) noexcept
    -> bool {
    uint16_t const NODE = node_for(topology, scratch, physical);
    if (NODE == acpi::numa::INVALID_NODE) {
        refuse(out, Error::COVERAGE, zone, physical, flags, kind, owner);
        return false;
    }
    if (!count(out.scanned, category, kind, owner, refs, head) || !count(out.nodes[NODE], category, kind, owner, refs, head) ||
        !count(scratch.zone_scanned, category, kind, owner, refs, head) || !add(out.scanned_frames, 1)) {
        refuse(out, Error::OVERFLOW, zone, physical, flags, kind, owner);
        return false;
    }
    return true;
}

inline auto identity(const Counts& counts) noexcept -> bool {
    uint64_t pages = counts.free_pages;
    return add(pages, counts.allocator_metadata_pages) && add(pages, counts.zone_descriptor_pages) && add(pages, counts.owner_pages) &&
           pages == counts.managed_pages && counts.cached_free_pages <= counts.free_pages;
}

inline auto scan_zone(const acpi::numa::Topology& topology, Snapshot& out, Workspace& scratch, uint16_t zone) noexcept -> bool {
    auto const& view = scratch.zones[zone];
    auto const& allocator = *view.allocator;
    std::memset(&scratch.zone_scanned, 0, sizeof(scratch.zone_scanned));
    if (view.regular_descriptor && !page(topology, out, scratch, zone, view_start(view), Category::DESCRIPTOR)) return false;
    uint32_t index = 0;
    while (index < allocator.total_pages) {
        uint8_t const FLAGS = allocator.page_flags[index];
        uint8_t const KIND = allocator.page_kinds[index].load(std::memory_order_acquire);
        uint8_t const OWNER = allocator.page_owners[index];
        uint64_t const PHYSICAL = view.physical_base + static_cast<uint64_t>(index) * paging::PAGE_SIZE;
        auto fail = [&]() {
            refuse(out, Error::METADATA, zone, PHYSICAL, FLAGS, KIND, OWNER);
            return false;
        };
        if (KIND >= KIND_COUNT) return fail();
        if (index < allocator.metadata_pages) {
            if (FLAGS != PageAllocator::FLAG_RESERVED || KIND != static_cast<uint8_t>(PageKind::RESERVED) || OWNER != 0 ||
                allocator.page_refcounts[index].load(std::memory_order_acquire) != 0)
                return fail();
            if (!page(topology, out, scratch, zone, PHYSICAL, Category::METADATA, FLAGS, KIND, OWNER)) return false;
            ++index;
            continue;
        }
        if (FLAGS == PageAllocator::FLAG_CACHED_ORDER0) {
            if (KIND != static_cast<uint8_t>(PageKind::FREE) || OWNER != 0 ||
                allocator.page_refcounts[index].load(std::memory_order_acquire) != 0)
                return fail();
            if (!page(topology, out, scratch, zone, PHYSICAL, Category::CACHED, FLAGS, KIND, OWNER)) return false;
            ++index;
            continue;
        }
        uint8_t const TAG = FLAGS & 0xC0U;
        uint8_t const ORDER = FLAGS & 0x1FU;
        if ((TAG != PageAllocator::FLAG_FREE_HEAD && TAG != PageAllocator::FLAG_ALLOC_HEAD) || ORDER > PageAllocator::MAX_ORDER ||
            FLAGS != static_cast<uint8_t>(TAG | ORDER))
            return fail();
        uint32_t const PAGES = uint32_t{1} << ORDER;
        if (index % PAGES != 0 || PAGES > allocator.total_pages - index) return fail();
        bool const OWNED = TAG == PageAllocator::FLAG_ALLOC_HEAD;
        if (OWNED && (!physical_page_owner_is_valid(static_cast<PhysicalPageOwner>(OWNER)) ||
                      KIND == static_cast<uint8_t>(PageKind::FREE) || KIND == static_cast<uint8_t>(PageKind::RESERVED)))
            return fail();
        uint32_t offset = 0;
        while (offset < PAGES) {
            uint64_t const RUN_ADDRESS = view.physical_base + static_cast<uint64_t>(index + offset) * paging::PAGE_SIZE;
            uint16_t const NODE = node_for(topology, scratch, RUN_ADDRESS);
            if (NODE == acpi::numa::INVALID_NODE) {
                refuse(out, Error::COVERAGE, zone, RUN_ADDRESS, FLAGS, KIND, OWNER);
                return false;
            }
            auto const& range = topology.memory[scratch.memory_cursor];
            uint64_t const RANGE_PAGES = (range.base + range.length - RUN_ADDRESS) / paging::PAGE_SIZE;
            uint32_t const RUN_PAGES = RANGE_PAGES < PAGES - offset ? static_cast<uint32_t>(RANGE_PAGES) : PAGES - offset;
            uint64_t zero_refs = 0, single_refs = 0, shared_refs = 0;
            for (uint32_t in_run = 0; in_run < RUN_PAGES; ++in_run) {
                uint32_t const CURRENT = index + offset + in_run;
                uint8_t const EXPECTED =
                    offset + in_run == 0 ? FLAGS : (OWNED ? PageAllocator::FLAG_ALLOC_CONT : PageAllocator::FLAG_FREE_INTERIOR);
                uint8_t const CURRENT_KIND = allocator.page_kinds[CURRENT].load(std::memory_order_acquire);
                uint8_t const CURRENT_OWNER = allocator.page_owners[CURRENT];
                uint32_t const REFS = allocator.page_refcounts[CURRENT].load(std::memory_order_acquire);
                uint64_t const ADDRESS = view.physical_base + static_cast<uint64_t>(CURRENT) * paging::PAGE_SIZE;
                if (allocator.page_flags[CURRENT] != EXPECTED || CURRENT_KIND != (OWNED ? KIND : static_cast<uint8_t>(PageKind::FREE)) ||
                    CURRENT_OWNER != (OWNED ? OWNER : 0) || (!OWNED && REFS != 0)) {
                    // Preserve the old valid-prefix output on metadata refusal.
                    if (in_run != 0 && !account_run(out, scratch, zone, NODE, RUN_ADDRESS, OWNED ? Category::OWNED : Category::FREE, FLAGS,
                                                    KIND, OWNER, in_run, OWNED && offset == 0, zero_refs, single_refs, shared_refs))
                        return false;
                    refuse(out, Error::METADATA, zone, ADDRESS, allocator.page_flags[CURRENT], CURRENT_KIND, CURRENT_OWNER);
                    return false;
                }
                if (OWNED) {
                    // Bounded by RUN_PAGES <= 1U << MAX_ORDER. These local
                    // samples are bulk-added with overflow checks below.
                    zero_refs += REFS == 0 ? 1 : 0;
                    single_refs += REFS == 1 ? 1 : 0;
                    shared_refs += REFS > 1 ? 1 : 0;
                }
            }
            if (!account_run(out, scratch, zone, NODE, RUN_ADDRESS, OWNED ? Category::OWNED : Category::FREE, FLAGS,
                             OWNED ? KIND : static_cast<uint8_t>(PageKind::FREE), OWNED ? OWNER : 0, RUN_PAGES, OWNED && offset == 0,
                             zero_refs, single_refs, shared_refs))
                return false;
            offset += RUN_PAGES;
        }
        index += PAGES;
    }
    auto const& scanned = scratch.zone_scanned;
    if (!identity(scanned) || scanned.free_pages != allocator.free_count || scanned.cached_free_pages != allocator.cached_order0_count ||
        scanned.allocator_metadata_pages != allocator.metadata_pages || allocator.owner_pages[0] != 0 || allocator.owner_objects[0] != 0 ||
        allocator.usable_pages != allocator.total_pages - allocator.metadata_pages) {
        refuse(out, Error::COUNTERS, zone);
        return false;
    }
    for (size_t owner = 1; owner < PHYSICAL_PAGE_OWNER_COUNT; ++owner) {
        if (scanned.owners[owner].pages != allocator.owner_pages[owner] ||
            scanned.owners[owner].objects != allocator.owner_objects[owner]) {
            refuse(out, Error::COUNTERS, zone);
            return false;
        }
    }
    return true;
}

// Requires every supplied actual allocator lock held by this caller. The
// scanner neither acquires/releases locks nor dereferences free-list payloads.
// Success remains incomplete until the collector releases all locks.
inline auto scan_locked(const acpi::numa::Topology& topology, Snapshot& out, Workspace& scratch) noexcept -> Result {
    if (!scratch.prepared || scratch.scanned) return refuse(out, Error::INVALID_INPUT);
    scratch.scanned = true;
    for (size_t zone = 0; zone < scratch.zone_count; ++zone) {
        auto const& view = scratch.zones[zone];
        auto const& allocator = *view.allocator;
        if (!allocator.lock_held.load(std::memory_order_acquire)) return refuse(out, Error::INVALID_INPUT, static_cast<uint32_t>(zone));
        if (!add(out.global.managed_pages, allocator.total_pages) || !add(out.global.managed_pages, view.regular_descriptor ? 1 : 0) ||
            !add(out.global.zone_descriptor_pages, view.regular_descriptor ? 1 : 0) || !add(out.global.free_pages, allocator.free_count) ||
            !add(out.global.allocator_metadata_pages, allocator.metadata_pages))
            return refuse(out, Error::OVERFLOW, static_cast<uint32_t>(zone));
        for (size_t owner = 1; owner < PHYSICAL_PAGE_OWNER_COUNT; ++owner) {
            if (!add(out.global.owners[owner].pages, allocator.owner_pages[owner]) ||
                !add(out.global.owners[owner].objects, allocator.owner_objects[owner]) ||
                !add(out.global.owner_pages, allocator.owner_pages[owner]))
                return refuse(out, Error::OVERFLOW, static_cast<uint32_t>(zone));
        }
    }
    for (size_t sorted = 0; sorted < scratch.zone_count; ++sorted)
        if (!scan_zone(topology, out, scratch, scratch.sorted[sorted])) return out.result;
    out.global.zone_count = scratch.zone_count;
    out.global.identity_pages = out.global.free_pages;
    if (!add(out.global.identity_pages, out.global.allocator_metadata_pages) ||
        !add(out.global.identity_pages, out.global.zone_descriptor_pages) || !add(out.global.identity_pages, out.global.owner_pages))
        return refuse(out, Error::OVERFLOW);
    out.global.identity_mismatch_pages = out.global.identity_pages > out.global.managed_pages
                                             ? out.global.identity_pages - out.global.managed_pages
                                             : out.global.managed_pages - out.global.identity_pages;
    out.global.untracked_unreclaimable_pages = out.global.identity_mismatch_pages;
    if (!identity(out.scanned) || out.scanned_frames != scratch.frame_count || out.scanned.managed_pages != out.global.managed_pages ||
        out.scanned.free_pages != out.global.free_pages || out.scanned.allocator_metadata_pages != out.global.allocator_metadata_pages ||
        out.scanned.zone_descriptor_pages != out.global.zone_descriptor_pages || out.scanned.owner_pages != out.global.owner_pages ||
        out.global.identity_mismatch_pages != 0)
        return refuse(out, Error::RECONCILIATION);
    for (size_t owner = 1; owner < PHYSICAL_PAGE_OWNER_COUNT; ++owner)
        if (out.scanned.owners[owner].pages != out.global.owners[owner].pages ||
            out.scanned.owners[owner].objects != out.global.owners[owner].objects)
            return refuse(out, Error::RECONCILIATION);
    // Reuse small scratch for an independent sum of the completed node rows.
    std::memset(&scratch.zone_scanned, 0, sizeof(scratch.zone_scanned));
    for (size_t node = 0; node < topology.node_count; ++node) {
        auto const& row = out.nodes[node];
        if (!identity(row) || row.managed_pages != scratch.topology_pages[node]) return refuse(out, Error::RECONCILIATION);
        if (!add(scratch.zone_scanned.managed_pages, row.managed_pages) || !add(scratch.zone_scanned.free_pages, row.free_pages) ||
            !add(scratch.zone_scanned.cached_free_pages, row.cached_free_pages) ||
            !add(scratch.zone_scanned.allocator_metadata_pages, row.allocator_metadata_pages) ||
            !add(scratch.zone_scanned.zone_descriptor_pages, row.zone_descriptor_pages) ||
            !add(scratch.zone_scanned.owner_pages, row.owner_pages))
            return refuse(out, Error::OVERFLOW);
        for (size_t owner = 1; owner < PHYSICAL_PAGE_OWNER_COUNT; ++owner)
            if (!add(scratch.zone_scanned.owners[owner].pages, row.owners[owner].pages) ||
                !add(scratch.zone_scanned.owners[owner].objects, row.owners[owner].objects))
                return refuse(out, Error::OVERFLOW);
    }
    auto const& sum = scratch.zone_scanned;
    if (sum.managed_pages != out.scanned.managed_pages || sum.free_pages != out.scanned.free_pages ||
        sum.cached_free_pages != out.scanned.cached_free_pages || sum.allocator_metadata_pages != out.scanned.allocator_metadata_pages ||
        sum.zone_descriptor_pages != out.scanned.zone_descriptor_pages || sum.owner_pages != out.scanned.owner_pages)
        return refuse(out, Error::RECONCILIATION);
    for (size_t owner = 1; owner < PHYSICAL_PAGE_OWNER_COUNT; ++owner)
        if (sum.owners[owner].pages != out.scanned.owners[owner].pages || sum.owners[owner].objects != out.scanned.owners[owner].objects)
            return refuse(out, Error::RECONCILIATION);
    out.result = {Error::NONE};
    return out.result;
}

// Caller must already have released every lock it acquired for scan_locked.
inline auto finish_unlocked(Snapshot& out, Workspace& scratch) noexcept -> Result {
    out.complete = scratch.prepared && scratch.scanned && out.result.valid();
    scratch.prepared = false;
    return out.result;
}

}  // namespace ker::mod::mm::numa::balance::detail
