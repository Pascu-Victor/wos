#include "numa_boot.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <platform/acpi/acpi.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/phys.hpp>

namespace ker::mod::acpi::numa {
namespace {
using log = ker::mod::dbg::logger<"numa">;

// This storage must not move onto the small early-boot stack. All mutations
// finish on the BSP before AP startup and before the release publication.
constinit Topology topology;
constinit Workspace workspace;
constinit BootStatus status;
constinit std::array<RamRange, MAX_RAM_RANGES> ram;
constinit std::atomic<bool> published{false};
static_assert(std::atomic<bool>::is_always_lock_free);

auto append_zone(const mm::paging::PageZone& zone, size_t& count, bool embedded_descriptor) -> bool {
    static_assert(sizeof(mm::paging::PageZone) <= PAGE_BYTES);
    uint64_t const HHDM = mm::addr::get_hhdm_offset();
    if (count == ram.size() || zone.allocator == nullptr || zone.start < HHDM || zone.len == 0 || zone.allocator->base != zone.start ||
        zone.len != static_cast<uint64_t>(zone.allocator->total_pages) * PAGE_BYTES || zone.start % PAGE_BYTES != 0 ||
        zone.len % PAGE_BYTES != 0)
        return false;
    uint64_t base = zone.start - HHDM;
    uint64_t length = zone.len;
    if (embedded_descriptor) {
        // init_page_zone embeds the descriptor in the immediately preceding
        // page. Global PMM balance includes it; retain its actual SRAT affinity,
        // even when a node boundary separates it from the allocator's first page.
        if (base < PAGE_BYTES || reinterpret_cast<uint64_t>(&zone) != zone.start - PAGE_BYTES || length > UINT64_MAX - PAGE_BYTES)
            return false;
        base -= PAGE_BYTES;
        length += PAGE_BYTES;
    }
    if (length > UINT64_MAX - base) return false;
    ram[count++] = {.base = base, .length = length};
    return true;
}
}  // namespace

void initialize_boot_topology(std::span<const CpuInput> cpus) {
    if (published.load(std::memory_order_acquire)) return;

    size_t count = 0;
    for (auto const* zone = mm::phys::get_zones(); zone != nullptr; zone = zone->next) {
        if (!append_zone(*zone, count, true)) dbg::panic_handler("NUMA managed RAM exceeds supported topology input");
    }
    if (auto const* huge = mm::phys::get_huge_page_zone(); huge != nullptr && !append_zone(*huge, count, false))
        dbg::panic_handler("NUMA huge RAM exceeds supported topology input");
    if (count == 0) dbg::panic_handler("NUMA boot has no managed physical RAM");
    std::sort(ram.begin(), ram.begin() + count, [](const RamRange& left, const RamRange& right) { return left.base < right.base; });
    auto const RAM = std::span<const RamRange>{ram.data(), count};

    auto const SRAT = find_table_bounded("SRAT");
    auto const SLIT = find_table_bounded("SLIT");
    status.srat = SRAT.error;
    status.slit = SLIT.error;
    status.rejected_firmware =
        (!SRAT.valid() && SRAT.error != bounded::Error::not_found) || (!SLIT.valid() && SLIT.error != bounded::Error::not_found);
    if (!status.rejected_firmware) {
        status.parser = parse(SRAT.bytes, SLIT.bytes, cpus, RAM, topology, workspace);
        status.rejected_firmware = !status.parser.valid();
    }
    if (status.rejected_firmware) {
        log::warn("firmware topology rejected: srat=%u slit=%u parse=%u table=%u offset=%zu; using UMA", static_cast<unsigned>(status.srat),
                  static_cast<unsigned>(status.slit), static_cast<unsigned>(status.parser.error),
                  static_cast<unsigned>(status.parser.table), status.parser.offset);
        if (!parse({}, {}, cpus, RAM, topology, workspace).valid()) dbg::panic_handler("NUMA UMA fallback rejected trusted CPU/RAM input");
    }

    published.store(true, std::memory_order_release);
    log::info("topology published: cpus=%u nodes=%u ranges=%u provenance=%u rejected=%u", static_cast<unsigned>(topology.cpu_count),
              static_cast<unsigned>(topology.node_count), static_cast<unsigned>(topology.memory_range_count),
              static_cast<unsigned>(topology.provenance), static_cast<unsigned>(status.rejected_firmware));
    for (size_t i = 0; i < topology.node_count; ++i) {
        auto const& node = topology.nodes[i];
        log::info("node=%zu proximity=%u cpu-mask=0x%016llx managed-bytes=%llu", i, node.proximity_id,
                  static_cast<unsigned long long>(node.cpus.words[0]), static_cast<unsigned long long>(node.usable_bytes));
    }
}

auto boot_topology() noexcept -> const Topology* { return published.load(std::memory_order_acquire) ? &topology : nullptr; }
auto boot_status() noexcept -> const BootStatus* { return published.load(std::memory_order_acquire) ? &status : nullptr; }

}  // namespace ker::mod::acpi::numa
